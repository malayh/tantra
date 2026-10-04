from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextvars import ContextVar
from typing import Any
from uuid import uuid4

import pytest

from tantra import Runtime
from tantra.agent import Agent
from tantra.errors import ProviderError
from tantra.events import (
    InputQueued,
    LoggedEvent,
    ReasoningDelta,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextDelta,
    ToolCallDelta,
    TurnCompleted,
    TurnFailed,
)
from tantra.hooks import Hook
from tantra.loop import RetryConfig, TurnEngine
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS
from tantra.stores.memory import MemoryStore
from tantra.stores.sqlite import SQLiteStore

DELTAS = (TextDelta, ReasoningDelta, ToolCallDelta)


class Bot(Agent):
    pass


class RecordingStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list[list[SessionEvent]] = []
        self.flushed = asyncio.Event()

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int:
        last = await super().append(sid, events)
        if events and isinstance(events[0], DELTAS):
            self.batches.append(list(events))
            self.flushed.set()
        return last


class ScriptProvider:
    def __init__(self, attempts: list[list[ProviderEvent | Exception]], *, pause: float = 0) -> None:
        self.attempts = attempts
        self.pause = pause
        self.calls = 0
        self.closed = 0

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        script = self.attempts[self.calls]
        self.calls += 1
        try:
            for event in script:
                if self.pause:
                    await asyncio.sleep(self.pause)
                if isinstance(event, Exception):
                    raise event
                yield event
        finally:
            self.closed += 1


async def engine_for(
    provider: Any, *, hooks: Sequence[Hook] = (), batching: bool = True
) -> tuple[TurnEngine, RecordingStore, InputQueued]:
    store = RecordingStore()
    header = SessionHeader(id=uuid4().hex, agent="bot")
    await store.create(header)
    queued = InputQueued(command_id=uuid4().hex, input="go")
    await store.enqueue(header.id, queued)
    engine = TurnEngine(
        store=store,
        provider=provider,
        header=header,
        agent=Bot,
        tools={},
        model="fake",
        hooks=hooks,
        retry=RetryConfig(max_attempts=2, base_delay=0),
        _batch_deltas=batching,
    )
    return engine, store, queued


def size(event: SessionEvent) -> int:
    return len(event.model_dump_json().encode())


@pytest.mark.parametrize("kind", DELTAS)
async def test_count_limit_preserves_each_original_event(kind: Any) -> None:
    original = [
        kind(index=index, args_fragment=str(index)) if kind is ToolCallDelta else kind(text=str(index))
        for index in range(65)
    ]
    provider = ScriptProvider([[*original, StreamEnd(text="done")]])
    engine, store, queued = await engine_for(provider)
    assert isinstance(await engine.run(queued), TurnCompleted)
    assert [len(batch) for batch in store.batches] == [32, 32, 1]
    logged = [item async for item in store.read(engine.header.id)]
    assert [item.event for item in logged if isinstance(item.event, DELTAS)] == original
    assert [item.seq for item in logged] == list(range(1, len(logged) + 1))
    assert provider.closed == 1


async def test_serialized_byte_limit_counts_unicode_escaping_and_extra_fields() -> None:
    original = [TextDelta(text='雪\n"\\' * 1000, extra="ø" * 1000, number=index) for index in range(8)]
    engine, store, queued = await engine_for(ScriptProvider([[*original, StreamEnd(text="done")]]))
    await engine.run(queued)
    assert [event for batch in store.batches for event in batch] == original
    assert len(store.batches) > 1
    assert all(sum(map(size, batch)) <= 65_536 for batch in store.batches)
    for batch in store.batches[:-1]:
        next_event = original[sum(len(previous) for previous in store.batches[: store.batches.index(batch) + 1])]
        assert sum(map(size, batch)) + size(next_event) > 65_536


async def test_exact_byte_bound_and_oversized_event_commit_alone() -> None:
    exact = TextDelta(text="x" * (65_536 - size(TextDelta(text=""))))
    oversized = TextDelta(text="y" * 70_000)
    original = [TextDelta(text="before"), exact, oversized, TextDelta(text="after")]
    engine, store, queued = await engine_for(ScriptProvider([[*original, StreamEnd(text="done")]]))
    await engine.run(queued)
    assert store.batches == [[original[0]], [exact], [oversized], [original[-1]]]
    assert size(exact) == 65_536


class StalledProvider:
    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = asyncio.Event()
        self.reads = 0

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        try:
            yield TextDelta(text="first")
            self.reads += 1
            self.waiting.set()
            await self.release.wait()
            yield StreamEnd(text="first")
        finally:
            self.closed.set()


async def test_timer_flushes_stalled_provider_without_cancelling_pending_read() -> None:
    provider = StalledProvider()
    engine, store, queued = await engine_for(provider)
    task = asyncio.create_task(engine.run(queued))
    try:
        await asyncio.wait_for(provider.waiting.wait(), 1)
        await asyncio.wait_for(store.flushed.wait(), 1)
        assert not provider.closed.is_set()
        assert provider.reads == 1
        assert store.batches == [[TextDelta(text="first")]]
        provider.release.set()
        assert isinstance(await task, TurnCompleted)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_continuous_arrivals_do_not_extend_first_delta_deadline() -> None:
    original = [TextDelta(text=str(index)) for index in range(24)]
    provider = ScriptProvider([[*original, StreamEnd(text="done")]], pause=0.004)
    engine, store, queued = await engine_for(provider)
    await engine.run(queued)
    assert len(store.batches) >= 3
    assert all(len(batch) < 12 for batch in store.batches)
    assert [event for batch in store.batches for event in batch] == original


async def test_cancellation_discards_buffer_and_closes_pending_provider_read() -> None:
    provider = StalledProvider()
    engine, store, queued = await engine_for(provider)
    task = asyncio.create_task(engine.run(queued))
    await asyncio.wait_for(provider.waiting.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert provider.closed.is_set()
    assert store.batches == []


async def test_complete_tool_call_flushes_before_provider_continues() -> None:
    provider = ScriptProvider([[TextDelta(text="before"), ToolCall(id="x", name="unused"), StreamEnd(text="done")]])
    engine, store, _ = await engine_for(provider)
    stream = engine._stream_events(SampleRequest(model="fake"))
    try:
        assert await anext(stream) == [TextDelta(text="before")]
        assert provider.closed == 0
        assert isinstance(await anext(stream), StreamEnd)
    finally:
        await stream.aclose()
    assert store.batches == []


async def test_retry_flushes_received_output_and_blocks_later_overflow_recovery() -> None:
    provider = ScriptProvider(
        [
            [TextDelta(text="partial"), ProviderError("retry", status_code=500)],
            [ProviderError("overflow", context_overflow=True)],
        ]
    )
    engine, store, queued = await engine_for(provider)
    terminal = await engine.run(queued)
    assert isinstance(terminal, TurnFailed)
    assert provider.calls == provider.closed == 2
    assert store.batches == [[TextDelta(text="partial")]]


@pytest.mark.parametrize("overflow", [False, True])
async def test_provider_failure_flushes_partial_output_before_failure(overflow: bool) -> None:
    provider = ScriptProvider([[TextDelta(text="partial"), ProviderError("failed", context_overflow=overflow)]])
    engine, store, queued = await engine_for(provider)
    assert isinstance(await engine.run(queued), TurnFailed)
    assert store.batches == [[TextDelta(text="partial")]]
    assert provider.calls == provider.closed == 1


async def test_missing_stream_end_flushes_before_retry() -> None:
    provider = ScriptProvider([[TextDelta(text="partial")]])
    engine, store, queued = await engine_for(provider)
    assert isinstance(await engine.run(queued), TurnFailed)
    assert store.batches == [[TextDelta(text="partial")]]


@pytest.mark.parametrize("batching", [False, True])
async def test_append_provider_error_is_not_retried(batching: bool) -> None:
    provider = ScriptProvider([[TextDelta(text="first"), StreamEnd(text="first")]])
    engine, _, queued = await engine_for(provider, batching=batching)
    append = engine.store.append

    async def fail(sid: str, events: Sequence[SessionEvent]) -> int:
        if events and isinstance(events[0], DELTAS):
            raise ProviderError("append failure", status_code=500)
        return await append(sid, events)

    engine.store.append = fail
    assert isinstance(await engine.run(queued), TurnFailed)
    assert provider.calls == provider.closed == 1


@pytest.mark.parametrize("instance_override", [False, True])
async def test_custom_event_hook_keeps_immediate_fallible_order(instance_override: bool) -> None:
    class Broken(Hook):
        async def on_event(self, event: LoggedEvent) -> None:
            if isinstance(event.event, TextDelta):
                raise RuntimeError("stop after first")

    hook = Hook() if instance_override else Broken()
    if instance_override:
        hook.on_event = Broken().on_event
    provider = ScriptProvider([[TextDelta(text="first"), TextDelta(text="second"), StreamEnd(text="done")]])
    engine, store, queued = await engine_for(provider, hooks=[hook])
    notified = []

    async def notify(item: Stamped) -> None:
        assert item in [row async for row in store.read(engine.header.id)]
        notified.append(item.event)

    engine.notify = notify
    with pytest.raises(RuntimeError, match="stop after first"):
        await engine.run(queued)
    assert store.batches == [[TextDelta(text="first")]]
    assert TextDelta(text="first") in notified
    assert provider.closed == 1


async def test_hooks_without_event_override_remain_eligible() -> None:
    class BeforeOnly(Hook):
        async def before_sample(self, turn: Any) -> None:
            return None

    original = [TextDelta(text="first"), TextDelta(text="second")]
    engine, store, queued = await engine_for(
        ScriptProvider([[*original, StreamEnd(text="done")]]), hooks=[BeforeOnly()]
    )
    await engine.run(queued)
    assert store.batches == [original]


async def test_provider_iterator_supports_future_reads_without_close() -> None:
    class FutureIterator:
        def __init__(self) -> None:
            self.events = iter([TextDelta(text="one"), TextDelta(text="two"), StreamEnd(text="done")])

        def __aiter__(self) -> Any:
            return self

        def __anext__(self) -> asyncio.Future[Any]:
            future = asyncio.get_running_loop().create_future()
            try:
                future.set_result(next(self.events))
            except StopIteration:
                future.set_exception(StopAsyncIteration())
            return future

    provider = ScriptProvider([])
    provider.stream = lambda req: FutureIterator()
    engine, store, queued = await engine_for(provider)
    await engine.run(queued)
    assert store.batches == [[TextDelta(text="one"), TextDelta(text="two")]]


@pytest.mark.parametrize("sqlite", [False, True])
async def test_non_postgres_runtime_keeps_immediate_commits(tmp_path: Any, sqlite: bool) -> None:
    store = SQLiteStore(tmp_path / "sessions.db") if sqlite else RecordingStore()
    await store.setup()
    batches = []
    original_append = store.append

    async def record(sid: str, events: Sequence[SessionEvent]) -> int:
        result = await original_append(sid, events)
        if events and isinstance(events[0], DELTAS):
            batches.append(list(events))
        return result

    store.append = record
    original = [TextDelta(text="one"), TextDelta(text="two")]
    provider = ScriptProvider([[*original, StreamEnd(text="done")]])
    runtime = Runtime(provider, store, [Bot], default_model="fake")
    try:
        root = await runtime.create(Bot)
        async with runtime.connect(root, writable=True) as connection:
            assert (await connection.prompt("go", command_id=uuid4())).text == "done"
        assert batches == [[event] for event in original]
    finally:
        await runtime.aclose()


@pytest.mark.parametrize("action", ["complete", "cancel", "append_failure"])
async def test_provider_context_scope_survives_reads_and_cleanup(action: str) -> None:
    scope = ContextVar("provider_scope", default="outside")
    waiting = asyncio.Event()
    closed = asyncio.Event()

    class ScopedProvider:
        def limits(self, model: str) -> ModelLimits:
            return FAKE_LIMITS

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            token = scope.set("inside")
            try:
                for index in range(64):
                    assert scope.get() == "inside"
                    yield TextDelta(text=str(index))
                    if action == "cancel":
                        waiting.set()
                        await asyncio.Event().wait()
                yield StreamEnd(text="done")
            finally:
                assert scope.get() == "inside"
                scope.reset(token)
                closed.set()

    engine, store, queued = await engine_for(ScopedProvider())
    if action == "append_failure":
        original = store.append

        async def fail(sid: str, events: Sequence[SessionEvent]) -> int:
            if events and isinstance(events[0], DELTAS):
                raise ProviderError("append failure")
            return await original(sid, events)

        store.append = fail
    task = asyncio.create_task(engine.run(queued))
    if action == "cancel":
        await asyncio.wait_for(waiting.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        result = await task
        assert isinstance(result, TurnCompleted if action == "complete" else TurnFailed)
    assert closed.is_set() and scope.get() == "outside"


@pytest.mark.parametrize("boundary", ["stream", "aiter"])
@pytest.mark.parametrize("batching", [False, True])
async def test_iterator_construction_context_matches_reads_and_closure(boundary: str, batching: bool) -> None:
    scope = ContextVar("iterator_scope", default="outside")
    closed = asyncio.Event()

    class ScopedIterator:
        def __init__(self) -> None:
            self.events = iter([TextDelta(text="one"), StreamEnd(text="done")])
            self.token = scope.set("inside") if boundary == "stream" else None

        def __aiter__(self) -> Any:
            if self.token is None:
                self.token = scope.set("inside")
            return self

        def __anext__(self) -> asyncio.Future[Any]:
            assert scope.get() == "inside"
            future = asyncio.get_running_loop().create_future()
            try:
                future.set_result(next(self.events))
            except StopIteration:
                future.set_exception(StopAsyncIteration())
            return future

        async def aclose(self) -> None:
            assert scope.get() == "inside"
            scope.reset(self.token)
            closed.set()

    provider = ScriptProvider([])
    provider.stream = lambda req: ScopedIterator()
    engine, _, queued = await engine_for(provider, batching=batching)
    await engine.run(queued)
    assert closed.is_set() and scope.get() == "outside"
