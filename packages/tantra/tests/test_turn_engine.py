from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from tantra.agent import Agent
from tantra.context import TurnContext
from tantra.errors import ProviderError
from tantra.events import (
    CompactionApplied,
    InputQueued,
    ReasoningDelta,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextDelta,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
    TurnFailed,
    Usage,
)
from tantra.hooks import Hook
from tantra.loop import Emitted, RetryConfig, TurnEngine
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, ToolCall, ToolResultMessage
from tantra.providers.fake import FAKE_LIMITS, FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import tool
from tantra.tracing import NULL_TRACER


def call(name: str, args: str, cid: str) -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


class ToolTracer:
    def __init__(self) -> None:
        self.started: list[Any] = []
        self.ended: list[tuple[Any, str]] = []

    def start_turn(self, *_args: Any, **_kwargs: Any) -> Any:
        return object()

    def end_turn(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def start_sample(self, *_args: Any, **_kwargs: Any) -> Any:
        return object()

    def end_sample(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def start_tool(self, *_args: Any, **_kwargs: Any) -> Any:
        span = object()
        self.started.append(span)
        return span

    def end_tool(self, span: Any, **kwargs: Any) -> None:
        self.ended.append((span, kwargs["outcome"]))

    def start_compaction(self, *_args: Any, **_kwargs: Any) -> Any:
        return object()

    def end_compaction(self, *_args: Any, **_kwargs: Any) -> None:
        return None


async def build(
    samples: list[Sample],
    agent: type[Agent],
    *,
    provider: Any = None,
    hooks: list[Hook] | None = None,
    compactor: Any = None,
    notify: Any = None,
    tracer: Any = None,
) -> tuple[TurnEngine, MemoryStore, InputQueued]:
    store = MemoryStore()
    await store.setup()
    header = SessionHeader(id=uuid.uuid4().hex, agent="engine")
    await store.create(header)
    queued = InputQueued(command_id=uuid.uuid4().hex, input="go")
    await store.enqueue(header.id, queued)
    engine = TurnEngine(
        store=store,
        provider=provider or FakeProvider(samples),
        header=header,
        agent=agent,
        tools={entry.name: entry for entry in agent.tools},
        model="fake/model",
        hooks=hooks or [],
        compactor=compactor,
        notify=notify,
        tracer=tracer or NULL_TRACER,
        retry=RetryConfig(max_attempts=1),
    )
    return engine, store, queued


async def events(store: MemoryStore, sid: str) -> list[SessionEvent]:
    return [item.event for item in await store.read_page(sid)]


async def test_deltas_are_durable_exact_events() -> None:
    class Bot(Agent):
        pass

    engine, store, queued = await build([Sample(text="hello there", reasoning="think now")], Bot)

    terminal = await engine.run(queued)
    journal = await events(store, engine.header.id)

    assert isinstance(terminal, TurnCompleted)
    assert [event for event in journal if isinstance(event, ReasoningDelta)] == [
        ReasoningDelta(text="think "),
        ReasoningDelta(text="now"),
    ]
    assert [event for event in journal if isinstance(event, TextDelta)] == [
        TextDelta(text="hello "),
        TextDelta(text="there"),
    ]


async def test_async_tools_overlap_and_preserve_provider_call_order() -> None:
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release_first = asyncio.Event()

    @tool
    async def first() -> str:
        first_started.set()
        await second_started.wait()
        await release_first.wait()
        return "first"

    @tool
    async def second() -> str:
        second_started.set()
        await first_started.wait()
        return "second"

    class Bot(Agent):
        tools = [first, second]

    async def notify(item: Stamped) -> None:
        if isinstance(item.event, ToolCallCompleted) and item.event.call_id == "c2":
            release_first.set()

    provider = FakeProvider(
        [
            Sample(tool_calls=[call("first", "{}", "c1"), call("second", "{}", "c2")]),
            Sample(text="done"),
        ]
    )
    engine, store, queued = await build([], Bot, provider=provider, notify=notify)

    terminal = await engine.run(queued)
    journal = await events(store, engine.header.id)
    completed = [event.call_id for event in journal if isinstance(event, ToolCallCompleted)]
    results = [message.call_id for message in provider.requests[1].messages if isinstance(message, ToolResultMessage)]

    assert isinstance(terminal, TurnCompleted)
    assert completed == ["c2", "c1"]
    assert results == ["c1", "c2"]


async def test_one_tool_failure_does_not_cancel_its_sibling() -> None:
    sibling_finished = asyncio.Event()

    @tool
    async def fail() -> str:
        raise RuntimeError("broken")

    @tool
    async def survive() -> str:
        await asyncio.sleep(0)
        sibling_finished.set()
        return "alive"

    class Bot(Agent):
        tools = [fail, survive]

    engine, store, queued = await build(
        [
            Sample(tool_calls=[call("fail", "{}", "c1"), call("survive", "{}", "c2")]),
            Sample(text="recovered"),
        ],
        Bot,
    )

    terminal = await engine.run(queued)
    completed = [event for event in await events(store, engine.header.id) if isinstance(event, ToolCallCompleted)]

    assert isinstance(terminal, TurnCompleted)
    assert sibling_finished.is_set()
    assert {event.call_id for event in completed} == {"c1", "c2"}
    assert next(event for event in completed if event.call_id == "c1").is_error


async def test_blocked_sync_tool_does_not_starve_an_unrelated_turn() -> None:
    started = threading.Event()
    release = threading.Event()

    @tool
    def blocked() -> str:
        started.set()
        release.wait(timeout=10)
        return "released"

    class Blocked(Agent):
        tools = [blocked]

    class Quick(Agent):
        pass

    blocked_engine, _, blocked_input = await build(
        [Sample(tool_calls=[call("blocked", "{}", "c1")]), Sample(text="done")],
        Blocked,
    )
    quick_engine, _, quick_input = await build([Sample(text="quick")], Quick)

    blocked_task = asyncio.create_task(blocked_engine.run(blocked_input))
    assert await asyncio.to_thread(started.wait, 5)
    quick_terminal = await asyncio.wait_for(quick_engine.run(quick_input), timeout=2)
    release.set()
    blocked_terminal = await asyncio.wait_for(blocked_task, timeout=5)

    assert isinstance(quick_terminal, TurnCompleted)
    assert isinstance(blocked_terminal, TurnCompleted)


class BrokenProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        raise ProviderError("vendor down")
        yield TextDelta(text="")


async def test_provider_error_becomes_turn_failed() -> None:
    class Bot(Agent):
        pass

    engine, store, queued = await build([], Bot, provider=BrokenProvider())

    terminal = await engine.run(queued)
    journal = await events(store, engine.header.id)

    assert terminal == TurnFailed(turn_id=queued.command_id, error="vendor down")
    assert sum(isinstance(event, TurnFailed) for event in journal) == 1


class Dashboard(BaseModel):
    title: str
    panels: int


async def test_structured_output_stops_later_calls() -> None:
    ran: list[str] = []

    @tool
    async def late() -> str:
        ran.append("late")
        return "late"

    class Structured(Agent):
        tools = [late]
        output_schema = Dashboard

    engine, store, queued = await build(
        [
            Sample(
                tool_calls=[
                    call("submit_output", '{"title":"p99","panels":2}', "c1"),
                    call("late", "{}", "c2"),
                ]
            )
        ],
        Structured,
    )

    terminal = await engine.run(queued)
    completed = [event for event in await events(store, engine.header.id) if isinstance(event, ToolCallCompleted)]

    assert terminal == TurnCompleted(
        turn_id=queued.command_id,
        stop_reason="output",
        output={"title": "p99", "panels": 2},
    )
    assert ran == []
    assert completed[-1].result == "not executed: turn completed"
    assert completed[-1].is_error


async def test_hooks_transform_tools_and_receive_terminal() -> None:
    calls: list[str] = []

    @tool
    async def echo(value: str) -> str:
        return value

    class Recorder(Hook):
        async def before_turn(self, turn: TurnContext) -> None:
            calls.append("before_turn")

        async def before_sample(self, turn: TurnContext) -> None:
            calls.append("before_sample")

        async def before_tool(
            self,
            requested: ToolCallRequested,
            turn: TurnContext,
        ) -> ToolCallRequested:
            calls.append("before_tool")
            return requested.model_copy(update={"args": {"value": "changed"}})

        async def after_tool(
            self,
            requested: ToolCallRequested,
            result: Any,
            is_error: bool,
            turn: TurnContext,
        ) -> Any:
            calls.append("after_tool")
            return f"{result}!"

        async def after_turn(self, turn: TurnContext, event: SessionEvent) -> None:
            calls.append("after_turn")

        async def on_event(self, emitted: Emitted) -> None:
            calls.append(type(emitted.event).__name__)

    class Bot(Agent):
        tools = [echo]

    engine, store, queued = await build(
        [Sample(tool_calls=[call("echo", '{"value":"original"}', "c1")]), Sample(text="done")],
        Bot,
        hooks=[Recorder()],
    )

    terminal = await engine.run(queued)
    completed = [event for event in await events(store, engine.header.id) if isinstance(event, ToolCallCompleted)]

    assert isinstance(terminal, TurnCompleted)
    assert completed[0].result == "changed!"
    assert calls.index("before_turn") < calls.index("before_sample") < calls.index("before_tool")
    assert calls[-1] == "after_turn"


async def test_permission_denial_never_runs_tool() -> None:
    ran: list[str] = []

    @tool
    async def secret() -> str:
        ran.append("secret")
        return "secret"

    class Guarded(Agent):
        tools = [secret]
        permissions = {"secret": "deny"}

    engine, store, queued = await build(
        [Sample(tool_calls=[call("secret", "{}", "c1")]), Sample(text="safe")],
        Guarded,
    )

    terminal = await engine.run(queued)
    completed = [event for event in await events(store, engine.header.id) if isinstance(event, ToolCallCompleted)]

    assert isinstance(terminal, TurnCompleted)
    assert ran == []
    assert completed[0].result == "denied by permissions: secret"
    assert completed[0].is_error


async def test_usage_accumulates_on_header() -> None:
    class Bot(Agent):
        pass

    engine, store, queued = await build(
        [Sample(text="done", usage=Usage(input_tokens=9, output_tokens=4, cache_read_tokens=2))],
        Bot,
    )

    await engine.run(queued)

    header = await store.header(engine.header.id)
    assert header is not None
    assert header.usage == Usage(input_tokens=9, output_tokens=4, cache_read_tokens=2)


class OneCompactor:
    def __init__(self) -> None:
        self.called = False

    async def compact(self, turn: TurnContext) -> list[SessionEvent]:
        if self.called:
            return []
        self.called = True
        return [
            CompactionApplied(
                strategy="test",
                tokens_before=10,
                tokens_after=3,
                summary="summary",
            )
        ]


async def test_compaction_is_journaled_before_sampling() -> None:
    class Bot(Agent):
        pass

    compactor = OneCompactor()
    engine, store, queued = await build([Sample(text="done")], Bot, compactor=compactor)

    await engine.run(queued)
    journal = await events(store, engine.header.id)
    compaction_index = next(index for index, event in enumerate(journal) if isinstance(event, CompactionApplied))
    delta_index = next(index for index, event in enumerate(journal) if isinstance(event, TextDelta))

    assert compactor.called
    assert compaction_index < delta_index


async def test_notify_precedes_fallible_on_event_hook() -> None:
    class Bot(Agent):
        pass

    class BrokenHook(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            if isinstance(emitted.event, TextDelta):
                raise RuntimeError("hook failed")

    notified: list[SessionEvent] = []

    def notify(item: Stamped) -> None:
        notified.append(item.event)

    engine, _, queued = await build(
        [Sample(text="done")],
        Bot,
        hooks=[BrokenHook()],
        notify=notify,
    )

    with pytest.raises(RuntimeError, match="hook failed"):
        await engine.run(queued)

    assert any(isinstance(event, TextDelta) for event in notified)


async def test_tool_span_ends_once_when_after_tool_fails() -> None:
    @tool
    async def answer() -> str:
        return "done"

    class BrokenHook(Hook):
        async def after_tool(
            self,
            _requested: ToolCallRequested,
            _result: Any,
            _is_error: bool,
            _turn: TurnContext,
        ) -> Any:
            raise RuntimeError("after tool failed")

    class Bot(Agent):
        tools = [answer]

    tracer = ToolTracer()
    engine, _, queued = await build(
        [Sample(tool_calls=[call("answer", "{}", "c1")])],
        Bot,
        hooks=[BrokenHook()],
        tracer=tracer,
    )

    with pytest.raises(RuntimeError, match="after tool failed"):
        await engine.run(queued)

    assert len(tracer.started) == 1
    assert tracer.ended == [(tracer.started[0], "aborted")]


async def test_tool_span_ends_once_when_turn_is_cancelled() -> None:
    entered = asyncio.Event()
    blocked = asyncio.Event()

    @tool
    async def wait_forever() -> str:
        entered.set()
        await blocked.wait()
        return "done"

    class Bot(Agent):
        tools = [wait_forever]

    tracer = ToolTracer()
    engine, _, queued = await build(
        [Sample(tool_calls=[call("wait_forever", "{}", "c1")])],
        Bot,
        tracer=tracer,
    )

    task = asyncio.create_task(engine.run(queued))
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(tracer.started) == 1
    assert tracer.ended == [(tracer.started[0], "aborted")]
