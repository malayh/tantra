from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import (
    Agent,
    ApprovalResponse,
    AskExpired,
    ChoiceResponse,
    CommandReceipt,
    FreeText,
    FreeTextResponse,
    InvalidCommandReuse,
    Runtime,
    TantraError,
    TurnResult,
    WriterReplaced,
    WriterRequired,
    tool,
)
from tantra.events import (
    AskAnswered,
    AskRaised,
    CancellationRequested,
    InputQueued,
    SessionCreated,
    SessionEvent,
    TextPart,
    ToolCallCompleted,
    TurnCancelled,
    TurnCompleted,
    TurnInterrupted,
    TurnStarted,
    Usage,
)
from tantra.providers.base import (
    ModelLimits,
    ProviderEvent,
    SampleRequest,
    StreamEnd,
    ToolCall,
    UserMessage,
)
from tantra.providers.fake import FAKE_LIMITS, FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context


class Bot(Agent):
    pass


def call(name: str, args: str = "{}", cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


async def history(store: MemoryStore, sid: UUID) -> list[SessionEvent]:
    return [item.event async for item in store.read(sid.hex)]


class EchoProvider:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        self.inputs.append(value)
        yield StreamEnd(text=value, usage=Usage(input_tokens=1, output_tokens=2))


class GateProvider(EchoProvider):
    def __init__(self, target: int = 1) -> None:
        super().__init__()
        self.target = target
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        self.inputs.append(value)
        if len(self.inputs) >= self.target:
            self.started.set()
        await self.release.wait()
        yield StreamEnd(text=value)


class BrokenProvider:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        raise RuntimeError("broken callback")
        yield StreamEnd()


class SuppressingProvider(EchoProvider):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        self.inputs.append(value)
        self.started.set()
        try:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                await self.release.wait()
            yield StreamEnd(text="late")
        finally:
            self.finished.set()


class OverlappingProvider(EchoProvider):
    def __init__(self) -> None:
        super().__init__()
        self.first_started = asyncio.Event()
        self.first_cancelled = asyncio.Event()
        self.first_release = asyncio.Event()
        self.replacement_started = asyncio.Event()
        self.replacement_release = asyncio.Event()
        self.replacement_release.set()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        self.inputs.append(value)
        if value == "old":
            self.first_started.set()
            try:
                await self.first_release.wait()
            except asyncio.CancelledError:
                self.first_cancelled.set()
                await self.first_release.wait()
            yield StreamEnd(text="late")
            return
        self.replacement_started.set()
        await self.replacement_release.wait()
        yield StreamEnd(text=value)


class GatedStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.enqueue_committed = asyncio.Event()
        self.enqueue_release = asyncio.Event()
        self.append_kind: type[SessionEvent] | None = None
        self.append_committed = asyncio.Event()
        self.append_release = asyncio.Event()

    async def enqueue(self, sid: str, event: InputQueued) -> Any:
        result = await super().enqueue(sid, event)
        self.enqueue_committed.set()
        await self.enqueue_release.wait()
        return result

    async def append(
        self,
        sid: str,
        events: Sequence[SessionEvent],
        *,
        expect_seq: int | None,
    ) -> int:
        result = await super().append(sid, events, expect_seq=expect_seq)
        if self.append_kind is not None and any(isinstance(event, self.append_kind) for event in events):
            self.append_committed.set()
            await self.append_release.wait()
        return result


class ObservedReadStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.empty_reads = 0
        self.waiting = asyncio.Event()

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Any]:
        page = await super().read_page(sid, after=after, limit=limit)
        if self.armed and not page:
            self.empty_reads += 1
            if self.empty_reads >= 2:
                self.waiting.set()
        return page


class DelayedRecoveryRuntime(Runtime):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.recovery_started = asyncio.Event()
        self.recovery_release = asyncio.Event()

    async def _interrupt_if_incomplete(self, root_id: str, event: Any) -> bool:
        self.recovery_started.set()
        try:
            await self.recovery_release.wait()
        except asyncio.CancelledError:
            await self.recovery_release.wait()
        return await super()._interrupt_if_incomplete(root_id, event)


async def test_create_persists_uuid_root_agent_model_and_metadata() -> None:
    class Fixed(Agent):
        model = "agent/model"

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Fixed], default_model="default/model")
    sid = uuid4()

    created = await runtime.create("fixed", session_id=sid, model="session/model", metadata={"team": 7})

    assert created == sid
    header = await store.header(sid.hex)
    assert header is not None
    assert header.id == sid.hex
    assert header.root_id == sid.hex
    assert header.agent == "fixed"
    assert header.model == "agent/model"
    assert header.metadata == {"team": 7}
    event = (await history(store, sid))[0]
    assert isinstance(event, SessionCreated)
    assert event.root_id == sid.hex
    assert event.model == "agent/model"
    with pytest.raises(TantraError, match="unknown agent"):
        await runtime.create("missing")
    await runtime.aclose()


async def test_send_is_fifo_and_command_reuse_is_global() -> None:
    provider = GateProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    first = uuid4()
    second = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        assert await connection.send("one", command_id=first) == CommandReceipt(first, False)
        await provider.started.wait()
        assert await connection.send("two", command_id=second) == CommandReceipt(second, False)
        assert await connection.send("one", command_id=first) == CommandReceipt(first, True)
        with pytest.raises(InvalidCommandReuse):
            await connection.send("changed", command_id=first)
        provider.release.set()
        first_result = await runtime._wait_result(sid.hex, first)
        second_result = await runtime._wait_result(sid.hex, second)
        with pytest.raises(InvalidCommandReuse):
            await connection.cancel(command_id=first)

    assert provider.inputs == ["one", "two"]
    assert first_result.text == "one"
    assert second_result.text == "two"
    await runtime.aclose()


async def test_roots_run_concurrently() -> None:
    provider = GateProvider(target=2)
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    first = await runtime.create(Bot)
    second = await runtime.create(Bot)

    async with runtime.connect(first, writable=True) as one, runtime.connect(second, writable=True) as two:
        one_task = asyncio.create_task(one.prompt("one", command_id=uuid4()))
        two_task = asyncio.create_task(two.prompt("two", command_id=uuid4()))
        await provider.started.wait()
        provider.release.set()
        results = await asyncio.gather(one_task, two_task)

    assert {result.text for result in results} == {"one", "two"}
    await runtime.aclose()


async def test_readers_replay_tail_and_never_activate() -> None:
    provider = EchoProvider()
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    sid = await runtime.create(Bot)

    async with runtime.connect(sid) as reader:
        created = await anext(reader)
        assert created.seq == 1
        assert created.agent_id == sid
        assert runtime.active == {}
        tail = asyncio.create_task(anext(reader))
        async with runtime.connect(sid, writable=True) as writer:
            command = uuid4()
            await writer.send("hello", command_id=command)
        queued = await tail
        assert queued.seq == 2
        assert isinstance(queued.event, InputQueued)
        await runtime._wait_result(sid.hex, command)

    replay = runtime.events(sid, after=1)
    suffix = [await anext(replay), await anext(replay)]
    assert [item.seq for item in suffix] == [2, 3]
    await replay.aclose()
    await runtime.aclose()


async def test_writer_takeover_and_connection_scope() -> None:
    runtime = Runtime(EchoProvider(), MemoryStore(), [Bot], default_model="m")
    sid = await runtime.create(Bot)
    old = runtime.connect(sid, writable=True)
    await old.__aenter__()
    newer = runtime.connect(sid, writable=True)
    await newer.__aenter__()

    with pytest.raises(WriterReplaced):
        await old.send("stale", command_id=uuid4())
    with pytest.raises(WriterReplaced):
        await anext(old)
    await newer.__aexit__(None, None, None)
    with pytest.raises(WriterRequired):
        await newer.send("closed", command_id=uuid4())
    read_only = runtime.connect(sid)
    async with read_only:
        with pytest.raises(WriterRequired):
            await read_only.send("no", command_id=uuid4())
    await old.__aexit__(None, None, None)
    await runtime.aclose()


async def test_prompt_result_and_local_wait_cancellation() -> None:
    provider = GateProvider()
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    sid = await runtime.create(Bot)
    command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        waiting = asyncio.create_task(connection.prompt("hello", command_id=command))
        await provider.started.wait()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        provider.release.set()
        result = await connection.prompt("hello", command_id=command)

    assert result == TurnResult(
        agent_id=sid,
        command_id=command,
        outcome="completed",
        stop_reason="completed",
        text="hello",
        output=None,
        usage=Usage(),
        error=None,
    )
    await runtime.aclose()


async def test_prompt_raises_programming_errors() -> None:
    provider = BrokenProvider()
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    sid = await runtime.create(Bot)

    async with runtime.connect(sid, writable=True) as connection:
        with pytest.raises(RuntimeError, match="broken callback"):
            await connection.prompt("hello", command_id=uuid4())

    await runtime.aclose()


async def test_live_tool_ask_is_durable_before_answer_and_deduplicates() -> None:
    @tool
    async def interview(ctx: Context) -> str:
        response = await ctx.ask(FreeText(prompt="name?"))
        return response.text

    class Asker(Agent):
        tools = [interview]

    provider = FakeProvider([Sample(tool_calls=[call("interview")]), Sample(text="done", usage=Usage(output_tokens=4))])
    store = MemoryStore()
    runtime = Runtime(provider, store, [Asker], default_model="m")
    sid = await runtime.create(Asker)
    turn_id = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=turn_id)
        while True:
            item = await anext(connection)
            if isinstance(item.event, AskRaised):
                raised = item.event
                break
        ask_id = UUID(hex=raised.ask_id)
        answer_id = uuid4()
        receipt = await connection.answer(ask_id, FreeTextResponse(text="Malay"), command_id=answer_id)
        duplicate = await connection.answer(ask_id, FreeTextResponse(text="Malay"), command_id=answer_id)
        result = await runtime._wait_result(sid.hex, turn_id)

    assert receipt == CommandReceipt(answer_id, False)
    assert duplicate == CommandReceipt(answer_id, True)
    assert result.text == "done"
    assert result.usage.output_tokens == 4
    logged = await history(store, sid)
    assert next(event for event in logged if isinstance(event, AskAnswered)).command_id == answer_id.hex
    with pytest.raises(AskExpired):
        async with runtime.connect(sid, writable=True) as connection:
            await connection.answer(ask_id, FreeTextResponse(text="late"), command_id=uuid4())
    await runtime.aclose()


async def test_permission_ask_requires_approval_response() -> None:
    called: list[str] = []

    @tool(permission="ask")
    async def dangerous() -> str:
        called.append("yes")
        return "ok"

    class Guarded(Agent):
        tools = [dangerous]

    provider = FakeProvider([Sample(tool_calls=[call("dangerous")]), Sample(text="done")])
    runtime = Runtime(provider, MemoryStore(), [Guarded], default_model="m")
    sid = await runtime.create(Guarded)
    turn_id = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=turn_id)
        while True:
            event = (await anext(connection)).event
            if isinstance(event, AskRaised):
                break
        ask_id = UUID(hex=event.ask_id)
        with pytest.raises(TantraError, match="needs a 'approval' response"):
            await connection.answer(ask_id, ChoiceResponse(selected="yes"), command_id=uuid4())
        await connection.answer(ask_id, ApprovalResponse(allow=True), command_id=uuid4())
        result = await runtime._wait_result(sid.hex, turn_id)

    assert result.outcome == "completed"
    assert called == ["yes"]
    await runtime.aclose()


async def test_cancel_marks_active_and_queued_then_root_is_reusable() -> None:
    provider = GateProvider()
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    sid = await runtime.create(Bot)
    active = uuid4()
    queued = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("active", command_id=active)
        await provider.started.wait()
        await connection.send("queued", command_id=queued)
        cancel_id = uuid4()
        assert await connection.cancel(command_id=cancel_id) == CommandReceipt(cancel_id, False)
        assert await connection.cancel(command_id=cancel_id) == CommandReceipt(cancel_id, True)
        active_result = await runtime._wait_result(sid.hex, active)
        queued_result = await runtime._wait_result(sid.hex, queued)
        provider.release.set()
        later = await connection.prompt("later", command_id=uuid4())

    assert active_result.outcome == "cancelled"
    assert queued_result.outcome == "cancelled"
    assert later.outcome == "completed"
    assert provider.inputs == ["active", "later"]
    await runtime.aclose()


async def test_aclose_interrupts_active_turn_and_invalidates_writer() -> None:
    provider = GateProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    command = uuid4()
    connection = runtime.connect(sid, writable=True)
    await connection.__aenter__()
    await connection.send("active", command_id=command)
    await provider.started.wait()

    await runtime.aclose()

    result = await runtime._wait_result(sid.hex, command)
    assert result.outcome == "interrupted"
    assert result.stop_reason == "runtime_closed"
    with pytest.raises(WriterReplaced):
        await connection.send("no", command_id=uuid4())
    await connection.__aexit__(None, None, None)


async def test_fresh_runtime_interrupts_old_started_turn_and_drains_fifo() -> None:
    store = MemoryStore()
    initial = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await initial.create(Bot)
    started = uuid4()
    waiting = uuid4()
    await store.enqueue(sid.hex, InputQueued(command_id=started.hex, input="lost"))
    await store.enqueue(sid.hex, InputQueued(command_id=waiting.hex, input="waiting"))
    await store.append(sid.hex, [TurnStarted(turn_id=started.hex, input="lost")], expect_seq=None)

    provider = EchoProvider()
    recovered = Runtime(provider, store, [Bot], default_model="different")
    fresh = uuid4()
    async with recovered.connect(sid, writable=True) as connection:
        await connection.send("fresh", command_id=fresh)
        interrupted = await recovered._wait_result(sid.hex, started)
        waiting_result = await recovered._wait_result(sid.hex, waiting)
        fresh_result = await recovered._wait_result(sid.hex, fresh)

    assert interrupted.outcome == "interrupted"
    assert interrupted.stop_reason == "process_stopped"
    assert waiting_result.text == "waiting"
    assert fresh_result.text == "fresh"
    assert provider.inputs == ["waiting", "fresh"]
    assert (await store.header(sid.hex)).model == "m"
    await recovered.aclose()


async def test_fresh_runtime_deduplicates_answer_and_cancel_commands() -> None:
    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    answer_id = uuid4()
    cancel_id = uuid4()
    ask_id = uuid4()
    await store.append(
        sid.hex,
        [
            AskAnswered(
                ask_id=ask_id.hex,
                response=ApprovalResponse(allow=True),
                command_id=answer_id.hex,
            ),
            CancellationRequested(command_id=cancel_id.hex),
        ],
        expect_seq=None,
    )
    fresh = Runtime(EchoProvider(), store, [Bot], default_model="m")

    async with fresh.connect(sid, writable=True) as connection:
        assert await connection.answer(
            ask_id,
            ApprovalResponse(allow=True),
            command_id=answer_id,
        ) == CommandReceipt(answer_id, True)
        assert await connection.cancel(command_id=cancel_id) == CommandReceipt(cancel_id, True)

    await fresh.aclose()


async def test_cancel_fences_a_provider_that_suppresses_task_cancellation() -> None:
    provider = SuppressingProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=command)
        await provider.started.wait()
        task = runtime.active[sid.hex]
        await connection.cancel(command_id=uuid4())
        result = await runtime._wait_result(sid.hex, command)
        await provider.cancelled.wait()
        provider.release.set()
        await asyncio.wait_for(task, timeout=1)

    logged = await history(store, sid)
    assert result.outcome == "cancelled"
    assert sum(isinstance(event, TurnCancelled) for event in logged) == 1
    assert not any(isinstance(event, TurnCompleted) for event in logged)
    assert not any(isinstance(event, TextPart) and event.text == "late" for event in logged)
    await runtime.aclose()


async def test_cancel_fences_a_tool_that_suppresses_task_cancellation() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    @tool
    async def stubborn() -> str:
        started.set()
        try:
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return "late"
        finally:
            finished.set()

    class Stubborn(Agent):
        tools = [stubborn]

    store = MemoryStore()
    runtime = Runtime(
        FakeProvider([Sample(tool_calls=[call("stubborn")]), Sample(text="should not run")]),
        store,
        [Stubborn],
        default_model="m",
    )
    sid = await runtime.create(Stubborn)
    command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=command)
        await started.wait()
        task = runtime.active[sid.hex]
        await connection.cancel(command_id=uuid4())
        await cancelled.wait()
        release.set()
        await finished.wait()
        await asyncio.wait_for(task, timeout=1)

    logged = await history(store, sid)
    assert sum(isinstance(event, TurnCancelled) for event in logged) == 1
    assert not any(isinstance(event, TurnCompleted) for event in logged)
    assert not any(isinstance(event, ToolCallCompleted) and event.result == "late" for event in logged)
    await runtime.aclose()


async def test_cancelled_prompt_still_activates_after_enqueue_commits() -> None:
    store = GatedStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        prompt = asyncio.create_task(connection.prompt("go", command_id=command))
        await store.enqueue_committed.wait()
        prompt.cancel()
        with pytest.raises(asyncio.CancelledError):
            await prompt
        store.enqueue_release.set()
        result = await runtime._wait_result(sid.hex, command)

    assert result.outcome == "completed"
    assert result.text == "go"
    await runtime.aclose()


async def test_cancelled_answer_still_resolves_after_answer_commits() -> None:
    @tool
    async def interview(ctx: Context) -> str:
        response = await ctx.ask(FreeText(prompt="name?"))
        return response.text

    class Asker(Agent):
        tools = [interview]

    store = GatedStore()
    store.enqueue_release.set()
    runtime = Runtime(
        FakeProvider([Sample(tool_calls=[call("interview")]), Sample(text="done")]),
        store,
        [Asker],
        default_model="m",
    )
    sid = await runtime.create(Asker)
    command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=command)
        while True:
            raised = (await anext(connection)).event
            if isinstance(raised, AskRaised):
                break
        store.append_kind = AskAnswered
        answer = asyncio.create_task(
            connection.answer(
                UUID(hex=raised.ask_id),
                FreeTextResponse(text="Malay"),
                command_id=uuid4(),
            )
        )
        await store.append_committed.wait()
        answer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await answer
        store.append_release.set()
        result = await runtime._wait_result(sid.hex, command)

    assert result.outcome == "completed"
    assert result.text == "done"
    await runtime.aclose()


async def test_cancelled_cancel_still_fences_after_command_commits() -> None:
    provider = SuppressingProvider()
    store = GatedStore()
    store.enqueue_release.set()
    store.append_kind = CancellationRequested
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=command)
        await provider.started.wait()
        task = runtime.active[sid.hex]
        cancellation = asyncio.create_task(connection.cancel(command_id=uuid4()))
        await store.append_committed.wait()
        cancellation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancellation
        store.append_release.set()
        result = await runtime._wait_result(sid.hex, command)
        await provider.cancelled.wait()
        provider.release.set()
        await asyncio.wait_for(task, timeout=1)

    assert result.outcome == "cancelled"
    await runtime.aclose()


async def test_aclose_returns_while_provider_suppresses_cancellation() -> None:
    provider = SuppressingProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    command = uuid4()
    connection = runtime.connect(sid, writable=True)
    await connection.__aenter__()
    await connection.send("go", command_id=command)
    await provider.started.wait()
    task = runtime.active[sid.hex]

    await asyncio.wait_for(runtime.aclose(), timeout=1)

    result = await runtime._wait_result(sid.hex, command)
    assert result.outcome == "interrupted"
    assert result.stop_reason == "runtime_closed"
    assert not task.done()
    provider.release.set()
    await asyncio.wait_for(task, timeout=1)
    logged = await history(store, sid)
    assert sum(isinstance(event, TurnInterrupted) for event in logged) == 1
    assert not any(isinstance(event, TurnCompleted) for event in logged)
    await connection.__aexit__(None, None, None)


async def test_multiple_readers_receive_identical_independent_sequences() -> None:
    runtime = Runtime(EchoProvider(), MemoryStore(), [Bot], default_model="m")
    sid = await runtime.create(Bot)

    async def consume() -> list[Any]:
        return [item async for item in runtime.events(sid)]

    first = asyncio.create_task(consume())
    second = asyncio.create_task(consume())
    async with runtime.connect(sid, writable=True) as connection:
        result = await connection.prompt("go", command_id=uuid4())
    assert result.outcome == "completed"
    await runtime.aclose()
    one, two = await asyncio.gather(first, second)

    assert [item.seq for item in one] == [item.seq for item in two]
    assert [item.event for item in one] == [item.event for item in two]
    assert [item.seq for item in one] == list(range(1, len(one) + 1))


async def test_writer_replacement_wakes_an_already_blocked_iterator() -> None:
    store = ObservedReadStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    old = runtime.connect(sid, writable=True)
    await old.__aenter__()
    await anext(old)
    store.armed = True
    blocked = asyncio.create_task(anext(old))
    await store.waiting.wait()

    async with runtime.connect(sid, writable=True):
        with pytest.raises(WriterReplaced):
            await asyncio.wait_for(blocked, timeout=1)

    await old.__aexit__(None, None, None)
    await runtime.aclose()


async def test_writer_takeover_serializes_with_command_acceptance() -> None:
    store = GatedStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    old = runtime.connect(sid, writable=True)
    await old.__aenter__()
    command = uuid4()
    sending = asyncio.create_task(old.send("go", command_id=command))
    await store.enqueue_committed.wait()
    newer = runtime.connect(sid, writable=True)
    takeover = asyncio.create_task(newer.__aenter__())
    _, pending = await asyncio.wait({takeover}, timeout=0)
    assert takeover in pending

    store.enqueue_release.set()
    assert await sending == CommandReceipt(command, False)
    await takeover
    with pytest.raises(WriterReplaced):
        await old.send("stale", command_id=uuid4())
    await runtime._wait_result(sid.hex, command)
    await newer.__aexit__(None, None, None)
    await old.__aexit__(None, None, None)
    await runtime.aclose()


async def test_later_input_runs_while_cancelled_resistant_task_remains_alive() -> None:
    provider = OverlappingProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    old_command = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("old", command_id=old_command)
        await provider.first_started.wait()
        old_task = runtime.active[sid.hex]
        await connection.cancel(command_id=uuid4())
        await provider.first_cancelled.wait()
        assert not old_task.done()
        assert sid.hex not in runtime.active
        new_result = await connection.prompt("new", command_id=uuid4())
        assert new_result.text == "new"
        assert not old_task.done()
        provider.first_release.set()
        await asyncio.wait_for(old_task, timeout=1)

    logged = await history(store, sid)
    old_terminals = [
        event
        for event in logged
        if isinstance(event, TurnCancelled | TurnCompleted | TurnInterrupted) and event.turn_id == old_command.hex
    ]
    assert len(old_terminals) == 1
    assert isinstance(old_terminals[0], TurnCancelled)
    assert not any(isinstance(event, TextPart) and event.text == "late" for event in logged)
    await runtime.aclose()


async def test_stale_cancelled_task_does_not_mark_running_replacement_idle() -> None:
    provider = OverlappingProvider()
    provider.replacement_release.clear()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("old", command_id=uuid4())
        await provider.first_started.wait()
        old_task = runtime.active[sid.hex]
        await connection.cancel(command_id=uuid4())
        await provider.first_cancelled.wait()
        replacement = asyncio.create_task(connection.prompt("new", command_id=uuid4()))
        await provider.replacement_started.wait()
        assert (await store.header(sid.hex)).status == "running"
        provider.first_release.set()
        await asyncio.wait_for(old_task, timeout=1)
        assert (await store.header(sid.hex)).status == "running"
        provider.replacement_release.set()
        assert (await replacement).text == "new"

    assert (await store.header(sid.hex)).status == "idle"
    await runtime.aclose()


async def test_send_commit_racing_close_never_executes_after_shutdown() -> None:
    store = GatedStore()
    provider = EchoProvider()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    connection = runtime.connect(sid, writable=True)
    await connection.__aenter__()
    command = uuid4()
    sending = asyncio.create_task(connection.send("queued", command_id=command))
    await store.enqueue_committed.wait()
    closing = asyncio.create_task(runtime.aclose())
    _, pending = await asyncio.wait({closing}, timeout=0)
    assert closing in pending

    store.enqueue_release.set()
    assert await sending == CommandReceipt(command, False)
    await asyncio.wait_for(closing, timeout=1)

    logged = await history(store, sid)
    assert provider.inputs == []
    assert sid.hex not in runtime.active
    assert any(isinstance(event, InputQueued) and event.command_id == command.hex for event in logged)
    assert not any(isinstance(event, TurnStarted) and event.turn_id == command.hex for event in logged)
    await connection.__aexit__(None, None, None)


async def test_delayed_recovery_interruption_racing_cancel_has_one_terminal() -> None:
    store = MemoryStore()
    initial = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await initial.create(Bot)
    abandoned = uuid4()
    await store.enqueue(sid.hex, InputQueued(command_id=abandoned.hex, input="lost"))
    await store.append(sid.hex, [TurnStarted(turn_id=abandoned.hex, input="lost")], expect_seq=None)
    runtime = DelayedRecoveryRuntime(EchoProvider(), store, [Bot], default_model="m")

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("fresh", command_id=uuid4())
        await runtime.recovery_started.wait()
        task = runtime.active[sid.hex]
        await connection.cancel(command_id=uuid4())
        assert sid.hex not in runtime.active
        runtime.recovery_release.set()
        await asyncio.wait_for(task, timeout=1)

    logged = await history(store, sid)
    terminals = [
        event
        for event in logged
        if isinstance(event, TurnCancelled | TurnCompleted | TurnInterrupted) and event.turn_id == abandoned.hex
    ]
    assert len(terminals) == 1
    assert isinstance(terminals[0], TurnCancelled)
    await runtime.aclose()


async def test_delayed_recovery_interruption_racing_close_has_one_terminal() -> None:
    store = MemoryStore()
    initial = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await initial.create(Bot)
    abandoned = uuid4()
    fresh = uuid4()
    await store.enqueue(sid.hex, InputQueued(command_id=abandoned.hex, input="lost"))
    await store.append(sid.hex, [TurnStarted(turn_id=abandoned.hex, input="lost")], expect_seq=None)
    runtime = DelayedRecoveryRuntime(EchoProvider(), store, [Bot], default_model="m")
    connection = runtime.connect(sid, writable=True)
    await connection.__aenter__()
    await connection.send("fresh", command_id=fresh)
    await runtime.recovery_started.wait()
    task = runtime.active[sid.hex]

    await asyncio.wait_for(runtime.aclose(), timeout=1)

    runtime.recovery_release.set()
    await asyncio.wait_for(task, timeout=1)
    logged = await history(store, sid)
    terminals = [
        event
        for event in logged
        if isinstance(event, TurnCancelled | TurnCompleted | TurnInterrupted) and event.turn_id == abandoned.hex
    ]
    assert len(terminals) == 1
    assert isinstance(terminals[0], TurnInterrupted)
    assert not any(isinstance(event, TurnStarted) and event.turn_id == fresh.hex for event in logged)
    await connection.__aexit__(None, None, None)
