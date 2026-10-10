from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from tantra import (
    Agent,
    ApprovalResponse,
    CommandReceipt,
    Connection,
    FreeText,
    FreeTextResponse,
    InvalidCommandReuse,
    Runtime,
    tool,
)
from tantra.context import TurnContext
from tantra.events import (
    AskAnswered,
    AskRaised,
    CancellationRequested,
    InputQueued,
    SessionEvent,
    ToolCallRequested,
    Usage,
)
from tantra.hooks import Hook
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall, UserMessage
from tantra.providers.fake import FAKE_LIMITS, FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context


class Bot(Agent):
    pass


def call(name: str, args: str = "{}", cid: str = "call") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


async def history(store: MemoryStore, sid: UUID | str) -> list[SessionEvent]:
    actor_id = sid.hex if isinstance(sid, UUID) else sid
    return [item.event async for item in store.read(actor_id)]


async def wait_idle(runtime: Runtime, *actor_ids: str) -> None:
    async def idle() -> None:
        while any(actor_id in runtime.active for actor_id in actor_ids):
            await asyncio.sleep(0)

    await asyncio.wait_for(idle(), timeout=2)


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        yield StreamEnd(text=value, usage=Usage(output_tokens=1))


class GateProvider(EchoProvider):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        await self.release.wait()
        yield StreamEnd(text="done")


@pytest.mark.parametrize("name", ["send", "prompt", "answer", "cancel"])
def test_connection_attribution_is_keyword_only(name: str) -> None:
    parameter = inspect.signature(getattr(Connection, name)).parameters["submitted_by"]

    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None


@pytest.mark.parametrize(
    "factory",
    [
        lambda value: InputQueued(command_id="command", input="go", submitted_by=value),
        lambda value: CancellationRequested(command_id="command", submitted_by=value),
        lambda value: AskAnswered(
            ask_id="ask",
            response=FreeTextResponse(text="yes"),
            command_id="command",
            submitted_by=value,
        ),
    ],
)
@pytest.mark.parametrize("value", ["", "x" * 257, 7])
def test_durable_attribution_is_a_bounded_strict_string(factory: Any, value: Any) -> None:
    with pytest.raises(ValidationError):
        factory(value)


async def test_send_deduplicates_exact_attribution_and_preserves_legacy_none() -> None:
    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    attributed = uuid4()
    legacy = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        assert await connection.send("one", command_id=attributed, submitted_by="user:1") == CommandReceipt(
            attributed, False
        )
        assert await connection.send("one", command_id=attributed, submitted_by="user:1") == CommandReceipt(
            attributed, True
        )
        with pytest.raises(InvalidCommandReuse):
            await connection.send("one", command_id=attributed, submitted_by="user:2")
        assert await connection.send("two", command_id=legacy) == CommandReceipt(legacy, False)
        assert await connection.send("two", command_id=legacy, submitted_by=None) == CommandReceipt(legacy, True)
        with pytest.raises(InvalidCommandReuse):
            await connection.send("two", command_id=legacy, submitted_by="user:1")

    queued = {event.command_id: event for event in await history(store, sid) if isinstance(event, InputQueued)}
    assert queued[attributed.hex].submitted_by == "user:1"
    assert queued[legacy.hex].submitted_by is None
    await runtime.aclose()


async def test_legacy_answer_and_cancel_retries_do_not_gain_attribution() -> None:
    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    ask_id = uuid4()
    answer_id = uuid4()
    cancel_id = uuid4()
    answer = AskAnswered(
        ask_id=ask_id.hex,
        response=FreeTextResponse(text="legacy"),
        command_id=answer_id.hex,
    )
    cancellation = CancellationRequested(command_id=cancel_id.hex, targets={sid.hex: ["lost"]})
    await store.append(sid.hex, [answer, cancellation])

    async with runtime.connect(sid, writable=True) as connection:
        assert await connection.answer(
            ask_id,
            FreeTextResponse(text="legacy"),
            command_id=answer_id,
        ) == CommandReceipt(answer_id, True)
        assert await connection.cancel(command_id=cancel_id) == CommandReceipt(cancel_id, True)
        with pytest.raises(InvalidCommandReuse):
            await connection.answer(
                ask_id,
                FreeTextResponse(text="legacy"),
                command_id=answer_id,
                submitted_by="user",
            )
        with pytest.raises(InvalidCommandReuse):
            await connection.cancel(command_id=cancel_id, submitted_by="user")

    events = await history(store, sid)
    stored_answer = next(event for event in events if isinstance(event, AskAnswered))
    stored_cancel = next(event for event in events if isinstance(event, CancellationRequested))
    assert stored_answer.submitted_by is None
    assert stored_answer.answered_by is None
    assert stored_cancel.submitted_by is None
    assert stored_cancel.targets == cancellation.targets
    await runtime.aclose()


async def test_prompt_attribution_reaches_turn_and_tool_context() -> None:
    turns: list[str | None] = []
    tools: list[str | None] = []

    class Capture(Hook):
        async def before_turn(self, turn: TurnContext) -> None:
            turns.append(turn.submitted_by)

    @tool
    async def capture(ctx: Context) -> str:
        tools.append(ctx.submitted_by)
        return "captured"

    class ContextBot(Agent):
        tools = [capture]

    provider = FakeProvider([Sample(tool_calls=[call("capture")]), Sample(text="done")])
    store = MemoryStore()
    runtime = Runtime(provider, store, [ContextBot], default_model="m", hooks=[Capture()])
    sid = await runtime.create(ContextBot)

    async with runtime.connect(sid, writable=True) as connection:
        result = await connection.prompt("go", command_id=uuid4(), submitted_by="user:context")

    queued = next(event for event in await history(store, sid) if isinstance(event, InputQueued))
    assert result.text == "done"
    assert queued.submitted_by == "user:context"
    assert turns == ["user:context"]
    assert tools == ["user:context"]
    await runtime.aclose()


async def test_answer_attribution_survives_expiry_and_keeps_root_answered_by() -> None:
    @tool
    async def interview(ctx: Context) -> str:
        response = await ctx.ask(FreeText(prompt="name?"))
        return response.text

    class Asker(Agent):
        tools = [interview]

    store = MemoryStore()
    runtime = Runtime(
        FakeProvider([Sample(tool_calls=[call("interview")]), Sample(text="done")]),
        store,
        [Asker],
        default_model="m",
    )
    sid = await runtime.create(Asker)
    turn_id = uuid4()
    answer_id = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=turn_id, submitted_by="requester")
        while True:
            raised = (await anext(connection)).event
            if isinstance(raised, AskRaised):
                break
        ask_id = UUID(hex=raised.ask_id)
        assert await connection.answer(
            ask_id,
            FreeTextResponse(text="Malay"),
            command_id=answer_id,
            submitted_by="reviewer",
        ) == CommandReceipt(answer_id, False)
        await runtime._wait_result(sid.hex, turn_id)
        assert await connection.answer(
            ask_id,
            FreeTextResponse(text="Malay"),
            command_id=answer_id,
            submitted_by="reviewer",
        ) == CommandReceipt(answer_id, True)
        with pytest.raises(InvalidCommandReuse):
            await connection.answer(
                ask_id,
                FreeTextResponse(text="Malay"),
                command_id=answer_id,
                submitted_by="other",
            )

    answered = next(event for event in await history(store, sid) if isinstance(event, AskAnswered))
    assert answered.submitted_by == "reviewer"
    assert answered.answered_by == sid.hex
    await runtime.aclose()


async def test_cancel_retry_retains_targets_and_attribution() -> None:
    provider = GateProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    sid = await runtime.create(Bot)
    active = uuid4()
    queued = uuid4()
    cancel_id = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("active", command_id=active)
        await provider.started.wait()
        await connection.send("queued", command_id=queued)
        assert await connection.cancel(command_id=cancel_id, submitted_by="operator") == CommandReceipt(
            cancel_id, False
        )
        cancellation = next(event for event in await history(store, sid) if isinstance(event, CancellationRequested))
        assert cancellation.targets == {sid.hex: [queued.hex, active.hex]}
        assert cancellation.submitted_by == "operator"
        assert await connection.cancel(command_id=cancel_id, submitted_by="operator") == CommandReceipt(cancel_id, True)
        with pytest.raises(InvalidCommandReuse):
            await connection.cancel(command_id=cancel_id, submitted_by="other")

    cancellations = [event for event in await history(store, sid) if isinstance(event, CancellationRequested)]
    assert cancellations == [cancellation]
    provider.release.set()
    await runtime.aclose()


async def test_fresh_runtime_recovers_attributed_send_after_lost_reply() -> None:
    store = MemoryStore()
    creator = Runtime(EchoProvider(), store, [Bot], default_model="m")
    sid = await creator.create(Bot)
    command_id = uuid4()
    await store.enqueue(
        sid.hex,
        InputQueued(command_id=command_id.hex, input="durable", submitted_by="user:recovery"),
    )
    await creator.aclose()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")

    async with runtime.connect(sid, writable=True) as connection:
        receipt = await connection.send("durable", command_id=command_id, submitted_by="user:recovery")
        result = await runtime._wait_result(sid.hex, command_id)

    assert receipt == CommandReceipt(command_id, True)
    assert result.text == "durable"
    assert len([event for event in await history(store, sid) if isinstance(event, InputQueued)]) == 1
    await runtime.aclose()


async def test_internal_actor_commands_do_not_inherit_external_attribution() -> None:
    class Child(Agent):
        pass

    class Root(Agent):
        subagents = [Child]

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None

    async def emit(_: str) -> None:
        return None

    root_context = Context(
        session_id=root.id,
        turn_id=uuid4().hex,
        call_id="spawn",
        depth=0,
        deps=None,
        store=store,
        emit=emit,
        submitted_by="external-user",
    )
    child_public = await runtime._actor_spawn(root, Root, root_context, "child", "work")
    child_id = UUID(child_public).hex
    await wait_idle(runtime, child_id, root.id)
    child = await store.header(child_id)
    assert child is not None
    child_context = Context(
        session_id=child.id,
        turn_id=uuid4().hex,
        call_id="send",
        depth=1,
        deps=None,
        store=store,
        emit=emit,
        submitted_by="external-user",
    )

    await runtime._actor_send(child, child_context, root_id, "message")
    await wait_idle(runtime, root.id)

    internal = [
        event
        for actor_id in (root.id, child.id)
        for event in await history(store, actor_id)
        if isinstance(event, InputQueued)
    ]
    assert len(internal) >= 2
    assert all(event.submitted_by is None for event in internal)
    await runtime.aclose()


async def test_approval_extra_uses_post_hook_arguments_and_context_keeps_submitter() -> None:
    invoked: list[tuple[str, str | None]] = []

    @tool(permission="ask")
    async def action(value: str, ctx: Context) -> str:
        invoked.append((value, ctx.submitted_by))
        return value

    class Transform(Hook):
        async def before_tool(self, call_event: ToolCallRequested, turn: TurnContext) -> ToolCallRequested | None:
            if call_event.name == "action":
                return call_event.model_copy(update={"args": {"value": "transformed"}})
            return None

    class Guarded(Agent):
        tools = [action]

    store = MemoryStore()
    runtime = Runtime(
        FakeProvider(
            [
                Sample(tool_calls=[call("action", '{"value":"original"}')]),
                Sample(text="done"),
            ]
        ),
        store,
        [Guarded],
        default_model="m",
        hooks=[Transform()],
    )
    sid = await runtime.create(Guarded)
    command_id = uuid4()

    async with runtime.connect(sid, writable=True) as connection:
        await connection.send("go", command_id=command_id, submitted_by="requester")
        while True:
            raised = (await anext(connection)).event
            if isinstance(raised, AskRaised):
                break
        assert raised.request.extra == {
            "permission": "action",
            "arguments": {"value": "transformed"},
        }
        await connection.answer(
            UUID(hex=raised.ask_id),
            ApprovalResponse(allow=True),
            command_id=uuid4(),
            submitted_by="reviewer",
        )
        await runtime._wait_result(sid.hex, command_id)

    assert invoked == [("transformed", "requester")]
    await runtime.aclose()
