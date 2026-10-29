from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from tantra import (
    Agent,
    CommandEnvelope,
    DeliverPayload,
    InvalidCommandReuse,
    Runtime,
    SessionNotFound,
    TantraError,
    WriterToken,
)
from tantra.events import (
    AgentFinished,
    InputQueued,
    SessionCreated,
    SessionHeader,
    TurnCompleted,
    TurnStarted,
)
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd
from tantra.providers.fake import FAKE_LIMITS
from tantra.stores.memory import MemoryStore
from tantra.tools import Context


class Child(Agent):
    model = "child"


class Root(Agent):
    model = "root"
    subagents = [Child]


class Bot(Agent):
    pass


class RecordingProvider:
    def __init__(self) -> None:
        self.requests: dict[str, list[SampleRequest]] = {"root": [], "child": []}

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.requests[req.model].append(req)
        yield StreamEnd(text=f"{req.model} {len(self.requests[req.model])}")


class BlockingProvider:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.inputs: list[str] = []

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.inputs.append(req.messages[-1].content)
        if len(self.inputs) == 1:
            self.started.set()
            await asyncio.Event().wait()
        yield StreamEnd(text="ok")


class DroppedActivationRuntime(Runtime):
    drop_target: str | None = None

    def _activate(self, agent_id: str, root_id: str) -> None:
        if self.drop_target == agent_id:
            self.drop_target = None
            return
        super()._activate(agent_id, root_id)


def context(store: MemoryStore, sid: str) -> Context:
    async def emit(_: str) -> None:
        return None

    return Context(session_id=sid, turn_id="turn", call_id="call", depth=0, deps=None, store=store, emit=emit)


async def wait_idle(runtime: Runtime, *ids: str) -> None:
    async def idle() -> None:
        while any(sid in runtime.active for sid in ids):
            await asyncio.sleep(0)

    await asyncio.wait_for(idle(), timeout=2)


async def inputs(store: MemoryStore, sid: str) -> list[InputQueued]:
    return [item.event for item in await store.read_page(sid) if isinstance(item.event, InputQueued)]


async def spawned(runtime: Runtime, store: MemoryStore) -> tuple[UUID, UUID]:
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_id = UUID(await runtime._actor_spawn(root, Root, context(store, root.id), "child", "start"))
    await wait_idle(runtime, child_id.hex, root.id)
    return root_id, child_id


async def test_deliver_wakes_an_idle_child_whose_reply_wakes_the_root_and_keeps_the_writer() -> None:
    provider = RecordingProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root])
    root_id, child_id = await spawned(runtime, store)
    command = uuid4()
    async with runtime.connect(root_id, writable=True) as connection:
        receipt = await runtime.deliver(child_id, "task 3f2a finished", command_id=command, source="sandbox")
        result = await asyncio.wait_for(runtime._wait_result(child_id.hex, command), timeout=2)
        await wait_idle(runtime, child_id.hex, root_id.hex)
        assert runtime.writers[root_id.hex] == connection._generation
        assert (await connection.prompt("still mine", command_id=uuid4())).outcome == "completed"

    assert (receipt.command_id, receipt.duplicate) == (command, False)
    assert result.outcome == "completed"
    event = (await inputs(store, child_id.hex))[-1]
    assert (event.command_id, event.input, event.origin, event.sender_id, event.submitted_by) == (
        command.hex,
        "[event sandbox] task 3f2a finished",
        "event",
        None,
        None,
    )
    assert provider.requests["child"][-1].messages[-1].content == "[event sandbox] task 3f2a finished"
    assert [request.messages[-1].content for request in provider.requests["root"]] == [
        f"[agent {child_id} replied] child 1",
        f"[agent {child_id} replied] child 2",
        "still mine",
    ]
    assert [item.origin for item in await inputs(store, root_id.hex)] == ["turn_ended", "turn_ended", "user"]
    await runtime.aclose()


async def test_deliver_retry_is_a_duplicate_that_wakes_again_and_changed_content_conflicts() -> None:
    store = MemoryStore()
    runtime = DroppedActivationRuntime(RecordingProvider(), store, [Root])
    root_id, child_id = await spawned(runtime, store)
    command = uuid4()
    runtime.drop_target = child_id.hex

    first = await runtime.deliver(child_id, "done", command_id=command, source="sandbox")
    assert child_id.hex not in runtime.active
    again = await runtime.deliver(child_id, "done", command_id=command, source="sandbox")
    result = await asyncio.wait_for(runtime._wait_result(child_id.hex, command), timeout=2)
    for target, text, source in (
        (child_id, "changed", "sandbox"),
        (child_id, "done", "timer"),
        (root_id, "done", "sandbox"),
    ):
        with pytest.raises(InvalidCommandReuse):
            await runtime.deliver(target, text, command_id=command, source=source)
    await wait_idle(runtime, child_id.hex, root_id.hex)

    assert (first.duplicate, again.duplicate) == (False, True)
    assert result.outcome == "completed"
    assert [item.command_id for item in await inputs(store, child_id.hex)].count(command.hex) == 1
    assert command.hex not in [item.command_id for item in await inputs(store, root_id.hex)]
    await runtime.aclose()


async def test_deliver_rejects_a_finished_target_an_unknown_id_and_a_bad_source() -> None:
    store = MemoryStore()
    root_id, child_id, turn = uuid4(), uuid4(), uuid4().hex
    await store.create(SessionHeader(id=root_id.hex, root_id=root_id.hex, agent="root", model="root"))
    await store.append(root_id.hex, [SessionCreated(agent="root", root_id=root_id.hex, model="root")])
    await store.create(
        SessionHeader(
            id=child_id.hex, root_id=root_id.hex, parent_id=root_id.hex, agent="child", depth=1, model="child"
        )
    )
    await store.append(
        child_id.hex,
        [
            SessionCreated(agent="child", root_id=root_id.hex, parent_id=root_id.hex, depth=1, model="child"),
            InputQueued(command_id=turn, input="work", sender_id=root_id.hex, origin="spawn"),
            TurnStarted(turn_id=turn, input="work"),
            TurnCompleted(turn_id=turn, stop_reason="finished", output="done"),
            AgentFinished(result="done"),
        ],
    )
    runtime = Runtime(RecordingProvider(), store, [Root])

    with pytest.raises(TantraError, match=f"agent {child_id} is finished"):
        await runtime.deliver(child_id, "late", command_id=uuid4(), source="sandbox")
    with pytest.raises(SessionNotFound):
        await runtime.deliver(uuid4(), "lost", command_id=uuid4(), source="sandbox")
    for source in ("", "Sandbox", "1sandbox", "sand-box", "sandbox\n", "a" * 33):
        with pytest.raises(ValueError):
            await runtime.deliver(root_id, "bad", command_id=uuid4(), source=source)

    assert [item.input for item in await inputs(store, child_id.hex)] == ["work"]
    assert await inputs(store, root_id.hex) == []
    await runtime.aclose()


@pytest.mark.parametrize("descendants", [False, True])
async def test_cancel_scope_decides_a_queued_event_and_legacy_input_is_still_a_user_input(descendants: bool) -> None:
    provider = BlockingProvider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Bot], default_model="m")
    root_id = await runtime.create(Bot)
    active, legacy, event = uuid4(), uuid4(), uuid4()
    await store.enqueue(root_id.hex, InputQueued(command_id=active.hex, input="active", origin="user"))
    await store.enqueue(root_id.hex, InputQueued(command_id=legacy.hex, input="legacy"))
    async with runtime.connect(root_id, writable=True) as connection:
        runtime._activate(root_id.hex, root_id.hex)
        await asyncio.wait_for(provider.started.wait(), timeout=2)
        await runtime.deliver(root_id, "done", command_id=event, source="sandbox")
        await connection.cancel(command_id=uuid4(), descendants=descendants)
        results = [
            await asyncio.wait_for(runtime._wait_result(root_id.hex, command), timeout=2)
            for command in (active, legacy, event)
        ]
        await wait_idle(runtime, root_id.hex)

    kept = not descendants
    assert [result.outcome for result in results] == ["cancelled", "cancelled", "completed" if kept else "cancelled"]
    assert provider.inputs == (["active", "[event sandbox] done"] if kept else ["active"])
    await runtime.aclose()


def test_deliver_envelope_rejects_a_writer_token_and_needs_none() -> None:
    root_id = uuid4().hex
    fields: dict[str, Any] = {
        "request_id": uuid4(),
        "root_id": root_id,
        "operation": "deliver",
        "payload": DeliverPayload(command_id=uuid4(), agent_id=uuid4(), input="done", source="sandbox"),
        "deadline": datetime.now(UTC),
    }

    envelope = CommandEnvelope(**fields)

    assert envelope.writer_token is None
    assert CommandEnvelope.model_validate_json(envelope.model_dump_json()) == envelope
    with pytest.raises(ValidationError, match="deliver cannot include a writer token"):
        CommandEnvelope(**fields, writer_token=WriterToken(root_id=root_id, connection_id=uuid4()))
