import asyncio
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest

from tantra import Agent, FakeProvider, MemoryStore, Runtime, SessionHeader, TantraError
from tantra.events import InputQueued, SessionCreated, Stamped
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd
from tantra.providers.fake import FAKE_LIMITS
from tantra.tools import Context


async def test_unregistered_legacy_actor_history_is_readable_but_send_does_not_mutate_it() -> None:
    class Subagent(Agent):
        pass

    class Root(Agent):
        subagents = [Subagent]

    async def emit(_: str) -> None:
        return None

    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    legacy_id = uuid4()
    legacy = SessionHeader(
        id=legacy_id.hex,
        root_id=root.id,
        parent_id=root.id,
        agent="researcher",
        depth=1,
        model="m",
    )
    await store.create(legacy)
    await store.append(
        legacy.id,
        [
            SessionCreated(
                agent="researcher",
                root_id=root.id,
                parent_id=root.id,
                depth=1,
                model="m",
            ),
            InputQueued(command_id=uuid4().hex, input="historical research"),
        ],
    )

    status = await runtime.status(legacy_id)
    tree = await runtime.tree_status(root_id)
    stream = runtime.events(legacy_id)
    first = await anext(stream)
    await stream.aclose()
    before = ((await store.header(legacy.id)).model_dump(), await store.read_page(legacy.id))
    ctx = Context(
        session_id=root.id,
        turn_id="turn",
        call_id="send",
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )

    with pytest.raises(TantraError, match="unknown agent 'researcher'"):
        await runtime._actor_send(root, ctx, legacy_id, "new work")

    after = ((await store.header(legacy.id)).model_dump(), await store.read_page(legacy.id))
    assert status.agent == status.name == "researcher"
    assert [actor.agent for actor in tree] == ["root", "researcher"]
    assert isinstance(first.event, SessionCreated)
    assert before == after
    assert legacy.id not in runtime.active
    await runtime.aclose()


class Worker(Agent):
    model = "worker"


class Lead(Agent):
    model = "lead"
    subagents = [Worker]


class Echo:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="done")


def legacy(store: MemoryStore, sid: str) -> None:
    store._events[sid] = [
        Stamped(seq=item.seq, event=item.event.model_copy(update={"sender_id": None, "origin": None}))
        if isinstance(item.event, InputQueued)
        else item
        for item in store._events[sid]
    ]


def inputs(store: MemoryStore, sid: str) -> list[InputQueued]:
    return [item.event for item in store._events[sid] if isinstance(item.event, InputQueued)]


def context(store: MemoryStore, sid: str, turn: str, call: str) -> Context:
    async def emit(_: str) -> None:
        return None

    return Context(session_id=sid, turn_id=turn, call_id=call, depth=0, deps=None, store=store, emit=emit)


async def quiet(runtime: Runtime) -> None:
    async def idle() -> None:
        while any(not task.done() for task in runtime.active.values()):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(idle(), 5)


async def test_retries_of_legacy_inputs_are_duplicates_on_every_path() -> None:
    store = MemoryStore()
    runtime = Runtime(Echo(), store, [Lead])
    root_id = await runtime.create(Lead)
    root = await store.header(root_id.hex)
    assert root is not None
    spawn_ctx = context(store, root.id, "t1", "s1")
    child_id = await runtime._actor_spawn(root, Lead, spawn_ctx, "worker", "start")
    child = await store.header(UUID(child_id).hex)
    assert child is not None
    await quiet(runtime)
    legacy(store, child.id)
    assert await runtime._actor_spawn(root, Lead, spawn_ctx, "worker", "start") == child_id
    send_ctx = context(store, child.id, "t2", "m1")
    assert (await runtime._actor_send(child, send_ctx, root_id, "hello"))["duplicate"] is False
    await quiet(runtime)
    legacy(store, root.id)
    assert (await runtime._actor_send(child, send_ctx, root_id, "hello"))["duplicate"] is True
    command = uuid4()
    async with runtime.connect(root_id, writable=True) as connection:
        assert (await connection.send("human", command_id=command)).duplicate is False
        await quiet(runtime)
        legacy(store, root.id)
        assert (await connection.send("human", command_id=command)).duplicate is True
    await quiet(runtime)
    assert [event.input for event in inputs(store, child.id)] == ["start"]
    root_inputs = [event.input for event in inputs(store, root.id)]
    assert sum(event == f"[agent {UUID(hex=child.id)}] hello" for event in root_inputs) == 1
    assert root_inputs.count("human") == 1
    assert all(event.origin is None for event in inputs(store, root.id))
    await runtime.aclose()


async def test_lifecycle_reconcile_on_a_fresh_runtime_accepts_legacy_notices() -> None:
    store = MemoryStore()
    runtime = Runtime(Echo(), store, [Lead])
    root_id = await runtime.create(Lead)
    root = await store.header(root_id.hex)
    assert root is not None
    child_id = await runtime._actor_spawn(root, Lead, context(store, root.id, "t1", "s1"), "worker", "start")
    await quiet(runtime)
    await runtime.aclose()
    child = await store.header(UUID(child_id).hex)
    assert child is not None and child.last_turn is not None
    notices = [event for event in inputs(store, root.id) if event.origin == "turn_ended"]
    assert len(notices) == 1
    legacy(store, root.id)
    fresh = Runtime(Echo(), store, [Lead])
    await fresh._deliver_lifecycle(child, child.last_turn)
    async with fresh.connect(root_id, writable=True) as connection:
        result = await connection.prompt("again", command_id=uuid4())
    await quiet(fresh)
    assert result.outcome == "completed"
    assert [event.command_id for event in inputs(store, root.id) if " replied] " in event.input] == [
        notices[0].command_id
    ]
    await fresh.aclose()
