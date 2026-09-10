from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from tantra import Agent, FreeText, FreeTextResponse, InvalidCommandReuse, MaxDepthExceeded, Runtime, TantraError, tool
from tantra.events import (
    AgentFinished,
    AskAnswered,
    AskRaised,
    CancellationRequested,
    ChildCreated,
    InputQueued,
    LoggedEvent,
    SessionEvent,
    ToolCallCompleted,
    TurnCancelled,
    TurnCompleted,
    TurnInterrupted,
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS
from tantra.stores.memory import MemoryStore
from tantra.tools import Context


def call(name: str, args: str, cid: str) -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def context(store: MemoryStore, sid: str, *, turn: str = "turn", cid: str = "call", depth: int = 0) -> Context:
    async def emit(_: str) -> None:
        return None

    return Context(
        session_id=sid,
        turn_id=turn,
        call_id=cid,
        depth=depth,
        deps=None,
        store=store,
        emit=emit,
    )


async def wait_idle(runtime: Runtime, *ids: str) -> None:
    async def idle() -> None:
        while any(sid in runtime.active for sid in ids):
            await asyncio.sleep(0)

    await asyncio.wait_for(idle(), timeout=2)


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="done")


async def journal(store: MemoryStore, sid: str) -> list[Any]:
    return [item.event for item in await store.read_page(sid)]


async def test_spawn_is_immediate_siblings_overlap_and_headers_are_independent() -> None:
    class Worker(Agent):
        model = "child-model"

    class Root(Agent):
        subagents = [Worker]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.root_calls = 0
            self.children_started = 0
            self.overlap = asyncio.Event()
            self.release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "root-model":
                self.root_calls += 1
                if self.root_calls == 1:
                    yield StreamEnd(
                        tool_calls=[
                            call("spawn", '{"agent_name":"worker","input":"one"}', "s1"),
                            call("spawn", '{"agent_name":"worker","input":"two"}', "s2"),
                        ]
                    )
                else:
                    yield StreamEnd(text="parent idle")
                return
            self.children_started += 1
            if self.children_started == 2:
                self.overlap.set()
            await self.release.wait()
            yield StreamEnd(text="child done")

    provider = Provider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root], default_model="root-model")
    root_id = await runtime.create(Root, metadata={"tenant": 7})
    async with runtime.connect(root_id, writable=True) as connection:
        result = await asyncio.wait_for(connection.prompt("start", command_id=uuid4()), timeout=2)
    await asyncio.wait_for(provider.overlap.wait(), timeout=2)

    children = await store.list(parent_id=root_id.hex)
    assert result.text == "parent idle"
    assert root_id.hex not in runtime.active
    assert len(children) == 2
    assert {child.depth for child in children} == {1}
    assert {child.root_id for child in children} == {root_id.hex}
    assert {child.model for child in children} == {"child-model"}
    assert {child.metadata["tenant"] for child in children} == {7}
    child_journals = await asyncio.gather(*(journal(store, child.id) for child in children))
    assert all(events[0].type == "session_created" for events in child_journals)
    assert {event.child_id for event in await journal(store, root_id.hex) if isinstance(event, ChildCreated)} == {
        child.id for child in children
    }

    provider.release.set()
    await wait_idle(runtime, *(child.id for child in children))
    assert (await store.header(root_id.hex)).last_seq != children[0].last_seq
    await runtime.aclose()


async def test_send_is_direct_prefixed_deduplicated_and_wakes_idle_target() -> None:
    class Worker(Agent):
        pass

    class Root(Agent):
        subagents = [Worker]

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "worker", "start")
    assert await runtime._actor_spawn(root, Root, context(store, root.id), "worker", "start") == child_public
    with pytest.raises(InvalidCommandReuse):
        await runtime._actor_spawn(root, Root, context(store, root.id), "worker", "changed")
    child_id = UUID(child_public).hex
    child_input = next(event for event in await journal(store, child_id) if isinstance(event, InputQueued))
    async with runtime.connect(root_id, writable=True) as connection:
        with pytest.raises(InvalidCommandReuse):
            await connection.send("human conflict", command_id=UUID(hex=child_input.command_id))
    await wait_idle(runtime, child_id)
    child = await store.header(child_id)
    assert child is not None
    ctx = context(store, child.id, turn="t", cid="c", depth=1)

    first = await runtime._actor_send(child, ctx, root_id, "hello")
    duplicate = await runtime._actor_send(child, ctx, root_id, "hello")
    with pytest.raises(InvalidCommandReuse):
        await runtime._actor_send(child, ctx, root_id, "changed")
    assert first["duplicate"] is False
    assert duplicate["duplicate"] is True
    queued = [event for event in await journal(store, root.id) if isinstance(event, InputQueued)][-1]
    assert queued.input == f"[agent {UUID(hex=child.id)}] hello"
    await wait_idle(runtime, root.id)

    sibling_public = await runtime._actor_spawn(
        root,
        Root,
        context(store, root.id, turn="other", cid="spawn"),
        "worker",
        "start",
    )
    with pytest.raises(TantraError, match="direct parent-child"):
        await runtime._actor_send(child, context(store, child.id, turn="t2", cid="c2"), UUID(sibling_public), "no")
    await runtime.aclose()


async def test_grandchildren_and_inclusive_depth_fail_before_create() -> None:
    class Four(Agent):
        pass

    class Three(Agent):
        subagents = [Four]

    class Two(Agent):
        subagents = [Three]

    class One(Agent):
        subagents = [Two]

    class Root(Agent):
        subagents = [One]

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Root], default_model="m", max_depth=3)
    root_id = await runtime.create(Root)
    parent = await store.header(root_id.hex)
    assert parent is not None
    chain: list[str] = []
    for depth, (agent, name) in enumerate(((Root, "one"), (One, "two"), (Two, "three")), start=1):
        child = await runtime._actor_spawn(
            parent,
            agent,
            context(store, parent.id, turn=f"t{depth}", cid=f"c{depth}", depth=depth - 1),
            name,
            "go",
        )
        chain.append(UUID(child).hex)
        parent = await store.header(UUID(child).hex)
        assert parent is not None and parent.depth == depth
    before = {header.id for header in await runtime._tree_headers(root_id.hex)}
    with pytest.raises(MaxDepthExceeded):
        await runtime._actor_spawn(
            parent,
            Three,
            context(store, parent.id, turn="t4", cid="c4", depth=3),
            "four",
            "go",
        )
    after = {header.id for header in await runtime._tree_headers(root_id.hex)}
    assert before == after
    assert len(chain) == 3
    await runtime.aclose()


async def test_root_writer_answers_descendant_ask_in_child_log() -> None:
    @tool
    async def question(ctx: Context) -> str:
        response = await ctx.ask(FreeText(prompt="name?"))
        return response.text

    class Child(Agent):
        tools = [question]

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.child_calls = 0

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                self.child_calls += 1
                if self.child_calls == 1:
                    yield StreamEnd(tool_calls=[call("question", "{}", "q")])
                else:
                    yield StreamEnd(text="answered")
                return
            yield StreamEnd(text="root")

    store = MemoryStore()
    runtime = Runtime(Provider(), store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    Child.model = "child"
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "ask")
    child_id = UUID(child_public).hex

    async def raised() -> AskRaised:
        while True:
            found = next((event for event in await journal(store, child_id) if isinstance(event, AskRaised)), None)
            if found is not None:
                return found
            await asyncio.sleep(0)

    ask = await asyncio.wait_for(raised(), timeout=2)
    async with runtime.connect(root_id, writable=True) as connection:
        await connection.answer(UUID(hex=ask.ask_id), FreeTextResponse(text="Malay"), command_id=uuid4())
    await wait_idle(runtime, child_id)
    child_events = await journal(store, child_id)
    assert any(isinstance(event, AskAnswered) and event.answered_by == root_id.hex for event in child_events)
    assert not any(isinstance(event, AskAnswered) for event in await journal(store, root_id.hex))
    await runtime.aclose()


class Result(BaseModel):
    z: int
    a: str


async def test_finish_boundary_closes_child_cancels_queue_and_delivers_once() -> None:
    ran: list[str] = []
    slow_started = asyncio.Event()
    release = asyncio.Event()

    @tool
    async def slow() -> str:
        slow_started.set()
        await release.wait()
        ran.append("slow")
        return "slow"

    @tool
    async def late() -> str:
        ran.append("late")
        return "late"

    class Child(Agent):
        model = "child"
        tools = [slow, late]
        output_schema = Result

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                yield StreamEnd(
                    tool_calls=[
                        call("slow", "{}", "slow"),
                        call("finish", '{"result":{"z":2,"a":"x"}}', "finish"),
                        call("late", "{}", "late"),
                    ]
                )
                return
            yield StreamEnd(text="root")

    store = MemoryStore()
    runtime = Runtime(Provider(), store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "work")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(slow_started.wait(), timeout=2)
    await store.enqueue(child_id, InputQueued(command_id=uuid4().hex, input="queued"))
    release.set()
    await wait_idle(runtime, child_id, root.id)

    child = await store.header(child_id)
    child_events = await journal(store, child_id)
    parent_inputs = [event for event in await journal(store, root.id) if isinstance(event, InputQueued)]
    assert child is not None and child.finished
    assert ran == ["slow"]
    assert sum(isinstance(event, AgentFinished) for event in child_events) == 1
    assert any(isinstance(event, TurnCompleted) and event.stop_reason == "finished" for event in child_events)
    assert isinstance(child_events[-2], TurnCompleted)
    assert isinstance(child_events[-1], AgentFinished)
    assert child.last_seq == len(child_events)
    assert any(isinstance(event, TurnCancelled) and event.reason == "agent_finished" for event in child_events)
    late_result = next(
        event for event in child_events if isinstance(event, ToolCallCompleted) and event.call_id == "late"
    )
    assert late_result.is_error and late_result.result == "not executed: turn completed"
    assert sum(' finished] {"a":"x","z":2}' in event.input for event in parent_inputs) == 1

    child_turn = next(event.turn_id for event in child_events if isinstance(event, TurnStarted))
    before_duplicate = list(child_events)
    duplicate = await runtime._actor_finish(
        child,
        Child,
        context(store, child_id, turn=child_turn, cid="finish", depth=1),
        {"z": 2, "a": "x"},
    )
    await wait_idle(runtime, root.id)
    assert duplicate.output == {"z": 2, "a": "x"}
    assert await journal(store, child_id) == before_duplicate
    assert (
        sum(
            isinstance(event, InputQueued) and ' finished] {"a":"x","z":2}' in event.input
            for event in await journal(store, root.id)
        )
        == 1
    )

    with pytest.raises(TantraError, match="finished"):
        await runtime._actor_send(
            root,
            context(store, root.id, turn="later", cid="send"),
            UUID(child_public),
            "new",
        )
    await runtime.aclose()


async def test_finish_rejects_unfinished_descendant_and_reserved_collisions() -> None:
    class Grandchild(Agent):
        pass

    class Child(Agent):
        subagents = [Grandchild]

    class Root(Agent):
        subagents = [Child]

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "go")
    child = await store.header(UUID(child_public).hex)
    assert child is not None
    await wait_idle(runtime, child.id)
    grandchild_public = await runtime._actor_spawn(
        child,
        Child,
        context(store, child.id, turn="grand", cid="spawn", depth=1),
        "grandchild",
        "go",
    )
    await wait_idle(runtime, UUID(grandchild_public).hex)
    with pytest.raises(TantraError, match=grandchild_public):
        await runtime._actor_finish(child, Child, context(store, child.id, turn="finish", cid="f", depth=1), "x")
    await runtime.aclose()

    @tool(name="finish")
    async def collision() -> str:
        return "no"

    class BadChild(Agent):
        tools = [collision]

    class BadRoot(Agent):
        subagents = [BadChild]

    with pytest.raises(TantraError, match="duplicate tool name 'finish'"):
        Runtime(EchoProvider(), MemoryStore(), [BadRoot], default_model="m")


async def test_tree_cancel_fences_child_late_writes_and_leaves_other_root_running() -> None:
    class Child(Agent):
        model = "blocked-child"

    class Root(Agent):
        subagents = [Child]

    class Other(Agent):
        model = "other"

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()
            self.release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model != "blocked-child":
                yield StreamEnd(text="other done")
                return
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                await self.release.wait()
            yield StreamEnd(text="late")

    provider = Provider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root, Other], default_model="root")
    root_id = await runtime.create(Root)
    other_id = await runtime.create(Other)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "block")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(provider.started.wait(), timeout=2)
    child_task = runtime.active[child_id]

    async with runtime.connect(root_id, writable=True) as connection:
        await connection.cancel(command_id=uuid4())
    async with runtime.connect(other_id, writable=True) as connection:
        other_result = await connection.prompt("go", command_id=uuid4())
    await asyncio.wait_for(provider.cancelled.wait(), timeout=2)
    provider.release.set()
    await asyncio.wait_for(child_task, timeout=2)

    child_events = await journal(store, child_id)
    assert other_result.text == "other done"
    assert sum(isinstance(event, TurnCancelled) for event in child_events) == 1
    assert not any(isinstance(event, TurnCompleted) for event in child_events)
    assert not any(getattr(event, "text", None) == "late" for event in child_events)
    await runtime.aclose()


async def test_finish_validation_failure_is_a_tool_error_and_provider_continues() -> None:
    class Child(Agent):
        model = "child"
        output_schema = Result

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.child_calls = 0

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model != "child":
                yield StreamEnd(text="root")
                return
            self.child_calls += 1
            if self.child_calls == 1:
                yield StreamEnd(tool_calls=[call("finish", '{"result":{"z":"bad","a":"x"}}', "finish")])
            else:
                yield StreamEnd(text="recovered")

    provider = Provider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "work")
    child_id = UUID(child_public).hex
    await wait_idle(runtime, child_id)

    child = await store.header(child_id)
    child_events = await journal(store, child_id)
    failed_finish = next(
        event for event in child_events if isinstance(event, ToolCallCompleted) and event.call_id == "finish"
    )
    assert child is not None and child.finished is False
    assert provider.child_calls == 2
    assert failed_finish.is_error
    assert not any(isinstance(event, AgentFinished) for event in child_events)
    assert any(isinstance(event, TurnCompleted) and event.stop_reason == "completed" for event in child_events)
    await runtime.aclose()


async def test_send_reusing_one_call_for_another_target_conflicts_tree_wide() -> None:
    class Leaf(Agent):
        pass

    class Middle(Agent):
        subagents = [Leaf]

    class Root(Agent):
        subagents = [Middle]

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    middle_public = await runtime._actor_spawn(root, Root, context(store, root.id), "middle", "start")
    middle = await store.header(UUID(middle_public).hex)
    assert middle is not None
    await wait_idle(runtime, middle.id)
    leaf_public = await runtime._actor_spawn(
        middle,
        Middle,
        context(store, middle.id, turn="spawn-leaf", cid="spawn", depth=1),
        "leaf",
        "start",
    )
    await wait_idle(runtime, UUID(leaf_public).hex)
    ctx = context(store, middle.id, turn="same-turn", cid="same-call", depth=1)

    receipt = await runtime._actor_send(middle, ctx, root_id, "to parent")
    with pytest.raises(InvalidCommandReuse):
        await runtime._actor_send(middle, ctx, UUID(leaf_public), "to child")

    command_id = UUID(receipt["command_id"]).hex
    occurrences = 0
    for header in await runtime._tree_headers(root.id):
        occurrences += sum(
            isinstance(event, InputQueued) and event.command_id == command_id
            for event in await journal(store, header.id)
        )
    assert occurrences == 1
    await runtime.aclose()


class DroppedActivationRuntime(Runtime):
    drop_target: str | None = None

    def _activate(self, agent_id: str, root_id: str) -> None:
        if self.drop_target == agent_id:
            self.drop_target = None
            return
        super()._activate(agent_id, root_id)


async def test_duplicate_send_reactivates_after_acceptance_before_activation() -> None:
    class Child(Agent):
        pass

    class Root(Agent):
        subagents = [Child]

    store = MemoryStore()
    runtime = DroppedActivationRuntime(EchoProvider(), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "start")
    child = await store.header(UUID(child_public).hex)
    assert child is not None
    await wait_idle(runtime, child.id)
    runtime.drop_target = root.id
    ctx = context(store, child.id, turn="turn", cid="send", depth=1)

    first = await runtime._actor_send(child, ctx, root_id, "accepted")
    assert root.id not in runtime.active
    duplicate = await runtime._actor_send(child, ctx, root_id, "accepted")
    result = await runtime._wait_result(root.id, UUID(first["command_id"]))

    assert duplicate["duplicate"] is True
    assert result.outcome == "completed"
    await runtime.aclose()


class FailingCancellationStore(MemoryStore):
    fail_cancellation = True

    async def append(
        self,
        sid: str,
        events: Sequence[SessionEvent],
    ) -> int:
        if self.fail_cancellation and any(isinstance(event, CancellationRequested) for event in events):
            raise RuntimeError("root cancellation append failed")
        return await super().append(sid, events)


async def test_cancel_root_append_failure_has_no_actor_side_effects() -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                self.started.set()
                await self.release.wait()
            yield StreamEnd(text="done")

    provider = Provider()
    store = FailingCancellationStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "block")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(provider.started.wait(), timeout=2)
    child_task = runtime.active[child_id]

    async with runtime.connect(root_id, writable=True) as connection:
        with pytest.raises(RuntimeError, match="root cancellation append failed"):
            await connection.cancel(command_id=uuid4())

    assert runtime.active[child_id] is child_task
    assert not child_task.cancelled()
    assert not any(isinstance(event, CancellationRequested | TurnCancelled) for event in await journal(store, child_id))
    assert not any(isinstance(event, CancellationRequested | TurnCancelled) for event in await journal(store, root.id))
    store.fail_cancellation = False
    provider.release.set()
    await wait_idle(runtime, child_id)
    await runtime.aclose()


class PartialCancellationStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_sid: str | None = None
        self.failed = False

    async def append(
        self,
        sid: str,
        events: Sequence[SessionEvent],
    ) -> int:
        if (
            sid == self.fail_sid
            and not self.failed
            and any(isinstance(event, TurnCancelled) and event.reason == "cancelled" for event in events)
        ):
            self.failed = True
            raise RuntimeError("actor cancellation append failed")
        return await super().append(sid, events)


async def test_cancel_retry_resumes_after_partial_actor_terminalization() -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.root_started = asyncio.Event()
            self.child_started = asyncio.Event()
            self.release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "root":
                self.root_started.set()
            else:
                self.child_started.set()
            await self.release.wait()
            yield StreamEnd(text="late")

    provider = Provider()
    store = PartialCancellationStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    connection = runtime.connect(root_id, writable=True)
    await connection.__aenter__()
    root_command = uuid4()
    await connection.send("root work", command_id=root_command)
    await asyncio.wait_for(provider.root_started.wait(), timeout=2)
    root_task = runtime.active[root.id]
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "child work")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(provider.child_started.wait(), timeout=2)
    child_task = runtime.active[child_id]
    store.fail_sid = child_id
    cancel_id = uuid4()

    with pytest.raises(RuntimeError, match="actor cancellation append failed"):
        await connection.cancel(command_id=cancel_id)
    await asyncio.wait_for(root_task, timeout=2)
    partial_root_events = await journal(store, root.id)
    assert runtime.active[child_id] is child_task
    assert sum(isinstance(event, CancellationRequested) for event in partial_root_events) == 1
    assert sum(isinstance(event, TurnCancelled) and event.reason == "cancelled" for event in partial_root_events) == 1
    assert not any(isinstance(event, TurnCancelled) for event in await journal(store, child_id))

    retry = await connection.cancel(command_id=cancel_id)
    await asyncio.wait_for(child_task, timeout=2)
    root_events = await journal(store, root.id)
    child_events = await journal(store, child_id)

    assert retry.duplicate is True
    assert sum(isinstance(event, CancellationRequested) for event in root_events) == 1
    assert sum(isinstance(event, TurnCancelled) and event.reason == "cancelled" for event in root_events) == 1
    assert sum(isinstance(event, TurnCancelled) and event.reason == "cancelled" for event in child_events) == 1
    await connection.__aexit__(None, None, None)
    await runtime.aclose()


async def test_cancel_retry_does_not_cancel_later_work() -> None:
    class Root(Agent):
        pass

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.calls = 0
            self.first_started = asyncio.Event()
            self.second_started = asyncio.Event()
            self.release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
            else:
                self.second_started.set()
            await self.release.wait()
            yield StreamEnd(text="done")

    provider = Provider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    connection = runtime.connect(root_id, writable=True)
    await connection.__aenter__()
    first_command = uuid4()
    await connection.send("first", command_id=first_command)
    await asyncio.wait_for(provider.first_started.wait(), timeout=2)
    first_task = runtime.active[root_id.hex]
    cancel_id = uuid4()

    accepted = await connection.cancel(command_id=cancel_id)
    await asyncio.wait_for(first_task, timeout=2)
    first_result = await runtime._wait_result(root_id.hex, first_command)
    request = next(event for event in await journal(store, root_id.hex) if isinstance(event, CancellationRequested))
    second_command = uuid4()
    await connection.send("second", command_id=second_command)
    await asyncio.wait_for(provider.second_started.wait(), timeout=2)
    second_task = runtime.active[root_id.hex]

    retry = await connection.cancel(command_id=cancel_id)

    assert accepted.duplicate is False
    assert retry.duplicate is True
    assert first_result.outcome == "cancelled"
    assert request.targets == {root_id.hex: [first_command.hex]}
    assert runtime.active[root_id.hex] is second_task
    assert not second_task.done()
    assert not any(
        isinstance(event, TurnCancelled) and event.turn_id == second_command.hex
        for event in await journal(store, root_id.hex)
    )

    provider.release.set()
    second_result = await runtime._wait_result(root_id.hex, second_command)
    await asyncio.wait_for(second_task, timeout=2)
    root_events = await journal(store, root_id.hex)

    assert second_result.outcome == "completed"
    assert sum(isinstance(event, CancellationRequested) for event in root_events) == 1
    assert not any(isinstance(event, TurnCancelled) and event.turn_id == second_command.hex for event in root_events)
    await connection.__aexit__(None, None, None)
    await runtime.aclose()


class CommittedCancellationStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_sid: str | None = None
        self.failed = False

    async def append(
        self,
        sid: str,
        events: Sequence[SessionEvent],
    ) -> int:
        result = await super().append(sid, events)
        if (
            sid == self.fail_sid
            and not self.failed
            and any(isinstance(event, TurnCancelled) and event.reason == "cancelled" for event in events)
        ):
            self.failed = True
            raise RuntimeError("actor cancellation header write failed")
        return result


async def test_cancel_retry_fences_a_target_after_terminal_commit_raises() -> None:
    class Root(Agent):
        pass

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.calls = 0
            self.first_started = asyncio.Event()
            self.first_cancelled = asyncio.Event()
            self.first_release = asyncio.Event()
            self.second_started = asyncio.Event()
            self.second_release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
                try:
                    await self.first_release.wait()
                except asyncio.CancelledError:
                    self.first_cancelled.set()
                    await self.first_release.wait()
                yield StreamEnd(text="late")
                return
            self.second_started.set()
            await self.second_release.wait()
            yield StreamEnd(text="later")

    provider = Provider()
    store = CommittedCancellationStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    connection = runtime.connect(root_id, writable=True)
    await connection.__aenter__()
    first_command = uuid4()
    await connection.send("first", command_id=first_command)
    await asyncio.wait_for(provider.first_started.wait(), timeout=2)
    first_task = runtime.active[root_id.hex]
    store.fail_sid = root_id.hex
    cancel_id = uuid4()

    with pytest.raises(RuntimeError, match="actor cancellation header write failed"):
        await connection.cancel(command_id=cancel_id)
    committed = await journal(store, root_id.hex)
    assert runtime.active[root_id.hex] is first_task
    assert sum(isinstance(event, TurnCancelled) and event.turn_id == first_command.hex for event in committed) == 1

    retry = await connection.cancel(command_id=cancel_id)
    await asyncio.wait_for(provider.first_cancelled.wait(), timeout=2)
    provider.first_release.set()
    await asyncio.wait_for(first_task, timeout=2)
    after_retry = await journal(store, root_id.hex)

    assert retry.duplicate is True
    assert not any(isinstance(event, TurnCompleted) and event.turn_id == first_command.hex for event in after_retry)

    second_command = uuid4()
    await connection.send("second", command_id=second_command)
    await asyncio.wait_for(provider.second_started.wait(), timeout=2)
    second_task = runtime.active[root_id.hex]
    later_retry = await connection.cancel(command_id=cancel_id)

    assert later_retry.duplicate is True
    assert runtime.active[root_id.hex] is second_task
    assert not second_task.done()

    provider.second_release.set()
    second_result = await runtime._wait_result(root_id.hex, second_command)
    await asyncio.wait_for(second_task, timeout=2)
    root_events = await journal(store, root_id.hex)

    assert second_result.outcome == "completed"
    assert second_result.text == "later"
    assert sum(isinstance(event, CancellationRequested) for event in root_events) == 1
    assert sum(isinstance(event, TurnCancelled) and event.turn_id == first_command.hex for event in root_events) == 1
    assert not any(isinstance(event, TurnCancelled) and event.turn_id == second_command.hex for event in root_events)
    await connection.__aexit__(None, None, None)
    await runtime.aclose()


class GatedFinishedStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.finished_committed = asyncio.Event()
        self.release_finished = asyncio.Event()
        self.finished_batch: list[SessionEvent] = []

    async def append(
        self,
        sid: str,
        events: Sequence[SessionEvent],
    ) -> int:
        result = await super().append(sid, events)
        if any(isinstance(event, AgentFinished) for event in events):
            self.finished_batch = list(events)
            self.finished_committed.set()
            await self.release_finished.wait()
        return result


class ObservedCancellationRuntime(Runtime):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.cancellation_started = asyncio.Event()

    async def _accept_cancel(self, connection: Any, command_id: UUID) -> Any:
        self.cancellation_started.set()
        return await super()._accept_cancel(connection, command_id)


async def test_cancel_racing_durable_finish_preserves_finished_terminal() -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                yield StreamEnd(tool_calls=[call("finish", '{"result":"done"}', "finish")])
                return
            yield StreamEnd(text="root")

    store = GatedFinishedStore()
    runtime = ObservedCancellationRuntime(Provider(), store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    connection = runtime.connect(root_id, writable=True)
    await connection.__aenter__()
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "work")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(store.finished_committed.wait(), timeout=2)
    cancellation = asyncio.create_task(connection.cancel(command_id=uuid4()))
    await runtime.cancellation_started.wait()
    store.release_finished.set()
    await cancellation
    await wait_idle(runtime, child_id)

    child_events = await journal(store, child_id)
    turn_id = next(event.turn_id for event in child_events if event.type == "turn_started")
    terminals = [
        event
        for event in child_events
        if isinstance(event, TurnCompleted | TurnCancelled | TurnInterrupted) and event.turn_id == turn_id
    ]
    assert terminals == [TurnCompleted(turn_id=turn_id, stop_reason="finished", output="done")]
    assert sum(isinstance(event, AgentFinished) for event in child_events) == 1
    assert isinstance(store.finished_batch[-2], TurnCompleted)
    assert isinstance(store.finished_batch[-1], AgentFinished)
    assert any(
        isinstance(event, InputQueued) and f"[agent {UUID(child_public)} finished]" in event.input
        for event in await journal(store, root.id)
    )
    await connection.__aexit__(None, None, None)
    await runtime.aclose()


class FailingFinishCleanupStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_sid: str | None = None
        self.failed = False

    async def append(
        self,
        sid: str,
        events: Sequence[SessionEvent],
    ) -> int:
        if (
            sid == self.fail_sid
            and not self.failed
            and any(isinstance(event, TurnCancelled) and event.reason == "agent_finished" for event in events)
        ):
            self.failed = True
            raise RuntimeError("finish cleanup append failed")
        return await super().append(sid, events)


async def test_failed_atomic_finish_retains_parent_delivery_without_partial_close() -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.child_started = asyncio.Event()
            self.release_child = asyncio.Event()
            self.root_calls = 0

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                self.child_started.set()
                await self.release_child.wait()
                yield StreamEnd(tool_calls=[call("finish", '{"result":"done"}', "finish")])
                return
            self.root_calls += 1
            yield StreamEnd(text="done")

    provider = Provider()
    store = FailingFinishCleanupStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "initial")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(provider.child_started.wait(), timeout=2)
    await store.enqueue(child_id, InputQueued(command_id=uuid4().hex, input="pending"))
    store.fail_sid = child_id
    provider.release_child.set()
    await wait_idle(runtime, child_id)

    child_events = await journal(store, child_id)
    parent_deliveries = [
        event
        for event in await journal(store, root.id)
        if isinstance(event, InputQueued) and ' finished] "done"' in event.input
    ]
    assert not any(isinstance(event, AgentFinished) for event in child_events)
    assert not any(
        isinstance(event, TurnCompleted | TurnCancelled) and event.turn_id == child_events[1].command_id
        for event in child_events
    )
    assert len(parent_deliveries) == 1
    assert root.id not in runtime.active

    async with runtime.connect(root_id, writable=True) as connection:
        result = await connection.prompt("continue", command_id=uuid4())
    assert result.outcome == "completed"
    assert provider.root_calls == 2
    await runtime.aclose()


class PersistentlyFailingFinishedHeaderStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.finished_patch_attempts = 0

    async def patch_header(self, sid: str, **fields: Any) -> Any:
        if fields.get("finished") is True:
            self.finished_patch_attempts += 1
            raise RuntimeError("finished header patch failed")
        return await super().patch_header(sid, **fields)


async def test_persistent_finished_header_patch_failure_does_not_strand_parent() -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.root_calls = 0

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                yield StreamEnd(tool_calls=[call("finish", '{"result":"done"}', "finish")])
                return
            self.root_calls += 1
            yield StreamEnd(text="done")

    provider = Provider()
    store = PersistentlyFailingFinishedHeaderStore()
    runtime = Runtime(provider, store, [Root], default_model="root")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "initial")
    child_id = UUID(child_public).hex
    await wait_idle(runtime, child_id, root.id)

    child_events = await journal(store, child_id)
    child = await store.header(child_id)
    parent_deliveries = [
        event
        for event in await journal(store, root.id)
        if isinstance(event, InputQueued) and ' finished] "done"' in event.input
    ]
    assert child is not None and child.finished is False
    assert store.finished_patch_attempts == 1
    assert isinstance(child_events[-2], TurnCompleted)
    assert isinstance(child_events[-1], AgentFinished)
    assert len(parent_deliveries) == 1
    assert provider.root_calls == 1
    with pytest.raises(TantraError, match="finished"):
        await runtime._actor_send(
            root,
            context(store, root.id, turn="later", cid="send"),
            UUID(child_public),
            "new",
        )
    await runtime.aclose()


async def test_post_append_hook_failure_reconciles_durable_finish() -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    class Provider(EchoProvider):
        def __init__(self) -> None:
            self.root_calls = 0

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                yield StreamEnd(tool_calls=[call("finish", '{"result":"done"}', "finish")])
                return
            self.root_calls += 1
            yield StreamEnd(text="done")

    class RaiseAfterDurableFinish(Hook):
        def __init__(self, store: MemoryStore) -> None:
            self.raised = False
            self.store = store

        async def on_event(self, emitted: LoggedEvent) -> None:
            if not self.raised and isinstance(emitted.event, TurnCompleted) and emitted.event.stop_reason == "finished":
                self.raised = True
                await self.store.patch_header(emitted.agent_id.hex, status="idle", pending_ask=None, finished=True)
                raise RuntimeError("post-append hook failed")

    provider = Provider()
    store = MemoryStore()
    hook = RaiseAfterDurableFinish(store)
    runtime = Runtime(provider, store, [Root], default_model="root", hooks=[hook])
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "initial")
    child_id = UUID(child_public).hex
    await wait_idle(runtime, child_id, root.id)

    child_events = await journal(store, child_id)
    child = await store.header(child_id)
    parent_deliveries = [
        event
        for event in await journal(store, root.id)
        if isinstance(event, InputQueued) and ' finished] "done"' in event.input
    ]
    assert hook.raised
    assert child is not None and child.finished is True
    assert isinstance(child_events[-2], TurnCompleted)
    assert isinstance(child_events[-1], AgentFinished)
    assert len(parent_deliveries) == 1
    assert provider.root_calls == 1
    assert not runtime._errors.get(child_id)
    await runtime.aclose()


async def test_finished_header_without_event_is_not_authoritative() -> None:
    class Child(Agent):
        pass

    class Root(Agent):
        subagents = [Child]

    store = MemoryStore()
    runtime = Runtime(EchoProvider(), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_public = await runtime._actor_spawn(root, Root, context(store, root.id), "child", "initial")
    child_id = UUID(child_public).hex
    await wait_idle(runtime, child_id)
    await store.patch_header(child_id, finished=True)
    orphan = uuid4()
    await store.enqueue(child_id, InputQueued(command_id=orphan.hex, input="orphan"))
    await store.append(child_id, [TurnStarted(turn_id=orphan.hex, input="orphan")])
    runtime._activations[child_id] = runtime._activations.get(child_id, 0) + 1
    runtime._activate(child_id, root.id)
    await wait_idle(runtime, child_id)

    child = await store.header(child_id)
    assert child is not None and child.finished is True
    child_events = await journal(store, child_id)
    assert not any(isinstance(event, AgentFinished) for event in child_events)
    assert not any(
        isinstance(event, TurnCompleted) and event.turn_id == orphan.hex and event.stop_reason == "finished"
        for event in child_events
    )
    assert any(
        isinstance(event, TurnInterrupted) and event.turn_id == orphan.hex and event.reason == "process_stopped"
        for event in child_events
    )

    receipt = await runtime._actor_send(
        root,
        context(store, root.id, turn="new", cid="send"),
        UUID(child_public),
        "accepted despite mirror",
    )
    result = await runtime._wait_result(child_id, UUID(receipt["command_id"]))
    assert result.outcome == "completed"
    await runtime.aclose()
