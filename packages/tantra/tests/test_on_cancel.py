from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import Agent, Runtime
from tantra.events import AgentFinished, CancellationRequested, SessionHeader, ToolCallCompleted, TurnCancelled
from tantra.hooks import Hook
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS
from tantra.stores.memory import MemoryStore
from tantra.tools import Context


class Grandchild(Agent):
    model = "grandchild"


class Child(Agent):
    model = "child"
    subagents = [Grandchild]


class Root(Agent):
    model = "root"
    subagents = [Child]


class Provider:
    def __init__(self, hold: str | None = None) -> None:
        self.hold = hold
        self.held = asyncio.Event()
        self.target = ""

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if req.model == self.hold and not self.held.is_set():
            self.held.set()
            await asyncio.Event().wait()
        if req.messages[-1].content == "stop it":
            yield StreamEnd(
                tool_calls=[ToolCall(id="k1", name="cancel_agent", args=json.dumps({"agent_id": self.target}))]
            )
            return
        yield StreamEnd(text=f"{req.model} done")


class Recorder(Hook):
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, list[UUID]]] = []
        self.fail = False

    async def on_cancel(self, root_id: UUID, agent_ids: list[UUID]) -> None:
        self.calls.append((root_id, sorted(agent_ids)))
        if self.fail:
            raise RuntimeError("hook down")


class Delivering(Hook):
    def __init__(self) -> None:
        self.runtime: Runtime | None = None
        self.delivered: list[UUID] = []

    async def on_cancel(self, root_id: UUID, agent_ids: list[UUID]) -> None:
        assert self.runtime is not None
        command = uuid4()
        await self.runtime.deliver(root_id, "stopped", command_id=command, source="sandbox")
        self.delivered.append(command)


def context(store: MemoryStore, sid: str, depth: int = 0) -> Context:
    async def emit(_: str) -> None:
        return None

    return Context(session_id=sid, turn_id="turn", call_id="call", depth=depth, deps=None, store=store, emit=emit)


async def wait_idle(runtime: Runtime, *ids: str) -> None:
    async def idle() -> None:
        while any(sid in runtime.active for sid in ids):
            await asyncio.sleep(0)

    await asyncio.wait_for(idle(), timeout=2)


async def tree(runtime: Runtime, store: MemoryStore) -> tuple[SessionHeader, UUID, UUID]:
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_id = UUID(await runtime._actor_spawn(root, Root, context(store, root.id), "child", "work"))
    child = await store.header(child_id.hex)
    assert child is not None
    grandchild_id = UUID(await runtime._actor_spawn(child, Child, context(store, child.id, 1), "grandchild", "work"))
    await wait_idle(runtime, grandchild_id.hex, child_id.hex, root.id)
    return root, child_id, grandchild_id


async def journal(store: MemoryStore, sid: str) -> list[Any]:
    return [item.event for item in await store.read_page(sid)]


async def test_tree_cancel_reports_every_agent_of_an_idle_tree_and_again_on_a_retry() -> None:
    recorder = Recorder()
    store = MemoryStore()
    runtime = Runtime(Provider(), store, [Root], hooks=[recorder])
    root, child_id, grandchild_id = await tree(runtime, store)
    await store.append(grandchild_id.hex, [AgentFinished(result="done")])
    await store.patch_header(grandchild_id.hex, status="finished", finished=True)
    command = uuid4()
    async with runtime.connect(UUID(hex=root.id), writable=True) as connection:
        first = await connection.cancel(command_id=command)
        assert recorder.calls == [(UUID(hex=root.id), sorted([UUID(hex=root.id), child_id, grandchild_id]))]
        again = await connection.cancel(command_id=command)

    assert (first.duplicate, again.duplicate) == (False, True)
    assert recorder.calls == [recorder.calls[0]] * 2
    cancellation = next(event for event in await journal(store, root.id) if isinstance(event, CancellationRequested))
    assert cancellation.targets == {}
    await runtime.aclose()


async def test_root_only_cancel_does_not_call_on_cancel() -> None:
    recorder = Recorder()
    store = MemoryStore()
    runtime = Runtime(Provider(), store, [Root], hooks=[recorder])
    root, _, _ = await tree(runtime, store)
    async with runtime.connect(UUID(hex=root.id), writable=True) as connection:
        await connection.cancel(command_id=uuid4(), descendants=False)

    assert recorder.calls == []
    await runtime.aclose()


async def test_cancel_agent_reports_an_idle_child_and_its_descendants() -> None:
    recorder = Recorder()
    provider = Provider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root], hooks=[recorder])
    root, child_id, grandchild_id = await tree(runtime, store)
    provider.target = str(child_id)
    async with runtime.connect(UUID(hex=root.id), writable=True) as connection:
        result = await asyncio.wait_for(connection.prompt("stop it", command_id=uuid4()), timeout=2)

    completed = next(
        event
        for event in await journal(store, root.id)
        if isinstance(event, ToolCallCompleted) and event.call_id == "k1"
    )
    assert result.text == "root done"
    assert completed.result == {"agent_id": str(child_id), "cancelled": False}
    assert recorder.calls == [(UUID(hex=root.id), sorted([child_id, grandchild_id]))]
    await runtime.aclose()


async def test_a_failing_hook_fails_the_cancel_keeps_it_saved_and_a_retry_runs_it_again() -> None:
    recorder = Recorder()
    store = MemoryStore()
    runtime = Runtime(Provider(), store, [Root], hooks=[recorder])
    root, _, _ = await tree(runtime, store)
    command = uuid4()
    recorder.fail = True
    async with runtime.connect(UUID(hex=root.id), writable=True) as connection:
        with pytest.raises(RuntimeError, match="hook down"):
            await connection.cancel(command_id=command)
        saved = [
            event
            for event in await journal(store, root.id)
            if isinstance(event, CancellationRequested) and event.command_id == command.hex
        ]
        recorder.fail = False
        again = await connection.cancel(command_id=command)

    assert len(saved) == 1
    assert again.duplicate is True
    assert len(recorder.calls) == 2
    await runtime.aclose()


async def test_a_failing_hook_makes_cancel_agent_a_tool_error_and_keeps_the_child_cancelled() -> None:
    recorder = Recorder()
    provider = Provider(hold="child")
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root], hooks=[recorder])
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    child_id = UUID(await runtime._actor_spawn(root, Root, context(store, root.id), "child", "work"))
    await asyncio.wait_for(provider.held.wait(), timeout=2)
    provider.target = str(child_id)
    recorder.fail = True
    async with runtime.connect(UUID(hex=root.id), writable=True) as connection:
        result = await asyncio.wait_for(connection.prompt("stop it", command_id=uuid4()), timeout=2)
    await wait_idle(runtime, child_id.hex, root.id)

    completed = next(
        event
        for event in await journal(store, root.id)
        if isinstance(event, ToolCallCompleted) and event.call_id == "k1"
    )
    cancelled = [event for event in await journal(store, child_id.hex) if isinstance(event, TurnCancelled)]
    assert result.text == "root done"
    assert completed.is_error
    assert completed.result == "hook down"
    assert [event.reason for event in cancelled] == ["cancelled_by_parent"]
    assert recorder.calls == [(UUID(hex=root.id), [child_id])]
    await runtime.aclose()


async def test_hooks_that_deliver_to_the_same_tree_complete() -> None:
    hook = Delivering()
    provider = Provider()
    store = MemoryStore()
    runtime = Runtime(provider, store, [Root], hooks=[hook])
    hook.runtime = runtime
    root, child_id, _ = await tree(runtime, store)
    provider.target = str(child_id)
    async with runtime.connect(UUID(hex=root.id), writable=True) as connection:
        await asyncio.wait_for(connection.cancel(command_id=uuid4()), timeout=2)
        result = await asyncio.wait_for(connection.prompt("stop it", command_id=uuid4()), timeout=2)
        delivered = [
            await asyncio.wait_for(runtime._wait_result(root.id, command), timeout=2) for command in hook.delivered
        ]

    completed = next(
        event
        for event in await journal(store, root.id)
        if isinstance(event, ToolCallCompleted) and event.call_id == "k1"
    )
    assert result.text == "root done"
    assert not completed.is_error
    assert [result.outcome for result in delivered] == ["completed", "completed"]
    await runtime.aclose()
