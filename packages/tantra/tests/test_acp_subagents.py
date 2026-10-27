from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from acp_support import Client, Scripted, reduce

from tantra import Agent, Runtime
from tantra.acp import AcpAccess, AcpAuth, AcpServer
from tantra.acp.jsonrpc import RpcError
from tantra.providers.base import ProviderEvent, SampleRequest, ToolCall
from tantra.providers.fake import Sample
from tantra.stores.memory import MemoryStore


class Leaf(Agent):
    model = "leaf"


class Mid(Agent):
    model = "mid"
    subagents = [Leaf]


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    subagents = [Worker, Mid]


def tool_call(tool: str, call_id: str, **args: Any) -> Sample:
    return Sample(tool_calls=[ToolCall(id=call_id, name=tool, args=json.dumps(args))])


def text(value: str) -> Sample:
    return Sample(text=value)


async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
    return {"user": "u"}


async def allow(access: AcpAccess) -> bool:
    return True


def serve(provider: Scripted) -> tuple[Runtime, AcpServer]:
    runtime = Runtime(provider, MemoryStore(), [Root])
    return runtime, AcpServer(runtime, agent=Root, scope=scope, policy=allow, name="tree")


def children(client: Client, parent: str) -> list[str]:
    return list(dict.fromkeys(update["sessionId"] for update in client.of(parent, "subagent_update")))


def mirrored(client: Client, parent: str, child: str) -> list[dict[str, Any]]:
    return [
        update["state"]
        for update in client.of(parent, "subagent_update")
        if update["sessionId"] == child and "state" in update
    ]


def announced_first(client: Client, child: str) -> bool:
    announced = next(
        index
        for index, params in enumerate(client.updates)
        if params["update"]["sessionUpdate"] == "subagent_update" and params["update"]["sessionId"] == child
    )
    traffic = [
        index
        for index, params in enumerate(client.updates)
        if params["sessionId"] == child
        or child in (params["update"].get("senderSessionId"), params["update"].get("recipientSessionId"))
    ]
    return all(index > announced for index in traffic)


def messages(client: Client, session_id: str) -> list[tuple[str, str, str]]:
    return [
        (update["senderSessionId"], update["recipientSessionId"], update["content"][0]["text"])
        for update in client.of(session_id, "session_message")
    ]


async def test_spawn_send_and_reply_flow_as_two_way_session_messages() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="worker", input="investigate", name="scout"), text("waiting")],
        worker=[],
    )
    provider.add("root", text("noted"), text("done"))
    runtime, server = serve(provider)
    async with Client(server) as client:
        root = await client.new()
        provider.add(
            "worker",
            tool_call("send", "w1", agent_id=root, input="progress"),
            text("The answer is 42."),
        )
        await client.prompt(root, "go")
        await client.idle(root, 3)
        [worker] = children(client, root)
        await client.until(lambda: mirrored(client, root, worker)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
    assert client.of(root, "subagent_update")[0] == {
        "sessionUpdate": "subagent_update",
        "sessionId": worker,
        "title": "scout",
    }
    assert announced_first(client, worker)
    assert messages(client, root) == [
        (root, worker, "investigate"),
        (worker, root, "progress"),
        (worker, root, "The answer is 42."),
    ]
    assert messages(client, worker) == [
        (root, worker, "investigate"),
        (worker, root, "progress"),
    ]
    reply_views = [
        update
        for update in client.updates
        if update["update"]["sessionUpdate"] == "session_message"
        and update["update"]["content"] == [{"type": "text", "text": "The answer is 42."}]
    ]
    assert [(view["sessionId"], view["update"]["_meta"]) for view in reply_views] == [
        (root, {"tantra": {"reply": True}})
    ]
    assert client.of(root, "tool_call_update") == client.of(worker, "tool_call_update") == []
    final = [update for update in client.of(root, "subagent_update") if "state" in update][-1]
    assert "_meta" not in final
    assert client.states(worker)[-1] == {"state": "idle", "stopReason": "end_turn"}
    await runtime.aclose()


async def test_an_idle_child_woken_by_send_mirrors_running_then_idle() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="worker", input="first"), text("spawned"), text("ack")],
        worker=[text("first done"), text("second done")],
    )
    runtime, server = serve(provider)
    async with Client(server) as client:
        root = await client.new()
        await client.prompt(root, "start")
        await client.idle(root, 2)
        [worker] = children(client, root)
        await client.until(lambda: {"state": "idle", "stopReason": "end_turn"} in mirrored(client, root, worker))
        before = len(mirrored(client, root, worker))
        provider.add("root", tool_call("send", "s2", agent_id=worker, input="again"), text("sent"), text("ack2"))
        await client.prompt(root, "wake it")
        await client.idle(root, 4)
        await client.until(lambda: len(mirrored(client, root, worker)) >= before + 2)
    assert mirrored(client, root, worker)[before:] == [
        {"state": "running"},
        {"state": "idle", "stopReason": "end_turn"},
    ]
    assert (root, worker, "again") in messages(client, root)
    assert (root, worker, "again") in messages(client, worker)
    worker_messages = [update["content"] for update in client.of(worker, "agent_message")]
    assert worker_messages == [[{"type": "text", "text": "first done"}], [{"type": "text", "text": "second done"}]]
    await runtime.aclose()


async def test_grandchildren_are_announced_on_their_parent() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="mid", input="delegate"), text("r1"), text("r2"), text("r3")],
        mid=[tool_call("spawn", "m1", agent_name="leaf", input="dig"), text("m2"), text("m3")],
        leaf=[text("leaf done")],
    )
    runtime, server = serve(provider)
    async with Client(server) as client:
        root = await client.new()
        await client.prompt(root, "go")
        await client.until(lambda: children(client, root) and children(client, children(client, root)[0]))
        [mid] = children(client, root)
        [leaf] = children(client, mid)
        await client.until(lambda: client.states(leaf)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
    assert announced_first(client, mid)
    assert announced_first(client, leaf)
    assert leaf not in children(client, root)
    assert (mid, leaf, "dig") in messages(client, mid)
    assert messages(client, leaf) == [(mid, leaf, "dig")]
    await runtime.aclose()


async def test_resume_from_start_replays_children_after_their_announcement() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="worker", input="investigate"), text("waiting"), text("done")],
        worker=[],
    )
    runtime, server = serve(provider)
    async with Client(server) as live:
        root = await live.new()
        provider.add("worker", Sample(reasoning="hmm", text="partial"))
        await live.prompt(root, "go")
        await live.idle(root, 2)
        [worker] = children(live, root)
        await live.until(lambda: live.states(worker)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
        async with Client(server) as replayed:
            await replayed.resume(root, replayFrom={"type": "start"})
            await replayed.until(lambda: replayed.of(root, "session_info_update"))
    assert announced_first(replayed, worker)
    assert reduce(replayed.updates, worker)["entities"] == reduce(live.updates, worker)["entities"]
    assert reduce(replayed.updates, worker)["order"] == reduce(live.updates, worker)["order"]
    assert reduce(replayed.updates, root)["entities"] == reduce(live.updates, root)["entities"]
    assert mirrored(replayed, root, worker)[-1] == {"state": "idle", "stopReason": "end_turn"}
    await runtime.aclose()


async def test_resume_without_replay_announces_children_before_live_traffic() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="worker", input="investigate"), text("waiting"), text("ack")],
        worker=[text("done")],
    )
    runtime, server = serve(provider)
    async with Client(server) as live:
        root = await live.new()
        await live.prompt(root, "go")
        await live.idle(root, 2)
        [worker] = children(live, root)
        async with Client(server) as later:
            await later.resume(root)
            await later.until(lambda: later.of(worker, "state_update"))
    updates = [params["update"]["sessionUpdate"] for params in later.updates]
    assert updates == ["session_info_update", "state_update", "subagent_update", "state_update"]
    assert later.of(root, "subagent_update")[0]["title"] == "worker"
    await runtime.aclose()


async def test_a_child_cannot_be_resumed_or_prompted() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="worker", input="x"), text("a"), text("b")], worker=[text("w")]
    )
    runtime, server = serve(provider)
    async with Client(server) as client:
        root = await client.new()
        await client.prompt(root, "go")
        await client.until(lambda: children(client, root))
        [worker] = children(client, root)
        with pytest.raises(RpcError) as resumed:
            await client.resume(worker)
        with pytest.raises(RpcError) as prompted:
            await client.prompt(worker, "hi")
        await client.idle(root, 2)
    assert resumed.value.code == prompted.value.code == -32602
    await runtime.aclose()


async def test_a_child_not_open_here_answers_like_an_unknown_id() -> None:
    provider = Scripted(
        root=[tool_call("spawn", "s1", agent_name="worker", input="x"), text("a"), text("b")], worker=[text("w")]
    )
    runtime = Runtime(provider, MemoryStore(), [Root])
    server = AcpServer(runtime, agent=Root, scope=scope, policy=allow, models=["root", "other"], name="tree")
    async with Client(server) as owner, Client(server) as stranger, Client(server, profile=True) as profiled:
        root = await owner.new()
        await owner.prompt(root, "go")
        await owner.until(lambda: children(owner, root))
        [worker] = children(owner, root)
        await owner.idle(root, 2)
        answers = {}
        for client in (stranger, profiled):
            for target in (worker, str(uuid4())):
                for method, params in (
                    ("session/resume", {"cwd": "/work"}),
                    ("session/delete", {}),
                    ("session/set_config_option", {"configId": "model", "type": "id", "value": "other"}),
                ):
                    with pytest.raises(RpcError) as failed:
                        await client.call(method, {"sessionId": target, **params})
                    error = failed.value
                    answers[(id(client), target == worker, method)] = (error.code, error.message, error.data)
        with pytest.raises(RpcError) as open_here:
            await owner.call("session/delete", {"sessionId": worker})
    assert {answers[key] for key in answers} == {(-32002, "Session not found", None)}
    assert open_here.value.code == -32602
    assert await runtime.store.header(UUID(worker).hex) is not None
    await runtime.aclose()


class HeldWorker(Scripted):
    def __init__(self, *, hold_root: int | None = None, **scripts: list[Sample]) -> None:
        super().__init__(**scripts)
        self.hold_root = hold_root
        self.root_calls = 0
        self.root_held = asyncio.Event()
        self.worker_held = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if req.model == "root":
            self.root_calls += 1
            if self.root_calls == self.hold_root:
                self.root_held.set()
                await asyncio.Event().wait()
        if req.model == "worker" and not self.release.is_set():
            self.worker_held.set()
            await self.release.wait()
        async for event in super().stream(req):
            yield event


async def test_a_root_only_cancel_leaves_working_children_running() -> None:
    provider = HeldWorker(
        hold_root=2,
        root=[tool_call("spawn", "s1", agent_name="worker", input="dig"), text("ack")],
        worker=[text("found")],
    )
    runtime = Runtime(provider, MemoryStore(), [Root])
    server = AcpServer(runtime, agent=Root, scope=scope, policy=allow, name="tree", cancel_descendants=False)
    async with Client(server) as client:
        root = await client.new()
        await client.prompt(root, "go")
        await asyncio.wait_for(provider.root_held.wait(), 5)
        await asyncio.wait_for(provider.worker_held.wait(), 5)
        [worker] = children(client, root)
        await client.until(lambda: mirrored(client, root, worker)[-1:] == [{"state": "running"}])
        await client.notify("session/cancel", {"sessionId": root})
        await client.idle(root)
        assert client.states(root)[-1] == {"state": "idle", "stopReason": "cancelled"}
        assert mirrored(client, root, worker)[-1] == {"state": "running"}
        provider.release.set()
        await client.idle(root, 2)
        await client.until(lambda: mirrored(client, root, worker)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
    assert {"state": "idle", "stopReason": "cancelled"} not in mirrored(client, root, worker)
    assert client.states(root)[-1] == {"state": "idle", "stopReason": "end_turn"}
    await runtime.aclose()


async def test_cancel_agent_shows_as_a_tool_call_and_stops_the_child_without_a_wake_up() -> None:
    provider = HeldWorker(root=[tool_call("spawn", "s1", agent_name="worker", input="dig"), text("spawned")], worker=[])
    runtime, server = serve(provider)
    async with Client(server) as client:
        root = await client.new()
        await client.prompt(root, "go")
        await client.idle(root)
        await asyncio.wait_for(provider.worker_held.wait(), 5)
        [worker] = children(client, root)
        provider.add("root", tool_call("cancel_agent", "k1", agent_id=worker), text("stopped"))
        await client.prompt(root, "stop it")
        await client.idle(root, 2)
        await client.until(
            lambda: mirrored(client, root, worker)[-1:] == [{"state": "idle", "stopReason": "cancelled"}]
        )
    calls = client.of(root, "tool_call_update")
    assert any(
        update.get("name") == "cancel_agent" and update.get("rawInput") == {"agent_id": worker} for update in calls
    )
    assert {"agent_id": worker, "cancelled": True} in [
        update.get("rawOutput") for update in calls if update.get("status") == "completed"
    ]
    assert provider.root_calls == 4
    assert len([state for state in client.states(root) if "stopReason" in state]) == 2
    await runtime.aclose()
