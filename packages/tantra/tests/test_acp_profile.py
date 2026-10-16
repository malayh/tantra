from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import types
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

import pytest
from acp_support import VALIDATOR, Client, Scripted, cursor_of, reduce

from tantra import Agent, Runtime, tool
from tantra.acp import AcpAccess, AcpAuth, AcpServer
from tantra.acp import agent as acp_agent
from tantra.acp.jsonrpc import RpcError
from tantra.acp.transport import Transport
from tantra.errors import CoordinatorUnavailable, LeaseLost, WriterReplaced
from tantra.events import (
    CancellationRequested,
    ChildCreated,
    CompactionApplied,
    InputQueued,
    LoggedEvent,
    ReasoningDelta,
    ToolCallCompleted,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
    Usage,
)
from tantra.providers.base import ProviderEvent, SampleRequest, TextDelta, ToolCall
from tantra.providers.fake import Sample
from tantra.stores.base import reduce_journal
from tantra.stores.memory import MemoryStore

FIXTURES = Path(__file__).parent / "fixtures" / "acp_profile"
PROPOSAL = Path(__file__).parents[3] / "website" / "src" / "content" / "docs" / "docs" / "acp" / "tantra-profile.md"
LITERAL = {
    "jsonrpc",
    "method",
    "sessionUpdate",
    "type",
    "reason",
    "code",
    "state",
    "stopReason",
    "level",
    "writer",
    "version",
    "protocolVersion",
}
LIVE_ONLY = {"_tantra_writer", "_tantra_queue_update", "_tantra_notice", "session_info_update", "config_option_update"}
RESPONSE_METHODS = {
    "initialize_response": "initialize",
    "prompt_response": "session/prompt",
    "claim_writer_response": "_tantra/session/claim_writer",
}
CLAIMED = {"sessionUpdate": "_tantra_writer", "writer": True, "reason": "claimed"}
REPLACED = {"sessionUpdate": "_tantra_writer", "writer": False, "reason": "replaced"}
LOST = {"sessionUpdate": "_tantra_writer", "writer": False, "reason": "lost"}
WRITER_REQUIRED = {"tantra": {"code": "writer_required"}}


@tool(permission="ask")
async def dangerous() -> str:
    return "ran"


class Bot(Agent):
    pass


class Guarded(Agent):
    tools = [dangerous]


class Leaf(Agent):
    model = "leaf"


class Mid(Agent):
    model = "mid"
    subagents = [Leaf]


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    tools = [dangerous]
    subagents = [Worker, Mid]


class OnceCompactor:
    def __init__(self) -> None:
        self.done = False

    async def compact(self, ctx: Any) -> list[Any]:
        if self.done:
            return []
        self.done = True
        return [CompactionApplied(strategy="t", tokens_before=90, tokens_after=10, summary="the brief")]


class GatedScripted(Scripted):
    def __init__(self, *gated: str, **scripts: list[Sample]) -> None:
        super().__init__(**scripts)
        self.gates = {model: asyncio.Event() for model in gated}

    @property
    def gate(self) -> asyncio.Event:
        return next(iter(self.gates.values()))

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        async for event in self.providers[req.model].stream(req):
            yield event
            gate = self.gates.get(req.model)
            if gate is not None and isinstance(event, TextDelta):
                await gate.wait()


class HeldTransport:
    def __init__(self, inner: Transport) -> None:
        self.inner = inner
        self.held = asyncio.Event()
        self.release = asyncio.Event()

    async def send(self, frame: str) -> None:
        if '"session_info_update"' in frame and not self.release.is_set():
            self.held.set()
            await self.release.wait()
        await self.inner.send(frame)

    async def receive(self) -> str | None:
        return await self.inner.receive()

    async def close(self) -> None:
        await self.inner.close()


class SlowTransport:
    def __init__(self, inner: Transport) -> None:
        self.inner = inner

    async def send(self, frame: str) -> None:
        await asyncio.sleep(0.01)
        await self.inner.send(frame)

    async def receive(self) -> str | None:
        return await self.inner.receive()

    async def close(self) -> None:
        await self.inner.close()


class DropResponse:
    def __init__(self, inner: Transport) -> None:
        self.inner = inner
        self.armed = False

    async def send(self, frame: str) -> None:
        if self.armed and '"result":{"messageId"' in frame:
            await self.inner.close()
            raise ConnectionError("response dropped")
        await self.inner.send(frame)

    async def receive(self) -> str | None:
        return await self.inner.receive()

    async def close(self) -> None:
        await self.inner.close()


async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
    return {"user": "u"}


async def allow(access: AcpAccess) -> bool:
    return True


async def approve(method: str, params: dict[str, Any]) -> Any:
    return {"outcome": {"outcome": "selected", "optionId": "allow"}}


def serve(provider: Any, agents: list[type[Agent]], **options: Any) -> tuple[Runtime, AcpServer]:
    runtime_options = {key: options.pop(key) for key in ("compactor",) if key in options}
    runtime = Runtime(provider, MemoryStore(), agents, default_model="m", **runtime_options)
    options.setdefault("policy", allow)
    return runtime, AcpServer(runtime, agent=agents[0], scope=scope, name="profile", **options)


def tantra(**values: Any) -> dict[str, Any]:
    return {"_meta": {"tantra": values}}


def calls(call_id: str, function: str, **args: Any) -> list[ToolCall]:
    return [ToolCall(id=call_id, name=function, args=json.dumps(args))]


def after(cursor: str | None) -> dict[str, Any]:
    return {"type": "_tantra_after", "cursor": cursor} if cursor is not None else {"type": "start"}


def cursored(client: Client, session_id: str) -> list[dict[str, Any]]:
    return [update for update in client.of(session_id) if cursor_of(update) is not None]


def position(update: dict[str, Any]) -> tuple[int, int]:
    seq, index = cursor_of(update).split(".")
    return int(seq), int(index)


def increasing(updates: list[dict[str, Any]]) -> bool:
    positions = [position(update) for update in updates]
    return all(left < right for left, right in zip(positions, positions[1:], strict=False))


def mirror(client: Client, parent: str) -> dict[str, dict[str, Any]]:
    return {
        update["sessionId"]: update
        for update in client.of(parent, "subagent_update")
        if "state" in update and cursor_of(update) is None
    }


def profile_only(client: Client) -> list[dict[str, Any]]:
    return [
        params["update"]
        for params in client.updates
        if params["update"]["sessionUpdate"].startswith("_tantra_") or cursor_of(params["update"]) is not None
    ]


def frame_index(client: Client, match: Any) -> int:
    return next(index for index, frame in enumerate(client.transport.received) if match(frame))


async def journal(runtime: Runtime, session_id: str) -> list[Any]:
    return [item.event for item in await runtime.store.read_page(UUID(session_id).hex)]


async def eventually(condition: Any, timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not await condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def settle(client: Client, session_id: str) -> None:
    await client.until(lambda: [u for u in client.of(session_id, "state_update") if cursor_of(u) is None])


async def replay(server: AcpServer, session_id: str, replay_from: dict[str, Any], root: str | None = None) -> Any:
    async with Client(server, profile=True) as client:
        if root is not None:
            await client.resume(root)
        await client.resume(session_id, replayFrom=replay_from)
        await settle(client, session_id)
    return cursored(client, session_id)


async def test_duplicate_command_ids_are_one_turn_and_reuse_conflicts() -> None:
    runtime, server = serve(Scripted(m=[Sample(text="once")]), [Bot])
    command = str(uuid4())
    async with Client(server, profile=True) as client:
        session_id = await client.new()
        first = await client.prompt(session_id, "hello", **tantra(commandId=command))
        second = await client.prompt(session_id, "hello", **tantra(commandId=command))
        await client.idle(session_id)
        with pytest.raises(RpcError) as conflict:
            await client.prompt(session_id, "different", **tantra(commandId=command))
        invalid = []
        for value in ("nope", command.upper(), UUID(command).hex, 42):
            with pytest.raises(RpcError) as rejected:
                await client.prompt(session_id, "x", **tantra(commandId=value))
            invalid.append(rejected.value.code)
    assert first == second == {"messageId": command}
    assert conflict.value.code == -32602
    assert conflict.value.data == {"tantra": {"code": "command_conflict"}}
    assert invalid == [-32602] * 4
    events = await journal(runtime, session_id)
    assert [event.turn_id for event in events if isinstance(event, TurnStarted)] == [UUID(command).hex]
    assert [message["messageId"] for message in client.of(session_id, "user_message")] == [command]
    await runtime.aclose()


async def test_a_lost_prompt_response_is_recovered_by_cursor_resume_and_retry(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="first answer"), Sample(text="second answer")])
    runtime, server = serve(provider, [Bot])
    droppers: list[DropResponse] = []

    def drop(inner: Transport) -> Transport:
        droppers.append(DropResponse(inner))
        return droppers[0]

    command = str(uuid4())
    lost = Client(server, profile=True, server_transport=drop)
    await lost.open()
    session_id = await lost.new()
    await lost.prompt(session_id, "first")
    await lost.idle(session_id)
    provider.gate.clear()
    droppers[0].armed = True
    with pytest.raises(ConnectionError):
        await lost.prompt(session_id, "second", **tantra(commandId=command))
    await lost.wait_closed()
    await lost.close()
    seen = cursored(lost, session_id)
    async with Client(server, profile=True) as retry:
        await retry.resume(session_id, replayFrom=after(cursor_of(seen[-1])), **tantra(mode="write"))
        accepted = await retry.prompt(session_id, "second", **tantra(commandId=command))
        provider.gate.set()
        await retry.until(lambda: [u for u in cursored(retry, session_id) if u.get("stopReason") == "end_turn"])
    assert accepted == {"messageId": command}
    events = await journal(runtime, session_id)
    assert [event.input for event in events if isinstance(event, TurnStarted)] == ["first", "second"]
    assert sum(isinstance(event, InputQueued) and event.command_id == UUID(command).hex for event in events) == 1
    assert seen + cursored(retry, session_id) == await replay(server, session_id, after(None))
    await runtime.aclose()


async def test_cursor_resume_at_every_cut_equals_the_uninterrupted_stream() -> None:
    provider = GatedScripted(
        "worker",
        root=[
            Sample(
                reasoning="plan the work",
                text="Starting now.",
                tool_calls=calls("c1", "dangerous"),
                usage=Usage(input_tokens=10, output_tokens=5),
            ),
            Sample(tool_calls=calls("s1", "spawn", agent_name="worker", input="dig", name="scout")),
            Sample(text="Spawned the scout.", usage=Usage(input_tokens=30)),
            Sample(text="Got the message."),
            Sample(text="Got the result."),
        ],
        worker=[],
    )
    runtime, server = serve(provider, [Root], compactor=OnceCompactor())
    async with Client(server, profile=True, responder=approve) as live, Client(server) as plain:
        root = await live.new()
        await plain.resume(root)
        provider.add(
            "worker",
            Sample(
                reasoning="digging in", text="Found it.", tool_calls=calls("w1", "send", agent_id=root, input="half")
            ),
            Sample(tool_calls=calls("w2", "finish", result={"ok": True})),
        )
        await live.prompt(root, "go")
        [worker] = await live.until(lambda: list(mirror(live, root)))
        await live.resume(worker, replayFrom=after(None))
        await settle(live, worker)
        snapshot = len(live.of(worker))
        provider.gate.set()
        await live.idle(root, 3)
        finished = {"state": "idle", "stopReason": "_tantra_finished"}
        await live.until(lambda: mirror(live, root)[worker]["state"] == finished)
        await live.until(lambda: live.states(worker)[-1:] == [finished])
        await plain.idle(root, 3)
        await plain.until(lambda: plain.states(worker)[-1:] == [finished])
        with pytest.raises(RpcError) as plain_after:
            await plain.resume(root, replayFrom=after("1.0"))
    full = cursored(live, root)
    bare = [update for update in live.of(root) if cursor_of(update) is None]
    assert [
        update
        for update in bare
        if update["sessionUpdate"] not in LIVE_ONLY
        and not (update["sessionUpdate"] == "subagent_update" and "state" in update)
    ] == [{"sessionUpdate": "state_update", "state": "idle"}]
    kinds = {update["sessionUpdate"] for update in full}
    assert {"agent_message_chunk", "agent_thought_chunk", "tool_call_update", "usage_update"} <= kinds
    assert {"compaction_update", "session_message", "subagent_update", "state_update"} <= kinds
    assert increasing(full)
    assert await replay(server, root, after(None)) == full
    for cut in range(len(full) + 1):
        resumed = await replay(server, root, after(cursor_of(full[cut - 1]) if cut else None))
        assert full[:cut] + resumed == full, cut
    child = cursored(live, worker)
    assert [update for update in live.of(worker) if cursor_of(update) is None] == [
        {"sessionUpdate": "state_update", "state": "running"}
    ]
    assert len(live.of(worker)) > snapshot and cursor_of(live.of(worker)[-1]) is not None
    assert increasing(child)
    assert await replay(server, worker, after(None), root=root) == child
    for cut in range(len(child) + 1):
        resumed = await replay(server, worker, after(cursor_of(child[cut - 1]) if cut else None), root=root)
        assert child[:cut] + resumed == child, cut
    pushed = reduce(plain.updates, worker)
    lazy = reduce([{"sessionId": worker, "update": update} for update in child], worker)
    assert lazy["entities"] == pushed["entities"]
    assert lazy["order"] == pushed["order"]
    assert lazy["states"] == pushed["states"]
    assert plain_after.value.code == -32602
    assert profile_only(plain) == []
    queued = [entry for update in live.of(root, "_tantra_queue_update") for entry in update["entries"]]
    assert [entry["content"] for entry in queued] == [[{"type": "text", "text": "go"}]]
    last_seq = (await runtime.status(UUID(root))).last_seq
    async with Client(server, profile=True) as client:
        codes = []
        for cursor in (f"{last_seq + 1}.0", "1", "a.b", "-1.0", "1.-1", "01.0", None, 5):
            with pytest.raises(RpcError) as rejected:
                await client.resume(root, replayFrom={"type": "_tantra_after", "cursor": cursor})
            codes.append(rejected.value.code)
        with pytest.raises(RpcError) as unknown:
            await client.resume(root, replayFrom={"type": "_other"})
    assert codes == [-32602] * 8
    assert unknown.value.code == -32602
    await runtime.aclose()


async def test_a_read_connection_never_claims_and_claim_writer_moves_the_asks() -> None:
    accesses: list[AcpAccess] = []

    async def record(access: AcpAccess) -> bool:
        accesses.append(access)
        return True

    provider = Scripted(m=[Sample(tool_calls=calls("c1", "dangerous")), Sample(text="done")])
    runtime, server = serve(provider, [Guarded], policy=record)
    async with (
        Client(server, AcpAuth(principal="one", submitted_by="one"), profile=True) as writer,
        Client(server, AcpAuth(principal="two", submitted_by="two"), profile=True, responder=approve) as reader,
        Client(server) as plain,
    ):
        session_id = await writer.new()
        await writer.prompt(session_id, "go")
        await writer.until(lambda: writer.requests)
        await plain.resume(session_id)
        await reader.resume(session_id)
        await reader.until(lambda: reader.of(session_id, "_tantra_queue_update"))
        errors = []
        for _ in range(2):
            with pytest.raises(RpcError) as denied:
                await reader.prompt(session_id, "mine")
            errors.append((denied.value.code, denied.value.data))
        await reader.notify("session/cancel", {"sessionId": session_id})
        notices = await reader.until(lambda: reader.of(session_id, "_tantra_notice"))
        assert reader.requests == []
        assert not [event for event in await journal(runtime, session_id) if isinstance(event, CancellationRequested)]
        assert await reader.call("_tantra/session/claim_writer", {"sessionId": session_id}) == {}
        assert await reader.call("_tantra/session/claim_writer", {"sessionId": session_id}) == {}
        await writer.until(lambda: writer.cancelled)
        await writer.until(lambda: len(writer.of(session_id, "_tantra_writer")) == 2)
        await reader.idle(session_id)
        with pytest.raises(RpcError) as plain_claim:
            await plain.call("_tantra/session/claim_writer", {"sessionId": session_id})
        await plain.idle(session_id)
    assert errors == [(-32603, WRITER_REQUIRED)] * 2
    assert notices == [
        {"sessionUpdate": "_tantra_notice", "level": "warning", "code": "writer_required", "message": "Writer required"}
    ]
    assert writer.of(session_id, "_tantra_writer") == [CLAIMED, REPLACED]
    assert reader.of(session_id, "_tantra_writer") == [CLAIMED]
    assert [method for method, _ in reader.requests] == ["session/request_permission"]
    assert reader.requests[0][1]["_meta"] == writer.requests[0][1]["_meta"]
    claim_id = next(
        frame["id"] for frame in reader.transport.sent if frame.get("method") == "_tantra/session/claim_writer"
    )
    response = frame_index(reader, lambda frame: frame.get("id") == claim_id and "result" in frame)
    claimed = frame_index(reader, lambda frame: frame.get("params", {}).get("update") == CLAIMED)
    request = frame_index(reader, lambda frame: frame.get("method") == "session/request_permission")
    assert response < claimed < request
    assert any(frame.get("method") == "$/cancel_request" for frame in writer.transport.received)
    assert reader.states(session_id)[-1] == {"state": "idle", "stopReason": "end_turn"}
    claims = [access.auth.principal for access in accesses if access.operation == "claim_writer"]
    assert claims == ["one", "two"]
    assert plain_claim.value.code == -32601
    assert profile_only(plain) == []
    await runtime.aclose()


async def test_resume_in_write_mode_claims_before_responding_and_issues_asks_after() -> None:
    runtime, server = serve(Scripted(m=[Sample(tool_calls=calls("c1", "dangerous")), Sample(text="done")]), [Guarded])
    async with Client(server, profile=True) as first, Client(server, profile=True, responder=approve) as second:
        session_id = await first.new()
        await first.prompt(session_id, "go")
        await first.until(lambda: first.requests)
        assert await second.resume(session_id, **tantra(mode="write")) == {}
        await second.idle(session_id)
        await first.until(lambda: first.cancelled)
        await first.until(lambda: len(first.of(session_id, "_tantra_writer")) == 2)
    resume_id = second.request_id("session/resume")
    response = frame_index(second, lambda frame: frame.get("id") == resume_id and "result" in frame)
    claimed = frame_index(second, lambda frame: frame.get("params", {}).get("update") == CLAIMED)
    request = frame_index(second, lambda frame: frame.get("method") == "session/request_permission")
    assert response < claimed < request
    assert first.of(session_id, "_tantra_writer") == [CLAIMED, REPLACED]
    await runtime.aclose()


async def test_a_denied_claim_fails_new_resume_and_claim_writer() -> None:
    async def no_guest_writer(access: AcpAccess) -> bool:
        return access.operation != "claim_writer" or access.auth.principal != "guest"

    runtime, server = serve(Scripted(m=[]), [Bot], policy=no_guest_writer)
    async with (
        Client(server, profile=True) as owner,
        Client(server, AcpAuth(principal="guest"), profile=True) as guest,
    ):
        session_id = await owner.new()
        with pytest.raises(RpcError) as created:
            await guest.new()
        roots = [header.id for header in await runtime.store.list(metadata={"user": "u"})]
        with pytest.raises(RpcError) as resumed:
            await guest.resume(session_id, **tantra(mode="write"))
        assert not guest.connection.sessions
        with pytest.raises(RpcError) as invalid:
            await guest.resume(session_id, **tantra(mode="admin"))
        await guest.resume(session_id)
        with pytest.raises(RpcError) as claimed:
            await guest.call("_tantra/session/claim_writer", {"sessionId": session_id})
        with pytest.raises(RpcError) as unknown:
            await guest.call("_tantra/session/claim_writer", {"sessionId": str(uuid4())})
        await owner.until(lambda: owner.of(session_id, "_tantra_writer"))
    assert created.value.code == resumed.value.code == claimed.value.code == unknown.value.code == -32002
    assert invalid.value.code == -32602
    assert owner.of(session_id, "_tantra_writer") == [CLAIMED]
    assert roots == [UUID(session_id).hex]
    await runtime.aclose()


async def test_a_lost_writer_is_released_at_once_and_reclaiming_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    stalled = types.ModuleType("asyncio")
    stalled.__getattr__ = lambda name: getattr(asyncio, name)

    async def stall(delay: float, result: Any = None) -> Any:
        await asyncio.Future()

    stalled.sleep = stall
    runtime, server = serve(Scripted(m=[Sample(text="after reclaim")]), [Bot])
    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        session = client.connection.sessions[UUID(session_id)]
        writer = session.writer
        assert writer is not None

        def lease_lost() -> None:
            raise LeaseLost("lease lost in test")

        monkeypatch.setattr(acp_agent, "asyncio", stalled)
        writer._check_iteration = lease_lost
        await runtime._notify(UUID(session_id).hex)
        await client.until(lambda: len(client.of(session_id, "_tantra_writer")) == 2)
        with pytest.raises(RpcError) as fenced:
            await client.prompt(session_id, "hi")
        assert await client.call("_tantra/session/claim_writer", {"sessionId": session_id}) == {}
        assert session.writer is not None and session.writer is not writer
        monkeypatch.setattr(acp_agent, "asyncio", asyncio)
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert fenced.value.data == WRITER_REQUIRED
    assert client.of(session_id, "_tantra_writer") == [CLAIMED, LOST, CLAIMED]
    await runtime.aclose()


async def test_queue_updates_follow_the_runtime_queue(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="one done"), Sample(text="two done"), Sample(text="three done")])
    provider.gate.clear()
    runtime, server = serve(provider, [Bot])
    async with (
        Client(server, AcpAuth(principal="a", submitted_by="a"), profile=True) as first,
        Client(server, AcpAuth(principal="b", submitted_by="b"), profile=True) as second,
        Client(server) as plain,
    ):
        session_id = await first.new()
        await plain.resume(session_id)
        await first.prompt(session_id, "one")
        await first.until(lambda: first.of(session_id, "agent_message_chunk"))
        await first.prompt(session_id, "two")
        await second.resume(session_id)
        [joined] = await second.until(lambda: second.of(session_id, "_tantra_queue_update"))
        await second.call("_tantra/session/claim_writer", {"sessionId": session_id})
        await second.prompt(session_id, "three")
        await first.until(lambda: len(first.of(session_id, "_tantra_queue_update")[-1]["entries"]) == 2)
        async with Client(server, profile=True) as third:
            await third.resume(session_id)
            [late] = await third.until(lambda: third.of(session_id, "_tantra_queue_update"))
        provider.gate.set()
        await first.idle(session_id, 3)
        await first.until(lambda: first.of(session_id, "_tantra_queue_update")[-1]["entries"] == [])
        await plain.idle(session_id, 3)
    events = await journal(runtime, session_id)

    def entries(pending: list[InputQueued]) -> list[dict[str, Any]]:
        return [
            {
                "commandId": str(UUID(hex=item.command_id)),
                "content": [{"type": "text", "text": item.input}],
                "submittedBy": item.submitted_by,
            }
            for item in pending
        ]

    expected: list[list[dict[str, Any]]] = [[]]
    for end in range(1, len(events) + 1):
        state = entries(reduce_journal(events[:end]).pending)
        if state != expected[-1]:
            expected.append(state)
    assert [update["entries"] for update in first.of(session_id, "_tantra_queue_update")] == expected
    queued = [event for event in events if isinstance(event, InputQueued)]
    assert joined["entries"] == entries(queued[1:2])
    assert late["entries"] == entries(queued[1:3])
    assert [entry["submittedBy"] for entry in late["entries"]] == ["a", "b"]
    assert all(cursor_of(update) is None for update in first.of(session_id, "_tantra_queue_update"))
    assert profile_only(plain) == []
    await runtime.aclose()


async def test_children_are_mirrored_and_delivered_only_when_resumed() -> None:
    provider = GatedScripted(
        "leaf",
        root=[
            Sample(tool_calls=calls("s1", "spawn", agent_name="mid", input="delegate", name="planner")),
            Sample(text="r1"),
            Sample(text="r2"),
            Sample(text="r3"),
        ],
        mid=[Sample(tool_calls=calls("m1", "spawn", agent_name="leaf", input="dig")), Sample(text="m2")],
        leaf=[Sample(text="leaf is done")],
    )
    provider.add("mid", Sample(text="m3"))
    runtime, server = serve(provider, [Root])
    running = {"state": "running"}
    async with Client(server, profile=True) as client, Client(server) as plain:
        root = await client.new()
        await plain.resume(root)
        await client.prompt(root, "go")
        [mid] = await client.until(lambda: list(mirror(client, root)))
        [leaf] = await client.until(lambda: [k for k, u in mirror(client, mid).items() if u["state"] == running])
        await client.until(lambda: mirror(client, root)[mid]["state"].get("stopReason") == "end_turn")
        assert [update["sessionUpdate"] for update in client.of(mid)] == ["subagent_update"] * len(client.of(mid))
        assert client.of(leaf) == []
        assert mirror(client, root)[mid]["title"] == "planner"
        async with Client(server, profile=True) as stranger:
            with pytest.raises(RpcError) as not_open:
                await stranger.resume(mid)
        with pytest.raises(RpcError) as writable:
            await client.resume(mid, **tantra(mode="write"))
        assert await client.resume(mid, replayFrom=after(None)) == {}
        await settle(client, mid)
        with pytest.raises(RpcError) as prompted:
            await client.prompt(mid, "hi")
        await client.resume(leaf, replayFrom=after(None))
        await settle(client, leaf)
        snapshot = [u for u in client.of(leaf, "state_update") if cursor_of(u) is None]
        assert await client.call("session/close", {"sessionId": leaf}) == {}
        closed = len(client.of(leaf))
        provider.gate.set()

        async def leaf_completed() -> bool:
            return any(isinstance(event, TurnCompleted) for event in await journal(runtime, leaf))

        await eventually(leaf_completed)
        await client.until(lambda: len([u for u in cursored(client, mid) if u.get("stopReason") == "end_turn"]) == 2)
        await client.until(lambda: mirror(client, mid)[leaf]["state"].get("stopReason") == "end_turn")
        await client.idle(root, 3)
        await plain.idle(root, 3)
        await plain.until(lambda: plain.states(mid)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
        await plain.until(lambda: plain.states(leaf)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
        async with Client(server, profile=True) as late:
            await late.resume(root)
            await late.until(lambda: mid in mirror(late, root) and leaf in mirror(late, mid))
        await client.call("session/close", {"sessionId": root})
        with pytest.raises(RpcError) as root_closed:
            await client.resume(mid)
    assert not_open.value.code == root_closed.value.code == -32002
    assert writable.value.code == prompted.value.code == -32602
    assert snapshot == [{"sessionUpdate": "state_update", "state": "running"}]
    assert client.of(leaf)[closed:] == []
    replayed_leaf = [update for update in client.of(leaf) if update not in snapshot]
    assert replayed_leaf and all(cursor_of(update) is not None for update in replayed_leaf)
    mid_stream = [update for update in client.of(mid) if update["sessionUpdate"] != "subagent_update"]
    assert increasing(cursored(client, mid))
    assert all(cursor_of(update) is not None for update in mid_stream if update["sessionUpdate"] != "state_update")
    assert reduce(client.updates, mid)["entities"] == reduce(plain.updates, mid)["entities"]
    order = [(params["sessionId"], params["update"].get("sessionId")) for params in late.updates]
    assert order.index((root, mid)) < order.index((mid, leaf))
    assert profile_only(plain) == []
    await runtime.aclose()


async def test_a_child_resume_checks_the_root_and_policy_before_the_mode() -> None:
    async def owner_only_children(access: AcpAccess) -> bool:
        return access.actor.id == access.root.id or access.auth.principal != "guest"

    provider = Scripted(
        root=[Sample(tool_calls=calls("s1", "spawn", agent_name="worker", input="x")), Sample(text="a")],
        worker=[Sample(text="w")],
    )
    runtime, server = serve(provider, [Root], policy=owner_only_children)
    async with (
        Client(server, profile=True) as owner,
        Client(server, AcpAuth(principal="guest"), profile=True) as guest,
        Client(server, profile=True) as stranger,
    ):
        root = await owner.new()
        await owner.prompt(root, "go")
        [worker] = await owner.until(lambda: list(mirror(owner, root)))
        await owner.idle(root)
        await guest.resume(root)
        answers = []
        for client in (guest, stranger):
            for target in (worker, str(uuid4())):
                for mode in ("write", "admin"):
                    with pytest.raises(RpcError) as failed:
                        await client.resume(target, **tantra(mode=mode))
                    answers.append((failed.value.code, failed.value.message, failed.value.data))
        with pytest.raises(RpcError) as writable:
            await owner.resume(worker, **tantra(mode="write"))
    assert set(answers) == {(-32002, "Session not found", None)}
    assert writable.value.code == -32602
    await runtime.aclose()


async def test_only_other_profile_connections_are_told_about_a_deletion() -> None:
    provider = GatedScripted(
        "worker",
        root=[spawning(("worker", "scout")), Sample(text="spawned")],
        worker=[Sample(text="still digging")],
    )
    runtime, server = serve(provider, [Root])
    async with (
        Client(server, profile=True) as deleter,
        Client(server, profile=True, server_transport=SlowTransport) as watcher,
        Client(server, profile=True) as bystander,
        Client(server) as plain,
    ):
        session_id = await deleter.new()
        kept = await bystander.new()
        await deleter.prompt(session_id, "go")
        [worker] = await deleter.until(
            lambda: [k for k, u in mirror(deleter, session_id).items() if u["state"] == {"state": "running"}]
        )
        for client in (watcher, bystander, plain):
            await client.resume(session_id)
            await settle(client, session_id)
        await watcher.resume(worker, replayFrom=after(None))
        await settle(watcher, worker)
        finish = runtime._finish_deletion

        async def slow_finish(*args: Any) -> None:
            await finish(*args)
            await asyncio.sleep(0.05)

        runtime._finish_deletion = slow_finish
        assert await deleter.call("session/delete", {"sessionId": session_id}) == {}

        async def stopped() -> bool:
            clients = (deleter, watcher, bystander, plain)
            return all(UUID(session_id) not in client.connection.sessions for client in clients)

        await eventually(stopped)
        await asyncio.sleep(0.1)
        assert list(bystander.connection.sessions) == [UUID(kept)]
        assert not watcher.connection.sessions
    notice = {
        "sessionUpdate": "_tantra_notice",
        "level": "warning",
        "code": "session_deleted",
        "message": "Session deleted",
    }
    assert watcher.of(session_id, "_tantra_notice") == bystander.of(session_id, "_tantra_notice") == [notice]
    assert [update for update in profile_only(watcher) if update["sessionUpdate"] == "_tantra_notice"] == [notice]
    assert [update for update in profile_only(deleter) if update["sessionUpdate"] == "_tantra_notice"] == []
    assert profile_only(plain) == []
    await runtime.aclose()


async def test_a_lost_session_is_reported_deleted_only_while_open_and_its_root_is_gone() -> None:
    runtime, server = serve(Scripted(m=[]), [Bot])
    watch_tree = runtime.watch_tree
    triggers: dict[str, asyncio.Event] = {}

    async def losing(root_id: UUID) -> Any:
        tree = watch_tree(root_id)
        try:
            yield await anext(tree)
            await triggers.setdefault(str(root_id), asyncio.Event()).wait()
            raise acp_agent.SessionNotFound(root_id.hex)
        finally:
            await tree.aclose()

    header = runtime.store.header
    gated: set[str] = set()
    entered, release = asyncio.Event(), asyncio.Event()

    async def gone(actor: str) -> Any:
        if actor in gated:
            entered.set()
            await release.wait()
            return None
        return await header(actor)

    runtime.watch_tree = losing
    runtime.store.header = gone
    async with Client(server, profile=True) as owner, Client(server, profile=True) as client:
        kept, closed = await owner.new(), await owner.new()
        for session_id in (kept, closed):
            triggers[session_id] = asyncio.Event()
            await client.resume(session_id)
            await settle(client, session_id)
        triggers[kept].set()

        async def lost() -> bool:
            return UUID(kept) not in client.connection.sessions

        await eventually(lost)
        gated.add(UUID(closed).hex)
        triggers[closed].set()
        await asyncio.wait_for(entered.wait(), timeout=2)
        assert await client.call("session/close", {"sessionId": closed}) == {}
        release.set()
        await asyncio.sleep(0.05)
        gated.clear()
        assert list(client.connection.sessions) == []
    assert [update for update in profile_only(client) if update["sessionUpdate"] == "_tantra_notice"] == []
    assert await runtime.store.header(UUID(kept).hex) is not None
    await runtime.aclose()


async def test_a_deletion_while_a_resume_replays_fails_it_without_a_notice() -> None:
    runtime, server = serve(Scripted(m=[]), [Bot])
    status = runtime.status

    async def deleting(agent_id: UUID) -> Any:
        found = await status(agent_id)
        await runtime.delete(agent_id, allow_active=True)
        return found

    async with Client(server, profile=True) as owner, Client(server, profile=True) as client:
        session_id = await owner.new()
        runtime.status = deleting
        with pytest.raises(RpcError) as failed:
            await client.resume(session_id)
        del runtime.status
        await owner.until(lambda: owner.of(session_id, "_tantra_notice"))
        await asyncio.sleep(0.05)
        assert not client.connection.sessions
    assert (failed.value.code, failed.value.message, failed.value.data) == (-32002, "Session not found", None)
    assert client.of(session_id) == []
    assert [update["code"] for update in owner.of(session_id, "_tantra_notice")] == ["session_deleted"]
    await runtime.aclose()


async def test_lazily_resumed_children_count_toward_the_session_limit() -> None:
    provider = Scripted(
        root=[
            Sample(tool_calls=calls("s1", "spawn", agent_name="worker", input="x")),
            Sample(text="a"),
            Sample(text="b"),
        ],
        worker=[Sample(text="w")],
    )
    runtime, server = serve(provider, [Root])
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")
        [worker] = await client.until(lambda: list(mirror(client, root)))
        for _ in range(6):
            await client.new()
        await client.resume(worker)
        with pytest.raises(RpcError) as full:
            await client.new()
        await client.resume(worker)
        await client.idle(root, 2)
        await client.call("session/close", {"sessionId": root})
        assert all(UUID(worker) not in session.tails for session in client.connection.sessions.values())
        with pytest.raises(RpcError) as orphaned:
            await client.resume(worker)
        await client.new()
    assert full.value.code == -32602
    assert full.value.message == "Too many open sessions"
    assert orphaned.value.code == -32002
    await runtime.aclose()


async def test_a_plain_client_ignores_profile_inputs() -> None:
    provider = Scripted(m=[Sample(tool_calls=calls("c1", "dangerous")), Sample(text="one"), Sample(text="two")])
    runtime, server = serve(provider, [Guarded])
    command = str(uuid4())
    async with Client(server, responder=approve) as plain, Client(server) as other:
        session_id = await plain.new(**tantra(mode="write"))
        assert plain.connection.sessions[UUID(session_id)].writer is None
        first = await plain.prompt(session_id, "one", **tantra(commandId=command))
        second = await plain.prompt(session_id, "two", **tantra(commandId=command))
        await plain.idle(session_id, 2)
        await other.resume(session_id, **tantra(mode="write"))
        assert other.connection.sessions[UUID(session_id)].writer is None
        with pytest.raises(RpcError) as cursor:
            await other.resume(session_id, replayFrom=after("1.0"))
    assert len({first["messageId"], second["messageId"], command}) == 3
    assert cursor.value.code == -32602
    assert profile_only(plain) == profile_only(other) == []
    await runtime.aclose()


def shape(value: Any, key: str | None = None, parent: str | None = None) -> Any:
    if key in LITERAL and not (key == "version" and parent == "info"):
        return value
    if isinstance(value, dict):
        return {name: shape(item, name, key) for name, item in value.items()}
    if isinstance(value, list):
        return [shape(item, None, key) for item in value]
    return type(value).__name__


async def captured_frames() -> list[Any]:
    provider = GatedScripted(
        "worker",
        root=[
            Sample(tool_calls=calls("s1", "spawn", agent_name="worker", input="dig", name="scout")),
            Sample(text="Spawned the scout."),
            Sample(tool_calls=calls("c1", "dangerous")),
        ],
        worker=[Sample(text="digging deeper")],
    )
    runtime, server = serve(provider, [Root])
    command = str(uuid4())
    clients: list[Client] = []
    async with (
        Client(server, profile=True) as first,
        Client(server, profile=True) as second,
        Client(server, profile=True) as third,
        Client(server, profile=True) as fourth,
    ):
        clients.extend((first, second, third, fourth))
        root = await first.new()
        await first.prompt(root, "Summarize the open issues.", **tantra(commandId=command))
        [worker] = await first.until(
            lambda: [k for k, u in mirror(first, root).items() if u["state"]["state"] == "running"]
        )
        await first.idle(root)
        with pytest.raises(RpcError):
            await first.prompt(root, "Something else.", **tantra(commandId=command))
        await first.prompt(root, "Check it.")
        await first.until(lambda: first.requests)
        await first.prompt(root, "Then draft a reply.")
        await first.until(lambda: [u for u in first.of(root, "_tantra_queue_update") if len(u["entries"]) == 1])
        await second.resume(root)
        await settle(second, root)
        with pytest.raises(RpcError):
            await second.prompt(root, "Mine.")
        await second.notify("session/cancel", {"sessionId": root, **tantra(commandId=str(uuid4()))})
        await second.until(lambda: second.of(root, "_tantra_notice"))
        await second.call("_tantra/session/claim_writer", {"sessionId": root})
        await first.until(lambda: REPLACED in first.of(root, "_tantra_writer"))
        await second.notify("session/cancel", {"sessionId": root, **tantra(commandId=command)})
        await second.until(lambda: len(second.of(root, "_tantra_notice")) == 2)
        await third.resume(root, replayFrom=after(cursor_of(cursored(first, root)[-1])))
        await third.resume(worker, replayFrom=after(None))
        await settle(third, worker)
        await fourth.resume(root, **tantra(mode="write"))
        await second.until(lambda: REPLACED in second.of(root, "_tantra_writer"))
        writer = fourth.connection.sessions[UUID(root)].writer
        assert writer is not None

        def lease_lost() -> None:
            raise LeaseLost("lease lost in test")

        writer._check_iteration = lease_lost
        await runtime._notify(UUID(root).hex)
        await fourth.until(lambda: LOST in fourth.of(root, "_tantra_writer"))
        deleted = await second.new()
        await third.resume(deleted)
        await second.call("session/delete", {"sessionId": deleted})
        await third.until(lambda: third.of(deleted, "_tantra_notice"))
        provider.gate.set()
    await runtime.aclose()
    return [frame for client in clients for frame in (*client.transport.sent, *client.transport.received)]


async def test_profile_fixtures_are_valid_and_match_real_messages() -> None:
    fixtures = {path.stem: json.loads(path.read_text()) for path in sorted(FIXTURES.glob("*.json"))}
    assert len(fixtures) == 21
    frames = await captured_frames()
    shapes = [shape(frame) for frame in frames]
    for name, fixture in fixtures.items():
        methods = {fixture.get("id"): RESPONSE_METHODS[name]} if name in RESPONSE_METHODS else {}
        assert VALIDATOR.frame_errors(fixture, methods) == [], name
        assert shape(fixture) in shapes, name


def test_the_proposal_examples_are_the_profile_fixtures() -> None:
    fixtures = {path.read_text().strip(): path.stem for path in FIXTURES.glob("*.json")}
    blocks = re.findall(r"^[ \t]*```json\b[^\n]*\n(.*?)^[ \t]*```", PROPOSAL.read_text(), re.M | re.S)
    used = [fixtures.get(block.strip()) for block in blocks]
    assert None not in used
    assert set(used) == set(fixtures.values())


def spawning(*children: tuple[str, str]) -> Sample:
    return Sample(
        tool_calls=[
            ToolCall(id=f"s{index}", name="spawn", args=json.dumps({"agent_name": agent, "input": "go", "name": name}))
            for index, (agent, name) in enumerate(children)
        ]
    )


async def test_the_mirror_redacts_names_and_errors_through_view() -> None:
    viewed: list[LoggedEvent] = []

    async def redact(item: LoggedEvent) -> LoggedEvent:
        viewed.append(item)
        if isinstance(item.event, ChildCreated):
            item.event.name = "redacted"
        if isinstance(item.event, TurnFailed):
            item.event.error = "redacted"
        return item

    provider = GatedScripted(
        "worker",
        root=[spawning(("worker", "secret-scout")), Sample(text="spawned"), Sample(text="noted")],
        worker=[Sample(text="digging", tool_calls=calls("x1", "missing"))],
    )
    runtime, server = serve(provider, [Root], view=redact)
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")
        [worker] = await client.until(lambda: list(mirror(client, root)))
        await client.until(lambda: mirror(client, root)[worker]["state"] == {"state": "running"})
        provider.gate.set()
        await client.until(lambda: mirror(client, root)[worker]["state"].get("stopReason") == "error")
        await client.idle(root, 2)
    failed = next(event for event in await journal(runtime, worker) if isinstance(event, TurnFailed))
    received = json.dumps(client.transport.received)
    assert mirror(client, root)[worker] == {
        "sessionUpdate": "subagent_update",
        "sessionId": worker,
        "title": "redacted",
        "state": {"state": "idle", "stopReason": "error", "error": {"code": -32603, "message": "redacted"}},
    }
    assert "secret-scout" not in received
    assert failed.error and failed.error not in received
    synthetic = [item for item in viewed if isinstance(item.event, ChildCreated) and item.event.turn_id == ""]
    failures = [item for item in viewed if isinstance(item.event, TurnFailed)]
    assert 1 <= len(synthetic) <= 3
    assert [(item.agent_id, item.event.turn_id) for item in failures] == [(UUID(worker), failed.turn_id)]
    await runtime.aclose()


async def test_a_view_that_hides_a_child_hides_its_subtree_from_the_mirror() -> None:
    async def hide_mid(item: LoggedEvent) -> LoggedEvent | None:
        if isinstance(item.event, ChildCreated) and item.event.agent == "mid":
            return None
        return item

    provider = Scripted(
        root=[spawning(("mid", "planner"), ("worker", "scout")), *[Sample(text=f"r{n}") for n in range(8)]],
        mid=[Sample(tool_calls=calls("m1", "spawn", agent_name="leaf", input="dig")), Sample(text="m2")],
        leaf=[Sample(text="leaf done")],
        worker=[Sample(text="worker done")],
    )
    provider.add("mid", Sample(text="m3"))
    runtime, server = serve(provider, [Root], view=hide_mid)
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")

        async def settled() -> bool:
            statuses = await runtime.tree_status(UUID(root))
            pending = reduce_journal(await journal(runtime, root))
            return len(statuses) == 4 and all(s.state == "idle" for s in statuses) and not pending.pending

        await eventually(settled)
        [worker] = await client.until(lambda: list(mirror(client, root)))
        await asyncio.sleep(0.2)
        statuses = {status.agent: str(status.agent_id) for status in await runtime.tree_status(UUID(root))}
    hidden = {statuses["mid"], statuses["leaf"]}
    assert worker == statuses["worker"]
    assert set(mirror(client, root)) == {worker}
    assert [params for params in client.updates if params["sessionId"] in hidden] == []
    assert [params for params in client.updates if params["update"].get("sessionId") in hidden] == []
    await runtime.aclose()


async def test_a_hidden_child_leaks_no_lifecycle_input_and_cuts_still_hold() -> None:
    async def hide_children(item: LoggedEvent) -> LoggedEvent | None:
        return None if isinstance(item.event, ChildCreated) else item

    provider = GatedScripted(
        "worker", root=[], worker=[], mid=[Sample(text="digging", tool_calls=calls("m1", "missing"))]
    )
    runtime, server = serve(provider, [Root], view=hide_children)
    command = uuid4()
    async with Client(server, profile=True) as live:
        root = await live.new()
        worker = str(uuid5(UUID(root), f"{command.hex}:s0:spawn"))
        provider.add(
            "root",
            spawning(("worker", "scout"), ("mid", "planner")),
            Sample(
                tool_calls=[
                    *calls("t1", "send", agent_id=worker, input="secret-send"),
                    *calls("t2", "send", agent_id=worker),
                    *calls("t3", "status", agent_id=worker),
                ]
            ),
            *[Sample(text=f"r{n}") for n in range(6)],
        )
        provider.add(
            "worker",
            Sample(text="working", tool_calls=calls("w1", "send", agent_id=root, input="secret-message")),
            Sample(tool_calls=calls("w2", "finish", result="secret-result")),
        )
        await live.prompt(root, "go", **tantra(commandId=str(command)))

        async def called() -> bool:
            completed = [event for event in await journal(runtime, root) if isinstance(event, ToolCallCompleted)]
            return {event.call_id for event in completed} >= {"t1", "t2", "t3"}

        await eventually(called)
        provider.gate.set()

        async def settled() -> bool:
            events = await journal(runtime, root)
            started = [event for event in events if isinstance(event, TurnStarted)]
            ended = [event for event in events if isinstance(event, TurnCompleted | TurnFailed)]
            return len(started) == len(ended) == 4 and not reduce_journal(events).pending

        await eventually(settled)
        await live.idle(root, 4)
    events = await journal(runtime, root)
    lifecycle = [event.input for event in events if isinstance(event, TurnStarted) and event.input != "go"]
    origins = {event.origin for event in events if isinstance(event, InputQueued)}
    children = [str(status.agent_id) for status in await runtime.tree_status(UUID(root)) if status.parent_id]
    outcomes = {event.call_id: event.is_error for event in events if isinstance(event, ToolCallCompleted)}
    received = json.dumps(live.transport.received)
    assert len(lifecycle) == 3 and len(children) == 2 and worker in children
    assert {"message", "finished", "turn_ended"} <= origins
    assert {key: outcomes[key] for key in ("t1", "t2", "t3")} == {"t1": False, "t2": True, "t3": False}
    secrets = [*lifecycle, *children, "secret-message", "secret-result", "secret-send", "scout", "planner"]
    assert [text for text in secrets if text in received] == []
    assert live.of(root, "tool_call_update") == live.of(root, "session_message") == []
    go = [{"type": "text", "text": "go"}]
    assert [update["content"] for update in live.of(root, "user_message")] == [go]
    assert [entry["content"] for update in live.of(root, "_tantra_queue_update") for entry in update["entries"]] == [go]
    full = cursored(live, root)
    assert increasing(full)
    for cut in range(len(full) + 1):
        resumed = await replay(server, root, after(cursor_of(full[cut - 1]) if cut else None))
        assert full[:cut] + resumed == full, cut
    await runtime.aclose()


async def test_a_hidden_child_resumes_like_an_unknown_id() -> None:
    async def hide_mid(item: LoggedEvent) -> LoggedEvent | None:
        if isinstance(item.event, ChildCreated) and item.event.agent == "mid":
            return None
        return item

    provider = Scripted(
        root=[spawning(("mid", "planner"), ("worker", "scout")), *[Sample(text=f"r{n}") for n in range(8)]],
        mid=[Sample(tool_calls=calls("m1", "spawn", agent_name="leaf", input="dig")), Sample(text="m2")],
        leaf=[Sample(text="leaf done")],
        worker=[Sample(text="worker done")],
    )
    provider.add("mid", Sample(text="m3"))
    runtime, server = serve(provider, [Root], view=hide_mid)
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")

        async def spawned() -> bool:
            return len(await runtime.tree_status(UUID(root))) == 4

        await eventually(spawned)
        statuses = {status.agent: str(status.agent_id) for status in await runtime.tree_status(UUID(root))}
        answers = []
        for target in (statuses["mid"], statuses["leaf"], str(uuid4())):
            with pytest.raises(RpcError) as failed:
                await client.resume(target, replayFrom=after(None))
            answers.append((failed.value.code, failed.value.message, failed.value.data))
        assert await client.resume(statuses["worker"]) == {}
        assert list(client.connection.sessions[UUID(root)].tails) == [UUID(root), UUID(statuses["worker"])]
    assert answers == [(-32002, "Session not found", None)] * 3
    await runtime.aclose()


async def test_fencing_before_go_live_sends_no_writer_update() -> None:
    runtime, server = serve(Scripted(m=[]), [Bot])
    held: list[HeldTransport] = []

    def hold(inner: Transport) -> Transport:
        held.append(HeldTransport(inner))
        return held[0]

    async with Client(server, profile=True) as other:
        session_id = await other.new(**tantra(mode="read"))
        async with Client(server, profile=True, server_transport=hold) as client:
            await client.resume(session_id, **tantra(mode="write"))
            await asyncio.wait_for(held[0].held.wait(), timeout=2)
            session = client.connection.sessions[UUID(session_id)]
            assert await other.call("_tantra/session/claim_writer", {"sessionId": session_id}) == {}

            async def fenced() -> bool:
                return session.writer is None

            await eventually(fenced)
            held[0].release.set()

            async def live() -> bool:
                return session.live.is_set()

            await eventually(live)
            await client.call("session/list", {})
        await other.until(lambda: other.of(session_id, "_tantra_writer"))
    assert client.of(session_id, "_tantra_writer") == []
    assert other.of(session_id, "_tantra_writer") == [CLAIMED]
    await runtime.aclose()


async def test_the_mirror_follows_the_tree_watch_without_tree_status() -> None:
    async def unused(root_id: UUID) -> Any:
        raise AssertionError("tree_status was polled")

    provider = GatedScripted(
        "root",
        "worker",
        root=[spawning(("worker", "scout")), Sample(text="spawned"), Sample(text="noted")],
        worker=[Sample(text="dug in")],
    )
    runtime = Runtime(provider, MemoryStore(), [Root])
    runtime.tree_status = unused
    server = AcpServer(runtime, agent=Root, scope=scope, policy=allow, name="profile")
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")
        [worker] = await client.until(lambda: list(mirror(client, root)))
        await client.until(lambda: mirror(client, root)[worker]["state"] == {"state": "running"})
        provider.gates["worker"].set()
        await client.until(lambda: mirror(client, root)[worker]["state"] == {"state": "idle", "stopReason": "end_turn"})
        root_state = client.states(root)[-1]
        provider.gates["root"].set()
        await client.idle(root, 2)
    assert root_state == {"state": "running"}
    await runtime.aclose()


async def test_re_resuming_an_open_root_replaces_it_and_follows_the_new_mode() -> None:
    provider = Scripted(
        root=[spawning(("worker", "scout")), Sample(text="a"), Sample(text="b"), Sample(text="c")],
        worker=[Sample(text="w")],
    )
    runtime, server = serve(provider, [Root])
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")
        [worker] = await client.until(lambda: list(mirror(client, root)))
        await client.idle(root, 2)
        await client.resume(worker)
        await settle(client, worker)
        snapshots = len(client.of(root, "_tantra_queue_update"))
        await client.resume(root)
        await client.until(lambda: len(client.of(root, "_tantra_queue_update")) > snapshots)
        read = client.connection.sessions[UUID(root)]
        assert read.writer is None and list(read.tails) == [UUID(root)]
        assert not runtime._connections
        with pytest.raises(RpcError) as dropped:
            await client.call("session/close", {"sessionId": worker})
        await client.resume(root, **tantra(mode="write"))
        await client.until(lambda: len(client.of(root, "_tantra_writer")) == 2)
        written = client.connection.sessions[UUID(root)]
        assert written.writer is not None and list(runtime._connections) == [written.writer._connection_id]
        running = {task for task in client.connection.tasks if not task.done()}
        assert running == {written.tails[UUID(root)].task, written.tree}
        await client.prompt(root, "again")
        await client.idle(root, 3)
    assert dropped.value.code == -32002
    assert client.of(root, "_tantra_writer") == [CLAIMED, CLAIMED]
    await runtime.aclose()


async def test_a_rejected_cursor_leaves_open_sessions_working() -> None:
    provider = GatedScripted(
        "worker",
        root=[spawning(("worker", "scout")), Sample(text="spawned"), Sample(text="noted")],
        worker=[Sample(text="working hard")],
    )
    runtime, server = serve(provider, [Root])
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")
        [worker] = await client.until(lambda: list(mirror(client, root)))
        await client.resume(worker, replayFrom=after(None))
        await settle(client, worker)
        codes = []
        for session_id in (root, worker):
            last_seq = (await runtime.status(UUID(session_id))).last_seq
            for cursor in (f"{last_seq + 1000}.0", "bad"):
                with pytest.raises(RpcError) as rejected:
                    await client.resume(session_id, replayFrom=after(cursor))
                codes.append(rejected.value.code)
        session = client.connection.sessions[UUID(root)]
        assert UUID(worker) in session.tails and session.writer is not None
        provider.gate.set()
        await client.until(lambda: client.states(worker)[-1:] == [{"state": "idle", "stopReason": "end_turn"}])
        await client.idle(root, 2)
    assert codes == [-32602] * 4
    assert client.of(root, "_tantra_writer") == [CLAIMED]
    await runtime.aclose()


async def test_only_an_integer_version_one_negotiates_the_profile() -> None:
    runtime, server = serve(Scripted(m=[]), [Bot])
    negotiated = []
    for version in (1, True, 1.0, "1", 2, None):
        async with Client(server, capabilities={"_meta": {"tantra": {"version": version}}}) as client:
            negotiated.append(client.connection.profile)
    assert negotiated == [True, False, False, False, False, False]
    await runtime.aclose()


async def test_cancel_failures_are_noticed_or_logged(gated_provider: Any, caplog: pytest.LogCaptureFixture) -> None:
    provider = gated_provider([Sample(text="slow answer")])
    provider.gate.clear()
    runtime, server = serve(provider, [Bot])
    attempts: list[dict[str, Any]] = []

    async def unavailable(**kwargs: Any) -> Any:
        attempts.append(kwargs)
        raise CoordinatorUnavailable("coordinator down")

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.prompt(session_id, "slow")
        await client.until(lambda: client.of(session_id, "agent_message_chunk"))
        session = client.connection.sessions[UUID(session_id)]
        assert session.writer is not None
        session.writer.cancel = unavailable
        await client.notify("session/cancel", {"sessionId": session_id, **tantra(commandId=str(uuid4()))})
        notices = await client.until(lambda: client.of(session_id, "_tantra_notice"))
        with caplog.at_level(logging.WARNING, logger="tantra.acp.agent"):
            await client.notify("session/cancel", {"sessionId": session_id, **tantra(commandId="nope")})

            async def logged() -> bool:
                return "invalid commandId" in caplog.text

            await eventually(logged)
        provider.gate.set()
        await client.idle(session_id)
    assert notices == [
        {"sessionUpdate": "_tantra_notice", "level": "warning", "code": "unavailable", "message": "Service unavailable"}
    ]
    assert len(client.of(session_id, "_tantra_notice")) == 1
    assert len(attempts) == acp_agent.SEND_ATTEMPTS
    assert len({attempt["command_id"] for attempt in attempts}) == 1
    assert not [event for event in await journal(runtime, session_id) if isinstance(event, CancellationRequested)]
    await runtime.aclose()


async def test_an_input_cancelled_while_queued_leaves_the_queue(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="slow one")])
    provider.gate.clear()
    runtime, server = serve(provider, [Bot])
    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.prompt(session_id, "one")
        await client.until(lambda: client.of(session_id, "agent_message_chunk"))
        two = await client.prompt(session_id, "two")
        await client.until(lambda: len(client.of(session_id, "_tantra_queue_update")[-1]["entries"]) == 1)
        await client.notify("session/cancel", {"sessionId": session_id})
        await client.idle(session_id)
        await client.until(lambda: client.of(session_id, "_tantra_queue_update")[-1]["entries"] == [])
        provider.gate.set()
    entries = [
        [entry["content"][0]["text"] for entry in u["entries"]] for u in client.of(session_id, "_tantra_queue_update")
    ]
    events = await journal(runtime, session_id)
    command = UUID(two["messageId"]).hex
    assert entries == [[], ["one"], [], ["two"], []]
    assert [event.input for event in events if isinstance(event, TurnStarted)] == ["one"]
    assert any(isinstance(event, TurnCancelled) and event.turn_id == command for event in events)
    await runtime.aclose()


async def test_hidden_events_leave_cursor_gaps_and_cuts_still_hold() -> None:
    async def hide_reasoning(item: LoggedEvent) -> LoggedEvent | None:
        return None if isinstance(item.event, ReasoningDelta) else item

    provider = Scripted(
        m=[Sample(reasoning="think it over", text="The answer is here."), Sample(text="Second answer now.")]
    )
    runtime, server = serve(provider, [Bot], view=hide_reasoning)
    async with Client(server, profile=True) as live:
        session_id = await live.new()
        await live.prompt(session_id, "one")
        await live.idle(session_id)
        await live.prompt(session_id, "two")
        await live.idle(session_id, 2)
    full = cursored(live, session_id)
    hidden = {
        item.seq
        for item in await runtime.store.read_page(UUID(session_id).hex)
        if isinstance(item.event, ReasoningDelta)
    }
    assert hidden and hidden.isdisjoint(position(update)[0] for update in full)
    assert "agent_thought_chunk" not in {update["sessionUpdate"] for update in full}
    assert increasing(full)
    for cut in range(len(full) + 1):
        resumed = await replay(server, session_id, after(cursor_of(full[cut - 1]) if cut else None))
        assert full[:cut] + resumed == full, cut
    await runtime.aclose()


async def test_a_fenced_plain_client_never_receives_profile_updates() -> None:
    provider = Scripted(m=[Sample(text="one"), Sample(text="two"), Sample(text="three")])
    runtime, server = serve(provider, [Bot])
    async with Client(server) as plain, Client(server, profile=True) as profiled:
        session_id = await plain.new()
        await plain.prompt(session_id, "one")
        await plain.idle(session_id)
        await profiled.resume(session_id, **tantra(mode="write"))
        await profiled.until(lambda: profiled.of(session_id, "_tantra_writer"))
        await profiled.prompt(session_id, "two")
        await plain.idle(session_id, 2)
        with pytest.raises(RpcError) as fenced:
            await plain.prompt(session_id, "three")
        await plain.prompt(session_id, "three")
        await plain.idle(session_id, 3)
        await profiled.until(lambda: len(profiled.of(session_id, "_tantra_writer")) == 2)
        await plain.notify("session/cancel", {"sessionId": session_id})
        await plain.call("session/list", {})
    assert fenced.value.data == WRITER_REQUIRED
    assert profile_only(plain) == []
    assert profiled.of(session_id, "_tantra_writer") == [CLAIMED, REPLACED]
    await runtime.aclose()


async def test_the_mirror_reads_the_tree_only_when_it_changes() -> None:
    provider = GatedScripted(
        "worker",
        root=[spawning(("worker", "scout")), Sample(text="spawned"), Sample(text="noted")],
        worker=[Sample(text="slow work")],
    )
    runtime, server = serve(provider, [Root])
    reads = 0
    tree_headers = runtime._tree_headers

    async def counted(root_id: str) -> Any:
        nonlocal reads
        reads += 1
        return await tree_headers(root_id)

    runtime._tree_headers = counted
    async with Client(server, profile=True) as client:
        root = await client.new()
        await client.prompt(root, "go")
        [worker] = await client.until(lambda: list(mirror(client, root)))
        await client.until(lambda: mirror(client, root)[worker]["state"] == {"state": "running"})
        await client.idle(root)
        await asyncio.sleep(0.5)
        start = reads
        await asyncio.sleep(0.3)
        busy = reads - start
        provider.gate.set()
        await client.idle(root, 2)
        await client.until(lambda: mirror(client, root)[worker]["state"].get("stopReason") == "end_turn")
        await asyncio.sleep(0.5)
        start = reads
        await asyncio.sleep(0.3)
        idle = reads - start
    assert busy == idle == 0
    await runtime.aclose()


async def test_a_cancelled_downgrade_still_reports_replaced_and_reclaim_announces() -> None:
    runtime, server = serve(Scripted(m=[Sample(text="after reclaim")]), [Bot])
    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        session = client.connection.sessions[UUID(session_id)]
        writer = session.writer
        assert writer is not None
        exiting, release = asyncio.Event(), asyncio.Event()
        exit_writer = writer.__aexit__

        async def fenced_send(*args: Any, **kwargs: Any) -> Any:
            raise WriterReplaced("fenced in test")

        async def slow_exit(*args: Any) -> None:
            exiting.set()
            await release.wait()
            await exit_writer(*args)

        writer.send = fenced_send
        writer.__aexit__ = slow_exit
        downgrading = asyncio.create_task(client.prompt(session_id, "hi"))
        await asyncio.wait_for(exiting.wait(), timeout=2)
        await client.notify("$/cancel_request", {"requestId": client.request_id("session/prompt")})
        with pytest.raises(RpcError) as cancelled:
            await downgrading
        await client.until(lambda: REPLACED in client.of(session_id, "_tantra_writer"))
        release.set()
        assert await client.call("_tantra/session/claim_writer", {"sessionId": session_id}) == {}
        await client.until(lambda: len(client.of(session_id, "_tantra_writer")) == 3)
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert cancelled.value.code == -32800
    assert client.of(session_id, "_tantra_writer") == [CLAIMED, REPLACED, CLAIMED]
    await runtime.aclose()


async def test_go_live_never_announces_on_a_closed_session() -> None:
    gate, reached = asyncio.Event(), asyncio.Event()

    async def held_prompt(access: AcpAccess) -> bool:
        if access.operation == "prompt":
            reached.set()
            await gate.wait()
        return True

    runtime, server = serve(Scripted(m=[Sample(text="x")]), [Bot], policy=held_prompt)
    tree_gate = asyncio.Event()
    watch_tree = runtime.watch_tree

    async def held_tree(root_id: UUID) -> Any:
        await tree_gate.wait()
        async for change in watch_tree(root_id):
            yield change

    runtime.watch_tree = held_tree
    async with Client(server, profile=True) as owner:
        session_id = await owner.new(**tantra(mode="read"))
        async with Client(server, profile=True) as client:
            await client.resume(session_id, **tantra(mode="write"))
            session = client.connection.sessions[UUID(session_id)]
            prompt = asyncio.create_task(client.prompt(session_id, "hi"))
            await asyncio.wait_for(reached.wait(), timeout=2)
            tree_gate.set()

            async def go_live_waiting() -> bool:
                return len(session.lock._waiters or ()) == 1

            await eventually(go_live_waiting)
            closing = asyncio.create_task(client.call("session/close", {"sessionId": session_id}))

            async def closed() -> bool:
                return session.closed

            await eventually(closed)
            gate.set()
            assert await closing == {}
            await asyncio.gather(prompt, return_exceptions=True)
            await client.call("session/list", {})
    assert client.of(session_id, "_tantra_writer") == []
    assert not session.live.is_set()
    assert not client.requests
    await runtime.aclose()


@pytest.mark.parametrize("failure", ["view", "store"])
async def test_a_failing_mirror_refresh_closes_the_connection(failure: str) -> None:
    async def broken_view(item: LoggedEvent) -> LoggedEvent:
        if failure == "view" and isinstance(item.event, ChildCreated) and item.event.turn_id == "":
            raise ValueError("view failed")
        return item

    provider = Scripted(
        root=[spawning(("worker", "scout")), Sample(text="a"), Sample(text="b")], worker=[Sample(text="w")]
    )
    runtime, server = serve(provider, [Root], view=broken_view)
    tree_status = runtime.tree_status
    watch_tree = runtime.watch_tree
    broken = asyncio.Event()

    async def failing_tree(root_id: UUID) -> Any:
        async for change in watch_tree(root_id):
            if broken.is_set():
                raise RuntimeError("store failed")
            yield change

    runtime.watch_tree = failing_tree
    async with Client(server, profile=True) as client:
        root = await client.new()
        broken.set()
        with contextlib.suppress(ConnectionError, RpcError):
            await client.prompt(root, "go")
        await client.wait_closed()
    assert not client.connection.sessions

    async def settled() -> bool:
        return all(status.state == "idle" for status in await tree_status(UUID(root)))

    await eventually(settled)
    async with Client(server, profile=True) as late:
        with contextlib.suppress(ConnectionError, RpcError):
            await late.resume(root)
        await late.wait_closed()
    assert not late.connection.sessions
    await runtime.aclose()


async def test_a_denied_write_resume_leaves_the_open_session_working() -> None:
    async def no_guest_writer(access: AcpAccess) -> bool:
        return access.operation != "claim_writer" or access.auth.principal != "guest"

    runtime, server = serve(Scripted(m=[Sample(text="still streaming")]), [Bot], policy=no_guest_writer)
    async with (
        Client(server, profile=True) as owner,
        Client(server, AcpAuth(principal="guest"), profile=True) as guest,
    ):
        session_id = await owner.new()
        await guest.resume(session_id)
        await settle(guest, session_id)
        opened = guest.connection.sessions[UUID(session_id)]
        with pytest.raises(RpcError) as denied:
            await guest.resume(session_id, **tantra(mode="write"))
        assert guest.connection.sessions[UUID(session_id)] is opened and not opened.closed
        await owner.prompt(session_id, "go")
        await guest.idle(session_id)
    assert denied.value.code == -32002
    assert guest.of(session_id, "agent_message")[-1]["content"] == [{"type": "text", "text": "still streaming"}]
    await runtime.aclose()


async def test_an_unavailable_write_resume_keeps_the_open_writer() -> None:
    runtime, server = serve(Scripted(m=[Sample(text="after the failed swap")]), [Bot])
    claim = runtime._claim

    async def unavailable(connection: Any) -> None:
        if connection.writable:
            raise CoordinatorUnavailable("coordinator down")
        await claim(connection)

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        session = client.connection.sessions[UUID(session_id)]
        writer = session.writer
        runtime._claim = unavailable
        with pytest.raises(RpcError) as failed:
            await client.resume(session_id, **tantra(mode="write"))
        del runtime._claim
        assert client.connection.sessions[UUID(session_id)] is session and not session.closed
        assert session.writer is writer and writer is not None
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert (failed.value.code, failed.value.data) == (-32603, {"tantra": {"code": "unavailable"}})
    assert client.of(session_id, "_tantra_writer") == [CLAIMED]
    assert client.of(session_id, "agent_message")[-1]["content"] == [{"type": "text", "text": "after the failed swap"}]
    await runtime.aclose()


async def test_a_cancelled_write_resume_keeps_the_open_writer() -> None:
    runtime, server = serve(Scripted(m=[Sample(text="after the cancelled swap")]), [Bot])
    claim = runtime._claim
    claiming = asyncio.Event()

    async def blocked(connection: Any) -> None:
        if connection.writable:
            claiming.set()
            await asyncio.Future()
        await claim(connection)

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        session = client.connection.sessions[UUID(session_id)]
        writer = session.writer
        runtime._claim = blocked
        resuming = asyncio.create_task(client.resume(session_id, **tantra(mode="write")))
        await asyncio.wait_for(claiming.wait(), timeout=2)
        await client.notify("$/cancel_request", {"requestId": client.request_id("session/resume")})
        with pytest.raises(RpcError) as cancelled:
            await resuming
        del runtime._claim
        assert client.connection.sessions[UUID(session_id)] is session and not session.closed
        assert session.writer is writer and writer is not None
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert cancelled.value.code == -32800
    assert client.of(session_id, "_tantra_writer") == [CLAIMED]
    assert list(runtime._connections) == []
    await runtime.aclose()


async def test_a_write_resume_over_the_writer_claims_before_closing_the_old_session() -> None:
    runtime, server = serve(Scripted(m=[Sample(text="on the new writer")]), [Bot])
    claim = runtime._claim

    async def slow(connection: Any) -> None:
        await claim(connection)
        if connection.writable:
            await asyncio.sleep(0.05)

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        old = client.connection.sessions[UUID(session_id)]
        runtime._claim = slow
        await client.resume(session_id, **tantra(mode="write"))
        del runtime._claim
        await client.until(lambda: len(client.of(session_id, "_tantra_writer")) == 2)
        new = client.connection.sessions[UUID(session_id)]
        assert new is not old and old.closed and old.writer is None
        assert new.writer is not None and list(runtime._connections) == [new.writer._connection_id]
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert client.of(session_id, "_tantra_writer") == [CLAIMED, CLAIMED]
    await runtime.aclose()


async def test_a_failed_open_after_the_claim_leaks_no_writer() -> None:
    runtime, server = serve(Scripted(m=[]), [Bot])
    status = runtime.status
    failures = [CoordinatorUnavailable("store down")]

    async def flaky(agent_id: UUID) -> Any:
        if failures:
            raise failures.pop()
        return await status(agent_id)

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        runtime.status = flaky
        with pytest.raises(RpcError) as failed:
            await client.resume(session_id, **tantra(mode="write"))
        del runtime.status
        assert not runtime._connections
        assert not client.connection.sessions
        await client.resume(session_id, **tantra(mode="write"))
        await client.until(lambda: len(client.of(session_id, "_tantra_writer")) == 2)
    assert failed.value.data == {"tantra": {"code": "unavailable"}}
    await runtime.aclose()


async def test_cancels_after_the_claim_still_complete_the_write_resume() -> None:
    runtime, server = serve(Scripted(m=[Sample(text="on the swapped writer")]), [Bot])
    status = runtime.status
    reached, release = asyncio.Event(), asyncio.Event()

    async def held(agent_id: UUID) -> Any:
        reached.set()
        await release.wait()
        return await status(agent_id)

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await client.until(lambda: client.of(session_id, "_tantra_writer"))
        old = client.connection.sessions[UUID(session_id)]
        runtime.status = held
        resuming = asyncio.create_task(client.resume(session_id, **tantra(mode="write")))
        await asyncio.wait_for(reached.wait(), timeout=2)
        for _ in range(2):
            await client.notify("$/cancel_request", {"requestId": client.request_id("session/resume")})
            await asyncio.sleep(0.05)
        release.set()
        assert await resuming == {}
        del runtime.status
        await client.until(lambda: len(client.of(session_id, "_tantra_writer")) == 2)
        new = client.connection.sessions[UUID(session_id)]
        assert new is not old and old.closed and new.writer is not None
        assert list(runtime._connections) == [new.writer._connection_id]
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert client.of(session_id, "_tantra_writer") == [CLAIMED, CLAIMED]
    await runtime.aclose()


async def test_closing_the_connection_during_the_swap_leaks_no_writer() -> None:
    runtime, server = serve(Scripted(m=[]), [Bot])
    status = runtime.status
    reached = asyncio.Event()

    async def held(agent_id: UUID) -> Any:
        reached.set()
        await asyncio.Future()
        return await status(agent_id)

    client = Client(server, profile=True)
    await client.open()
    session_id = await client.new()
    await client.until(lambda: client.of(session_id, "_tantra_writer"))
    runtime.status = held
    resuming = asyncio.create_task(client.resume(session_id, **tantra(mode="write")))
    await asyncio.wait_for(reached.wait(), timeout=2)
    await client.close()
    await asyncio.gather(resuming, return_exceptions=True)
    del runtime.status

    async def released() -> bool:
        return not runtime._connections

    await eventually(released)
    assert not client.connection.sessions
    await runtime.aclose()
