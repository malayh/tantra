import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from conftest import SharedProvider
from httpx_ws import AsyncWebSocketSession, WebSocketDisconnect

from sarathi.agent import Sarathi, deps_factory
from sarathi.api.ws import SocketBridge
from sarathi.schemas import ServerErrorFrame
from tantra import Runtime, Sample, SessionHeader
from tantra.events import AgentFinished, ChildCreated, InputQueued, SessionCreated, TurnStarted
from tantra.providers.base import SampleRequest, ToolCall

Signup = Callable[..., Awaitable[str]]
NewSession = Callable[..., Awaitable[str]]
Socket = Callable[[str, str], AbstractAsyncContextManager[AsyncWebSocketSession]]

RECEIVE_TIMEOUT = 5.0
SILENCE_TIMEOUT = 0.2
WRITER_REPLACED = 4009


def _kind(frame: dict[str, Any]) -> str:
    return str(frame["event"]["type"] if frame.get("type") == "event" else frame["type"])


async def _receive(ws: AsyncWebSocketSession) -> dict[str, Any]:
    return json.loads(await ws.receive_text(timeout=RECEIVE_TIMEOUT))


async def _until(ws: AsyncWebSocketSession, kind: str, limit: int = 120) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for _ in range(limit):
        frame = await _receive(ws)
        frames.append(frame)
        if _kind(frame) == kind:
            return frames
    raise AssertionError(f"never saw {kind}: {[_kind(frame) for frame in frames]}")


async def _send(ws: AsyncWebSocketSession, frame: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(frame))


async def _subscribe(
    ws: AsyncWebSocketSession,
    agent_id: str,
    *,
    after: int = 0,
    writable: bool = False,
) -> list[dict[str, Any]]:
    await _send(
        ws,
        {"type": "subscribe", "agent_id": agent_id, "after": after, "writable": writable},
    )
    frames: list[dict[str, Any]] = []
    while True:
        frame = await _receive(ws)
        frames.append(frame)
        if frame["type"] == "subscription_ready" and frame["agent_id"] == agent_id:
            return frames


def _message(text: str, command_id: str | None = None) -> dict[str, Any]:
    return {
        "type": "user_message",
        "command_id": command_id or uuid4().hex,
        "text": text,
        "attachments": [],
    }


async def _uid(client: httpx.AsyncClient, token: str) -> str:
    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    return str(response.json()["id"])


def _disconnect_code(error: BaseException) -> int:
    if isinstance(error, BaseExceptionGroup):
        matched, _ = error.split(WebSocketDisconnect)
        assert matched is not None
        return _disconnect_code(matched.exceptions[0])
    assert isinstance(error, WebSocketDisconnect)
    return int(error.code)


async def _idle(resources: Any, *agent_ids: str) -> None:
    async def wait() -> None:
        while any(agent_id in resources.runtime.active for agent_id in agent_ids):
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=5)


async def test_root_subscription_streams_durable_events_and_resumes_after_cursor(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(reasoning="think", text="hello"), Sample(text="A title")])
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        replay = await _subscribe(ws, sid, writable=True)
        assert [_kind(frame) for frame in replay] == ["session_created", "subscription_ready"]
        command_id = uuid4().hex
        await _send(ws, _message("hi", command_id))
        turn = await _until(ws, "turn_completed")
        titles = [frame for frame in turn if _kind(frame) == "title_updated"]
        title = titles[-1] if titles else (await _until(ws, "title_updated"))[-1]

    events = [frame for frame in turn if frame["type"] == "event"]
    assert [_kind(frame) for frame in events[:3]] == ["input_queued", "turn_started", "sample_started"]
    assert "reasoning_delta" in [_kind(frame) for frame in events]
    assert "text_delta" in [_kind(frame) for frame in events]
    assert all(frame["agent_id"] == sid and isinstance(frame["seq"], int) for frame in events)
    assert title == {"type": "title_updated", "title": "A title"}
    cursor = events[-1]["seq"]

    async with socket(sid, token) as ws:
        resumed = await _subscribe(ws, sid, after=cursor)
    assert resumed == [{"type": "subscription_ready", "agent_id": sid, "seq": cursor, "active": False}]


async def test_duplicate_command_is_acknowledged_only_by_existing_durable_events(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="once"), Sample(text="title")])
    token = await signup()
    sid = await new_session(token)
    command_id = uuid4().hex

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        await _send(ws, _message("hi", command_id))
        first = await _until(ws, "turn_completed")
        if "title_updated" not in [_kind(frame) for frame in first]:
            await _until(ws, "title_updated")
        await _send(ws, _message("hi", command_id))
        with pytest.raises(TimeoutError):
            await ws.receive_text(timeout=SILENCE_TIMEOUT)

    assert sum(_kind(frame) == "input_queued" for frame in first) == 1
    assert len(provider.requests) == 2


async def test_writer_takeover_closes_the_old_socket_and_new_writer_can_send(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="new writer"), Sample(text="title")])
    token = await signup()
    sid = await new_session(token)

    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(sid, token) as first:
            await _subscribe(first, sid, writable=True)
            async with socket(sid, token) as second:
                await _subscribe(second, sid, writable=True)
                await first.receive_text(timeout=RECEIVE_TIMEOUT)
                await _send(second, _message("mine"))
                turn = await _until(second, "turn_completed")
                assert turn[-1]["agent_id"] == sid
    assert _disconnect_code(raised.value) == WRITER_REPLACED


async def test_read_only_root_and_child_writable_subscription_cannot_mutate(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid)
        await _send(ws, _message("no"))
        error = (await _until(ws, "server_error"))[-1]
    assert "writable=true" in error["message"]


async def test_unsubscribe_stops_delivery_and_replacing_subscription_uses_cursor(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="done"), Sample(text="title")])
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        replay = await _subscribe(ws, sid, writable=True)
        cursor = replay[-1]["seq"]
        await _send(ws, {"type": "unsubscribe", "agent_id": sid})
        await _send(ws, _message("blocked"))
        await _until(ws, "server_error")
        await _subscribe(ws, sid, after=cursor, writable=True)
        await _send(ws, _message("works"))
        await _until(ws, "turn_completed")


async def test_disconnect_does_not_stop_zero_socket_execution(
    socket: Socket,
    provider: SharedProvider,
    resources: Any,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="keeps running"), Sample(text="title")])
    provider.gate.clear()
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        await _send(ws, _message("go"))
        await _until(ws, "text_delta")

    provider.gate.set()
    await _idle(resources, sid)
    header = await resources.store.header(sid)
    assert header is not None and header.status == "idle"

    async with socket(sid, token) as ws:
        replay = await _subscribe(ws, sid)
    assert "turn_completed" in [_kind(frame) for frame in replay]


async def test_attachment_ownership_is_enforced(
    socket: Socket,
    client: httpx.AsyncClient,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    upload_dir: Path,
) -> None:
    provider.samples.extend([Sample(text="read"), Sample(text="title")])
    token = await signup()
    sid = await new_session(token)
    owned = upload_dir / await _uid(client, token) / "x.pdf"

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        bad = _message("bad")
        bad["attachments"] = [{"path": "/etc/shadow", "name": "x.pdf"}]
        await _send(ws, bad)
        assert (await _until(ws, "server_error"))[-1]["message"] == "invalid attachment path"
        good = _message("summarize")
        good["attachments"] = [{"path": str(owned), "name": "x.pdf"}]
        await _send(ws, good)
        frames = await _until(ws, "turn_completed")

    queued = next(frame for frame in frames if _kind(frame) == "input_queued")
    assert queued["event"]["input"] == f"summarize\n[attachment: x.pdf path={owned}]"


async def test_explicit_child_subscription_replays_completion_and_parent_synthesis(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    counts = {"root": 0, "child": 0}

    def route(request: SampleRequest) -> Sample:
        prompt = request.system[0].text
        if "title generator" in prompt:
            return Sample(text="Research title")
        if "research subagent" in prompt:
            counts["child"] += 1
            return Sample(tool_calls=[ToolCall(id="f", name="finish", args='{"result":"findings"}')])
        counts["root"] += 1
        if counts["root"] == 1:
            return Sample(
                tool_calls=[ToolCall(id="s", name="spawn", args='{"agent_name":"researcher","input":"look"}')]
            )
        if counts["root"] == 2:
            return Sample(text="research started")
        return Sample(text="synthesized findings")

    provider.route = route
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        await _send(ws, _message("research"))
        root_frames = await _until(ws, "child_created")
        created = root_frames[-1]["event"]
        child_id = created["child_id"]
        frames = [*root_frames, *(await _subscribe(ws, child_id))]
        while True:
            synthesized = [
                frame["event"]["command_id"]
                for frame in frames
                if frame.get("agent_id") == sid
                and _kind(frame) == "input_queued"
                and frame["event"]["input"].startswith("[agent ")
            ]
            synthesis_id = synthesized[-1] if synthesized else None
            child_finished = any(
                frame.get("agent_id") == child_id and _kind(frame) == "agent_finished" for frame in frames
            )
            completed = any(
                frame.get("agent_id") == sid
                and _kind(frame) == "turn_completed"
                and frame["event"]["turn_id"] == synthesis_id
                for frame in frames
            )
            if child_finished and synthesis_id is not None and completed:
                break
            frames.append(await _receive(ws))

    assert created["agent"] == "researcher"
    assert any(frame.get("agent_id") == child_id and _kind(frame) == "agent_finished" for frame in frames)
    assert synthesis_id is not None
    assert counts["child"] == 1


async def test_descendant_live_ask_routes_through_the_root_writer(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    counts = {"root": 0, "child": 0}

    def route(request: SampleRequest) -> Sample:
        prompt = request.system[0].text
        if "title generator" in prompt:
            return Sample(text="Ask title")
        if "research subagent" in prompt:
            counts["child"] += 1
            if counts["child"] == 1:
                return Sample(tool_calls=[ToolCall(id="w", name="web_fetch", args='{"url":"https://example.com"}')])
            return Sample(tool_calls=[ToolCall(id="f", name="finish", args='{"result":"denied"}')])
        counts["root"] += 1
        if counts["root"] == 1:
            return Sample(
                tool_calls=[ToolCall(id="s", name="spawn", args='{"agent_name":"researcher","input":"look"}')]
            )
        return Sample(text="root")

    provider.route = route
    from sarathi.agent import Researcher

    old_permissions = Researcher.permissions
    Researcher.permissions = {"web_fetch": "ask"}
    try:
        token = await signup()
        sid = await new_session(token)
        async with socket(sid, token) as ws:
            await _subscribe(ws, sid, writable=True)
            await _send(ws, _message("research"))
            created = (await _until(ws, "child_created"))[-1]["event"]
            child_id = created["child_id"]
            child_frames = await _subscribe(ws, child_id)
            while "ask_raised" not in [_kind(frame) for frame in child_frames]:
                child_frames.append(await _receive(ws))
            ask = next(frame for frame in child_frames if _kind(frame) == "ask_raised")["event"]
            await _send(
                ws,
                {
                    "type": "ask_response",
                    "command_id": uuid4().hex,
                    "ask_id": ask["ask_id"],
                    "response": "deny",
                },
            )
            await _until(ws, "ask_answered")
    finally:
        Researcher.permissions = old_permissions


async def test_expired_ask_returns_typed_frame(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    sid = await new_session(token)
    ask_id = uuid4().hex

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        await _send(
            ws,
            {
                "type": "ask_response",
                "command_id": uuid4().hex,
                "ask_id": ask_id,
                "response": "allow",
            },
        )
        expired = (await _until(ws, "ask_expired"))[-1]
    assert expired["agent_id"] == sid
    assert expired["ask_id"] == ask_id


async def test_cancel_stops_the_live_tree_and_socket_stays_usable(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="blocked"), Sample(text="next"), Sample(text="title")])
    provider.gate.clear()
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        await _send(ws, _message("stop me"))
        await _until(ws, "text_delta")
        await _send(ws, {"type": "cancel", "command_id": uuid4().hex})
        cancelled = await _until(ws, "turn_cancelled")
        provider.gate.set()
        await _send(ws, _message("again"))
        completed = await _until(ws, "turn_completed")

    assert "cancellation_requested" in [_kind(frame) for frame in cancelled]
    assert completed[-1]["event"]["stop_reason"] == "completed"


async def test_fresh_process_replays_stopped_work_and_only_recovers_after_send(
    socket: Socket,
    provider: SharedProvider,
    resources: Any,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    sid = await new_session(token)
    old_command = uuid4().hex
    await resources.store.append(
        sid,
        [InputQueued(command_id=old_command, input="old"), TurnStarted(turn_id=old_command, input="old")],
    )
    await resources.store.patch_header(sid, status="running")
    await resources.runtime.aclose()
    fresh = Runtime(
        provider,
        resources.store,
        [Sarathi],
        default_model="test-model",
        deps_factory=deps_factory,
        memory=resources.memory,
    )
    resources.runtime = fresh
    provider.samples.extend([Sample(text="new"), Sample(text="title")])
    try:
        async with socket(sid, token) as ws:
            replay = await _subscribe(ws, sid, writable=True)
            assert replay[-1]["active"] is False
            assert provider.requests == []
            await _send(ws, _message("recover"))
            completed = await _until(ws, "turn_completed")
        kinds = [_kind(frame) for frame in completed]
        assert "turn_interrupted" in kinds
        assert "input_queued" in kinds
        assert len(provider.requests) >= 1
    finally:
        await fresh.aclose()


async def test_child_and_grandchild_subscriptions_replay_independent_journals(
    socket: Socket,
    resources: Any,
    client: httpx.AsyncClient,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    sid = await new_session(token)
    user_id = await _uid(client, token)
    child_id = uuid4().hex
    grandchild_id = uuid4().hex
    metadata = {"user": user_id, "kind": "root"}
    await resources.store.create(
        SessionHeader(
            id=child_id,
            root_id=sid,
            parent_id=sid,
            agent="researcher",
            depth=1,
            model="test-model",
            metadata=metadata,
        )
    )
    await resources.store.create(
        SessionHeader(
            id=grandchild_id,
            root_id=sid,
            parent_id=child_id,
            agent="researcher",
            depth=2,
            model="test-model",
            metadata=metadata,
        )
    )
    await resources.store.append(
        child_id,
        [
            SessionCreated(
                agent="researcher",
                root_id=sid,
                parent_id=sid,
                depth=1,
                model="test-model",
                metadata=metadata,
            ),
            ChildCreated(
                child_id=grandchild_id,
                agent="researcher",
                turn_id=uuid4().hex,
                call_id="spawn-grandchild",
            ),
            AgentFinished(result="child result"),
        ],
    )
    await resources.store.append(
        grandchild_id,
        [
            SessionCreated(
                agent="researcher",
                root_id=sid,
                parent_id=child_id,
                depth=2,
                model="test-model",
                metadata=metadata,
            ),
            AgentFinished(result="grandchild result"),
        ],
    )

    async with socket(sid, token) as ws:
        await _subscribe(ws, sid, writable=True)
        await _send(
            ws,
            {"type": "subscribe", "agent_id": child_id, "after": 0, "writable": True},
        )
        assert (await _until(ws, "server_error"))[-1]["message"] == "only the root subscription can be writable"
        child = await _subscribe(ws, child_id)
        grandchild = await _subscribe(ws, grandchild_id)

    child_events = [frame for frame in child if frame["type"] == "event"]
    grandchild_events = [frame for frame in grandchild if frame["type"] == "event"]
    assert [frame["agent_id"] for frame in child_events] == [child_id] * 3
    assert [_kind(frame) for frame in child_events] == ["session_created", "child_created", "agent_finished"]
    assert [frame["agent_id"] for frame in grandchild_events] == [grandchild_id] * 2
    assert [_kind(frame) for frame in grandchild_events] == ["session_created", "agent_finished"]
    assert child[-1] == {
        "type": "subscription_ready",
        "agent_id": child_id,
        "seq": 3,
        "active": False,
    }
    assert grandchild[-1] == {
        "type": "subscription_ready",
        "agent_id": grandchild_id,
        "seq": 2,
        "active": False,
    }


async def test_slow_socket_does_not_block_another_socket(resources: Any) -> None:
    class BlockingSocket:
        def __init__(self, blocked: bool) -> None:
            self.blocked = blocked
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.sent: list[str] = []

        async def send_text(self, payload: str) -> None:
            self.started.set()
            if self.blocked:
                await self.release.wait()
            self.sent.append(payload)

    slow_socket = BlockingSocket(True)
    fast_socket = BlockingSocket(False)
    slow = SocketBridge(slow_socket, resources, uuid4().hex, "user")
    fast = SocketBridge(fast_socket, resources, uuid4().hex, "user")
    slow_send = asyncio.create_task(slow.send(ServerErrorFrame(message="slow")))
    await slow_socket.started.wait()
    await asyncio.wait_for(fast.send(ServerErrorFrame(message="fast")), timeout=0.1)
    assert [json.loads(payload)["message"] for payload in fast_socket.sent] == ["fast"]
    assert not slow_send.done()
    slow_socket.release.set()
    await slow_send


@pytest.mark.parametrize("session", ["missing", "0" * 32])
async def test_unknown_or_invalid_session_closes_with_policy_violation(
    socket: Socket,
    signup: Signup,
    session: str,
) -> None:
    token = await signup()
    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(session, token) as ws:
            await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1008


async def test_other_users_session_closes_with_policy_violation(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
) -> None:
    owner = await signup("owner@example.com")
    intruder = await signup("intruder@example.com")
    sid = await new_session(owner)
    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(sid, intruder) as ws:
            await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1008
