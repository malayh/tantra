import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from conftest import SharedProvider, SharedStore
from httpx_ws import AsyncWebSocketSession, WebSocketDisconnect
from starlette.websockets import WebSocketDisconnect as StarletteWebSocketDisconnect
from starlette.websockets import WebSocketState

from sarathi.agent import HarnessFactory, Researcher, Sarathi, _wire_tools
from sarathi.api.ws import CONNECTIONS, Connection
from tantra import BuiltinMemory, Context, MemoryWrite, Sample, SessionBusy, tool
from tantra.events import AgentMessageQueued, CancelRequested, TurnCompleted, TurnStarted
from tantra.providers.base import ToolCall

Signup = Callable[..., Awaitable[str]]
NewSession = Callable[..., Awaitable[str]]
Socket = Callable[[str, str], AbstractAsyncContextManager[AsyncWebSocketSession]]

RECEIVE_TIMEOUT = 5.0
SILENCE_TIMEOUT = 0.5


def _kind(frame: dict[str, Any]) -> str:
    event = frame.get("event")
    return str(event["type"]) if event else str(frame["type"])


async def _until(ws: AsyncWebSocketSession, kind: str, limit: int = 60) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for _ in range(limit):
        frames.append(json.loads(await ws.receive_text(timeout=RECEIVE_TIMEOUT)))
        if _kind(frames[-1]) == kind:
            return frames
    raise AssertionError(f"never saw {kind}: {[_kind(frame) for frame in frames]}")


async def _turn(ws: AsyncWebSocketSession, sid: str, limit: int = 120) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for _ in range(limit):
        try:
            payload = await ws.receive_text(timeout=RECEIVE_TIMEOUT)
        except TimeoutError:
            break
        frames.append(json.loads(payload))
        if _kind(frames[-1]) == "turn_completed" and frames[-1].get("session_id") == sid:
            return frames
    raise AssertionError(f"never saw turn_completed for {sid}: {[_kind(frame) for frame in frames]}")


async def _send(ws: AsyncWebSocketSession, frame: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(frame))


async def _uid(client: httpx.AsyncClient, token: str) -> str:
    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    return str(response.json()["id"])


async def _cancel_recorded(store: SharedStore, sid: str) -> None:
    for _ in range(200):
        events = [stamped.event async for stamped in store.read(sid)]
        if any(isinstance(event, CancelRequested) for event in events):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"session {sid} never recorded a cancel request")


def _message(text: str, request_id: str | None = None) -> dict[str, Any]:
    return {"type": "user_message", "text": text, "attachments": [], "request_id": request_id or str(uuid4())}


def _write_call(content: str) -> ToolCall:
    args = json.dumps({"kind": "preference", "title": content, "body": content})
    return ToolCall(id="m1", name="memory_write", args=args)


async def _answer(ws: AsyncWebSocketSession, frames: list[dict[str, Any]], response: str) -> None:
    ask = next(frame for frame in frames if _kind(frame) == "ask_raised")["event"]
    await _send(ws, {"type": "ask_response", "ask_id": ask["ask_id"], "response": response})


async def _settled(store: SharedStore, sid: str) -> None:
    for _ in range(200):
        header = await store.header(sid)
        if header is not None and header.lease is None:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"session {sid} never released its lease")


def _disconnect_code(error: BaseException) -> int:
    if isinstance(error, BaseExceptionGroup):
        matched, _ = error.split(WebSocketDisconnect)
        assert matched is not None
        return _disconnect_code(matched.exceptions[0])
    assert isinstance(error, WebSocketDisconnect)
    return int(error.code)


async def test_turn_streams_deltas_then_parts(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(reasoning="think hard", text="hello world"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        replay = await _until(ws, "replay_done")
        assert [_kind(frame) for frame in replay] == ["session_created", "replay_done"]

        await _send(ws, _message("hi"))
        turn = await _until(ws, "turn_completed")

    kinds = [_kind(frame) for frame in turn]
    assert kinds[0] == "turn_started"
    assert kinds[1] == "message_accepted"
    assert kinds[2] == "sample_started"
    assert kinds[-1] == "turn_completed"
    assert (
        kinds.index("reasoning_delta")
        < kinds.index("text_delta")
        < kinds.index("reasoning_part")
        < kinds.index("text_part")
        < kinds.index("sample_completed")
    )

    by_kind: dict[str, list[dict[str, Any]]] = {}
    for frame in turn:
        by_kind.setdefault(_kind(frame), []).append(frame)
    assert all(frame["seq"] is None for frame in by_kind["reasoning_delta"] + by_kind["text_delta"])
    assert all(frame["seq"] is not None for frame in by_kind["reasoning_part"] + by_kind["text_part"])
    assert "".join(frame["event"]["text"] for frame in by_kind["text_delta"]) == "hello world"
    assert by_kind["text_part"][0]["event"]["text"] == "hello world"
    assert by_kind["reasoning_part"][0]["event"]["text"] == "think hard"


async def test_user_message_appends_attachment_markers(
    socket: Socket,
    client: httpx.AsyncClient,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    upload_dir: Path,
) -> None:
    provider.samples.append(Sample(text="read it"))
    token = await signup()
    sid = await new_session(token)
    attachment = upload_dir / await _uid(client, token) / "x.pdf"

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(
            ws,
            {
                "type": "user_message",
                "text": "summarise this",
                "attachments": [{"path": str(attachment), "name": "x.pdf"}],
                "request_id": str(uuid4()),
            },
        )
        turn = await _until(ws, "turn_completed")

    assert turn[0]["event"]["input"] == f"summarise this\n[attachment: x.pdf path={attachment}]"


async def test_reconnect_replays_persisted_events_only(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(reasoning="think hard", text="hello world"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("hi"))
        await _until(ws, "turn_completed")

    async with socket(sid, token) as ws:
        replay = await _until(ws, "replay_done")

    assert [_kind(frame) for frame in replay] == [
        "session_created",
        "turn_started",
        "sample_started",
        "reasoning_part",
        "text_part",
        "sample_completed",
        "turn_completed",
        "replay_done",
    ]


async def test_busy_frame_when_lease_is_held(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="unused"))
    token = await signup()
    sid = await new_session(token)
    assert await store.acquire_lease(sid, "other-writer", 60)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("hi"))
        busy = (await _until(ws, "busy"))[-1]

    assert 50 < busy["retry_in"] <= 60


async def test_bad_token_closes_socket(socket: Socket, signup: Signup, new_session: NewSession) -> None:
    token = await signup()
    sid = await new_session(token)

    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(sid, "not-a-jwt") as ws:
            await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1008


async def test_other_users_session_closes_socket(socket: Socket, signup: Signup, new_session: NewSession) -> None:
    owner = await signup("a@example.com")
    intruder = await signup("b@example.com")
    sid = await new_session(owner)

    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(sid, intruder) as ws:
            await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1008


async def test_unknown_session_closes_socket(socket: Socket, signup: Signup) -> None:
    token = await signup()

    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket("missing", token) as ws:
            await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1008


async def test_unexpected_pump_failure_sends_error_and_closes_1011(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(_connection: Connection) -> None:
        raise RuntimeError("raw tool result")

    monkeypatch.setattr(Connection, "pump_loop", fail)
    token = await signup()
    sid = await new_session(token)

    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(sid, token) as ws:
            replay = await _until(ws, "replay_done")
            assert [_kind(frame) for frame in replay] == ["session_created", "replay_done"]
            error = json.loads(await ws.receive_text(timeout=RECEIVE_TIMEOUT))
            assert error == {"type": "server_error", "message": "Unexpected server error", "request_id": None}
            await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1011


@pytest.mark.parametrize("failure", [SessionBusy("busy"), StarletteWebSocketDisconnect(1006)])
async def test_expected_pump_exit_is_not_reported_as_server_error(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    async def fail(_connection: Connection) -> None:
        raise failure

    monkeypatch.setattr(Connection, "pump_loop", fail)
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        with pytest.raises(TimeoutError):
            await ws.receive_text(timeout=SILENCE_TIMEOUT)


async def test_pump_failure_does_not_send_or_close_after_disconnect(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(connection: Connection) -> None:
        connection.websocket.application_state = WebSocketState.DISCONNECTED
        raise RuntimeError("raw tool result")

    monkeypatch.setattr(Connection, "pump_loop", fail)
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        with pytest.raises(TimeoutError):
            await ws.receive_text(timeout=SILENCE_TIMEOUT)


async def test_cancel_without_running_turn_keeps_socket_usable(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="still here"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, {"type": "cancel"})
        await _send(ws, _message("hi"))
        turn = await _until(ws, "turn_completed")

    assert _kind(turn[0]) == "turn_started"
    assert turn[-1]["event"]["stop_reason"] != "cancelled"


async def test_abandoned_turn_resumes_on_connect(
    socket: Socket,
    factory: HarnessFactory,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="finished later"))
    token = await signup()
    sid = await new_session(token)

    harness = factory(None)
    stream = harness.run(sid, "hi")
    assert (await anext(stream)).event.type == "turn_started"
    await stream.aclose()

    async with socket(sid, token) as ws:
        replay = await _until(ws, "replay_done")
        resumed = await _until(ws, "turn_completed")

    assert [_kind(frame) for frame in replay] == ["session_created", "turn_started", "replay_done"]
    kinds = [_kind(frame) for frame in resumed]
    assert kinds[0] == "sample_started"
    assert kinds[-1] == "turn_completed"
    assert "text_part" in kinds


async def test_disconnect_mid_turn_resumes_on_reconnect(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="abandoned start"), Sample(text="finished later")])
    token = await signup()
    sid = await new_session(token)
    provider.gate.clear()

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("hi"))
        await _until(ws, "text_delta")

    provider.gate.set()
    await _settled(store, sid)

    async with socket(sid, token) as ws:
        replay = await _until(ws, "replay_done")
        resumed = await _until(ws, "turn_completed")

    assert [_kind(frame) for frame in replay] == [
        "session_created",
        "turn_started",
        "sample_started",
        "replay_done",
    ]
    assert _kind(resumed[0]) == "sample_started"
    assert [frame["event"]["text"] for frame in resumed if _kind(frame) == "text_part"] == ["finished later"]
    assert resumed[-1]["event"]["stop_reason"] != "cancelled"


async def test_subagent_frames_ride_the_socket_under_the_child_session(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="d1", name="researcher", args='{"task": "look"}')]),
            Sample(text="child findings"),
            Sample(text="parent answer"),
        ]
    )
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("research it"))
        turn = await _turn(ws, sid)

    kinds = [_kind(frame) for frame in turn]
    assert "child_session_spawned" in kinds
    spawned = next(frame for frame in turn if _kind(frame) == "child_session_spawned")
    child_sid = spawned["event"]["child_session_id"]
    assert spawned["event"]["agent"] == "researcher"
    assert spawned["event"]["call_id"] == "d1"

    child = [frame for frame in turn if frame.get("session_id", sid) != sid]
    assert child
    assert {frame["session_id"] for frame in child} == {child_sid}
    assert all(frame["depth"] == 1 for frame in child)
    assert [frame["event"]["text"] for frame in child if _kind(frame) == "text_part"] == ["child findings"]

    delegate = next(
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "d1"
    )
    assert delegate["event"]["result"] == {"task_id": child_sid, "agent": "researcher"}
    assert delegate["event"]["is_error"] is False
    assert turn[-1]["event"]["stop_reason"] == "completed"


async def test_reconnect_replays_child_frames_depth_first(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="d1", name="researcher", args='{"task": "look"}')]),
            Sample(text="child findings"),
            Sample(text="parent answer"),
        ]
    )
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("research it"))
        await _turn(ws, sid)

    async with socket(sid, token) as ws:
        replay = await _until(ws, "replay_done")

    kinds = [_kind(frame) for frame in replay]
    assert kinds[-1] == "replay_done"
    spawned = [frame for frame in replay if _kind(frame) == "child_session_spawned"]
    assert len(spawned) == 1
    child_sid = spawned[0]["event"]["child_session_id"]
    child = [frame for frame in replay if frame.get("session_id") == child_sid]
    assert [_kind(frame) for frame in child] == [
        "session_created",
        "turn_started",
        "sample_started",
        "text_part",
        "sample_completed",
        "turn_completed",
    ]
    launches = [
        frame for frame in replay if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "d1"
    ]
    assert [frame["event"]["result"]["task_id"] for frame in launches] == [child_sid]


async def test_unknown_tool_completes_with_an_error_and_the_turn_continues(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="u1", name="nonexistent", args="{}")]),
            Sample(text="recovered"),
        ]
    )
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("use it"))
        turn = await _turn(ws, sid)

    started = [frame for frame in turn if _kind(frame) == "tool_call_started"]
    assert [frame["event"]["call_id"] for frame in started] == ["u1"]

    failed = next(
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "u1"
    )
    assert failed["event"]["is_error"] is True
    assert [frame["event"]["text"] for frame in turn if _kind(frame) == "text_part"] == ["recovered"]
    assert turn[-1]["event"]["stop_reason"] == "completed"


async def test_cancel_mid_sample_ends_the_turn_cancelled_without_running_the_child(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(text="starting", tool_calls=[ToolCall(id="d1", name="researcher", args='{"task": "look"}')]),
            Sample(text="child findings"),
            Sample(text="parent answer"),
        ]
    )
    token = await signup()
    sid = await new_session(token)
    provider.gate.clear()

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("research it"))
        await _until(ws, "text_delta")
        await _send(ws, {"type": "cancel"})
        await _cancel_recorded(store, sid)
        provider.gate.set()
        turn = await _turn(ws, sid)

    kinds = [_kind(frame) for frame in turn]
    assert turn[-1]["event"]["stop_reason"] == "cancelled"
    assert "child_session_spawned" not in kinds
    assert kinds.index("tool_call_started") < kinds.index("tool_call_completed")
    assert all(frame["session_id"] == sid for frame in turn)


async def test_cancel_mid_child_sample_ends_the_child_and_the_parent_cancelled(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="d1", name="researcher", args='{"task": "look"}')]),
            Sample(text="child findings", tool_calls=[ToolCall(id="c1", name="nonexistent", args="{}")]),
            Sample(text="Cancelled Research"),
        ]
    )
    token = await signup()
    sid = await new_session(token)
    provider.gate.clear()

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("research it"))
        streaming = await _until(ws, "child_session_spawned")
        spawned = next(frame for frame in streaming if _kind(frame) == "child_session_spawned")
        child_sid = spawned["event"]["child_session_id"]
        await _send(ws, {"type": "cancel"})
        await _cancel_recorded(store, child_sid)
        await _cancel_recorded(store, sid)
        provider.gate.set()
        turn = await _turn(ws, sid)

    child = [frame for frame in turn if frame["session_id"] == child_sid and _kind(frame) == "turn_completed"]
    assert [frame["event"]["stop_reason"] for frame in child] == ["cancelled"]
    assert turn[-1]["event"]["stop_reason"] == "cancelled"

    delegate = next(
        frame
        for frame in [*streaming, *turn]
        if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "d1"
    )
    assert delegate["event"]["is_error"] is False
    assert delegate["event"]["result"] == {"task_id": child_sid, "agent": "researcher"}


async def test_attachment_outside_the_user_directory_is_refused(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    upload_dir: Path,
) -> None:
    provider.samples.append(Sample(text="second turn"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(
            ws,
            {
                "type": "user_message",
                "text": "summarise this",
                "attachments": [{"path": "/etc/shadow.pdf", "name": "x.pdf"}],
                "request_id": str(uuid4()),
            },
        )
        refused = await _until(ws, "server_error")
        await _send(ws, _message("hi"))
        turn = await _until(ws, "turn_completed")

    assert [_kind(frame) for frame in refused] == ["server_error"]
    assert refused[-1]["message"] == "invalid attachment path"
    assert turn[0]["event"]["input"] == "hi"


async def test_memory_write_asks_first_then_writes_the_row_for_the_user(
    socket: Socket,
    client: httpx.AsyncClient,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(tool_calls=[_write_call("prefers tea")]), Sample(text="saved")])
    token = await signup()
    uid = await _uid(client, token)
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("remember I prefer tea"))
        raised = await _until(ws, "ask_raised")

        assert "turn_completed" not in [_kind(frame) for frame in raised]
        assert "tool_call_started" not in [_kind(frame) for frame in raised]
        assert raised[-1]["event"]["request"]["extra"] == {"permission": "memory_write"}

        await _answer(ws, raised, "allow")
        turn = await _turn(ws, sid)

    kinds = [_kind(frame) for frame in turn]
    assert kinds.index("ask_answered") < kinds.index("tool_call_started") < kinds.index("tool_call_completed")
    completed = next(frame for frame in turn if _kind(frame) == "tool_call_completed")
    assert completed["event"]["is_error"] is False
    assert turn[-1]["event"]["stop_reason"] == "completed"

    rows = await store.memory_all()
    assert [(row.metadata.get("user"), row.body, row.kind) for row in rows] == [(uid, "prefers tea", "preference")]


async def test_denying_the_memory_ask_completes_the_turn_without_writing(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(tool_calls=[_write_call("prefers tea")]), Sample(text="not saved then")])
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("remember I prefer tea"))
        raised = await _until(ws, "ask_raised")
        await _answer(ws, raised, "deny")
        turn = await _turn(ws, sid)

    kinds = [_kind(frame) for frame in turn]
    assert kinds.index("tool_call_started") < kinds.index("tool_call_completed")
    completed = next(frame for frame in turn if _kind(frame) == "tool_call_completed")
    assert completed["event"]["is_error"] is True
    assert completed["event"]["result"] == "denied by user"
    assert turn[-1]["event"]["stop_reason"] == "completed"
    assert await store.memory_all() == []


async def test_a_pending_ask_survives_a_reconnect_and_still_writes(
    socket: Socket,
    client: httpx.AsyncClient,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(tool_calls=[_write_call("prefers tea")]), Sample(text="saved")])
    token = await signup()
    uid = await _uid(client, token)
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("remember I prefer tea"))
        await _until(ws, "ask_raised")

    await _settled(store, sid)

    async with socket(sid, token) as ws:
        replay = await _until(ws, "replay_done")
        assert "ask_raised" in [_kind(frame) for frame in replay]
        await _answer(ws, replay, "allow")
        turn = await _turn(ws, sid)

    assert "ask_raised" in [_kind(frame) for frame in turn]
    assert [frame["event"]["text"] for frame in turn if _kind(frame) == "text_part"] == ["saved"]

    rows = await store.memory_all()
    assert [(row.metadata.get("user"), row.body) for row in rows] == [(uid, "prefers tea")]


async def test_memory_recall_returns_only_the_callers_rows(
    socket: Socket,
    client: httpx.AsyncClient,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="r1", name="memory_recall", args='{"query": "tea"}')]),
            Sample(text="done"),
        ]
    )
    token = await signup()
    uid = await _uid(client, token)
    memory = BuiltinMemory(store)
    await memory.write(MemoryWrite(kind="preference", title="mine", body="drinks tea daily", metadata={"user": uid}))
    await memory.write(
        MemoryWrite(kind="preference", title="theirs", body="hates tea entirely", metadata={"user": "someone-else"})
    )
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("what do I drink"))
        turn = await _turn(ws, sid)

    assert "ask_raised" not in [_kind(frame) for frame in turn]
    completed = next(
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "r1"
    )
    assert [row["body"] for row in completed["event"]["result"]] == ["drinks tea daily"]
    assert turn[-1]["event"]["stop_reason"] == "completed"


async def test_the_first_completed_turn_titles_the_session(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="hello"), Sample(text="My Session Title")])
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("what is tantra"))
        await _until(ws, "turn_completed")
        titled = await _until(ws, "title_updated")

    assert titled == [{"type": "title_updated", "title": "My Session Title"}]
    header = await store.header(sid)
    assert header is not None and header.title == "My Session Title"
    assert len(provider.requests) == 2
    assert "You are a title generator" in provider.requests[1].system[0].text
    assert "what is tantra" in provider.requests[1].messages[0].content


async def test_a_titled_session_is_never_titled_again(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="hello"), Sample(text="My Session Title"), Sample(text="second answer")])
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("first"))
        await _until(ws, "turn_completed")
        await _until(ws, "title_updated")
        await _send(ws, _message("second"))
        following = await _until(ws, "turn_completed")

    assert "title_updated" not in [_kind(frame) for frame in following]
    assert len(provider.requests) == 3
    header = await store.header(sid)
    assert header is not None and header.title == "My Session Title"


async def test_a_failed_title_leaves_the_session_untitled(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="hello"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("first"))
        turn = await _until(ws, "turn_completed")
        with pytest.raises(TimeoutError):
            await ws.receive_text(timeout=SILENCE_TIMEOUT)

    assert turn[-1]["event"]["stop_reason"] == "completed"
    header = await store.header(sid)
    assert header is not None and header.title is None


async def test_a_failed_title_is_retried_only_on_a_new_connection(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="hello"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("first"))
        await _until(ws, "turn_completed")
        with pytest.raises(TimeoutError):
            await ws.receive_text(timeout=SILENCE_TIMEOUT)

        provider.samples.extend([Sample(text="second answer"), Sample(text="Late Title")])
        await _send(ws, _message("second"))
        following = await _until(ws, "turn_completed")
        with pytest.raises(TimeoutError):
            await ws.receive_text(timeout=SILENCE_TIMEOUT)

    assert "title_updated" not in [_kind(frame) for frame in following]
    assert len(provider.requests) == 2
    header = await store.header(sid)
    assert header is not None and header.title is None

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        titled = await _until(ws, "title_updated")

    assert titled == [{"type": "title_updated", "title": "Late Title"}]
    assert len(provider.requests) == 3


async def test_a_pending_ask_defers_the_title_until_the_turn_completes(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [Sample(tool_calls=[_write_call("prefers tea")]), Sample(text="saved"), Sample(text="Tea Preference")]
    )
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("remember I prefer tea"))
        raised = await _until(ws, "ask_raised")
        assert len(provider.requests) == 1

        await _answer(ws, raised, "allow")
        await _turn(ws, sid)
        titled = await _until(ws, "title_updated")

    assert titled == [{"type": "title_updated", "title": "Tea Preference"}]
    header = await store.header(sid)
    assert header is not None and header.title == "Tea Preference"


async def test_switching_the_model_applies_to_the_next_turn(
    socket: Socket,
    client: httpx.AsyncClient,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="hello"), Sample(text="Model Switch"), Sample(text="second answer")])
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("first"))
        await _until(ws, "turn_completed")
        await _until(ws, "title_updated")

        patched = await client.patch(
            f"/api/sessions/{sid}",
            json={"model": "other-model"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert patched.status_code == 200

        await _send(ws, _message("second"))
        following = await _until(ws, "turn_completed")

    started = next(frame for frame in following if _kind(frame) == "sample_started")
    assert started["event"]["model"] == "other-model"
    assert provider.requests[-1].model == "other-model"


async def test_active_user_message_is_persisted_and_absorbed_during_a_sample(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(text="stale", tool_calls=[ToolCall(id="stale", name="nonexistent", args="{}")]),
            Sample(text="guided answer"),
        ]
    )
    token = await signup()
    sid = await new_session(token)
    provider.gate.clear()

    request_id = str(uuid4())
    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("start"))
        await _until(ws, "text_delta")
        await _send(ws, _message("new guidance", request_id))
        first_accept = (await _until(ws, "message_accepted"))[-1]
        await _send(ws, _message("new guidance", request_id))
        second_accept = (await _until(ws, "message_accepted"))[-1]
        assert first_accept["request_id"] == second_accept["request_id"] == request_id
        for _ in range(200):
            events = [stamped.event async for stamped in store.read(sid)]
            if any(isinstance(event, AgentMessageQueued) for event in events):
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("active message was not persisted")
        provider.gate.set()
        turn = await _turn(ws, sid)

    messages = [event for event in events if isinstance(event, AgentMessageQueued)]
    assert [event.text for event in messages] == ["new guidance"]
    assert messages[0].message_id == request_id.replace("-", "")
    queued = [frame for frame in turn if _kind(frame) == "agent_message_queued"]
    assert [frame["event"]["message_id"] for frame in queued] == [messages[0].message_id]
    skipped = next(
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "stale"
    )
    assert skipped["event"]["result"] == "skipped: newer agent message"
    assert [frame["event"]["text"] for frame in turn if _kind(frame) == "text_part"] == ["stale", "guided answer"]


async def test_second_authorized_root_socket_can_persist_active_guidance(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="stale"), Sample(text="guided")])
    token = await signup()
    sid = await new_session(token)
    provider.gate.clear()
    await store.patch_header(sid, title="Existing")

    async with socket(sid, token) as owner:
        await _until(owner, "replay_done")
        await _send(owner, _message("start"))
        await _until(owner, "text_delta")
        async with socket(sid, token) as guide:
            await _until(guide, "replay_done")
            assert len(CONNECTIONS.connections[sid]) == 2
            await _send(guide, _message("from tab two"))
            accepted = (await _until(guide, "message_accepted"))[-1]
            for _ in range(200):
                events = [stamped.event async for stamped in store.read(sid)]
                if any(isinstance(event, AgentMessageQueued) and event.text == "from tab two" for event in events):
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("second socket guidance was not persisted")
            assert accepted["request_id"] is not None
            provider.gate.set()
            owner_turn = await _turn(owner, sid)
            assert len(CONNECTIONS.connections[sid]) == 2
            guide_turn = await _turn(guide, sid)

    for turn in (owner_turn, guide_turn):
        delivered = [frame for frame in turn if _kind(frame) == "agent_message_queued"]
        assert [frame["event"]["text"] for frame in delivered] == ["from tab two"]

    assert len([event for event in events if isinstance(event, AgentMessageQueued)]) == 1


async def test_idle_message_is_steered_when_competitor_holds_lease_before_turn_started(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider.samples.extend([Sample(text="stale"), Sample(text="guided")])
    token = await signup()
    sid = await new_session(token)
    await store.patch_header(sid, title="Existing")
    provider.gate.clear()
    winner_acquired = asyncio.Event()
    loser_collided = asyncio.Event()
    release_started = asyncio.Event()
    original_acquire = store.acquire_lease
    original_append = store.append

    async def gated_acquire(session_id: str, holder: str, ttl: float) -> bool:
        acquired = await original_acquire(session_id, holder, ttl)
        if session_id == sid and not acquired and winner_acquired.is_set():
            loser_collided.set()
        return acquired

    async def gated_append(session_id: str, events: Any, *, expect_seq: int | None) -> int:
        if session_id == sid and any(isinstance(event, TurnStarted) for event in events):
            winner_acquired.set()
            await release_started.wait()
        return await original_append(session_id, events, expect_seq=expect_seq)

    monkeypatch.setattr(store, "acquire_lease", gated_acquire)
    monkeypatch.setattr(store, "append", gated_append)

    async with socket(sid, token) as owner:
        await _until(owner, "replay_done")
        await _send(owner, _message("owner starts"))
        await asyncio.wait_for(winner_acquired.wait(), 2)
        async with socket(sid, token) as guide:
            await _until(guide, "replay_done")
            await _send(guide, _message("race guidance"))
            await asyncio.wait_for(loser_collided.wait(), 2)
            release_started.set()
            for _ in range(200):
                events = [stamped.event async for stamped in store.read(sid)]
                messages = [
                    event for event in events if isinstance(event, AgentMessageQueued) and event.text == "race guidance"
                ]
                if messages:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("pre-start guidance was not persisted")
            provider.gate.set()
            turn = await _turn(owner, sid)

    events = [stamped.event async for stamped in store.read(sid)]
    messages = [event for event in events if isinstance(event, AgentMessageQueued) and event.text == "race guidance"]
    assert len(messages) == 1
    assert [event.input for event in events if isinstance(event, TurnStarted)] == ["owner starts"]
    assert [
        frame["event"]["message_id"]
        for frame in turn
        if _kind(frame) == "agent_message_queued" and frame["event"]["text"] == "race guidance"
    ] == [messages[0].message_id]


async def test_idle_message_starts_after_competitor_completes_before_lease_release(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider.samples.extend([Sample(text="first answer"), Sample(text="next answer")])
    token = await signup()
    sid = await new_session(token)
    await store.patch_header(sid, title="Existing")
    completed_while_leased = asyncio.Event()
    loser_collided = asyncio.Event()
    release_lease = asyncio.Event()
    original_acquire = store.acquire_lease
    original_release = store.release_lease

    async def gated_acquire(session_id: str, holder: str, ttl: float) -> bool:
        acquired = await original_acquire(session_id, holder, ttl)
        if session_id == sid and not acquired and completed_while_leased.is_set():
            loser_collided.set()
        return acquired

    async def gated_release(session_id: str, holder: str) -> None:
        events = [stamped.event async for stamped in store.read(session_id)]
        if session_id == sid and any(isinstance(event, TurnCompleted) for event in events):
            completed_while_leased.set()
            await release_lease.wait()
        await original_release(session_id, holder)

    monkeypatch.setattr(store, "acquire_lease", gated_acquire)
    monkeypatch.setattr(store, "release_lease", gated_release)

    async with socket(sid, token) as owner:
        await _until(owner, "replay_done")
        await _send(owner, _message("owner starts"))
        await asyncio.wait_for(completed_while_leased.wait(), 2)
        async with socket(sid, token) as guide:
            await _until(guide, "replay_done")
            await _send(guide, _message("next message"))
            await asyncio.wait_for(loser_collided.wait(), 2)
            release_lease.set()
            following = await asyncio.wait_for(_turn(guide, sid), 3)

    events = [stamped.event async for stamped in store.read(sid)]
    assert [event.input for event in events if isinstance(event, TurnStarted)] == ["owner starts", "next message"]
    assert not [event for event in events if isinstance(event, AgentMessageQueued)]
    assert [frame["event"]["input"] for frame in following if _kind(frame) == "turn_started"] == ["next message"]


async def test_active_attachment_message_reuses_owned_marker_encoding(
    socket: Socket,
    client: httpx.AsyncClient,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    upload_dir: Path,
) -> None:
    provider.samples.extend([Sample(text="stale"), Sample(text="guided")])
    token = await signup()
    sid = await new_session(token)
    attachment = upload_dir / await _uid(client, token) / "x.pdf"
    provider.gate.clear()

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("start"))
        await _until(ws, "text_delta")
        await _send(
            ws,
            {
                "type": "user_message",
                "text": "read this too",
                "attachments": [{"path": str(attachment), "name": "x.pdf"}],
                "request_id": str(uuid4()),
            },
        )
        for _ in range(200):
            events = [stamped.event async for stamped in store.read(sid)]
            messages = [event for event in events if isinstance(event, AgentMessageQueued)]
            if messages:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("active attachment message was not persisted")
        provider.gate.set()
        await _turn(ws, sid)

    assert messages[0].text == f"read this too\n[attachment: x.pdf path={attachment}]"


async def test_child_session_socket_is_rejected(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="d1", name="researcher", args='{"task": "look"}')]),
            Sample(text="child findings"),
            Sample(text="parent answer"),
        ]
    )
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("research it"))
        turn = await _turn(ws, sid)
    child_sid = next(frame for frame in turn if _kind(frame) == "child_session_spawned")["event"]["child_session_id"]

    with pytest.raises((WebSocketDisconnect, BaseExceptionGroup)) as raised:
        async with socket(child_sid, token) as child:
            await child.receive_text(timeout=RECEIVE_TIMEOUT)
    assert _disconnect_code(raised.value) == 1008


async def test_active_message_wakes_task_wait_inside_a_running_tool(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waiting = asyncio.Event()

    @tool
    async def spawn_wait(ctx: Context) -> dict[str, object]:
        ref = await ctx.spawn(Researcher, "hold")
        waiting.set()
        return await ctx.task_wait([ref.task_id])

    _wire_tools()
    monkeypatch.setattr(Sarathi, "tools", [*Sarathi.tools, spawn_wait])
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="spawn-wait", name="spawn_wait", args="{}")]),
            Sample(text="child done"),
            Sample(text="guided root"),
            Sample(text="guided root"),
        ]
    )
    token = await signup()
    sid = await new_session(token)
    provider.gate.clear()

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("start"))
        await asyncio.wait_for(waiting.wait(), 1)
        await _send(ws, _message("wake and continue"))
        for _ in range(200):
            root_events = [stamped.event async for stamped in store.read(sid)]
            messages = [event for event in root_events if isinstance(event, AgentMessageQueued)]
            if messages:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("task_wait guidance was not persisted")
        provider.gate.set()
        turn = await _turn(ws, sid)

    completed = [
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "spawn-wait"
    ]
    assert len(completed) == 1
    assert completed[0]["event"]["is_error"] is False
    assert [event.text for event in messages] == ["wake and continue"]


async def test_duplicate_idle_delivery_is_acknowledged_once_without_another_turn(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="done"))
    token = await signup()
    sid = await new_session(token)
    await store.patch_header(sid, title="Existing")
    provider.gate.clear()
    request_id = str(uuid4())

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("once", request_id))
        first = (await _until(ws, "message_accepted"))[-1]
        await _send(ws, _message("once", request_id))
        second = (await _until(ws, "message_accepted"))[-1]
        await _send(ws, _message("different", request_id))
        conflict = (await _until(ws, "server_error"))[-1]
        provider.gate.set()
        await _turn(ws, sid)

    events = [stamped.event async for stamped in store.read(sid)]
    assert first["request_id"] == second["request_id"] == request_id
    assert conflict["request_id"] == request_id
    assert "different input" in conflict["message"]
    assert [event.input for event in events if isinstance(event, TurnStarted)] == ["once"]
    assert len(provider.requests) == 1


async def test_cancel_then_immediate_message_starts_exactly_one_new_turn(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="first"), Sample(text="second")])
    token = await signup()
    sid = await new_session(token)
    await store.patch_header(sid, title="Existing")
    provider.gate.clear()
    next_id = str(uuid4())

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(ws, _message("start"))
        await _until(ws, "text_delta")
        await _send(ws, {"type": "cancel"})
        await _send(ws, _message("follow up", next_id))
        await _cancel_recorded(store, sid)
        provider.gate.set()
        cancelled = await _turn(ws, sid)
        following = await _turn(ws, sid)

    events = [stamped.event async for stamped in store.read(sid)]
    started = [event for event in events if isinstance(event, TurnStarted)]
    completed = [event for event in events if isinstance(event, TurnCompleted)]
    assert [event.input for event in started] == ["start", "follow up"]
    assert started[1].turn_id == next_id.replace("-", "")
    assert [event.stop_reason for event in completed] == ["cancelled", "completed"]
    assert cancelled[-1]["event"]["stop_reason"] == "cancelled"
    assert [frame["request_id"] for frame in following if _kind(frame) == "message_accepted"] == [next_id]
    assert len(provider.requests) == 2


async def test_two_tabs_receive_root_and_descendant_live_events_once(
    socket: Socket,
    store: SharedStore,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend(
        [
            Sample(tool_calls=[ToolCall(id="d1", name="researcher", args='{"task": "look"}')]),
            Sample(text="child findings"),
            Sample(text="parent answer"),
        ]
    )
    token = await signup()
    sid = await new_session(token)
    await store.patch_header(sid, title="Existing")

    async with socket(sid, token) as owner:
        await _until(owner, "replay_done")
        async with socket(sid, token) as observer:
            await _until(observer, "replay_done")
            await _send(owner, _message("research it"))
            owner_turn = await _turn(owner, sid)
            observer_turn = await _turn(observer, sid)

    def persisted(frames: list[dict[str, Any]]) -> list[tuple[str, int]]:
        return [(str(frame["session_id"]), int(frame["seq"])) for frame in frames if frame.get("seq") is not None]

    owner_events = persisted(owner_turn)
    observer_events = persisted(observer_turn)
    assert owner_events == observer_events
    assert len(owner_events) == len(set(owner_events))
    assert any(frame.get("session_id") != sid for frame in owner_turn)
