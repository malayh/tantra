import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any

import httpx
import pytest
from conftest import SharedProvider, SharedStore
from httpx_ws import AsyncWebSocketSession, WebSocketDisconnect

from sarathi.agent import HarnessFactory
from tantra import BuiltinMemory, MemoryWrite, Sample
from tantra.events import CancelRequested
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
        frames.append(json.loads(await ws.receive_text(timeout=RECEIVE_TIMEOUT)))
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


def _message(text: str) -> dict[str, Any]:
    return {"type": "user_message", "text": text, "attachments": []}


def _write_call(content: str) -> ToolCall:
    return ToolCall(id="m1", name="memory_write", args=json.dumps({"content": content, "kind": "preference"}))


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
    assert kinds[1] == "sample_started"
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

    child = [frame for frame in turn if frame["session_id"] != sid]
    assert child
    assert {frame["session_id"] for frame in child} == {child_sid}
    assert all(frame["depth"] == 1 for frame in child)
    assert [frame["event"]["text"] for frame in child if _kind(frame) == "text_part"] == ["child findings"]

    delegate = next(
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "d1"
    )
    assert delegate["event"]["result"] == "child findings"
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
    spawn = kinds.index("child_session_spawned")
    completed = kinds.index("tool_call_completed")
    assert spawn < completed
    assert kinds[-1] == "replay_done"

    child = replay[spawn + 1 : completed]
    assert child
    assert all(frame["session_id"] != sid for frame in child)
    assert [_kind(frame) for frame in child] == [
        "session_created",
        "turn_started",
        "sample_started",
        "text_part",
        "sample_completed",
        "turn_completed",
    ]
    assert [frame["session_id"] for frame in replay[:spawn]] == [sid] * spawn


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
    assert [frame["event"]["call_id"] for frame in started] == []

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
    assert "tool_call_started" not in kinds
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
        streaming = await _until(ws, "text_delta")
        spawned = next(frame for frame in streaming if _kind(frame) == "child_session_spawned")
        child_sid = spawned["event"]["child_session_id"]
        assert streaming[-1]["session_id"] == child_sid

        await _send(ws, {"type": "cancel"})
        await _cancel_recorded(store, child_sid)
        await _cancel_recorded(store, sid)
        provider.gate.set()
        turn = await _turn(ws, sid)

    child = [frame for frame in turn if frame["session_id"] == child_sid and _kind(frame) == "turn_completed"]
    assert [frame["event"]["stop_reason"] for frame in child] == ["cancelled"]
    assert turn[-1]["event"]["stop_reason"] == "cancelled"

    delegate = next(
        frame for frame in turn if _kind(frame) == "tool_call_completed" and frame["event"]["call_id"] == "d1"
    )
    assert delegate["event"]["is_error"] is True
    assert "cancelled" in delegate["event"]["result"]


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

    assert "tool_call_started" not in [_kind(frame) for frame in turn]
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
