import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
from conftest import SharedProvider, SharedStore
from httpx_ws import AsyncWebSocketSession, WebSocketDisconnect

from sarathi.agent import HarnessFactory
from tantra import Sample

Signup = Callable[..., Awaitable[str]]
NewSession = Callable[..., Awaitable[str]]
Socket = Callable[[str, str], AbstractAsyncContextManager[AsyncWebSocketSession]]

RECEIVE_TIMEOUT = 5.0


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


async def _send(ws: AsyncWebSocketSession, frame: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(frame))


def _message(text: str) -> dict[str, Any]:
    return {"type": "user_message", "text": text, "attachments": []}


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
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.append(Sample(text="read it"))
    token = await signup()
    sid = await new_session(token)

    async with socket(sid, token) as ws:
        await _until(ws, "replay_done")
        await _send(
            ws,
            {
                "type": "user_message",
                "text": "summarise this",
                "attachments": [{"path": "/data/uploads/1/x.pdf", "name": "x.pdf"}],
            },
        )
        turn = await _until(ws, "turn_completed")

    assert turn[0]["event"]["input"] == "summarise this\n[attachment: x.pdf path=/data/uploads/1/x.pdf]"


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
