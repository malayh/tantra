import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import httpx
import jwt
import pytest
from conftest import SharedProvider
from httpx_ws import AsyncWebSocketSession, WebSocketDisconnect, WebSocketUpgradeError, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sarathi.api.ws import SocketPrincipal, build_session_socket
from sarathi.auth import (
    LOGIN_PURPOSE,
    SOCKET_TICKET_AUDIENCE,
    SOCKET_TICKET_PURPOSE,
    create_socket_ticket,
    decode_socket_ticket,
)
from sarathi.config import get_settings
from sarathi.models import User
from tantra import (
    Approval,
    ApprovalResponse,
    FreeTextResponse,
    LocatedAsk,
    Sample,
    SessionHeader,
    SocketAccess,
    SocketAuth,
    SocketBinding,
    SocketLimits,
)
from tantra.events import AskRaised

Signup = Callable[..., Awaitable[str]]
NewSession = Callable[..., Awaitable[str]]
Socket = Callable[..., AbstractAsyncContextManager[AsyncWebSocketSession]]

RECEIVE_TIMEOUT = 5.0


def _canonical(value: str) -> str:
    return str(UUID(hex=value))


def _kind(frame: dict[str, Any]) -> str:
    if frame["type"] in ("event", "notice") and frame["body"] is not None:
        return str(frame["body"]["type"])
    return str(frame["type"])


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
    actor_id: str,
    *,
    after: int = 0,
    mode: str = "read",
) -> tuple[str, list[dict[str, Any]]]:
    subscription_id = str(uuid4())
    await _send(
        ws,
        {
            "type": "subscribe",
            "subscription_id": subscription_id,
            "actor_id": _canonical(actor_id),
            "after": after,
            "mode": mode,
        },
    )
    frames: list[dict[str, Any]] = []
    while True:
        frame = await _receive(ws)
        frames.append(frame)
        if frame["type"] == "ready" and frame["subscription_id"] == subscription_id:
            return subscription_id, frames


async def _uid(client: httpx.AsyncClient, token: str) -> str:
    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    return str(response.json()["id"])


@asynccontextmanager
async def _raw_socket(
    app,
    session_id: str,
    ticket: str,
    *,
    origin: str | None = "http://localhost:3000",
    subprotocols: list[str] | None = None,
) -> AsyncIterator[AsyncWebSocketSession]:
    transport = ASGIWebSocketTransport(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Origin": origin} if origin is not None else None
        async with aconnect_ws(
            f"/api/ws/sessions/{session_id}?ticket={ticket}",
            client,
            keepalive_ping_interval_seconds=None,
            subprotocols=subprotocols or ["tantra.session.v1"],
            headers=headers,
        ) as ws:
            yield ws


async def test_ticket_defaults_to_write_and_cannot_authenticate_http(
    client: httpx.AsyncClient,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)

    response = await client.post(
        f"/api/sessions/{session_id}/socket-ticket",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    ticket = decode_socket_ticket(body["ticket"])
    assert ticket.root_id.hex == session_id
    assert ticket.user_id == body["user_id"]
    assert ticket.capability == "write"
    assert datetime.fromisoformat(body["expires_at"]) <= datetime.fromisoformat(body["login_expires_at"])
    denied = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {body['ticket']}"})
    assert denied.status_code == 401


async def test_ticket_requires_an_owned_persisted_root(
    client: httpx.AsyncClient,
    resources: Any,
    signup: Signup,
    new_session: NewSession,
) -> None:
    owner = await signup("owner@example.com")
    stranger = await signup("stranger@example.com")
    root_id = await new_session(owner)
    child_id = uuid4().hex
    await resources.store.create(
        SessionHeader(
            id=child_id,
            root_id=root_id,
            parent_id=root_id,
            agent="sarathi",
            metadata={"user": await _uid(client, owner)},
        )
    )

    headers = {"Authorization": f"Bearer {stranger}"}
    assert (await client.post(f"/api/sessions/{root_id}/socket-ticket", json={}, headers=headers)).status_code == 404
    owner_headers = {"Authorization": f"Bearer {owner}"}
    assert (
        await client.post(f"/api/sessions/{child_id}/socket-ticket", json={}, headers=owner_headers)
    ).status_code == 404


def test_ticket_rejects_tampering_expiry_and_wrong_purpose() -> None:
    now = datetime.now(UTC)
    ticket, _ = create_socket_ticket(1, uuid4(), "write", now + timedelta(minutes=5))
    tampered = ticket[:-1] + ("a" if ticket[-1] != "a" else "b")
    with pytest.raises(jwt.PyJWTError):
        decode_socket_ticket(tampered)

    base = {
        "aud": SOCKET_TICKET_AUDIENCE,
        "purpose": SOCKET_TICKET_PURPOSE,
        "root_id": str(uuid4()),
        "user_id": "1",
        "capability": "write",
        "iat": int((now - timedelta(minutes=2)).timestamp()),
        "exp": int((now - timedelta(minutes=1)).timestamp()),
        "login_exp": int((now + timedelta(minutes=1)).timestamp()),
    }
    expired = jwt.encode(base, get_settings().SECRET_KEY, algorithm="HS256")
    with pytest.raises(jwt.ExpiredSignatureError):
        decode_socket_ticket(expired)
    wrong = jwt.encode(
        {**base, "exp": int((now + timedelta(minutes=1)).timestamp()), "purpose": LOGIN_PURPOSE},
        get_settings().SECRET_KEY,
        algorithm="HS256",
    )
    with pytest.raises(jwt.InvalidTokenError):
        decode_socket_ticket(wrong)


async def test_ticket_is_shortened_to_original_login_expiry(
    client: httpx.AsyncClient,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)
    user_id = await _uid(client, token)
    now = datetime.now(UTC)
    login_expiry = now + timedelta(seconds=5)
    short_login = jwt.encode(
        {
            "sub": user_id,
            "purpose": LOGIN_PURPOSE,
            "iat": int(now.timestamp()),
            "exp": int(login_expiry.timestamp()),
        },
        get_settings().SECRET_KEY,
        algorithm="HS256",
    )

    response = await client.post(
        f"/api/sessions/{session_id}/socket-ticket",
        json={"mode": "read"},
        headers={"Authorization": f"Bearer {short_login}"},
    )

    assert response.status_code == 200
    assert datetime.fromisoformat(response.json()["expires_at"]) == datetime.fromtimestamp(
        int(login_expiry.timestamp()), UTC
    )


async def test_handshake_requires_ticket_origin_and_subprotocol(
    app,
    client: httpx.AsyncClient,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)
    issued = await client.post(
        f"/api/sessions/{session_id}/socket-ticket",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    ticket = issued.json()["ticket"]

    rejected = (WebSocketDisconnect, WebSocketUpgradeError, BaseExceptionGroup)
    with pytest.raises(rejected):
        async with _raw_socket(app, session_id, token):
            pass
    with pytest.raises(rejected):
        async with _raw_socket(app, session_id, ticket, origin=None):
            pass
    with pytest.raises(rejected):
        async with _raw_socket(app, session_id, ticket, origin="https://denied.example"):
            pass
    with pytest.raises(rejected):
        async with _raw_socket(app, session_id, ticket, subprotocols=["other"]):
            pass


async def test_native_wire_streams_events_receipts_and_canonical_ids(
    socket: Socket,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(reasoning="think", text="hello"), Sample(text="A title")])
    token = await signup()
    session_id = await new_session(token)

    async with socket(session_id, token) as ws:
        subscription_id, replay = await _subscribe(ws, session_id, mode="write")
        command_id = str(uuid4())
        await _send(
            ws,
            {
                "type": "send",
                "subscription_id": subscription_id,
                "command_id": command_id,
                "input": {"text": "hi", "attachments": []},
            },
        )
        accepted = await _until(ws, "receipt")
        frames = accepted
        if "turn_completed" not in [_kind(frame) for frame in frames]:
            frames.extend(await _until(ws, "turn_completed"))

    assert replay[-1]["actor_id"] == _canonical(session_id)
    assert replay[-1]["mode"] == "write"
    assert accepted[-1]["command_id"] == command_id
    events = [frame for frame in frames if frame["type"] == "event"]
    assert all(frame["actor_id"] == _canonical(session_id) for frame in events)
    assert "reasoning_delta" in [_kind(frame) for frame in events]
    assert "text_delta" in [_kind(frame) for frame in events]


async def test_read_ticket_cannot_acquire_writer(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)

    async with socket(session_id, token, view="readonly") as ws:
        subscription_id, _ = await _subscribe(ws, session_id)
        await _send(ws, {"type": "acquire_writer", "subscription_id": subscription_id})
        denied = (await _until(ws, "error"))[-1]

    assert denied["code"] == "forbidden"
    assert denied["subscription_id"] == subscription_id


async def test_attachment_paths_are_scoped_to_the_authenticated_user(
    socket: Socket,
    signup: Signup,
    new_session: NewSession,
    upload_dir,
) -> None:
    token = await signup()
    session_id = await new_session(token)

    async with socket(session_id, token) as ws:
        subscription_id, _ = await _subscribe(ws, session_id, mode="write")
        command_id = str(uuid4())
        await _send(
            ws,
            {
                "type": "send",
                "subscription_id": subscription_id,
                "command_id": command_id,
                "input": {
                    "text": "read",
                    "attachments": [{"path": str(upload_dir / "other" / "x.pdf"), "name": "x.pdf"}],
                },
            },
        )
        denied = (await _until(ws, "error"))[-1]

    assert denied["code"] == "forbidden"
    assert denied["command_id"] == command_id


async def test_policy_requires_root_typed_recognized_approval(
    resources: Any,
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)
    user_id = await _uid(client, token)
    root = await resources.store.header(session_id)
    assert root is not None
    endpoint = build_session_socket(resources, session_factory)

    async def valid() -> bool:
        return True

    binding = SocketBinding(
        UUID(hex=session_id),
        SocketAuth(
            principal=SocketPrincipal(user_id, "write"),
            submitted_by=user_id,
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            revalidate=valid,
        ),
    )
    response = ApprovalResponse(allow=True)

    def access(extra: dict[str, Any], response_value=response) -> SocketAccess:
        ask = LocatedAsk(
            UUID(hex=session_id),
            1,
            AskRaised(ask_id=uuid4().hex, request=Approval(title="Confirm", extra=extra)),
        )
        return SocketAccess("answer", binding, root, root, response=response_value, ask=ask)

    arguments = {"kind": "preference", "title": "Drink", "body": "Tea"}
    assert await endpoint.policy(access({"permission": "memory_write", "arguments": arguments})) is True
    assert await endpoint.policy(access({"arguments": arguments})) is False
    assert await endpoint.policy(access({"permission": "unknown", "arguments": arguments})) is False
    unknown_argument = {"permission": "memory_write", "arguments": {**arguments, "extra": 1}}
    assert await endpoint.policy(access(unknown_argument)) is False
    assert (
        await endpoint.policy(
            access({"permission": "memory_write", "arguments": arguments}, FreeTextResponse(text="forged"))
        )
        is False
    )


async def test_title_finishes_after_the_submitting_socket_disconnects(
    socket: Socket,
    resources: Any,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="keeps running"), Sample(text="Independent title")])
    provider.gate.clear()
    token = await signup()
    session_id = await new_session(token)

    async with socket(session_id, token) as ws:
        subscription_id, _ = await _subscribe(ws, session_id, mode="write")
        await _send(
            ws,
            {
                "type": "send",
                "subscription_id": subscription_id,
                "command_id": str(uuid4()),
                "input": {"text": "name this", "attachments": []},
            },
        )
        await _until(ws, "receipt")

    provider.gate.set()

    async def titled() -> None:
        while (header := await resources.store.header(session_id)) is None or header.title is None:
            await asyncio.sleep(0)

    await asyncio.wait_for(titled(), 5)
    assert (await resources.store.header(session_id)).title == "Independent title"


async def test_cancelled_first_turn_still_generates_a_title(
    socket: Socket,
    resources: Any,
    provider: SharedProvider,
    signup: Signup,
    new_session: NewSession,
) -> None:
    provider.samples.extend([Sample(text="blocked"), Sample(text="Cancelled title")])
    provider.gate.clear()
    token = await signup()
    session_id = await new_session(token)

    async with socket(session_id, token) as ws:
        subscription_id, _ = await _subscribe(ws, session_id, mode="write")
        await _send(
            ws,
            {
                "type": "send",
                "subscription_id": subscription_id,
                "command_id": str(uuid4()),
                "input": {"text": "cancel me", "attachments": []},
            },
        )
        started = await _until(ws, "text_delta")
        if not any(frame["type"] == "receipt" and frame["operation"] == "send" for frame in started):
            await _until(ws, "receipt")
        cancel_id = str(uuid4())
        await _send(
            ws,
            {
                "type": "cancel",
                "subscription_id": subscription_id,
                "command_id": cancel_id,
            },
        )
        cancelled = await _until(ws, "receipt")
        assert cancelled[-1]["operation"] == "cancel"
        assert cancelled[-1]["command_id"] == cancel_id

    provider.gate.set()

    async def titled() -> None:
        while (header := await resources.store.header(session_id)) is None or header.title is None:
            await asyncio.sleep(0)

    await asyncio.wait_for(titled(), 5)
    assert (await resources.store.header(session_id)).title == "Cancelled title"


async def test_periodic_revalidation_closes_after_user_deletion(
    app,
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)
    user_id = int(await _uid(client, token))
    issued = await client.post(
        f"/api/sessions/{session_id}/socket-ticket",
        json={"mode": "read"},
        headers={"Authorization": f"Bearer {token}"},
    )
    route = app.router.routes[-1]
    route.app.limits = SocketLimits(revalidate_interval=0.01)

    async with _raw_socket(app, session_id, issued.json()["ticket"]) as ws:
        await _subscribe(ws, session_id)
        async with session_factory() as db:
            user = await db.get(User, user_id)
            assert user is not None
            await db.delete(user)
            await db.commit()
        with pytest.raises(WebSocketDisconnect) as closed:
            await ws.receive_text(timeout=2)

    assert closed.value.code == 1008


async def test_header_notices_include_current_and_changed_state(
    resources: Any,
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    signup: Signup,
    new_session: NewSession,
) -> None:
    token = await signup()
    session_id = await new_session(token)
    user_id = int(await _uid(client, token))
    login_expiry = datetime.now(UTC) + timedelta(minutes=5)
    ticket, _ = create_socket_ticket(user_id, UUID(hex=session_id), "read", login_expiry)
    update = asyncio.Event()

    class Coordinator:
        async def watch(self, actor_id: str):
            await update.wait()
            yield SimpleNamespace(kind="header", actor_id=actor_id)

    previous = resources.runtime.coordinator
    resources.runtime.coordinator = Coordinator()
    try:
        endpoint = build_session_socket(resources, session_factory)
        binding = await endpoint.bind(
            {"query_string": f"ticket={ticket}".encode(), "path_params": {"session_id": session_id}}
        )
        notices = endpoint.notices(binding)

        assert await anext(notices) == {"type": "header_updated", "title": None, "model": "test-model"}
        await resources.store.patch_header(session_id, model="other-model")
        update.set()
        assert await anext(notices) == {"type": "header_updated", "title": None, "model": "other-model"}
        await notices.aclose()
    finally:
        resources.runtime.coordinator = previous
