from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
import uvicorn
from acp_support import VALIDATOR, RecordingTransport
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from tantra import Agent, Runtime
from tantra.acp import AcpAccess, AcpAuth, AcpServer, AcpWebSocket
from tantra.acp import websocket as acp_websocket
from tantra.acp.jsonrpc import Peer
from tantra.acp.transport import MAX_FRAME
from tantra.events import TurnCompleted
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore

ORIGIN = "https://app.example"
INITIALIZE = {"protocolVersion": 2, "info": {"name": "test", "version": "0"}, "capabilities": {}}


class Bot(Agent):
    pass


async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
    return {"user": "u"}


async def allow(access: AcpAccess) -> bool:
    return True


class Binder:
    def __init__(self, result: Any = None) -> None:
        self.result = AcpAuth(principal="user", submitted_by="user") if result is None else result
        self.calls = 0

    async def __call__(self, scope: Any) -> AcpAuth:
        self.calls += 1
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class Expiring(Binder):
    async def __call__(self, scope: Any) -> AcpAuth:
        self.calls += 1
        return AcpAuth(principal="user", expires_at=datetime.now(UTC) + timedelta(seconds=0.3))


class Gated(Binder):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, scope: Any) -> AcpAuth:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return self.result


def serve(provider: Any, binder: Binder | None = None, **options: Any) -> tuple[Runtime, AcpWebSocket, Binder]:
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    server = AcpServer(runtime, agent=Bot, scope=scope, policy=allow, name="bot")
    binder = binder or Binder()
    options.setdefault("allowed_origins", {ORIGIN})
    return runtime, server.websocket(bind=binder, **options), binder


class Socket:
    def __init__(self, headers: list[tuple[bytes, bytes]] | None = None) -> None:
        self.scope = {"type": "websocket", "headers": [(b"origin", ORIGIN.encode())] if headers is None else headers}
        self.inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.frames: asyncio.Queue[str | None] = asyncio.Queue()
        self.sent: list[dict[str, Any]] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.failure: BaseException | None = None
        self.inbox.put_nowait({"type": "websocket.connect"})

    async def receive(self) -> dict[str, Any]:
        return await self.inbox.get()

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)
        if self.failure is not None:
            raise self.failure
        await self.gate.wait()
        if message["type"] == "websocket.send":
            self.frames.put_nowait(message["text"])
        elif message["type"] == "websocket.close":
            self.frames.put_nowait(None)

    def text(self, frame: str) -> None:
        self.inbox.put_nowait({"type": "websocket.receive", "text": frame})

    def disconnect(self) -> None:
        self.inbox.put_nowait({"type": "websocket.disconnect", "code": 1001})
        self.frames.put_nowait(None)

    def closes(self) -> list[int]:
        return [message["code"] for message in self.sent if message["type"] == "websocket.close"]

    def accepted(self) -> bool:
        return any(message["type"] == "websocket.accept" for message in self.sent)


class SocketClient:
    def __init__(self, socket: Socket) -> None:
        self.socket = socket

    async def send(self, frame: str) -> None:
        self.socket.text(frame)

    async def receive(self) -> str | None:
        return await self.socket.frames.get()

    async def close(self) -> None:
        self.socket.disconnect()


class Wire:
    def __init__(self, endpoint: AcpWebSocket, socket: Socket | None = None) -> None:
        self.socket = socket or Socket()
        self.updates: list[dict[str, Any]] = []
        self.peer = Peer(SocketClient(self.socket), {"session/update": self._update})
        self.task = asyncio.create_task(endpoint(self.socket.scope, self.socket.receive, self.socket.send))
        self.reader = asyncio.create_task(self.peer.run())

    async def _update(self, params: dict[str, Any]) -> None:
        self.updates.append(params)

    async def call(self, method: str, params: Any = None) -> Any:
        return await asyncio.wait_for(self.peer.send_request(method, params), timeout=5)

    async def finished(self) -> None:
        await asyncio.wait_for(self.task, timeout=5)
        self.socket.frames.put_nowait(None)
        await asyncio.wait_for(self.reader, timeout=5)

    def of(self, kind: str) -> list[dict[str, Any]]:
        return [params["update"] for params in self.updates if params["update"]["sessionUpdate"] == kind]


async def eventually(condition: Callable[[], Any], timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def test_allowed_origin_is_accepted_without_a_subprotocol() -> None:
    runtime, endpoint, binder = serve(FakeProvider([]))
    wire = Wire(endpoint)
    result = await wire.call("initialize", INITIALIZE)
    assert result["protocolVersion"] == 2
    assert wire.socket.sent[0] == {"type": "websocket.accept"}
    assert binder.calls == 1
    wire.socket.disconnect()
    await wire.finished()
    assert wire.socket.closes() == []
    await runtime.aclose()


@pytest.mark.parametrize(
    "headers",
    [
        [(b"origin", b"https://evil.example")],
        [(b"origin", ORIGIN.encode()), (b"Origin", ORIGIN.encode())],
        [],
    ],
    ids=["wrong", "duplicate", "missing"],
)
async def test_origin_denials_close_before_accept_without_calling_bind(headers: list[tuple[bytes, bytes]]) -> None:
    runtime, endpoint, binder = serve(FakeProvider([]))
    socket = Socket(headers)
    await asyncio.wait_for(endpoint(socket.scope, socket.receive, socket.send), timeout=5)
    assert [message["type"] for message in socket.sent] == ["websocket.close"]
    assert socket.closes() == [1008]
    assert binder.calls == 0
    await runtime.aclose()


async def test_missing_origin_is_accepted_when_allowed() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]), allow_missing_origin=True)
    wire = Wire(endpoint, Socket([]))
    assert (await wire.call("initialize", INITIALIZE))["protocolVersion"] == 2
    wire.socket.disconnect()
    await wire.finished()
    await runtime.aclose()


@pytest.mark.parametrize(
    "result",
    [
        PermissionError("denied"),
        "not auth",
        AcpAuth(principal="user", expires_at=datetime.now(UTC) - timedelta(seconds=1)),
    ],
    ids=["permission", "not-auth", "expired"],
)
async def test_bind_denials_close_1008_before_accept(result: Any) -> None:
    runtime, endpoint, binder = serve(FakeProvider([]), Binder(result))
    socket = Socket()
    await asyncio.wait_for(endpoint(socket.scope, socket.receive, socket.send), timeout=5)
    assert [message["type"] for message in socket.sent] == ["websocket.close"]
    assert socket.closes() == [1008]
    assert binder.calls == 1
    await runtime.aclose()


async def test_bind_failure_closes_1011_before_accept_and_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    runtime, endpoint, _ = serve(FakeProvider([]), Binder(RuntimeError("secret detail")))
    socket = Socket()
    with caplog.at_level(logging.ERROR, logger="tantra.acp.websocket"):
        await asyncio.wait_for(endpoint(socket.scope, socket.receive, socket.send), timeout=5)
    assert socket.sent == [{"type": "websocket.close", "code": 1011, "reason": "internal error"}]
    assert any(record.exc_info and "secret detail" in str(record.exc_info[1]) for record in caplog.records)
    await runtime.aclose()


async def test_allowed_origins_and_bind_are_validated() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    server = AcpServer(runtime, agent=Bot, scope=scope, policy=allow, name="bot")
    for origins in ("https://x", {"*"}, {ORIGIN, "*"}):
        with pytest.raises(ValueError):
            server.websocket(bind=Binder(), allowed_origins=origins)
    with pytest.raises(TypeError):
        server.websocket(bind=None, allowed_origins={ORIGIN})
    await runtime.aclose()


def padded(size: int) -> str:
    prefix, suffix = '{"jsonrpc":"2.0","id":1,"method":"_pad","params":{"pad":"', '"}}'
    room = size - len(prefix) - len(suffix)
    return prefix + "é" * (room // 2) + "a" * (room % 2) + suffix


async def test_frame_limit_counts_utf8_bytes() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    socket = Socket()
    task = asyncio.create_task(endpoint(socket.scope, socket.receive, socket.send))
    exact = padded(MAX_FRAME)
    assert len(exact.encode()) == MAX_FRAME
    socket.text(exact)
    reply = json.loads(await asyncio.wait_for(socket.frames.get(), timeout=5))
    assert reply["id"] == 1 and reply["error"]["code"] == -32601
    over = padded(MAX_FRAME + 1)
    assert len(over.encode()) == MAX_FRAME + 1 and len(over) < MAX_FRAME
    socket.text(over)
    await asyncio.wait_for(task, timeout=5)
    assert socket.closes() == [1009]
    await runtime.aclose()


async def test_a_stalled_send_closes_1013(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(acp_websocket, "SEND_TIMEOUT", 0.1)
    runtime, endpoint, _ = serve(FakeProvider([]))
    socket = Socket()
    task = asyncio.create_task(endpoint(socket.scope, socket.receive, socket.send))
    await eventually(socket.accepted)
    socket.gate.clear()
    socket.text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": INITIALIZE}))
    await asyncio.wait_for(task, timeout=5)
    assert [message["type"] for message in socket.sent] == ["websocket.accept", "websocket.send", "websocket.close"]
    assert socket.closes() == [1013]
    await runtime.aclose()


async def test_expiry_closes_1008_after_accept() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]), Expiring())
    socket = Socket()
    await asyncio.wait_for(endpoint(socket.scope, socket.receive, socket.send), timeout=5)
    assert socket.sent == [
        {"type": "websocket.accept"},
        {"type": "websocket.close", "code": 1008, "reason": "authentication expired"},
    ]
    await runtime.aclose()


async def test_binary_frames_are_ignored() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    wire = Wire(endpoint)
    wire.socket.inbox.put_nowait({"type": "websocket.receive", "bytes": b'{"jsonrpc":"2.0","id":9,"method":"x"}'})
    assert (await wire.call("initialize", INITIALIZE))["protocolVersion"] == 2
    assert [message["type"] for message in wire.socket.sent] == ["websocket.accept", "websocket.send"]
    wire.socket.disconnect()
    await wire.finished()
    assert wire.socket.closes() == []
    await runtime.aclose()


async def test_a_disconnect_leaves_the_turn_running(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="survives disconnect")])
    provider.gate.clear()
    runtime, endpoint, _ = serve(provider)
    wire = Wire(endpoint)
    await wire.call("initialize", INITIALIZE)
    session_id = (await wire.call("session/new", {"cwd": "/work", "mcpServers": []}))["sessionId"]
    prompt = {"sessionId": session_id, "prompt": [{"type": "text", "text": "work"}]}
    await wire.call("session/prompt", prompt)
    await eventually(lambda: wire.of("agent_message_chunk"))
    wire.socket.disconnect()
    await wire.finished()
    assert wire.socket.closes() == []
    provider.gate.set()

    async def completed() -> bool:
        events = [item.event for item in await runtime.store.read_page(UUID(session_id).hex)]
        return any(isinstance(event, TurnCompleted) for event in events)

    async def poll() -> None:
        while not await completed():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout=5)
    await runtime.aclose()


SEND_FAILURES = [
    OSError("client disconnected"),
    RuntimeError("Unexpected ASGI message 'websocket.send', after sending 'websocket.close'."),
]


@pytest.mark.parametrize("failure", SEND_FAILURES, ids=["oserror", "runtimeerror"])
async def test_an_asgi_send_error_ends_the_connection_without_a_close_frame(failure: BaseException) -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    wire = Wire(endpoint)
    await wire.call("initialize", INITIALIZE)
    wire.socket.failure = failure
    request = asyncio.ensure_future(wire.peer.send_request("session/list", {}))
    await wire.finished()
    with pytest.raises(ConnectionError):
        await request
    assert [message["type"] for message in wire.socket.sent] == ["websocket.accept", "websocket.send", "websocket.send"]
    await asyncio.wait_for(endpoint.aclose(), timeout=5)
    await runtime.aclose()


@pytest.mark.parametrize("failure", SEND_FAILURES, ids=["oserror", "runtimeerror"])
async def test_a_failing_close_frame_still_lets_aclose_return(failure: BaseException) -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    wire = Wire(endpoint)
    await wire.call("initialize", INITIALIZE)
    wire.socket.failure = failure
    await asyncio.wait_for(endpoint.aclose(), timeout=5)
    await wire.finished()
    assert wire.socket.sent[-1] == {"type": "websocket.close", "code": 1012, "reason": "server shutdown"}
    await runtime.aclose()


async def test_a_failed_accept_returns_without_connecting(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    connected: list[Any] = []

    async def connect(transport: Any, auth: AcpAuth) -> None:
        connected.append(auth)

    monkeypatch.setattr(endpoint.server, "connect", connect)
    socket = Socket()
    socket.failure = OSError("client disconnected")
    await asyncio.wait_for(endpoint(socket.scope, socket.receive, socket.send), timeout=5)
    assert socket.sent == [{"type": "websocket.accept"}]
    assert connected == []
    await asyncio.wait_for(endpoint.aclose(), timeout=5)
    await runtime.aclose()


async def test_a_connection_oserror_is_logged_and_closes_1011(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))

    async def connect(transport: Any, auth: AcpAuth) -> None:
        raise OSError("store unavailable")

    monkeypatch.setattr(endpoint.server, "connect", connect)
    socket = Socket()
    with caplog.at_level(logging.ERROR, logger="tantra.acp.websocket"):
        await asyncio.wait_for(endpoint(socket.scope, socket.receive, socket.send), timeout=5)
    assert socket.sent == [
        {"type": "websocket.accept"},
        {"type": "websocket.close", "code": 1011, "reason": "internal error"},
    ]
    assert any(record.exc_info and "store unavailable" in str(record.exc_info[1]) for record in caplog.records)
    await runtime.aclose()


async def test_closing_the_runtime_closes_1011() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    wire = Wire(endpoint)
    await wire.call("initialize", INITIALIZE)
    await wire.call("session/new", {"cwd": "/work", "mcpServers": []})
    await runtime.aclose()
    await wire.finished()
    assert wire.socket.closes() == [1011]


async def test_endpoint_aclose_closes_live_sockets_1012_and_refuses_new_ones() -> None:
    runtime, endpoint, binder = serve(FakeProvider([]))
    wires = [Wire(endpoint), Wire(endpoint)]
    for wire in wires:
        await wire.call("initialize", INITIALIZE)
    await asyncio.wait_for(endpoint.aclose(), timeout=5)
    for wire in wires:
        assert wire.task.done()
        await wire.finished()
        assert wire.socket.sent[-1] == {"type": "websocket.close", "code": 1012, "reason": "server shutdown"}
    late = Socket()
    await asyncio.wait_for(endpoint(late.scope, late.receive, late.send), timeout=5)
    assert [message["type"] for message in late.sent] == ["websocket.close"]
    assert late.closes() == [1012]
    assert binder.calls == 2
    await asyncio.wait_for(endpoint.aclose(), timeout=5)
    await runtime.aclose()


async def test_aclose_during_bind_denies_the_handshake_1012() -> None:
    runtime, endpoint, binder = serve(FakeProvider([]), Gated())
    socket = Socket()
    task = asyncio.create_task(endpoint(socket.scope, socket.receive, socket.send))
    await asyncio.wait_for(binder.entered.wait(), timeout=5)
    await asyncio.wait_for(endpoint.aclose(), timeout=5)
    binder.release.set()
    await asyncio.wait_for(task, timeout=5)
    assert socket.sent == [{"type": "websocket.close", "code": 1012, "reason": "server shutdown"}]
    await runtime.aclose()


@contextlib.asynccontextmanager
async def serving(endpoint: AcpWebSocket) -> AsyncIterator[str]:
    app = Starlette(routes=[WebSocketRoute("/acp", endpoint)])
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, lifespan="off", log_level="warning"))
    task = asyncio.create_task(server.serve())
    try:
        await eventually(lambda: server.started or task.done())
        port = server.servers[0].sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}/acp"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10)


def dial(url: str, origin: str = ORIGIN) -> Any:
    return connect(url, origin=origin, proxy=None)


class Socketed:
    def __init__(self, connection: ClientConnection) -> None:
        self.connection = connection

    async def send(self, frame: str) -> None:
        try:
            await self.connection.send(frame)
        except ConnectionClosed as exc:
            raise ConnectionError("closed") from exc

    async def receive(self) -> str | None:
        try:
            frame = await self.connection.recv()
        except ConnectionClosed:
            return None
        return frame if isinstance(frame, str) else frame.decode()

    async def close(self) -> None:
        await self.connection.close()


async def closed_with(connection: ClientConnection) -> int | None:
    await asyncio.wait_for(connection.wait_closed(), timeout=5)
    return connection.close_code


async def test_uvicorn_end_to_end_prompt() -> None:
    runtime, endpoint, _ = serve(FakeProvider([Sample(text="over the wire")]))
    updates: list[dict[str, Any]] = []
    changed = asyncio.Event()

    async def update(params: dict[str, Any]) -> None:
        updates.append(params)
        changed.set()

    async with serving(endpoint) as url, dial(url) as connection:
        transport = RecordingTransport(Socketed(connection))
        peer = Peer(transport, {"session/update": update})
        reader = asyncio.create_task(peer.run())

        async def call(method: str, params: Any) -> Any:
            return await asyncio.wait_for(peer.send_request(method, params), timeout=5)

        assert (await call("initialize", INITIALIZE))["protocolVersion"] == 2
        session_id = (await call("session/new", {"cwd": "/work", "mcpServers": []}))["sessionId"]
        await call("session/prompt", {"sessionId": session_id, "prompt": [{"type": "text", "text": "hi"}]})

        def ended() -> bool:
            return any(
                params["update"].get("sessionUpdate") == "state_update"
                and params["update"].get("state") == "idle"
                and params["update"].get("stopReason") == "end_turn"
                for params in updates
            )

        async def wait() -> None:
            while not ended():
                changed.clear()
                await changed.wait()

        await asyncio.wait_for(wait(), timeout=5)
        await connection.close()
        await asyncio.wait_for(reader, timeout=5)
    methods = {frame["id"]: frame["method"] for frame in transport.sent if "method" in frame and "id" in frame}
    assert [problem for frame in transport.received for problem in VALIDATOR.frame_errors(frame, methods)] == []
    assert any(params["update"]["sessionUpdate"] == "agent_message" for params in updates)
    await runtime.aclose()


async def test_uvicorn_rejects_a_wrong_origin_with_403() -> None:
    runtime, endpoint, binder = serve(FakeProvider([]))
    async with serving(endpoint) as url:
        with pytest.raises(InvalidStatus) as rejected:
            async with dial(url, "https://evil.example"):
                pass
    assert rejected.value.response.status_code == 403
    assert binder.calls == 0
    await runtime.aclose()


async def test_uvicorn_client_sees_1009_for_an_oversized_frame() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    async with serving(endpoint) as url, dial(url) as connection:
        await connection.send("x" * (MAX_FRAME + 1))
        assert await closed_with(connection) == 1009
    await runtime.aclose()


async def test_uvicorn_client_sees_1012_on_endpoint_aclose() -> None:
    runtime, endpoint, _ = serve(FakeProvider([]))
    async with serving(endpoint) as url, dial(url) as connection:
        await asyncio.wait_for(endpoint.aclose(), timeout=5)
        assert await closed_with(connection) == 1012
    await runtime.aclose()
