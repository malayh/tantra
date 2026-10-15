from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from tantra.acp.server import AcpAuth, AcpServer, Bind
from tantra.acp.transport import MAX_FRAME

SEND_TIMEOUT = 10.0

ASGIReceive = Callable[[], Awaitable[Mapping[str, Any]]]
ASGISend = Callable[[Mapping[str, Any]], Awaitable[None]]
logger = logging.getLogger(__name__)


class _Denied(Exception):
    def __init__(self, code: int, reason: str) -> None:
        self.code = code
        self.reason = reason
        super().__init__(reason)


class WebSocketTransport:
    def __init__(self, receive: ASGIReceive, send: ASGISend) -> None:
        self._receive = receive
        self._send = send
        self.code: int | None = None
        self.reason = ""
        self.disconnected = False
        self._closed = asyncio.Event()
        self._lock = asyncio.Lock()

    async def receive(self) -> str | None:
        while not self._closed.is_set():
            incoming = asyncio.ensure_future(self._receive())
            closed = asyncio.ensure_future(self._closed.wait())
            try:
                await asyncio.wait((incoming, closed), return_when=asyncio.FIRST_COMPLETED)
            finally:
                incoming.cancel()
                closed.cancel()
            if not incoming.done():
                return None
            message = incoming.result()
            if message["type"] == "websocket.disconnect":
                self.disconnected = True
                return None
            text = message.get("text")
            if text is None:
                continue
            if len(text.encode()) > MAX_FRAME:
                await self.close(1009, "message too big")
                return None
            return text
        return None

    async def send(self, frame: str) -> None:
        async with self._lock:
            if self.code is not None or self.disconnected:
                raise ConnectionError("transport is closed")
            try:
                async with asyncio.timeout(SEND_TIMEOUT):
                    await self._send({"type": "websocket.send", "text": frame})
            except TimeoutError:
                await self.close(1013, "send timeout")
                raise ConnectionError("send timed out") from None
            except (OSError, RuntimeError) as exc:
                self.disconnected = True
                self._closed.set()
                raise ConnectionError("client disconnected") from exc

    async def close(self, code: int = 1011, reason: str = "internal error") -> None:
        if self.code is None:
            self.code, self.reason = code, reason
            self._closed.set()

    async def finish(self) -> None:
        with contextlib.suppress(OSError, RuntimeError):
            async with asyncio.timeout(SEND_TIMEOUT), self._lock:
                if self.code is not None and not self.disconnected:
                    await self._send({"type": "websocket.close", "code": self.code, "reason": self.reason})


class AcpWebSocket:
    def __init__(
        self,
        server: AcpServer,
        *,
        bind: Bind,
        allowed_origins: Iterable[str],
        allow_missing_origin: bool = False,
    ) -> None:
        if not callable(bind):
            raise TypeError("bind must be callable")
        if isinstance(allowed_origins, str):
            raise ValueError("allowed_origins must be a collection of explicit origins")
        self.allowed_origins = frozenset(allowed_origins)
        if "*" in self.allowed_origins:
            raise ValueError("allowed_origins must contain explicit origins")
        self.server = server
        self.bind = bind
        self.allow_missing_origin = allow_missing_origin
        self._live: dict[WebSocketTransport, asyncio.Event] = {}
        self._closing = False

    async def __call__(self, scope: Mapping[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") != "websocket":
            raise ValueError("AcpWebSocket requires a websocket ASGI scope")
        if (await receive())["type"] != "websocket.connect":
            return
        try:
            auth = await self._admit(scope)
        except _Denied as denied:
            await send({"type": "websocket.close", "code": denied.code, "reason": denied.reason})
            return
        transport = WebSocketTransport(receive, send)
        self._live[transport] = done = asyncio.Event()
        expiry: asyncio.Task[None] | None = None
        try:
            try:
                await send({"type": "websocket.accept"})
            except OSError:
                return
            if auth.expires_at is not None:
                expiry = asyncio.create_task(self._expire(transport, auth.expires_at))
            await self.server.connect(transport, auth)
        except Exception:
            logger.exception("ACP WebSocket connection failed")
            await transport.close()
        finally:
            if expiry is not None:
                expiry.cancel()
            try:
                await transport.finish()
            finally:
                del self._live[transport]
                done.set()

    async def aclose(self) -> None:
        self._closing = True
        for transport in list(self._live):
            await transport.close(1012, "server shutdown")
        await asyncio.gather(*(done.wait() for done in list(self._live.values())))

    async def _admit(self, scope: Mapping[str, Any]) -> AcpAuth:
        if self._closing:
            raise _Denied(1012, "server shutdown")
        origins = [value.decode("latin-1") for name, value in scope.get("headers", []) if name.lower() == b"origin"]
        if len(origins) > 1 or origins and origins[0] not in self.allowed_origins:
            raise _Denied(1008, "origin denied")
        if not origins and not self.allow_missing_origin:
            raise _Denied(1008, "origin required")
        try:
            auth = await self.bind(scope)
        except PermissionError:
            raise _Denied(1008, "access denied") from None
        except Exception:
            logger.exception("ACP WebSocket bind failed")
            raise _Denied(1011, "internal error") from None
        if not isinstance(auth, AcpAuth) or auth.expires_at is not None and auth.expires_at <= datetime.now(UTC):
            raise _Denied(1008, "access denied")
        if self._closing:
            raise _Denied(1012, "server shutdown")
        return auth

    async def _expire(self, transport: WebSocketTransport, expires_at: datetime) -> None:
        await asyncio.sleep((expires_at - datetime.now(UTC)).total_seconds())
        await transport.close(1008, "authentication expired")
