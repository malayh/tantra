from __future__ import annotations

import asyncio
import itertools
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from tantra.acp.transport import Transport

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
REQUEST_CANCELLED = -32800
RESOURCE_NOT_FOUND = -32002

MESSAGES = {
    PARSE_ERROR: "Parse error",
    INVALID_REQUEST: "Invalid request",
    METHOD_NOT_FOUND: "Method not found",
    INVALID_PARAMS: "Invalid params",
    INTERNAL_ERROR: "Internal error",
    REQUEST_CANCELLED: "Request cancelled",
    RESOURCE_NOT_FOUND: "Resource not found",
}
CANCEL_REQUEST = "$/cancel_request"

Handler = Callable[[Any], Awaitable[Any]]
logger = logging.getLogger(__name__)


class RpcError(Exception):
    def __init__(self, code: int, message: str | None = None, data: Any = None) -> None:
        self.code = code
        self.message = message or MESSAGES.get(code, "Error")
        self.data = data
        super().__init__(self.message)


@dataclass(frozen=True)
class Reply:
    result: Any
    after: Callable[[], Awaitable[None]] | None = None


def _reject_constant(value: str) -> Any:
    raise ValueError(f"invalid JSON constant {value}")


def _valid_id(value: Any) -> bool:
    return value is None or type(value) in (int, str)


def _error(request_id: Any, code: int, message: str | None = None, data: Any = None) -> dict[str, Any]:
    body: dict[str, Any] = {"code": code, "message": message or MESSAGES.get(code, "Error")}
    if data is not None:
        body["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": body}


class Peer:
    def __init__(self, transport: Transport, handlers: Mapping[str, Handler]) -> None:
        self.transport = transport
        self.handlers = dict(handlers)
        self.closed = False
        self._ids = itertools.count()
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._incoming: dict[Any, asyncio.Task[Any]] = {}
        self._cancelled: set[Any] = set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._lock = asyncio.Lock()

    async def run(self) -> None:
        try:
            while (frame := await self.transport.receive()) is not None:
                try:
                    message = json.loads(frame, parse_constant=_reject_constant)
                except (ValueError, RecursionError):
                    self._spawn(self._reply(_error(None, PARSE_ERROR), None))
                    continue
                if isinstance(message, list):
                    self._spawn(self._batch(message))
                elif not self._inline(message):
                    self._spawn(self._single(message), message)
        finally:
            self.closed = True
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(ConnectionError("peer closed"))
            self._pending.clear()
            tasks = list(self._tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def send_request(self, method: str, params: Any = None) -> Any:
        if self.closed:
            raise ConnectionError("peer closed")
        request_id = next(self._ids)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        try:
            await self._write(message)
            return await future
        except asyncio.CancelledError:
            if self._pending.pop(request_id, None) is not None and not self.closed:
                self._spawn(self._quiet(CANCEL_REQUEST, {"requestId": request_id}))
            raise
        finally:
            self._pending.pop(request_id, None)

    async def send_notification(self, method: str, params: Any = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        await self._write(message)

    async def _write(self, message: Any) -> None:
        frame = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._lock:
            await self.transport.send(frame)

    async def _quiet(self, method: str, params: Any) -> None:
        try:
            await self.send_notification(method, params)
        except ConnectionError:
            pass

    def _spawn(self, coroutine: Awaitable[Any], message: Any = None) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        request_id = message.get("id") if isinstance(message, dict) and "method" in message else None
        tracked = isinstance(message, dict) and "method" in message and "id" in message and _valid_id(request_id)
        if tracked:
            self._incoming[request_id] = task

        def finished(done: asyncio.Task[Any]) -> None:
            self._tasks.discard(done)
            if tracked and self._incoming.get(request_id) is done:
                del self._incoming[request_id]
                self._cancelled.discard(request_id)
            if not done.cancelled() and done.exception() is not None:
                logger.error("JSON-RPC task failed", exc_info=done.exception())

        task.add_done_callback(finished)
        return task

    def _forget(self, request_id: Any) -> None:
        if self._incoming.get(request_id) is asyncio.current_task():
            del self._incoming[request_id]
            self._cancelled.discard(request_id)

    def _inline(self, message: Any) -> bool:
        if not isinstance(message, dict):
            return False
        if "method" not in message and "id" in message and ("result" in message or "error" in message):
            self._resolve(message)
            return True
        if message.get("method") == CANCEL_REQUEST and "id" not in message:
            params = message.get("params")
            request_id = params.get("requestId") if isinstance(params, dict) else None
            task = self._incoming.get(request_id) if _valid_id(request_id) else None
            if task is not None and not task.done():
                self._cancelled.add(request_id)
                task.cancel()
            return True
        return False

    def _resolve(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        future = self._pending.pop(request_id, None) if type(request_id) is int else None
        if future is None or future.done():
            return
        if "error" not in message:
            future.set_result(message.get("result"))
            return
        error = message["error"]
        code = error.get("code") if isinstance(error, dict) else None
        text = error.get("message") if isinstance(error, dict) else None
        future.set_exception(
            RpcError(
                code if type(code) is int else INTERNAL_ERROR,
                text if isinstance(text, str) else None,
                error.get("data") if isinstance(error, dict) else None,
            )
        )

    async def _single(self, message: Any) -> None:
        response, after = await self._handle(message)
        await self._reply(response, after)

    async def _batch(self, messages: list[Any]) -> None:
        if not messages:
            await self._reply(_error(None, INVALID_REQUEST), None)
            return
        tasks = [self._spawn(self._handle(message), message) for message in messages if not self._inline(message)]
        results = await asyncio.gather(*tasks)
        responses = [response for response, _ in results if response is not None]
        try:
            if responses:
                await self._write(responses)
        except ConnectionError:
            return
        for _, after in results:
            if after is not None:
                self._spawn(self._after(after))

    async def _reply(self, response: dict[str, Any] | None, after: Callable[[], Awaitable[None]] | None) -> None:
        try:
            if response is not None:
                try:
                    await self._write(response)
                except (TypeError, ValueError):
                    logger.exception("JSON-RPC response could not be encoded")
                    await self._write(_error(response.get("id"), INTERNAL_ERROR))
                    return
        except ConnectionError:
            return
        if after is not None:
            self._spawn(self._after(after))

    async def _after(self, after: Callable[[], Awaitable[None]]) -> None:
        try:
            await after()
        except ConnectionError:
            pass
        except Exception:
            logger.exception("JSON-RPC follow-up failed")

    async def _handle(self, message: Any) -> tuple[dict[str, Any] | None, Callable[[], Awaitable[None]] | None]:
        if not isinstance(message, dict) or not _valid_id(message.get("id")):
            return _error(None, INVALID_REQUEST), None
        method = message.get("method")
        params = message.get("params")
        has_id = "id" in message
        request_id = message.get("id")
        if message.get("jsonrpc") != "2.0" or not isinstance(method, str) or not isinstance(params, dict | list | None):
            return _error(request_id, INVALID_REQUEST), None
        handler = self.handlers.get(method)
        if handler is None:
            return (_error(request_id, METHOD_NOT_FOUND) if has_id else None), None
        try:
            result = await handler(params)
        except asyncio.CancelledError:
            if not has_id or request_id not in self._cancelled or self.closed:
                raise
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
            self._forget(request_id)
            return _error(request_id, REQUEST_CANCELLED), None
        except RpcError as exc:
            self._forget(request_id)
            return (_error(request_id, exc.code, exc.message, exc.data) if has_id else None), None
        except Exception:
            self._forget(request_id)
            logger.exception("JSON-RPC handler for %s failed", method)
            return (_error(request_id, INTERNAL_ERROR) if has_id else None), None
        self._forget(request_id)
        after = None
        if isinstance(result, Reply):
            result, after = result.result, result.after
        return ({"jsonrpc": "2.0", "id": request_id, "result": result} if has_id else None), after
