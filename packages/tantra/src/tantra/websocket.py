from __future__ import annotations

import asyncio
import logging
import math
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from tantra._socket_protocol import (
    AcquireWriterFrame,
    AnswerFrame,
    CancelFrame,
    ErrorFrame,
    EventFrame,
    NoticeFrame,
    ReadyFrame,
    ReceiptFrame,
    SendFrame,
    ServerFrame,
    SubscribeFrame,
    UnsubscribedFrame,
    UnsubscribeFrame,
    WriterLostFrame,
    parse_frame,
)
from tantra.ask import AskResponse
from tantra.errors import (
    AskExpired,
    CommandTimeout,
    CoordinatorUnavailable,
    InvalidCommandReuse,
    LeaseLost,
    SessionNotFound,
    TantraError,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import LoggedEvent, SessionHeader, SubmittedBy
from tantra.runtime import Connection, LocatedAsk, Runtime

ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class SocketAuth:
    principal: Any
    revalidate: Callable[[], Awaitable[bool]]
    submitted_by: str | None = None
    expires_at: datetime | None = None
    revoked: asyncio.Event | None = None
    anonymous: bool = False

    def __post_init__(self) -> None:
        if self.principal is None and not self.anonymous:
            raise ValueError("anonymous access must be explicit")
        if not callable(self.revalidate):
            raise TypeError("revalidate is required")
        TypeAdapter(SubmittedBy | None).validate_python(self.submitted_by)
        if self.expires_at is not None and (self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None):
            raise ValueError("expires_at must be timezone-aware")


@dataclass(frozen=True)
class SocketBinding:
    root_id: UUID
    auth: SocketAuth

    def __post_init__(self) -> None:
        if not isinstance(self.root_id, UUID) or not isinstance(self.auth, SocketAuth):
            raise TypeError("root_id and auth are required")


@dataclass(frozen=True)
class SocketAccess:
    operation: Literal["subscribe", "acquire_writer", "send", "answer", "cancel"]
    binding: SocketBinding
    root: SessionHeader
    actor: SessionHeader
    input: Any = None
    response: AskResponse | None = None
    ask: LocatedAsk | None = None


@dataclass(frozen=True)
class SocketViewContext:
    binding: SocketBinding
    root: SessionHeader
    actor: SessionHeader
    after: int


class InputCodec[T]:
    def __init__(self, schema: Any, encode: Callable[[T], str]) -> None:
        self.adapter: TypeAdapter[T] = TypeAdapter(schema)
        self.encode = encode

    def validate(self, value: Any) -> tuple[T, str]:
        validated = self.adapter.validate_python(value, strict=True)
        encoded = self.encode(validated)
        if not isinstance(encoded, str):
            raise TypeError("input encoder must return a string")
        return validated, encoded


EventView = Callable[[LoggedEvent], Awaitable[Any]]
ViewFactory = Callable[[SocketViewContext], Awaitable[EventView]]
AccessPolicy = Callable[[SocketAccess], Awaitable[bool]]
NoticeFactory = Callable[[SocketBinding], AsyncIterator[Any]]


@dataclass(frozen=True, kw_only=True)
class SocketLimits:
    subscriptions: int = 8
    inbound_bytes: int = 256 * 1024
    outbound_bytes: int = 1024 * 1024
    queue_frames: int = 256
    queue_bytes: int = 4 * 1024 * 1024
    queue_timeout: float = 10.0
    revalidate_interval: float = 30.0

    def __post_init__(self) -> None:
        for value in (self.subscriptions, self.inbound_bytes, self.outbound_bytes, self.queue_frames, self.queue_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("socket capacity limits must be positive integers")
        if any(
            type(value) not in (int, float) or not math.isfinite(value) or value <= 0
            for value in (self.queue_timeout, self.revalidate_interval)
        ):
            raise ValueError("socket deadlines must be positive")


class _Close(Exception):
    def __init__(self, code: int, reason: str) -> None:
        self.code = code
        self.reason = reason


@dataclass(eq=False)
class _Subscription:
    id: UUID
    actor_id: UUID
    cursor: int
    queued_cursor: int
    view: EventView
    connection: Connection | None = None
    task: asyncio.Task[None] | None = None
    writable: bool = False
    ready: bool = False
    epoch: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _Outbound:
    frame: ServerFrame
    text: str
    size: int
    subscription: _Subscription | None = None
    epoch: int | None = None


class _Queue:
    def __init__(self, limits: SocketLimits) -> None:
        self.limits = limits
        self.items: deque[_Outbound] = deque()
        self.condition = asyncio.Condition()
        self.frames = 0
        self.bytes = 0

    async def put(self, item: _Outbound) -> None:
        if item.size > self.limits.outbound_bytes or item.size > self.limits.queue_bytes:
            raise _Close(1009, "outbound frame too large")
        try:
            async with asyncio.timeout(self.limits.queue_timeout):
                async with self.condition:
                    await self.condition.wait_for(
                        lambda: (
                            self.frames < self.limits.queue_frames and self.bytes + item.size <= self.limits.queue_bytes
                        )
                    )
                    self.items.append(item)
                    self.frames += 1
                    self.bytes += item.size
                    self.condition.notify_all()
        except TimeoutError:
            raise _Close(1013, "client is too slow") from None

    async def get(self) -> _Outbound:
        async with self.condition:
            await self.condition.wait_for(lambda: bool(self.items))
            return self.items.popleft()

    async def done(self, item: _Outbound) -> None:
        async with self.condition:
            self.frames -= 1
            self.bytes -= item.size
            self.condition.notify_all()


class SessionSocket:
    def __init__(
        self,
        runtime: Runtime,
        *,
        bind: Callable[[Mapping[str, Any]], Awaitable[SocketBinding]],
        policy: AccessPolicy,
        input_codec: InputCodec[Any],
        views: ViewFactory,
        allowed_origins: set[str] | frozenset[str],
        notices: NoticeFactory | None = None,
        allow_missing_origin: bool = False,
        limits: SocketLimits | None = None,
    ) -> None:
        if not all(callable(value) for value in (bind, policy, views)):
            raise TypeError("bind, policy, and views are required")
        if not isinstance(input_codec, InputCodec):
            raise TypeError("input_codec is required")
        if isinstance(allowed_origins, str) or "*" in allowed_origins:
            raise ValueError("allowed_origins must contain explicit origins")
        self.runtime = runtime
        self.bind = bind
        self.policy = policy
        self.input_codec = input_codec
        self.views = views
        self.notices = notices
        self.allowed_origins = frozenset(allowed_origins)
        self.allow_missing_origin = allow_missing_origin
        self.limits = limits or SocketLimits()

    async def __call__(self, scope: Mapping[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") != "websocket":
            raise ValueError("SessionSocket requires a websocket ASGI scope")
        if (await receive()).get("type") != "websocket.connect":
            return
        try:
            origins = [value.decode("latin-1") for name, value in scope.get("headers", []) if name.lower() == b"origin"]
            if len(origins) > 1 or origins and origins[0] not in self.allowed_origins:
                raise _Close(1008, "origin denied")
            if not origins and not self.allow_missing_origin:
                raise _Close(1008, "origin required")
            if "tantra.session.v1" not in scope.get("subprotocols", []):
                raise _Close(1002, "tantra.session.v1 required")
            async with asyncio.timeout(self.limits.queue_timeout):
                binding = await self.bind(scope)
            if not isinstance(binding, SocketBinding):
                raise _Close(1008, "authentication required")
            if not origins and binding.auth.anonymous:
                raise _Close(1008, "authenticated service client required")
            session = _SocketSession(self, binding, receive, send)
            await session.run()
        except _Close as closed:
            if closed.code != 1000:
                await send({"type": "websocket.close", "code": closed.code, "reason": closed.reason})
        except (PermissionError, ValidationError):
            await send({"type": "websocket.close", "code": 1008, "reason": "access denied"})
        except SessionNotFound:
            await send({"type": "websocket.close", "code": 4404, "reason": "session not found"})
        except Exception:
            logger.exception("session socket failed")
            await send({"type": "websocket.close", "code": 1011, "reason": "service unavailable"})


class _SocketSession:
    def __init__(self, endpoint: SessionSocket, binding: SocketBinding, receive: ASGIReceive, send: ASGISend) -> None:
        self.endpoint = endpoint
        self.runtime = endpoint.runtime
        self.binding = binding
        self.receive = receive
        self.send = send
        self.limits = endpoint.limits
        self.queue = _Queue(self.limits)
        self.subscriptions: dict[UUID, _Subscription] = {}
        self.tasks: set[asyncio.Task[None]] = set()
        self.stop: asyncio.Future[_Close] = asyncio.get_running_loop().create_future()
        self.auth_lock = asyncio.Lock()
        self.delivery = asyncio.Event()
        self.closing = False

    def _credentials(self) -> None:
        auth = self.binding.auth
        if auth.revoked is not None and auth.revoked.is_set():
            raise _Close(1008, "authentication revoked")
        if auth.expires_at is not None and datetime.now(UTC) >= auth.expires_at:
            raise _Close(1008, "authentication expired")

    async def _headers(self, actor_id: UUID) -> tuple[SessionHeader, SessionHeader]:
        root = await self.runtime.store.header(self.binding.root_id.hex)
        if root is None:
            raise SessionNotFound(self.binding.root_id.hex)
        if root.parent_id is not None:
            raise PermissionError("binding must be a root")
        actor = root if actor_id == self.binding.root_id else await self.runtime.store.header(actor_id.hex)
        if actor is None:
            raise SessionNotFound(actor_id.hex)
        ancestor = actor
        seen: set[str] = set()
        while ancestor.id != root.id:
            if ancestor.id in seen or ancestor.parent_id is None:
                raise PermissionError("actor outside bound tree")
            seen.add(ancestor.id)
            parent = await self.runtime.store.header(ancestor.parent_id)
            if parent is None:
                raise PermissionError("actor outside bound tree")
            ancestor = parent
        return root, actor

    async def _authorized(
        self,
        operation: Literal["subscribe", "acquire_writer", "send", "answer", "cancel"],
        actor_id: UUID,
        *,
        input: Any = None,
        response: AskResponse | None = None,
        ask: LocatedAsk | None = None,
    ) -> tuple[SessionHeader, SessionHeader]:
        root, actor = await self._headers(actor_id)
        async with asyncio.timeout(self.limits.queue_timeout):
            allowed = await self.endpoint.policy(
                SocketAccess(
                    operation,
                    self.binding,
                    root.model_copy(deep=True),
                    actor.model_copy(deep=True),
                    input,
                    response.model_copy(deep=True) if response is not None else None,
                    ask,
                )
            )
        self._credentials()
        if allowed is not True:
            raise PermissionError("access denied")
        return root, actor

    async def _authenticate(self, *, reads: bool = False) -> None:
        async with self.auth_lock:
            self.delivery.clear()
            try:
                self._credentials()
                async with asyncio.timeout(self.limits.queue_timeout):
                    valid = await self.binding.auth.revalidate()
                if valid is not True:
                    raise _Close(1008, "authentication denied")
                self._credentials()
                if reads:
                    await self._authorized("subscribe", self.binding.root_id)
                    for sub in list(self.subscriptions.values()):
                        if sub.actor_id != self.binding.root_id:
                            await self._authorized("subscribe", sub.actor_id)
            except BaseException as error:
                if not self.stop.done():
                    if isinstance(error, _Close):
                        closed = error
                    elif isinstance(error, SessionNotFound):
                        closed = _Close(4404, "session not found")
                    elif isinstance(error, PermissionError):
                        closed = _Close(1008, "access denied")
                    else:
                        closed = _Close(1011, "service unavailable")
                    self.stop.set_result(closed)
                raise
            else:
                self.delivery.set()

    def _spawn(self, coroutine: Awaitable[None], *, terminal: bool = False) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            self.tasks.discard(done)
            if done.cancelled() or self.closing:
                return
            error = done.exception()
            if error is not None:
                if isinstance(error, _Close):
                    closed = error
                elif isinstance(error, SessionNotFound):
                    closed = _Close(4404, "session not found")
                elif isinstance(error, PermissionError):
                    closed = _Close(1008, "access denied")
                else:
                    logger.error("session socket task failed", exc_info=error)
                    closed = _Close(1011, "service unavailable")
                if not self.stop.done():
                    self.stop.set_result(closed)
            elif terminal and not self.stop.done():
                self.stop.set_result(_Close(1000, "disconnected"))

        task.add_done_callback(finished)
        return task

    async def run(self) -> None:
        self._spawn(self._expiry())
        if self.binding.auth.revoked is not None:
            self._spawn(self._revocation())
        try:
            authentication = self._spawn(self._authenticate(reads=True))
            done, _ = await asyncio.wait([authentication, self.stop], return_when=asyncio.FIRST_COMPLETED)
            if self.stop in done:
                raise self.stop.result()
            await authentication
            await self.send({"type": "websocket.accept", "subprotocol": "tantra.session.v1"})
            self._spawn(self._sender())
            self._spawn(self._reader(), terminal=True)
            self._spawn(self._periodic_auth())
            if self.endpoint.notices is not None:
                self._spawn(self._notices())
            raise await self.stop
        finally:
            self.closing = True
            self.delivery.clear()
            tasks = list(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(
                *(self._close_subscription(sub) for sub in list(self.subscriptions.values())), return_exceptions=True
            )

    async def _expiry(self) -> None:
        expires = self.binding.auth.expires_at
        if expires is None:
            await asyncio.Future()
        else:
            await asyncio.sleep(max(0.0, (expires - datetime.now(UTC)).total_seconds()))
            raise _Close(1008, "authentication expired")

    async def _revocation(self) -> None:
        assert self.binding.auth.revoked is not None
        await self.binding.auth.revoked.wait()
        raise _Close(1008, "authentication revoked")

    async def _periodic_auth(self) -> None:
        while True:
            await asyncio.sleep(self.limits.revalidate_interval)
            await self._authenticate(reads=True)
            for sub in list(self.subscriptions.values()):
                if sub.writable:
                    try:
                        await self._authorized("acquire_writer", sub.actor_id)
                    except PermissionError:
                        async with sub.lock:
                            await self._downgrade(sub)

    async def _emit(self, frame: ServerFrame, sub: _Subscription | None = None, *, epoch: int | None = None) -> None:
        text = frame.model_dump_json()
        await self.queue.put(_Outbound(frame, text, len(text.encode("utf-8")), sub, epoch))

    async def _sender(self) -> None:
        while True:
            item = await self.queue.get()
            try:
                await self.delivery.wait()
                self._credentials()
                sub = item.subscription
                if sub is not None and self.subscriptions.get(sub.id) is not sub:
                    continue
                if sub is not None and item.epoch is not None and item.epoch != sub.epoch:
                    continue
                async with asyncio.timeout(self.limits.queue_timeout):
                    await self.send({"type": "websocket.send", "text": item.text})
                if sub is not None:
                    if isinstance(item.frame, EventFrame):
                        sub.cursor = item.frame.seq
                    elif isinstance(item.frame, ReadyFrame) and item.epoch == sub.epoch:
                        sub.ready = True
            except TimeoutError:
                raise _Close(1013, "client is too slow") from None
            except OSError:
                raise _Close(1000, "disconnected") from None
            finally:
                await self.queue.done(item)

    async def _reader(self) -> None:
        while True:
            message = await self.receive()
            if message.get("type") == "websocket.disconnect":
                return
            if message.get("type") != "websocket.receive" or not isinstance(message.get("text"), str):
                raise _Close(1002, "JSON text frames required")
            text = message["text"]
            try:
                if len(text.encode("utf-8")) > self.limits.inbound_bytes:
                    raise _Close(1009, "inbound frame too large")
                frame = parse_frame(text)
            except (ValueError, RecursionError):
                raise _Close(1002, "invalid protocol frame") from None
            try:
                await self._handle(frame)
            except PermissionError:
                await self._error(frame, "forbidden", "Action denied")
            except ValidationError:
                await self._error(frame, "invalid_input", "Input does not match the application schema")
            except InvalidCommandReuse:
                await self._error(frame, "command_conflict", "Command identity was reused with different content")
            except AskExpired:
                await self._error(frame, "ask_expired", "This request is no longer awaiting an answer")
            except WriterReplaced:
                sub = self.subscriptions.get(frame.subscription_id)
                if sub is not None:
                    async with sub.lock:
                        await self._downgrade(sub)
                await self._error(frame, "writer_required", "Acquire the writer before retrying")
            except WriterRequired:
                await self._error(frame, "writer_required", "A caught-up writable subscription is required")
            except CommandTimeout:
                await self._error(
                    frame, "unknown_outcome", "Retry the same command identity", retryable=True, uncertain=True
                )
            except (CoordinatorUnavailable, LeaseLost):
                await self._error(
                    frame, "unavailable", "Service unavailable; retry the same command", retryable=True, uncertain=True
                )
            except SessionNotFound:
                raise
            except TantraError:
                await self._error(frame, "runtime_error", "Command rejected")
            except ValueError:
                await self._error(frame, "invalid_request", "Invalid request")

    async def _error(
        self, frame: Any, code: str, message: str, *, retryable: bool = False, uncertain: bool = False
    ) -> None:
        await self._emit(
            ErrorFrame(
                code=code,
                message=message,
                subscription_id=frame.subscription_id,
                command_id=getattr(frame, "command_id", None),
                retryable=retryable,
                uncertain=uncertain,
            )
        )

    async def _handle(self, frame: Any) -> None:
        if isinstance(frame, SubscribeFrame):
            await self._subscribe(frame)
            return
        sub = self.subscriptions.get(frame.subscription_id)
        if sub is None:
            raise ValueError("unknown subscription")
        if isinstance(frame, UnsubscribeFrame):
            await self._close_subscription(sub)
            await self._emit(UnsubscribedFrame(subscription_id=frame.subscription_id))
            return
        await self._authenticate(reads=True)
        async with sub.lock:
            if isinstance(frame, AcquireWriterFrame):
                await self._acquire(sub)
                return
            if not sub.writable or not sub.ready or sub.connection is None:
                raise WriterRequired("writable caught-up subscription required")
            if isinstance(frame, SendFrame):
                validated, encoded = self.endpoint.input_codec.validate(frame.input)
                await self._authorized("send", sub.actor_id, input=validated)
                receipt = await sub.connection.send(
                    encoded, command_id=frame.command_id, submitted_by=self.binding.auth.submitted_by
                )
                operation = "send"
            elif isinstance(frame, AnswerFrame):
                ask = await self.runtime.lookup_ask(self.binding.root_id, frame.ask_id)
                if ask is None:
                    raise AskExpired("unknown ask")
                await self._authorized("answer", ask.actor_id, response=frame.response, ask=ask)
                receipt = await sub.connection.answer(
                    frame.ask_id,
                    frame.response,
                    command_id=frame.command_id,
                    submitted_by=self.binding.auth.submitted_by,
                )
                operation = "answer"
            elif isinstance(frame, CancelFrame):
                await self._authorized("cancel", sub.actor_id)
                receipt = await sub.connection.cancel(
                    command_id=frame.command_id, submitted_by=self.binding.auth.submitted_by
                )
                operation = "cancel"
            else:
                raise ValueError("unknown operation")
            await self._emit(
                ReceiptFrame(
                    subscription_id=sub.id,
                    command_id=receipt.command_id,
                    operation=operation,
                    duplicate=receipt.duplicate,
                ),
                sub,
            )

    async def _subscribe(self, frame: SubscribeFrame) -> None:
        if frame.subscription_id in self.subscriptions or any(
            sub.actor_id == frame.actor_id for sub in self.subscriptions.values()
        ):
            raise ValueError("subscription already exists")
        if len(self.subscriptions) >= self.limits.subscriptions:
            raise ValueError("subscription limit reached")
        if frame.mode == "write" and frame.actor_id != self.binding.root_id:
            raise PermissionError("descendants are read-only")
        await self._authenticate(reads=True)
        root, actor = await self._authorized("subscribe", frame.actor_id)
        if frame.after > actor.last_seq:
            raise ValueError("cursor exceeds journal")
        view = await self._new_view(root, actor, frame.after)
        sub = _Subscription(frame.subscription_id, frame.actor_id, frame.after, frame.after, view)
        self.subscriptions[sub.id] = sub
        try:
            if frame.mode == "write":
                async with sub.lock:
                    await self._acquire(sub)
            else:
                await self._open_stream(sub)
        except BaseException:
            await self._close_subscription(sub)
            raise

    async def _open_stream(self, sub: _Subscription, *, writable: bool = False) -> None:
        sub.ready = False
        sub.writable = writable
        sub.epoch += 1
        if sub.actor_id == self.binding.root_id:
            connection = self.runtime.connect(sub.actor_id, after=sub.queued_cursor, writable=writable)
            await connection.__aenter__()
            sub.connection = connection
        status = await self.runtime.status(sub.actor_id)
        sub.task = self._spawn(self._stream(sub, status.last_seq, status.active, sub.epoch))

    async def _stop_stream(self, sub: _Subscription) -> None:
        if sub.task is not None and sub.task is not asyncio.current_task():
            sub.task.cancel()
            await asyncio.gather(sub.task, return_exceptions=True)
        sub.task = None
        if sub.connection is not None:
            connection, sub.connection = sub.connection, None
            await connection.__aexit__(None, None, None)

    async def _acquire(self, sub: _Subscription) -> None:
        if sub.actor_id != self.binding.root_id:
            raise PermissionError("descendants are read-only")
        root, actor = await self._authorized("acquire_writer", sub.actor_id)
        restarting = sub.task is not None
        await self._stop_stream(sub)
        if restarting:
            await self._close_view(sub)
            sub.view = await self._new_view(root, actor, sub.queued_cursor)
        await self._open_stream(sub, writable=True)

    async def _downgrade(self, sub: _Subscription) -> None:
        if not sub.writable:
            return
        sub.ready = False
        sub.writable = False
        await self._stop_stream(sub)
        root, actor = await self._authorized("subscribe", sub.actor_id)
        await self._close_view(sub)
        sub.view = await self._new_view(root, actor, sub.queued_cursor)
        await self._emit(WriterLostFrame(subscription_id=sub.id), sub)
        await self._open_stream(sub)

    async def _new_view(self, root: SessionHeader, actor: SessionHeader, after: int) -> EventView:
        async with asyncio.timeout(self.limits.queue_timeout):
            view = await self.endpoint.views(
                SocketViewContext(self.binding, root.model_copy(deep=True), actor.model_copy(deep=True), after)
            )
        if not callable(view):
            raise TypeError("views must return an event mapper")
        return view

    async def _close_view(self, sub: _Subscription) -> None:
        close_view = getattr(sub.view, "aclose", None)
        if close_view is not None:
            async with asyncio.timeout(self.limits.queue_timeout):
                await close_view()

    async def _stream(self, sub: _Subscription, watermark: int, active: bool, epoch: int) -> None:
        iterator = (
            sub.connection if sub.connection is not None else self.runtime.events(sub.actor_id, after=sub.queued_cursor)
        )
        ready = False

        async def mark_ready() -> None:
            await self._emit(
                ReadyFrame(
                    subscription_id=sub.id,
                    actor_id=sub.actor_id,
                    cursor=watermark,
                    mode="write" if sub.writable else "read",
                    active=active,
                ),
                sub,
                epoch=epoch,
            )

        try:
            if sub.queued_cursor >= watermark:
                await mark_ready()
                ready = True
            async for event in iterator:
                async with asyncio.timeout(self.limits.queue_timeout):
                    body = await sub.view(LoggedEvent(event.agent_id, event.seq, event.event.model_copy(deep=True)))
                await self._emit(
                    EventFrame(subscription_id=sub.id, actor_id=sub.actor_id, seq=event.seq, body=body), sub
                )
                sub.queued_cursor = event.seq
                if not ready and event.seq >= watermark:
                    await mark_ready()
                    ready = True
            raise _Close(1012, "runtime stopped")
        except WriterReplaced:
            async with sub.lock:
                if self.subscriptions.get(sub.id) is sub and sub.epoch == epoch:
                    await self._downgrade(sub)
        finally:
            if sub.connection is None and hasattr(iterator, "aclose"):
                await iterator.aclose()

    async def _close_subscription(self, sub: _Subscription) -> None:
        if self.subscriptions.get(sub.id) is sub:
            self.subscriptions.pop(sub.id)
        async with sub.lock:
            try:
                await self._stop_stream(sub)
            finally:
                await self._close_view(sub)

    async def _notices(self) -> None:
        assert self.endpoint.notices is not None
        iterator = self.endpoint.notices(self.binding)
        try:
            async for body in iterator:
                await self._emit(NoticeFrame(body=body))
        finally:
            if hasattr(iterator, "aclose"):
                await iterator.aclose()
