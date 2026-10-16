from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ValidationError

from tantra.acp import wire
from tantra.acp.jsonrpc import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    RESOURCE_NOT_FOUND,
    Peer,
    Reply,
    RpcError,
)
from tantra.acp.projection import (
    STOP_REASONS,
    AskPending,
    AskResolved,
    ChildAnnounced,
    Output,
    Projector,
    QueueChanged,
    TurnState,
    UsageSample,
)
from tantra.acp.server import AcpAccess, AcpAuth, AcpServer, Operation
from tantra.acp.transport import Transport
from tantra.ask import Approval, ApprovalResponse, AskResponse, Choice, ChoiceResponse, FreeTextResponse
from tantra.errors import (
    AskExpired,
    CommandTimeout,
    CoordinatorUnavailable,
    InvalidCommandReuse,
    LeaseLost,
    ModelChangeBusy,
    SessionNotFound,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import ActorStatus, ChildCreated, LoggedEvent, SessionHeader, TreeChange, TurnFailed
from tantra.runtime import Connection, LocatedAsk

MAX_SESSIONS = 8
SEND_ATTEMPTS = 4
LIST_PAGE = 50
TAIL_RETRIES = 8
TRANSIENT = (CoordinatorUnavailable, CommandTimeout, LeaseLost)
STOPPED = {"finished": "_tantra_finished", "cancelled": "cancelled", "interrupted": "_tantra_interrupted"}
PERMISSION_OPTIONS = [
    wire.PermissionOption(option_id="allow", name="Allow", kind="allow_once"),
    wire.PermissionOption(option_id="deny", name="Deny", kind="reject_once"),
]
_VERSION = version("tantra-harness")
logger = logging.getLogger(__name__)


def _writer_required() -> RpcError:
    return RpcError(INTERNAL_ERROR, "Writer required", {"tantra": {"code": "writer_required"}})


def _unavailable() -> RpcError:
    return RpcError(INTERNAL_ERROR, "Service unavailable", {"tantra": {"code": "unavailable"}})


def _command_conflict() -> RpcError:
    return RpcError(INVALID_PARAMS, "Command conflict", {"tantra": {"code": "command_conflict"}})


def _child_session() -> RpcError:
    return RpcError(INVALID_PARAMS, "Subagent sessions are read-only")


def _parse[T: BaseModel](model: type[T], params: Any) -> T:
    return model.model_validate(params if params is not None else {})


def _session_id(text: str) -> UUID:
    try:
        value = UUID(text)
    except (TypeError, ValueError):
        raise SessionNotFound(text) from None
    if str(value) != text:
        raise SessionNotFound(text)
    return value


def _consume(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _threshold(after: tuple[int, int | None] | None, last_seq: int) -> tuple[int, int | None]:
    if after is None:
        return last_seq, None
    if after[0] > last_seq:
        raise RpcError(INVALID_PARAMS, "Cursor beyond the end of the session")
    return after


def _cursored(update: dict[str, Any], cursor: str) -> dict[str, Any]:
    meta = dict(update.get("_meta") or {})
    meta["tantra"] = {**meta.get("tantra", {}), "cursor": cursor}
    return {**update, "_meta": meta}


def _subagent(child_id: UUID, title: str | None, state: dict[str, Any]) -> dict[str, Any]:
    finished = state.get("stopReason") == "_tantra_finished"
    update = wire.SubagentUpdate(
        session_id=str(child_id),
        title=title,
        state=state,
        meta={"tantra": {"finished": True}} if finished else None,
    )
    return wire.dump(update)


def _failure(status: ActorStatus) -> str | None:
    return status.last_turn.error if status.state == "failed" and status.last_turn is not None else None


def _child_state(status: ActorStatus, error: str | None) -> dict[str, Any]:
    if status.state in ("queued", "running"):
        state = wire.State(state="running")
    elif status.state == "awaiting_input":
        state = wire.State(state="requires_action")
    elif status.state == "failed":
        failure = wire.ErrorObject(code=-32603, message=error or "")
        state = wire.State(state="idle", stop_reason="error", error=failure)
    elif status.state in STOPPED:
        state = wire.State(state="idle", stop_reason=STOPPED[status.state])
    elif status.last_turn is None:
        state = wire.State(state="idle")
    else:
        state = wire.State(state="idle", stop_reason=STOP_REASONS.get(status.last_turn.stop_reason or "", "end_turn"))
    return wire.dump(state)


@dataclass(eq=False)
class _Tail:
    actor_id: UUID
    parent_id: UUID | None
    title: str
    projector: Projector
    watermark: int
    emit_after: int
    live: asyncio.Event
    emit_index: int | None = None
    cursor: int = 0
    usage: UsageSample | None = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    task: asyncio.Task[None] | None = None


@dataclass(eq=False)
class _Session:
    root_id: UUID
    replay: bool
    title: str | None = None
    model: str | None = None
    tails: dict[UUID, _Tail] = field(default_factory=dict)
    writer: Connection | None = None
    announced: bool = False
    fenced: bool = False
    lost: bool = False
    answered: bool = False
    closed: bool = False
    live: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    asks: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    exits: set[asyncio.Task[None]] = field(default_factory=set)
    mirror: dict[UUID, dict[str, Any]] = field(default_factory=dict)
    viewed: dict[UUID, tuple[tuple[Any, ...], tuple[str, str | None] | None]] = field(default_factory=dict)
    tree: asyncio.Task[None] | None = None
    changes: AsyncIterator[TreeChange] | None = None
    teardown: asyncio.Task[None] | None = None
    gone: asyncio.Task[None] | None = None


class AcpConnection:
    def __init__(self, server: AcpServer, transport: Transport, auth: AcpAuth) -> None:
        self.server = server
        self.runtime = server.runtime
        self.transport = transport
        self.auth = auth
        self.sessions: dict[UUID, _Session] = {}
        self.opening = asyncio.Lock()
        self.deleting: list[UUID] = []
        self.elicitation = False
        self.profile = False
        self.tasks: set[asyncio.Task[Any]] = set()
        self.cleanups: set[asyncio.Task[Any]] = set()
        handlers: dict[str, Callable[[Any], Awaitable[Any]]] = {
            "initialize": self._initialize,
            "session/new": self._new,
            "session/list": self._list,
            "session/resume": self._resume,
            "session/prompt": self._prompt,
            "session/cancel": self._cancel,
            "session/close": self._close,
            "session/delete": self._delete,
            "session/set_config_option": self._set_config_option,
            "_tantra/session/claim_writer": self._claim_writer,
        }
        self.peer = Peer(transport, {method: self._guard(handler) for method, handler in handlers.items()})

    async def run(self) -> None:
        try:
            await self.peer.run()
        finally:
            for session in list(self.sessions.values()):
                await self._shutdown(session)
            await asyncio.gather(*self.cleanups, return_exceptions=True)
            tasks = list(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn(self, coroutine: Awaitable[Any], *, cleanup: bool = False) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coroutine)
        self.tasks.add(task)
        if cleanup:
            self.cleanups.add(task)

        def finished(done: asyncio.Task[Any]) -> None:
            self.tasks.discard(done)
            self.cleanups.discard(done)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None and not isinstance(error, ConnectionError):
                logger.error("ACP task failed", exc_info=error)

        task.add_done_callback(finished)
        return task

    def _guard(self, handler: Callable[[Any], Awaitable[Any]]) -> Callable[[Any], Awaitable[Any]]:
        async def guarded(params: Any) -> Any:
            try:
                return await handler(params)
            except (SessionNotFound, PermissionError):
                raise RpcError(RESOURCE_NOT_FOUND, "Session not found") from None
            except ValidationError:
                raise RpcError(INVALID_PARAMS) from None
            except (WriterReplaced, WriterRequired):
                raise _writer_required() from None
            except (CoordinatorUnavailable, CommandTimeout, LeaseLost):
                raise _unavailable() from None
            except ModelChangeBusy:
                raise RpcError(INTERNAL_ERROR, "Model busy", {"tantra": {"code": "model_busy"}}) from None
            except InvalidCommandReuse:
                raise _command_conflict() from None

        return guarded

    async def _load(self, root_id: UUID) -> SessionHeader:
        header = await self.runtime.store.header(root_id.hex)
        if header is None:
            raise SessionNotFound(root_id.hex)
        if header.parent_id is not None:
            if any(root_id in session.tails for session in self.sessions.values()):
                raise _child_session()
            raise SessionNotFound(root_id.hex)
        if header.agent != self.server.agent:
            raise SessionNotFound(root_id.hex)
        return header

    async def _allowed(
        self,
        operation: Operation,
        root: SessionHeader,
        meta: Mapping[str, Any],
        *,
        actor: SessionHeader | None = None,
        ask: LocatedAsk | None = None,
        response: AskResponse | None = None,
    ) -> bool:
        access = AcpAccess(
            operation,
            self.auth,
            root.model_copy(deep=True),
            (actor if actor is not None else root).model_copy(deep=True),
            dict(meta),
            ask,
            response.model_copy(deep=True) if response is not None else None,
        )
        return await self.server.policy(access) is True

    async def _authorize(
        self, operation: Operation, root: SessionHeader, meta: Mapping[str, Any], *, actor: SessionHeader | None = None
    ) -> None:
        if not await self._allowed(operation, root, meta, actor=actor):
            raise PermissionError("access denied")

    async def _scope(self, meta: Mapping[str, Any]) -> dict[str, str]:
        scope = await self.server.scope(self.auth, dict(meta))
        if not isinstance(scope, Mapping) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in scope.items()
        ):
            raise TypeError("scope must return a mapping of strings")
        return dict(scope)

    def _open_session(self, text: str) -> _Session:
        root_id = _session_id(text)
        session = self.sessions.get(root_id)
        if session is None or session.closed:
            if any(root_id in other.tails for other in self.sessions.values()):
                raise _child_session()
            raise SessionNotFound(text)
        return session

    def _capacity(self) -> None:
        opened = sum(len(session.tails) if self.profile else 1 for session in self.sessions.values())
        if opened >= MAX_SESSIONS:
            raise RpcError(INVALID_PARAMS, "Too many open sessions")

    def _command_id(self, meta: Mapping[str, Any]) -> UUID:
        tantra = meta.get("tantra") if self.profile else None
        value = tantra.get("commandId") if isinstance(tantra, dict) else None
        if value is None:
            return uuid4()
        try:
            command_id = UUID(value) if isinstance(value, str) else None
        except ValueError:
            command_id = None
        if command_id is None or str(command_id) != value:
            raise RpcError(INVALID_PARAMS, "Invalid commandId")
        return command_id

    def _writes(self, meta: Mapping[str, Any], default: str) -> bool:
        if not self.profile:
            return False
        tantra = meta.get("tantra")
        mode = tantra.get("mode", default) if isinstance(tantra, dict) else default
        if mode not in ("read", "write"):
            raise RpcError(INVALID_PARAMS, "Invalid mode")
        return mode == "write"

    def _replay_from(self, replay_from: dict[str, Any] | None) -> tuple[int, int | None] | None:
        if replay_from is None:
            return None
        kind = replay_from.get("type")
        if kind == "start":
            return 0, None
        cursor = wire.parse_cursor(replay_from.get("cursor")) if kind == "_tantra_after" and self.profile else None
        if cursor is None:
            raise RpcError(INVALID_PARAMS, "Unsupported replayFrom")
        return cursor

    def _config(self, model: str | None) -> list[dict[str, Any]] | None:
        if not self.server.models:
            return None
        values = list(self.server.models)
        if model is not None and model not in values:
            values.insert(0, model)
        option = wire.ConfigOption(
            config_id="model",
            name="Model",
            category="model",
            current_value=model or values[0],
            options=[wire.ConfigValue(value=value, name=value) for value in values],
        )
        return [wire.dump(option)]

    async def _update(self, session_id: UUID, update: dict[str, Any]) -> None:
        await self.peer.send_notification("session/update", {"sessionId": str(session_id), "update": update})

    async def _writer(self, session: _Session, reason: str) -> None:
        if self.profile:
            update = wire.WriterUpdate(writer=reason == "claimed", reason=reason)
            await self._update(session.root_id, wire.dump(update))

    async def _notice(self, session: _Session, code: str, message: str) -> None:
        if self.profile:
            await self._update(session.root_id, wire.dump(wire.NoticeUpdate(code=code, message=message)))

    async def _initialize(self, params: Any) -> Any:
        request = _parse(wire.InitializeParams, params)
        elicitation = request.capabilities.get("elicitation")
        self.elicitation = isinstance(elicitation, dict) and isinstance(elicitation.get("form"), dict)
        meta = request.capabilities.get("_meta")
        tantra = meta.get("tantra") if isinstance(meta, dict) else None
        negotiated = tantra.get("version") if isinstance(tantra, dict) else None
        self.profile = type(negotiated) is int and negotiated == 1
        result = wire.InitializeResult(
            capabilities={
                "session": {"prompt": {}, "mcp": {}, "delete": {}},
                "_meta": {"tantra": {"version": 1}},
            },
            info=wire.Implementation(name=self.server.name, version=_VERSION),
        )
        return wire.dump(result)

    async def _new(self, params: Any) -> Any:
        request = _parse(wire.NewSessionParams, params)
        meta = request.meta or {}
        write = self._writes(meta, "write")
        scope = await self._scope(meta)
        session_id = uuid4()
        metadata = {**scope, "acp_cwd": request.cwd}
        provisional = SessionHeader(
            id=session_id.hex, root_id=session_id.hex, agent=self.server.agent, metadata=metadata
        )
        await self._authorize("new", provisional, meta)
        async with self.opening:
            self._capacity()
            root_id = await self.runtime.create(self.server.agent, session_id=session_id, metadata=metadata)
            session = await self._open(root_id, None)
        if write:
            try:
                await self._claim_or_close(session, meta)
            except BaseException:
                await asyncio.shield(self.runtime.delete(root_id, allow_active=True))
                raise
        result = wire.NewSessionResult(session_id=str(root_id), config_options=self._config(session.model))
        return Reply(wire.dump(result), lambda: self._go_live(session))

    async def _list(self, params: Any) -> Any:
        request = _parse(wire.ListSessionsParams, params)
        scope = await self._scope(request.meta or {})
        wanted: dict[str, Any] = dict(scope)
        if request.cwd is not None:
            wanted["acp_cwd"] = request.cwd
        before = request.cursor
        if before is not None:
            anchor = await self.runtime.store.header(before)
            if (
                anchor is None
                or anchor.parent_id is not None
                or anchor.agent != self.server.agent
                or any(anchor.metadata.get(key) != value for key, value in scope.items())
            ):
                raise RpcError(INVALID_PARAMS, "Invalid cursor")
        roots: list[SessionHeader] = []
        exhausted = False
        while len(roots) < LIST_PAGE and not exhausted:
            page = await self.runtime.store.list(metadata=wanted, limit=LIST_PAGE, before=before)
            exhausted = len(page) < LIST_PAGE
            for header in page:
                before = header.id
                if header.parent_id is None and header.agent == self.server.agent:
                    roots.append(header)
                    if len(roots) == LIST_PAGE:
                        break
        sessions = [
            wire.SessionInfo(
                session_id=str(UUID(hex=header.id)),
                cwd=str(header.metadata.get("acp_cwd") or "/"),
                title=header.title,
                updated_at=header.updated_at.isoformat(),
            )
            for header in roots
        ]
        next_cursor = roots[-1].id if len(roots) == LIST_PAGE else None
        return wire.dump(wire.ListSessionsResult(sessions=sessions, next_cursor=next_cursor))

    async def _resume(self, params: Any) -> Any:
        request = _parse(wire.ResumeSessionParams, params)
        root_id = _session_id(request.session_id)
        if self.profile:
            found = await self.runtime.store.header(root_id.hex)
            if found is not None and found.parent_id is not None:
                return await self._resume_child(request, found)
        meta = request.meta or {}
        header = await self._load(root_id)
        await self._authorize("resume", header, meta)
        after = self._replay_from(request.replay_from)
        write = self._writes(meta, "read")
        if after is not None:
            _threshold(after, (await self.runtime.status(root_id)).last_seq)
        if write:
            await self._authorize("claim_writer", header, meta)
        async with self.opening:
            existing = self.sessions.get(root_id)
            if existing is None:
                self._capacity()
            if write:
                writer = await self._enter_writer(root_id, existing)
                session = await self._swap(root_id, after, existing, writer)
            else:
                session = await self._replace(root_id, after, existing, None)
        result = wire.ResumeSessionResult(config_options=self._config(session.model))
        return Reply(wire.dump(result), lambda: self._go_live(session))

    async def _replace(
        self, root_id: UUID, after: tuple[int, int | None] | None, existing: _Session | None, writer: Connection | None
    ) -> _Session:
        if existing is not None:
            try:
                await self._shutdown(existing)
                self._capacity()
            except BaseException:
                if writer is not None:
                    await self._discard(writer)
                raise
        return await self._open(root_id, after, writer)

    async def _swap(
        self, root_id: UUID, after: tuple[int, int | None] | None, existing: _Session | None, writer: Connection
    ) -> _Session:
        task = asyncio.ensure_future(self._replace(root_id, after, existing, writer))
        while True:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if task.done() or self.peer.closed or current is None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
                current.uncancel()

    async def _enter_writer(self, root_id: UUID, existing: _Session | None) -> Connection:
        connection = self.runtime.connect(root_id, writable=True)
        if existing is None:
            await connection.__aenter__()
            return connection
        async with existing.lock:
            await connection.__aenter__()
            existing.announced = False
        return connection

    async def _discard(self, connection: Connection) -> None:
        task = self._spawn(self._exit(connection), cleanup=True)
        await asyncio.shield(task)

    async def _resume_child(self, request: wire.ResumeSessionParams, header: SessionHeader) -> Any:
        meta = request.meta or {}
        root_id = UUID(hex=header.root_id or header.id)
        if root_id not in self.sessions or await self._concealed(UUID(hex=header.id)):
            raise SessionNotFound(header.id)
        root = await self._load(root_id)
        await self._authorize("resume", root, meta, actor=header)
        if self._writes(meta, "read"):
            raise _child_session()
        after = self._replay_from(request.replay_from)
        if after is not None:
            _threshold(after, (await self.runtime.status(UUID(hex=header.id))).last_seq)
        async with self.opening:
            session = self.sessions.get(root_id)
            if session is None or session.closed:
                raise SessionNotFound(header.id)
            existing = session.tails.get(UUID(hex=header.id))
            if existing is not None:
                await self._detach(session, existing)
            self._capacity()
            tail = await self._open_child(session, header, after)
        return Reply(wire.dump(wire.ResumeSessionResult()), lambda: self._child_live(session, tail))

    async def _claim_or_close(self, session: _Session, meta: Mapping[str, Any]) -> None:
        try:
            async with session.lock:
                await self._claim(session, meta)
        except BaseException:
            await self._shutdown(session)
            raise

    async def _prompt(self, params: Any) -> Any:
        request = _parse(wire.PromptParams, params)
        session = self._open_session(request.session_id)
        meta = request.meta or {}
        command_id = self._command_id(meta)
        async with session.lock:
            root = await self._load(session.root_id)
            await self._authorize("prompt", root, meta)
            try:
                text = await self.server.encode_prompt(self.auth, [dict(block) for block in request.prompt], dict(meta))
            except ValueError:
                raise RpcError(INVALID_PARAMS) from None
            if not isinstance(text, str):
                raise TypeError("encode_prompt must return a string")
            if session.writer is None:
                if session.fenced or self.profile:
                    session.fenced = False
                    raise _writer_required()
                await self._claim(session, meta)
            try:
                await self._accept(session, text, command_id)
            except (WriterReplaced, WriterRequired):
                await self._downgrade(session, fenced=False, reason="replaced")
                raise
        return wire.dump(wire.PromptResult(message_id=str(command_id)))

    async def _accept(self, session: _Session, text: str, command_id: UUID) -> None:
        writer = session.writer
        assert writer is not None
        send = functools.partial(writer.send, text, command_id=command_id, submitted_by=self.auth.submitted_by)
        task = asyncio.ensure_future(self._retry(send, SEND_ATTEMPTS))
        task.add_done_callback(_consume)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if self.peer.closed or current is None:
                raise
            current.uncancel()
            await task

    async def _retry(self, operation: Callable[[], Awaitable[Any]], attempts: int) -> None:
        for attempt in range(attempts):
            try:
                await operation()
                return
            except TRANSIENT:
                if attempt == attempts - 1:
                    raise
                await asyncio.sleep(0.1 * 2**attempt)

    async def _cancel(self, params: Any) -> None:
        request = _parse(wire.SessionParams, params)
        session = self.sessions.get(_session_id(request.session_id))
        if session is None or session.closed:
            return
        meta = request.meta or {}
        try:
            command_id = self._command_id(meta)
        except RpcError:
            logger.warning("cancel for %s has an invalid commandId", session.root_id)
            return
        async with session.lock:
            root = await self._load(session.root_id)
            await self._authorize("cancel", root, meta)
            for _ in range(2):
                if self.profile and session.writer is None:
                    await self._notice(session, "writer_required", "Writer required")
                    return
                await self._claim(session, meta)
                assert session.writer is not None
                cancel = functools.partial(
                    session.writer.cancel, command_id=command_id, submitted_by=self.auth.submitted_by
                )
                try:
                    await self._retry(cancel, SEND_ATTEMPTS if self.profile else 1)
                    return
                except (WriterReplaced, WriterRequired):
                    await self._downgrade(session, fenced=False, reason="replaced")
                except InvalidCommandReuse:
                    await self._notice(session, "command_conflict", "Command conflict")
                    return
                except TRANSIENT:
                    if not self.profile:
                        raise
                    await self._notice(session, "unavailable", "Service unavailable")
                    return

    async def _claim_writer(self, params: Any) -> Any:
        if not self.profile:
            raise RpcError(METHOD_NOT_FOUND)
        request = _parse(wire.SessionParams, params)
        session = self._open_session(request.session_id)
        async with session.lock:
            claimed = await self._claim(session, request.meta or {})
        return Reply({}, (lambda: self._announce_live(session)) if claimed else None)

    async def _close(self, params: Any) -> Any:
        request = _parse(wire.SessionParams, params)
        if self.profile:
            actor_id = _session_id(request.session_id)
            for session in list(self.sessions.values()):
                tail = session.tails.get(actor_id)
                if tail is not None and tail.parent_id is not None:
                    await self._detach(session, tail)
                    return {}
        await self._shutdown(self._open_session(request.session_id))
        return {}

    async def _delete(self, params: Any) -> Any:
        request = _parse(wire.SessionParams, params)
        root_id = _session_id(request.session_id)
        header = await self._load(root_id)
        await self._authorize("delete", header, request.meta or {})
        self.deleting.append(root_id)
        try:
            if not await self.runtime.delete(root_id, allow_active=True):
                raise SessionNotFound(root_id.hex)
            session = self.sessions.get(root_id)
            if session is not None:
                await self._shutdown(session)
        finally:
            self.deleting.remove(root_id)
        return {}

    async def _set_config_option(self, params: Any) -> Any:
        request = _parse(wire.SetConfigOptionParams, params)
        root_id = _session_id(request.session_id)
        models = self.server.models or []
        if request.config_id != "model" or request.type != "id" or request.value not in models:
            raise RpcError(INVALID_PARAMS)
        header = await self._load(root_id)
        await self._authorize("set_model", header, request.meta or {})
        patched = await self.runtime.patch_header(root_id, model=request.value)
        session = self.sessions.get(root_id)
        if session is not None:
            session.model = patched.model
        return wire.dump(wire.SetConfigOptionResult(config_options=self._config(patched.model) or []))

    def _hidden(self, agent: str, parent_id: UUID | None) -> set[str]:
        declared = self.runtime.agents.get(agent)
        hidden: set[str] = set()
        if declared is not None and declared.subagents:
            hidden.update(("spawn", "send"))
        if parent_id is not None:
            hidden.update(("send", "finish"))
        return hidden

    def _tail(
        self,
        session: _Session,
        actor_id: UUID,
        parent_id: UUID | None,
        agent: str,
        title: str,
        watermark: int,
        emit_after: int,
        emit_index: int | None = None,
        *,
        live: asyncio.Event | None = None,
    ) -> _Tail:
        projector = Projector(
            actor_id,
            parent_id,
            self.server.decode_input,
            hidden=self._hidden(agent, parent_id),
            profile=self.profile,
        )
        gate = live if live is not None else session.live
        tail = _Tail(actor_id, parent_id, title, projector, watermark, emit_after, gate, emit_index)
        session.tails[actor_id] = tail
        tail.task = self._spawn(self._run(session, tail))
        return tail

    async def _open(
        self, root_id: UUID, after: tuple[int, int | None] | None, writer: Connection | None = None
    ) -> _Session:
        try:
            header = await self._load(root_id)
            status = await self.runtime.status(root_id)
            emit_after, emit_index = _threshold(after, status.last_seq)
        except BaseException:
            if writer is not None:
                await self._discard(writer)
            raise
        session = _Session(root_id, after is not None, title=header.title, model=header.model, writer=writer)
        self.sessions[root_id] = session
        try:
            self._tail(
                session,
                root_id,
                None,
                header.agent,
                header.name or header.agent,
                status.last_seq,
                emit_after,
                emit_index,
            )
            while pending := [tail.ready.wait() for tail in session.tails.values() if not tail.ready.is_set()]:
                await asyncio.gather(*pending)
            if session.lost:
                raise SessionNotFound(root_id.hex)
        except BaseException:
            await self._shutdown(session)
            raise
        return session

    async def _open_child(
        self, session: _Session, header: SessionHeader, after: tuple[int, int | None] | None
    ) -> _Tail:
        child_id = UUID(hex=header.id)
        status = await self.runtime.status(child_id)
        emit_after, emit_index = _threshold(after, status.last_seq)
        parent_id = UUID(hex=header.parent_id) if header.parent_id is not None else None
        tail = self._tail(
            session,
            child_id,
            parent_id,
            header.agent,
            status.name,
            status.last_seq,
            emit_after,
            emit_index,
            live=asyncio.Event(),
        )
        try:
            await tail.ready.wait()
            if session.lost or session.closed:
                raise SessionNotFound(header.id)
        except BaseException:
            await self._detach(session, tail)
            raise
        return tail

    async def _detach(self, session: _Session, tail: _Tail) -> None:
        if session.tails.get(tail.actor_id) is tail:
            del session.tails[tail.actor_id]
        await self._stop(tail)

    async def _go_live(self, session: _Session) -> None:
        session.answered = True
        if session.lost:
            self._lose(session)
            return
        if session.closed:
            return
        tree = self.runtime.watch_tree(session.root_id)
        following = False
        try:
            change = await anext(tree, None)
            if change is None:
                return
            session.title, session.model = change.title, change.model
            await self._info(session, change)
            root = session.tails[session.root_id]
            for tail in [root] if self.profile else list(session.tails.values()):
                if tail.parent_id is not None:
                    await self._update(tail.parent_id, _subagent(tail.actor_id, tail.title, tail.projector.state))
                await self._update(tail.actor_id, {"sessionUpdate": "state_update", **tail.projector.state})
            if self.profile:
                await self._update(session.root_id, wire.dump(wire.QueueUpdate(entries=root.projector.queue)))
                await self._mirror(session, change)
            async with session.lock:
                if session.closed:
                    return
                if session.writer is not None:
                    await self._announce(session)
                session.live.set()
            if not session.closed:
                session.changes = tree
                session.tree = self._spawn(self._follow(session, tree))
                following = True
        except SessionNotFound:
            self._lose(session)
        except ConnectionError:
            raise
        except Exception:
            logger.exception("ACP mirror for %s failed", session.root_id)
            self._spawn(self.transport.close())
        finally:
            if not following:
                await tree.aclose()

    async def _child_live(self, session: _Session, tail: _Tail) -> None:
        if session.closed or session.tails.get(tail.actor_id) is not tail:
            return
        await self._update(tail.actor_id, {"sessionUpdate": "state_update", **tail.projector.state})
        tail.live.set()

    async def _run(self, session: _Session, tail: _Tail) -> None:
        failures = 0
        try:
            if tail.cursor >= tail.watermark:
                tail.ready.set()
            while not session.closed:
                writer = session.writer if tail.parent_id is None else None
                source = writer if writer is not None else self.runtime.events(tail.actor_id, after=tail.cursor)
                try:
                    async for item in source:
                        failures = 0
                        if item.seq > tail.watermark and not tail.live.is_set():
                            await tail.live.wait()
                        async with tail.lock:
                            if item.seq > tail.cursor:
                                await self._process(session, tail, item)
                    if not session.closed:
                        raise RuntimeError("event stream ended")
                    return
                except (WriterReplaced, WriterRequired, CoordinatorUnavailable, LeaseLost) as error:
                    lost = isinstance(error, CoordinatorUnavailable | LeaseLost)
                    if writer is not None:
                        async with session.lock:
                            if session.writer is writer:
                                await self._release(session, fenced=True, reason="lost" if lost else "replaced")
                        continue
                    if not lost:
                        raise
                    failures += 1
                    if failures > TAIL_RETRIES:
                        raise
                    await asyncio.sleep(min(0.1 * 2 ** (failures - 1), 2.0))
                finally:
                    if writer is None:
                        await source.aclose()
        except SessionNotFound:
            self._lose(session)
        except ConnectionError:
            pass
        except Exception:
            logger.exception("ACP stream for %s failed", tail.actor_id)
            self._spawn(self.transport.close())
        finally:
            tail.ready.set()

    async def _process(self, session: _Session, tail: _Tail, item: LoggedEvent) -> None:
        if item.seq <= tail.emit_after:
            mode = "discard"
        elif item.seq <= tail.watermark:
            mode = "replay"
        else:
            mode = "live"
        event: LoggedEvent | None = item
        if self.server.view is not None:
            viewed = await self.server.view(LoggedEvent(item.agent_id, item.seq, item.event.model_copy(deep=True)))
            event = LoggedEvent(item.agent_id, item.seq, viewed.event) if viewed is not None else None
            if isinstance(item.event, ChildCreated) and (event is None or not isinstance(event.event, ChildCreated)):
                tail.projector.withhold(item.event)
        replay = mode != "live" and not self.profile
        outputs = tail.projector.feed(event, replay=replay) if event is not None else []
        tail.cursor = item.seq
        settled = not tail.ready.is_set() and tail.cursor >= tail.watermark
        if settled:
            outputs = [*outputs, *tail.projector.flush()]
        split = tail.emit_index if mode == "discard" and item.seq == tail.emit_after else None
        for index, output in enumerate(outputs):
            shown = "replay" if split is not None and index > split else mode
            await self._apply(session, tail, output, shown, f"{item.seq}.{index}")
        if settled:
            if tail.usage is not None:
                usage, tail.usage = tail.usage, None
                await self._usage(tail, usage)
            if (
                not self.profile
                and session.live.is_set()
                and tail.parent_id is not None
                and tail.projector.state != {"state": "idle"}
            ):
                await self._update(tail.parent_id, _subagent(tail.actor_id, None, tail.projector.state))
            tail.ready.set()

    async def _apply(self, session: _Session, tail: _Tail, output: Output, mode: str, cursor: str) -> None:
        if isinstance(output, dict):
            if mode != "discard":
                await self._update(tail.actor_id, _cursored(output, cursor) if self.profile else output)
        elif isinstance(output, ChildAnnounced):
            if not self.profile:
                await self._child(session, tail, output, mode)
        elif isinstance(output, UsageSample):
            if self.profile:
                if mode != "discard":
                    await self._usage(tail, output, cursor)
            elif mode == "replay":
                tail.usage = output
            elif mode == "live":
                await self._usage(tail, output)
        elif mode == "live":
            if isinstance(output, QueueChanged):
                await self._update(tail.actor_id, wire.dump(wire.QueueUpdate(entries=output.entries)))
            elif isinstance(output, TurnState):
                if not self.profile and tail.parent_id is not None:
                    await self._update(tail.parent_id, _subagent(tail.actor_id, None, output.state))
            elif isinstance(output, AskPending) and session.writer is not None:
                self._issue(session, output)
            elif isinstance(output, AskResolved):
                self._withdraw(session, output.ask_id)

    async def _usage(self, tail: _Tail, sample: UsageSample, cursor: str | None = None) -> None:
        window = await self.server.context_window(sample.model)
        if window is not None:
            update = wire.dump(wire.UsageUpdate(used=sample.used, size=window))
            await self._update(tail.actor_id, _cursored(update, cursor) if cursor is not None else update)

    async def _child(self, session: _Session, parent: _Tail, announced: ChildAnnounced, mode: str) -> None:
        if announced.child_id in session.tails:
            return
        try:
            status = await self.runtime.status(announced.child_id)
        except SessionNotFound:
            return
        self._tail(
            session,
            announced.child_id,
            parent.actor_id,
            status.agent,
            announced.title,
            status.last_seq,
            status.last_seq if mode == "discard" else 0,
        )

    async def _stop(self, tail: _Tail) -> None:
        task = tail.task
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _restart(self, session: _Session, tail: _Tail) -> None:
        if not session.closed:
            tail.task = self._spawn(self._run(session, tail))

    async def _claim(self, session: _Session, meta: Mapping[str, Any]) -> bool:
        if session.closed:
            raise SessionNotFound(session.root_id.hex)
        if session.writer is not None:
            return False
        root = await self._load(session.root_id)
        await self._authorize("claim_writer", root, meta)
        tail = session.tails[session.root_id]
        async with tail.lock:
            try:
                await self._stop(tail)
                connection = self.runtime.connect(session.root_id, after=tail.cursor, writable=True)
                await connection.__aenter__()
                session.writer = connection
                session.fenced = False
                if session.live.is_set() and not self.profile:
                    for pending in tail.projector.pending_asks:
                        self._issue(session, pending)
            finally:
                self._restart(session, tail)
        return True

    async def _announce(self, session: _Session) -> None:
        if not session.announced:
            session.announced = True
            await self._writer(session, "claimed")
        tail = session.tails[session.root_id]
        async with tail.lock:
            for pending in tail.projector.pending_asks:
                self._issue(session, pending)

    async def _announce_live(self, session: _Session) -> None:
        async with session.lock:
            if session.live.is_set() and session.writer is not None and not session.closed:
                await self._announce(session)

    async def _downgrade(self, session: _Session, *, fenced: bool, reason: str | None = None) -> None:
        if session.writer is None:
            return
        tail = session.tails[session.root_id]
        async with tail.lock:
            try:
                await self._stop(tail)
                await self._release(session, fenced=fenced, reason=reason)
            finally:
                self._restart(session, tail)

    async def _release(self, session: _Session, *, fenced: bool, reason: str | None = None) -> None:
        connection, session.writer = session.writer, None
        announced, session.announced = session.announced, False
        session.fenced = fenced
        current = asyncio.current_task()
        for task in session.asks.values():
            if task is not current:
                task.cancel()
        session.asks.clear()
        if connection is not None:
            if reason is not None and announced:
                self._spawn(self._writer(session, reason))
            task = self._spawn(self._exit(connection), cleanup=True)
            session.exits.add(task)
            task.add_done_callback(session.exits.discard)
            await asyncio.shield(task)

    async def _exit(self, connection: Connection) -> None:
        try:
            await connection.__aexit__(None, None, None)
        except Exception:
            logger.debug("writer release failed", exc_info=True)

    def _lose(self, session: _Session) -> None:
        session.lost = True
        if session.answered and not session.closed and session.gone is None:
            session.gone = self._spawn(self._gone(session))

    async def _gone(self, session: _Session) -> None:
        try:
            deleted = self.profile and await self.runtime.store.header(session.root_id.hex) is None
            if deleted and not session.closed and session.root_id not in self.deleting:
                await self._notice(session, "session_deleted", "Session deleted")
        finally:
            await self._shutdown(session)

    async def _shutdown(self, session: _Session) -> None:
        session.closed = True
        if self.sessions.get(session.root_id) is session:
            del self.sessions[session.root_id]
        if session.teardown is None:
            session.teardown = self._spawn(self._teardown(session), cleanup=True)
        if session.teardown is not asyncio.current_task():
            await asyncio.shield(session.teardown)

    async def _teardown(self, session: _Session) -> None:
        tasks = [tail.task for tail in session.tails.values()]
        tasks.extend(session.asks.values())
        tasks.append(session.tree)
        pending = [task for task in tasks if task is not None]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if session.changes is not None:
            await session.changes.aclose()
        async with session.lock:
            await self._release(session, fenced=False)
        await asyncio.gather(*session.exits, return_exceptions=True)

    def _issue(self, session: _Session, pending: AskPending) -> None:
        if pending.ask_id not in session.asks:
            session.asks[pending.ask_id] = self._spawn(self._ask(session, pending))

    def _withdraw(self, session: _Session, ask_id: str) -> None:
        task = session.asks.pop(ask_id, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    def _ask_request(
        self, session: _Session, located: LocatedAsk, tool_call_id: str | None
    ) -> tuple[str, dict[str, Any]] | None:
        event = located.event
        request = event.request
        meta = {"tantra": {"askId": str(UUID(hex=event.ask_id))}}
        if isinstance(request, Approval):
            subject = {"type": "tool_call", "toolCall": {"toolCallId": tool_call_id}} if tool_call_id else None
            params = wire.RequestPermissionParams(
                session_id=str(session.root_id),
                title=request.title,
                description=request.body or None,
                subject=subject,
                options=PERMISSION_OPTIONS,
                meta=meta,
            )
            return "session/request_permission", wire.dump(params)
        if not self.elicitation:
            return None
        if isinstance(request, Choice):
            choice: dict[str, Any] = {"type": "string", "title": request.title}
            if request.options:
                choice["enum"] = list(request.options)
            message, schema = request.title, {"choice": choice}
        else:
            message, schema = request.prompt, {"text": {"type": "string", "title": request.prompt}}
        params = wire.ElicitationParams(
            session_id=str(session.root_id),
            tool_call_id=tool_call_id,
            message=message,
            requested_schema={"type": "object", "properties": schema, "required": list(schema)},
            meta=meta,
        )
        return "elicitation/create", wire.dump(params)

    def _ask_response(self, located: LocatedAsk, result: Any) -> AskResponse | None:
        request = located.event.request
        try:
            if isinstance(request, Approval):
                outcome = wire.PermissionResult.model_validate(result).outcome
                if outcome.outcome != "selected" or outcome.option_id not in ("allow", "deny"):
                    return None
                return ApprovalResponse(allow=outcome.option_id == "allow")
            answer = wire.ElicitationResult.model_validate(result)
        except ValidationError:
            return None
        if answer.action != "accept" or answer.content is None:
            return None
        if isinstance(request, Choice):
            selected = answer.content.get("choice")
            if not isinstance(selected, str) or request.options and selected not in request.options:
                return None
            return ChoiceResponse(selected=selected)
        text = answer.content.get("text")
        return FreeTextResponse(text=text) if isinstance(text, str) else None

    async def _ask(self, session: _Session, pending: AskPending) -> None:
        current = asyncio.current_task()
        try:
            ask_id = UUID(hex=pending.ask_id)
            located = await self.runtime.lookup_ask(session.root_id, ask_id)
            if located is None:
                return
            root = await self._load(session.root_id)
            if not await self._allowed("answer", root, {}, ask=located):
                return
            request = self._ask_request(session, located, pending.tool_call_id)
            if request is None:
                return
            try:
                result = await self.peer.send_request(*request)
            except RpcError:
                return
            response = self._ask_response(located, result)
            if response is None:
                return
            root = await self._load(session.root_id)
            if not await self._allowed("answer", root, {}, ask=located, response=response):
                return
            async with session.lock:
                writer = session.writer
                if writer is None or session.asks.get(pending.ask_id) is not current:
                    return
                try:
                    await writer.answer(ask_id, response, command_id=uuid4(), submitted_by=self.auth.submitted_by)
                except AskExpired:
                    return
                except (WriterReplaced, WriterRequired):
                    await self._downgrade(session, fenced=True, reason="replaced")
        except (SessionNotFound, ConnectionError, PermissionError):
            return
        finally:
            if session.asks.get(pending.ask_id) is current:
                del session.asks[pending.ask_id]

    async def _info(self, session: _Session, change: TreeChange) -> None:
        info = wire.SessionInfoUpdate(title=change.title, updated_at=change.actors[0].updated_at.isoformat())
        await self._update(session.root_id, wire.dump(info))

    async def _follow(self, session: _Session, tree: AsyncIterator[TreeChange]) -> None:
        try:
            async for change in tree:
                if session.closed:
                    return
                if change.title != session.title:
                    session.title = change.title
                    await self._info(session, change)
                if change.model != session.model:
                    session.model = change.model
                    config = self._config(change.model)
                    if config is not None:
                        await self._update(session.root_id, wire.dump(wire.ConfigOptionUpdate(config_options=config)))
                if self.profile:
                    await self._mirror(session, change)
        except SessionNotFound:
            self._lose(session)
        except ConnectionError:
            pass
        except Exception:
            logger.exception("ACP mirror for %s failed", session.root_id)
            self._spawn(self.transport.close())
        finally:
            await tree.aclose()

    async def _mirror(self, session: _Session, change: TreeChange) -> None:
        statuses = change.actors
        seqs = {status.agent_id: status.last_seq for status in statuses}
        hidden: set[UUID] = set()
        for status in statuses:
            if status.parent_id is None or session.closed:
                continue
            if status.parent_id in hidden:
                hidden.add(status.agent_id)
                continue
            raw = (status.name, status.state, _failure(status))
            cached = session.viewed.get(status.agent_id)
            if cached is None or cached[0] != raw:
                cached = raw, await self._viewed(status, seqs[status.parent_id])
                session.viewed[status.agent_id] = cached
            shown = cached[1]
            if shown is None:
                hidden.add(status.agent_id)
                continue
            update = _subagent(status.agent_id, shown[0], _child_state(status, shown[1]))
            if session.mirror.get(status.agent_id) != update:
                session.mirror[status.agent_id] = update
                await self._update(status.parent_id, update)

    async def _concealed(self, child_id: UUID) -> bool:
        if self.server.view is None:
            return False
        status = await self.runtime.status(child_id)
        while status.parent_id is not None:
            parent = await self.runtime.status(status.parent_id)
            if await self._viewed(status, parent.last_seq) is None:
                return True
            status = parent
        return False

    async def _viewed(self, status: ActorStatus, parent_seq: int) -> tuple[str, str | None] | None:
        error = _failure(status)
        view = self.server.view
        if view is None or status.parent_id is None:
            return status.name, error
        created = ChildCreated(
            child_id=status.agent_id.hex, agent=status.agent, name=status.name, turn_id="", call_id=""
        )
        shown = await view(LoggedEvent(status.parent_id, parent_seq, created))
        if shown is None or not isinstance(shown.event, ChildCreated):
            return None
        title = shown.event.name or shown.event.agent
        if error is None or status.last_turn is None:
            return title, None
        failed = TurnFailed(turn_id=status.last_turn.turn_id.hex, error=error)
        redacted = await view(LoggedEvent(status.agent_id, status.last_seq, failed))
        if redacted is None or not isinstance(redacted.event, TurnFailed):
            return title, "Subagent failed"
        return title, redacted.event.error
