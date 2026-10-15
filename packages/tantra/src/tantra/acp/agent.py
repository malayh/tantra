from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ValidationError

from tantra.acp import wire
from tantra.acp.jsonrpc import INTERNAL_ERROR, INVALID_PARAMS, RESOURCE_NOT_FOUND, Peer, Reply, RpcError
from tantra.acp.projection import (
    AskPending,
    AskResolved,
    ChildAnnounced,
    Output,
    Projector,
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
    LeaseLost,
    ModelChangeBusy,
    SessionNotFound,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import LoggedEvent, SessionHeader
from tantra.runtime import Connection, LocatedAsk

MAX_SESSIONS = 8
SEND_ATTEMPTS = 4
LIST_PAGE = 50
TAIL_RETRIES = 8
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


@dataclass(eq=False)
class _Tail:
    actor_id: UUID
    parent_id: UUID | None
    title: str
    projector: Projector
    watermark: int
    emit_after: int
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
    fenced: bool = False
    lost: bool = False
    closed: bool = False
    live: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    asks: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    exits: set[asyncio.Task[None]] = field(default_factory=set)
    watch: asyncio.Task[None] | None = None
    teardown: asyncio.Task[None] | None = None


class AcpConnection:
    def __init__(self, server: AcpServer, transport: Transport, auth: AcpAuth) -> None:
        self.server = server
        self.runtime = server.runtime
        self.transport = transport
        self.auth = auth
        self.sessions: dict[UUID, _Session] = {}
        self.opening = asyncio.Lock()
        self.elicitation = False
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

        return guarded

    async def _load(self, root_id: UUID) -> SessionHeader:
        header = await self.runtime.store.header(root_id.hex)
        if header is None:
            raise SessionNotFound(root_id.hex)
        if header.parent_id is not None:
            raise _child_session()
        if header.agent != self.server.agent:
            raise SessionNotFound(root_id.hex)
        return header

    async def _allowed(
        self,
        operation: Operation,
        root: SessionHeader,
        meta: Mapping[str, Any],
        *,
        ask: LocatedAsk | None = None,
        response: AskResponse | None = None,
    ) -> bool:
        access = AcpAccess(
            operation,
            self.auth,
            root.model_copy(deep=True),
            root.model_copy(deep=True),
            dict(meta),
            ask,
            response.model_copy(deep=True) if response is not None else None,
        )
        return await self.server.policy(access) is True

    async def _authorize(self, operation: Operation, root: SessionHeader, meta: Mapping[str, Any]) -> None:
        if not await self._allowed(operation, root, meta):
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
        if len(self.sessions) >= MAX_SESSIONS:
            raise RpcError(INVALID_PARAMS, "Too many open sessions")

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

    async def _initialize(self, params: Any) -> Any:
        request = _parse(wire.InitializeParams, params)
        elicitation = request.capabilities.get("elicitation")
        self.elicitation = isinstance(elicitation, dict) and isinstance(elicitation.get("form"), dict)
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
        scope = await self._scope(request.meta or {})
        async with self.opening:
            self._capacity()
            root_id = await self.runtime.create(
                self.server.agent, session_id=uuid4(), metadata={**scope, "acp_cwd": request.cwd}
            )
            session = await self._open(root_id, replay=False)
        result = wire.NewSessionResult(session_id=str(root_id), config_options=self._config(session.model))
        return Reply(wire.dump(result), lambda: self._go_live(session))

    async def _list(self, params: Any) -> Any:
        request = _parse(wire.ListSessionsParams, params)
        wanted: dict[str, Any] = await self._scope(request.meta or {})
        if request.cwd is not None:
            wanted["acp_cwd"] = request.cwd
        before = request.cursor
        if before is not None:
            anchor = await self.runtime.store.header(before)
            if anchor is None or anchor.parent_id is not None:
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
        header = await self._load(root_id)
        await self._authorize("resume", header, request.meta or {})
        replay_from = request.replay_from
        if replay_from is not None and replay_from.get("type") != "start":
            raise RpcError(INVALID_PARAMS, "Unsupported replayFrom")
        async with self.opening:
            existing = self.sessions.get(root_id)
            if existing is not None:
                await self._shutdown(existing)
            self._capacity()
            session = await self._open(root_id, replay=replay_from is not None)
        result = wire.ResumeSessionResult(config_options=self._config(session.model))
        return Reply(wire.dump(result), lambda: self._go_live(session))

    async def _prompt(self, params: Any) -> Any:
        request = _parse(wire.PromptParams, params)
        session = self._open_session(request.session_id)
        meta = request.meta or {}
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
                if session.fenced:
                    session.fenced = False
                    raise _writer_required()
                await self._claim(session, meta)
            command_id = uuid4()
            try:
                await self._accept(session, text, command_id)
            except (WriterReplaced, WriterRequired):
                await self._downgrade(session, fenced=False)
                raise
        return wire.dump(wire.PromptResult(message_id=str(command_id)))

    async def _accept(self, session: _Session, text: str, command_id: UUID) -> None:
        writer = session.writer
        assert writer is not None
        task = asyncio.ensure_future(self._send(writer, text, command_id))
        task.add_done_callback(_consume)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if self.peer.closed or current is None:
                raise
            current.uncancel()
            await task

    async def _send(self, writer: Connection, text: str, command_id: UUID) -> None:
        for attempt in range(SEND_ATTEMPTS):
            try:
                await writer.send(text, command_id=command_id, submitted_by=self.auth.submitted_by)
                return
            except (CoordinatorUnavailable, CommandTimeout, LeaseLost):
                if attempt == SEND_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(0.1 * 2**attempt)

    async def _cancel(self, params: Any) -> None:
        request = _parse(wire.SessionParams, params)
        session = self.sessions.get(_session_id(request.session_id))
        if session is None or session.closed:
            return
        meta = request.meta or {}
        async with session.lock:
            root = await self._load(session.root_id)
            await self._authorize("cancel", root, meta)
            command_id = uuid4()
            for _ in range(2):
                await self._claim(session, meta)
                assert session.writer is not None
                try:
                    await session.writer.cancel(command_id=command_id, submitted_by=self.auth.submitted_by)
                    return
                except (WriterReplaced, WriterRequired):
                    await self._downgrade(session, fenced=False)

    async def _close(self, params: Any) -> Any:
        request = _parse(wire.SessionParams, params)
        await self._shutdown(self._open_session(request.session_id))
        return {}

    async def _delete(self, params: Any) -> Any:
        request = _parse(wire.SessionParams, params)
        root_id = _session_id(request.session_id)
        header = await self._load(root_id)
        await self._authorize("delete", header, request.meta or {})
        if not await self.runtime.delete(root_id, allow_active=True):
            raise SessionNotFound(root_id.hex)
        session = self.sessions.get(root_id)
        if session is not None:
            await self._shutdown(session)
        return {}

    async def _set_config_option(self, params: Any) -> Any:
        request = _parse(wire.SetConfigOptionParams, params)
        root_id = _session_id(request.session_id)
        models = self.server.models or []
        if request.config_id != "model" or request.type != "id" or request.value not in models:
            raise RpcError(INVALID_PARAMS)
        header = await self._load(root_id)
        await self._authorize("set_model", header, request.meta or {})
        patched = await self.runtime.store.patch_header(root_id.hex, model=request.value)
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
    ) -> _Tail:
        projector = Projector(actor_id, parent_id, self.server.decode_input, hidden=self._hidden(agent, parent_id))
        tail = _Tail(actor_id, parent_id, title, projector, watermark, emit_after)
        session.tails[actor_id] = tail
        tail.task = self._spawn(self._run(session, tail))
        return tail

    async def _open(self, root_id: UUID, *, replay: bool) -> _Session:
        header = await self._load(root_id)
        status = await self.runtime.status(root_id)
        session = _Session(root_id, replay, title=header.title, model=header.model)
        self.sessions[root_id] = session
        try:
            self._tail(
                session,
                root_id,
                None,
                header.agent,
                header.name or header.agent,
                status.last_seq,
                0 if replay else status.last_seq,
            )
            while pending := [tail.ready.wait() for tail in session.tails.values() if not tail.ready.is_set()]:
                await asyncio.gather(*pending)
            if session.lost:
                raise SessionNotFound(root_id.hex)
        except BaseException:
            await self._shutdown(session)
            raise
        return session

    async def _go_live(self, session: _Session) -> None:
        if session.closed:
            return
        header = await self.runtime.store.header(session.root_id.hex)
        if header is None:
            await self._shutdown(session)
            return
        session.title, session.model = header.title, header.model
        info = wire.SessionInfoUpdate(title=header.title, updated_at=header.updated_at.isoformat())
        await self._update(session.root_id, wire.dump(info))
        for tail in list(session.tails.values()):
            if tail.parent_id is not None:
                await self._update(tail.parent_id, self._subagent(tail, tail.projector.state, titled=True))
            await self._update(tail.actor_id, {"sessionUpdate": "state_update", **tail.projector.state})
        session.live.set()
        if self.runtime.coordinator is not None and not session.closed:
            session.watch = self._spawn(self._watch_header(session))

    def _subagent(self, tail: _Tail, state: dict[str, Any], *, titled: bool = False) -> dict[str, Any]:
        finished = state.get("stopReason") == "_tantra_finished"
        update = wire.SubagentUpdate(
            session_id=str(tail.actor_id),
            title=tail.title if titled else None,
            state=state,
            meta={"tantra": {"finished": True}} if finished else None,
        )
        return wire.dump(update)

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
                        if item.seq > tail.watermark and not session.live.is_set():
                            await session.live.wait()
                        async with tail.lock:
                            if item.seq > tail.cursor:
                                await self._process(session, tail, item)
                    if not session.closed:
                        raise RuntimeError("event stream ended")
                    return
                except (WriterReplaced, WriterRequired, CoordinatorUnavailable, LeaseLost) as error:
                    if writer is None and isinstance(error, WriterReplaced | WriterRequired):
                        raise
                    if isinstance(error, CoordinatorUnavailable | LeaseLost):
                        failures += 1
                        if failures > TAIL_RETRIES:
                            raise
                        await asyncio.sleep(min(0.1 * 2 ** (failures - 1), 2.0))
                    if writer is not None:
                        async with session.lock:
                            if session.writer is writer:
                                await self._release(session, fenced=True)
                finally:
                    if writer is None:
                        await source.aclose()
        except SessionNotFound:
            session.lost = True
            if not session.closed:
                self._spawn(self._shutdown(session))
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
        outputs = tail.projector.feed(event, replay=mode != "live") if event is not None else []
        tail.cursor = item.seq
        settled = not tail.ready.is_set() and tail.cursor >= tail.watermark
        if settled:
            outputs = [*outputs, *tail.projector.flush()]
        for output in outputs:
            await self._apply(session, tail, output, mode)
        if settled:
            if tail.usage is not None:
                usage, tail.usage = tail.usage, None
                await self._usage(tail, usage)
            if session.live.is_set() and tail.parent_id is not None and tail.projector.state != {"state": "idle"}:
                await self._update(tail.parent_id, self._subagent(tail, tail.projector.state))
            tail.ready.set()

    async def _apply(self, session: _Session, tail: _Tail, output: Output, mode: str) -> None:
        if isinstance(output, dict):
            if mode != "discard":
                await self._update(tail.actor_id, output)
        elif isinstance(output, ChildAnnounced):
            await self._child(session, tail, output, mode)
        elif isinstance(output, UsageSample):
            if mode == "replay":
                tail.usage = output
            elif mode == "live":
                await self._usage(tail, output)
        elif mode == "live":
            if isinstance(output, TurnState) and tail.parent_id is not None:
                await self._update(tail.parent_id, self._subagent(tail, output.state))
            elif isinstance(output, AskPending) and session.writer is not None:
                self._issue(session, output)
            elif isinstance(output, AskResolved):
                self._withdraw(session, output.ask_id)

    async def _usage(self, tail: _Tail, sample: UsageSample) -> None:
        window = await self.server.context_window(sample.model)
        if window is not None:
            await self._update(tail.actor_id, wire.dump(wire.UsageUpdate(used=sample.used, size=window)))

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

    async def _claim(self, session: _Session, meta: Mapping[str, Any]) -> None:
        if session.closed:
            raise SessionNotFound(session.root_id.hex)
        if session.writer is not None:
            return
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
                for pending in tail.projector.pending_asks:
                    self._issue(session, pending)
            finally:
                self._restart(session, tail)

    async def _downgrade(self, session: _Session, *, fenced: bool) -> None:
        if session.writer is None:
            return
        tail = session.tails[session.root_id]
        async with tail.lock:
            try:
                await self._stop(tail)
                await self._release(session, fenced=fenced)
            finally:
                self._restart(session, tail)

    async def _release(self, session: _Session, *, fenced: bool) -> None:
        connection, session.writer = session.writer, None
        session.fenced = fenced
        current = asyncio.current_task()
        for task in session.asks.values():
            if task is not current:
                task.cancel()
        session.asks.clear()
        if connection is not None:
            task = self._spawn(self._exit(connection), cleanup=True)
            session.exits.add(task)
            task.add_done_callback(session.exits.discard)
            await asyncio.shield(task)

    async def _exit(self, connection: Connection) -> None:
        try:
            await connection.__aexit__(None, None, None)
        except Exception:
            logger.debug("writer release failed", exc_info=True)

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
        tasks.append(session.watch)
        pending = [task for task in tasks if task is not None]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
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
                    await self._downgrade(session, fenced=True)
        except (SessionNotFound, ConnectionError, PermissionError):
            return
        finally:
            if session.asks.get(pending.ask_id) is current:
                del session.asks[pending.ask_id]

    async def _watch_header(self, session: _Session) -> None:
        coordinator = self.runtime.coordinator
        assert coordinator is not None
        root = session.root_id.hex
        try:
            async for notice in coordinator.watch(root):
                if notice.kind == "deleted":
                    return
                if notice.kind != "header" or notice.actor_id != root:
                    continue
                header = await self.runtime.store.header(root)
                if header is None:
                    return
                if header.title != session.title:
                    session.title = header.title
                    info = wire.SessionInfoUpdate(title=header.title, updated_at=header.updated_at.isoformat())
                    await self._update(session.root_id, wire.dump(info))
                if header.model != session.model:
                    session.model = header.model
                    config = self._config(header.model)
                    if config is not None:
                        await self._update(session.root_id, wire.dump(wire.ConfigOptionUpdate(config_options=config)))
        except CoordinatorUnavailable:
            logger.warning("header watch for %s stopped", root)
