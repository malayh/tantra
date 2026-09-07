from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import aclosing
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.websockets import WebSocketDisconnect, WebSocketState

from sarathi.agent import FactoryDep, close_harness
from sarathi.auth import resolve_user
from sarathi.config import get_settings
from sarathi.db import get_db
from sarathi.schemas import (
    AskResponseFrame,
    BusyFrame,
    CancelFrame,
    ClientFrame,
    MessageAcceptedFrame,
    ReplayDoneFrame,
    ServerErrorFrame,
    TitleUpdatedFrame,
    UserMessageFrame,
)
from sarathi.titles import generate_title
from tantra import (
    ApprovalResponse,
    AskResponse,
    ChoiceResponse,
    Emitted,
    FreeTextResponse,
    Harness,
    SessionBusy,
    TantraError,
    TurnNotAcceptingMessages,
)
from tantra.events import (
    AgentMessageQueued,
    AskRaised,
    ChildSessionSpawned,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)

router = APIRouter(prefix="/api/ws", tags=["ws"])

CLIENT_FRAME_ADAPTER: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)
FALLBACK_RETRY_IN = 5.0
BUSY_TRANSITION_TIMEOUT = 0.5
BUSY_POLL_INTERVAL = 0.01
POLICY_VIOLATION = 1008
INTERNAL_ERROR = 1011
ATTACHMENT_MARKER = "[attachment: "
logger = logging.getLogger(__name__)


def _typed_response(kind: str, response: str) -> AskResponse:
    if kind == "choice":
        return ChoiceResponse(selected=response)
    if kind == "free_text":
        return FreeTextResponse(text=response)
    return ApprovalResponse(allow=response == "allow")


class ConnectionHub:
    def __init__(self) -> None:
        self.connections: dict[str, set[Connection]] = {}
        self.lock = asyncio.Lock()

    async def register(self, connection: Connection) -> None:
        async with self.lock:
            self.connections.setdefault(connection.sid, set()).add(connection)

    async def activate(self, connection: Connection) -> None:
        async with self.lock:
            connection.replaying = False
            buffered, connection.buffered = connection.buffered, []
            for frame in buffered:
                await connection.deliver(frame)

    async def unregister(self, connection: Connection) -> None:
        async with self.lock:
            members = self.connections.get(connection.sid)
            if members is None:
                return
            members.discard(connection)
            if not members:
                self.connections.pop(connection.sid, None)

    async def publish(self, sid: str, frame: BaseModel) -> None:
        async with self.lock:
            failed: list[Connection] = []
            for connection in self.connections.get(sid, ()):
                try:
                    if connection.replaying:
                        connection.buffered.append(frame)
                    else:
                        await connection.deliver(frame)
                except Exception:
                    failed.append(connection)
            for connection in failed:
                self.connections[sid].discard(connection)
            if sid in self.connections and not self.connections[sid]:
                self.connections.pop(sid)


CONNECTIONS = ConnectionHub()


class Connection:
    def __init__(
        self, websocket: WebSocket, harness: Harness, sid: str, uid: str, hub: ConnectionHub | None = None
    ) -> None:
        self.websocket = websocket
        self.harness = harness
        self.sid = sid
        self.uid = uid
        self.hub = hub
        self.asks: dict[str, tuple[str, str]] = {}
        self.titled = False
        self.queue: asyncio.Queue[UserMessageFrame | AskResponseFrame] = asyncio.Queue()
        self.send_lock = asyncio.Lock()
        self.replaying = hub is not None
        self.buffered: list[BaseModel] = []
        self.seen: set[tuple[str, int]] = set()

    async def send(self, frame: BaseModel) -> None:
        async with self.send_lock:
            await self.websocket.send_text(frame.model_dump_json())

    async def deliver(self, frame: BaseModel) -> None:
        if isinstance(frame, Emitted):
            if frame.seq is not None:
                key = (frame.session_id, frame.seq)
                if key in self.seen:
                    return
                self.seen.add(key)
            self.track(frame)
        await self.send(frame)

    async def publish(self, frame: BaseModel) -> None:
        if self.hub is None:
            await self.deliver(frame)
        else:
            await self.hub.publish(self.sid, frame)

    def track(self, emitted: Emitted) -> None:
        if isinstance(emitted.event, AskRaised):
            self.asks[emitted.event.ask_id] = (emitted.session_id, emitted.event.request.kind)

    async def replay(self, sid: str) -> None:
        async with aclosing(self.harness.replay(sid)) as stream:
            async for emitted in stream:
                await self.deliver(emitted)
                if isinstance(emitted.event, ChildSessionSpawned):
                    await self.replay(emitted.event.child_session_id)

    async def incomplete(self, sid: str) -> bool:
        last: Any = None
        async for stamped in self.harness.store.read(sid):
            if isinstance(stamped.event, TurnStarted | TurnCompleted | TurnFailed):
                last = stamped.event
        return isinstance(last, TurnStarted)

    async def retry_in(self, sid: str) -> float:
        header = await self.harness.store.header(sid)
        lease = header.lease if header is not None else None
        if lease is None:
            return FALLBACK_RETRY_IN
        return max((lease.expires_at - datetime.now(UTC)).total_seconds(), 0.0)

    async def pump(
        self, stream: AsyncIterator[Emitted], *, report_busy: bool = True, request_id: UUID | None = None
    ) -> TantraError | None:
        try:
            async with aclosing(stream) as events:
                async for emitted in events:
                    await self.publish(emitted)
            return None
        except SessionBusy as exc:
            if report_busy:
                await self.send(BusyFrame(retry_in=await self.retry_in(exc.sid)))
            return exc
        except TantraError as exc:
            await self.send(ServerErrorFrame(message=str(exc), request_id=request_id))
            return exc

    async def read_loop(self) -> None:
        while True:
            raw = await self.websocket.receive_text()
            try:
                frame = CLIENT_FRAME_ADAPTER.validate_json(raw)
            except ValidationError:
                continue
            if isinstance(frame, CancelFrame):
                try:
                    await self.harness.cancel(self.sid, recursive=True)
                except TantraError as exc:
                    await self.send(ServerErrorFrame(message=str(exc)))
                continue
            if isinstance(frame, UserMessageFrame):
                await self.receive_message(frame)
            else:
                await self.queue.put(frame)

    async def maybe_title(self) -> None:
        if self.titled:
            return
        header = await self.harness.store.header(self.sid)
        if header is None:
            return
        if header.title is not None:
            self.titled = True
            return
        if await self.incomplete(self.sid):
            return
        started: Any = None
        async for stamped in self.harness.store.read(self.sid):
            if isinstance(stamped.event, TurnStarted):
                started = stamped.event
        if started is None:
            return
        lines = [line for line in started.input.splitlines() if not line.startswith(ATTACHMENT_MARKER)]
        text = "\n".join(lines).strip()
        if not text:
            return
        self.titled = True
        title = await generate_title(self.harness.provider, self.harness.default_model or "", text)
        if not title:
            return
        header = await self.harness.store.header(self.sid)
        if header is None or header.title is not None:
            return
        await self.harness.store.patch_header(self.sid, title=title)
        await self.publish(TitleUpdatedFrame(title=title))

    async def pump_loop(self) -> None:
        if await self.incomplete(self.sid):
            await self.pump(self.harness.resume(self.sid))
        await self.maybe_title()
        while True:
            frame = await self.queue.get()
            if isinstance(frame, UserMessageFrame):
                await self.run_turn(frame)
            else:
                await self.answer_ask(frame)
            await self.maybe_title()

    def owns_attachments(self, frame: UserMessageFrame) -> bool:
        root = (Path(get_settings().UPLOAD_DIR) / self.uid).resolve()
        return all(Path(item.path).resolve().is_relative_to(root) for item in frame.attachments)

    def message_text(self, frame: UserMessageFrame) -> str | None:
        if not self.owns_attachments(frame):
            return None
        lines = [frame.text]
        lines.extend(f"{ATTACHMENT_MARKER}{item.name} path={item.path}]" for item in frame.attachments)
        return "\n".join(lines)

    async def accept(self, frame: UserMessageFrame) -> None:
        await self.send(MessageAcceptedFrame(request_id=frame.request_id))

    async def reject(self, frame: UserMessageFrame, error: TantraError | str) -> None:
        await self.send(ServerErrorFrame(message=str(error), request_id=frame.request_id))

    async def request_recorded(self, frame: UserMessageFrame, message: str) -> bool:
        request_id = frame.request_id.hex
        async for stamped in self.harness.store.read(self.sid):
            event = stamped.event
            if isinstance(event, AgentMessageQueued) and event.message_id == request_id:
                if event.sender_session_id is not None or event.source != "user" or event.text != message:
                    raise TantraError(f"request_id {request_id!r} already exists with different text")
                return True
            if isinstance(event, TurnStarted) and event.turn_id == request_id:
                if event.input != message:
                    raise TantraError(f"request_id {request_id!r} already exists with different input")
                return True
        return False

    async def receive_message(self, frame: UserMessageFrame) -> None:
        message = self.message_text(frame)
        if message is None:
            await self.reject(frame, "invalid attachment path")
            return
        try:
            if await self.request_recorded(frame, message):
                await self.accept(frame)
                return
            if not await self.incomplete(self.sid):
                await self.queue.put(frame)
                return
            await self.harness.send_user_message(self.sid, message, message_id=frame.request_id.hex)
        except TurnNotAcceptingMessages:
            await self.queue.put(frame)
        except TantraError as exc:
            await self.reject(frame, exc)
        else:
            await self.accept(frame)

    async def retain_busy_message(self, frame: UserMessageFrame, message: str, sid: str) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + BUSY_TRANSITION_TIMEOUT
        while True:
            try:
                if await self.request_recorded(frame, message):
                    await self.accept(frame)
                    return
                if await self.incomplete(self.sid):
                    await self.harness.send_user_message(self.sid, message, message_id=frame.request_id.hex)
                    await self.accept(frame)
                    return
            except TurnNotAcceptingMessages:
                pass
            except TantraError as exc:
                await self.reject(frame, exc)
                return
            header = await self.harness.store.header(self.sid)
            lease = header.lease if header is not None else None
            now = datetime.now(UTC)
            if lease is None or lease.expires_at <= now:
                await self.queue.put(frame)
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                retry_in = await self.retry_in(sid)
                await self.send(BusyFrame(retry_in=retry_in))
                await asyncio.sleep(min(max(retry_in, BUSY_POLL_INTERVAL), FALLBACK_RETRY_IN))
                await self.queue.put(frame)
                return
            lease_remaining = max((lease.expires_at - now).total_seconds(), 0.0)
            await asyncio.sleep(min(BUSY_POLL_INTERVAL, remaining, lease_remaining))

    async def run_turn(self, frame: UserMessageFrame) -> None:
        message = self.message_text(frame)
        if message is None:
            await self.reject(frame, "invalid attachment path")
            return
        try:
            if await self.request_recorded(frame, message):
                await self.accept(frame)
                return
        except TantraError as exc:
            await self.reject(frame, exc)
            return
        header = await self.harness.store.header(self.sid)
        model = header.metadata.get("model") if header is not None else None
        if model:
            self.harness.default_model = model
        accepted = False

        async def stream() -> AsyncIterator[Emitted]:
            nonlocal accepted
            async for emitted in self.harness.run(self.sid, message, turn_id=frame.request_id.hex):
                yield emitted
                if (
                    not accepted
                    and emitted.session_id == self.sid
                    and isinstance(emitted.event, TurnStarted)
                    and emitted.event.turn_id == frame.request_id.hex
                ):
                    await self.accept(frame)
                    accepted = True

        failed = await self.pump(stream(), report_busy=False, request_id=frame.request_id)
        if isinstance(failed, SessionBusy):
            await self.retain_busy_message(frame, message, failed.sid)
        elif failed is None and not accepted:
            try:
                if await self.request_recorded(frame, message):
                    await self.accept(frame)
            except TantraError as exc:
                await self.reject(frame, exc)

    async def answer_ask(self, frame: AskResponseFrame) -> None:
        target, kind = self.asks.get(frame.ask_id, (self.sid, "approval"))
        await self.pump(self.harness.resume(target, frame.ask_id, _typed_response(kind, frame.response)))
        if target != self.sid and await self.incomplete(self.sid):
            await self.pump(self.harness.resume(self.sid))


@router.websocket("/sessions/{session_id}")
async def session_socket(
    websocket: WebSocket,
    session_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    factory: FactoryDep,
    token: Annotated[str | None, Query()] = None,
) -> None:
    await websocket.accept()
    try:
        user = await resolve_user(token, db)
    except HTTPException:
        await websocket.close(code=POLICY_VIOLATION)
        return

    harness = factory(None)
    try:
        header = await harness.store.header(session_id)
        if header is None or header.parent_id is not None or header.metadata.get("user") != str(user.id):
            await websocket.close(code=POLICY_VIOLATION)
            return
        harness.default_model = header.metadata.get("model") or harness.default_model

        connection = Connection(websocket, harness, session_id, str(user.id), CONNECTIONS)
        await CONNECTIONS.register(connection)
        try:
            await connection.replay(session_id)
            await CONNECTIONS.activate(connection)
            await connection.send(ReplayDoneFrame())

            read_task = asyncio.create_task(connection.read_loop())
            pump_task = asyncio.create_task(connection.pump_loop())
            tasks = [read_task, pump_task]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if pump_task in done:
                    try:
                        pump_task.result()
                    except (SessionBusy, WebSocketDisconnect):
                        pass
                    except Exception:
                        logger.exception("session pump failed")
                        if (
                            websocket.client_state is WebSocketState.CONNECTED
                            and websocket.application_state is WebSocketState.CONNECTED
                        ):
                            try:
                                await connection.send(ServerErrorFrame(message="Unexpected server error"))
                            except Exception:
                                pass
                            if websocket.application_state is WebSocketState.CONNECTED:
                                await websocket.close(code=INTERNAL_ERROR)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await CONNECTIONS.unregister(connection)
    finally:
        await close_harness(harness)
