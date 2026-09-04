import asyncio
from collections.abc import AsyncIterator
from contextlib import aclosing
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from sarathi.agent import FactoryDep, close_harness
from sarathi.auth import resolve_user
from sarathi.config import get_settings
from sarathi.db import get_db
from sarathi.schemas import (
    AskResponseFrame,
    BusyFrame,
    CancelFrame,
    ClientFrame,
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
)
from tantra.events import AskRaised, ChildSessionSpawned, TurnCompleted, TurnFailed, TurnStarted

router = APIRouter(prefix="/api/ws", tags=["ws"])

CLIENT_FRAME_ADAPTER: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)
FALLBACK_RETRY_IN = 5.0
BUSY_TRANSITION_TIMEOUT = 0.5
BUSY_POLL_INTERVAL = 0.01
POLICY_VIOLATION = 1008
ATTACHMENT_MARKER = "[attachment: "


def _typed_response(kind: str, response: str) -> AskResponse:
    if kind == "choice":
        return ChoiceResponse(selected=response)
    if kind == "free_text":
        return FreeTextResponse(text=response)
    return ApprovalResponse(allow=response == "allow")


class Connection:
    def __init__(self, websocket: WebSocket, harness: Harness, sid: str, uid: str) -> None:
        self.websocket = websocket
        self.harness = harness
        self.sid = sid
        self.uid = uid
        self.asks: dict[str, tuple[str, str]] = {}
        self.titled = False
        self.queue: asyncio.Queue[UserMessageFrame | AskResponseFrame] = asyncio.Queue()

    async def send(self, frame: BaseModel) -> None:
        await self.websocket.send_text(frame.model_dump_json())

    def track(self, emitted: Emitted) -> None:
        if isinstance(emitted.event, AskRaised):
            self.asks[emitted.event.ask_id] = (emitted.session_id, emitted.event.request.kind)

    async def replay(self, sid: str) -> None:
        async with aclosing(self.harness.replay(sid)) as stream:
            async for emitted in stream:
                self.track(emitted)
                await self.send(emitted)
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

    async def pump(self, stream: AsyncIterator[Emitted], *, report_busy: bool = True) -> TantraError | None:
        try:
            async with aclosing(stream) as events:
                async for emitted in events:
                    self.track(emitted)
                    await self.send(emitted)
            return None
        except SessionBusy as exc:
            if report_busy:
                await self.send(BusyFrame(retry_in=await self.retry_in(exc.sid)))
            return exc
        except TantraError as exc:
            await self.send(ServerErrorFrame(message=str(exc)))
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
        await self.send(TitleUpdatedFrame(title=title))

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

    async def receive_message(self, frame: UserMessageFrame) -> None:
        message = self.message_text(frame)
        if message is None:
            await self.send(ServerErrorFrame(message="invalid attachment path"))
            return
        if not await self.incomplete(self.sid):
            await self.queue.put(frame)
            return
        try:
            await self.harness.send_user_message(self.sid, message)
        except TantraError as exc:
            ended = str(exc) == f"session {self.sid} has no incomplete turn to receive a user message"
            if ended and not await self.incomplete(self.sid):
                await self.queue.put(frame)
            else:
                await self.send(ServerErrorFrame(message=str(exc)))

    async def retain_busy_message(self, frame: UserMessageFrame, message: str, sid: str) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + BUSY_TRANSITION_TIMEOUT
        while True:
            if await self.incomplete(self.sid):
                try:
                    await self.harness.send_user_message(self.sid, message)
                    return
                except TantraError as exc:
                    ended = str(exc) == f"session {self.sid} has no incomplete turn to receive a user message"
                    if not ended:
                        await self.send(ServerErrorFrame(message=str(exc)))
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
            await self.send(ServerErrorFrame(message="invalid attachment path"))
            return
        header = await self.harness.store.header(self.sid)
        model = header.metadata.get("model") if header is not None else None
        if model:
            self.harness.default_model = model
        failed = await self.pump(self.harness.run(self.sid, message), report_busy=False)
        if isinstance(failed, SessionBusy):
            await self.retain_busy_message(frame, message, failed.sid)

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

        connection = Connection(websocket, harness, session_id, str(user.id))
        await connection.replay(session_id)
        await connection.send(ReplayDoneFrame())

        tasks = [asyncio.create_task(connection.read_loop()), asyncio.create_task(connection.pump_loop())]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await close_harness(harness)
