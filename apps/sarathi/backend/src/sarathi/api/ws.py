import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from sarathi.agent import RuntimeResources
from sarathi.auth import resolve_user
from sarathi.config import get_settings
from sarathi.db import get_db
from sarathi.schemas import (
    AskExpiredFrame,
    AskResponseFrame,
    CancelFrame,
    ClientFrame,
    EventFrame,
    ServerErrorFrame,
    SubscribeFrame,
    SubscriptionReadyFrame,
    TitleUpdatedFrame,
    UnsubscribeFrame,
    UserMessageFrame,
)
from sarathi.titles import generate_title
from tantra import (
    ApprovalResponse,
    AskExpired,
    AskResponse,
    ChoiceResponse,
    Connection,
    FreeTextResponse,
    LoggedEvent,
    TantraError,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import (
    AskAnswered,
    AskRaised,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
)

router = APIRouter(prefix="/api/ws", tags=["ws"])

CLIENT_FRAME_ADAPTER: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)
POLICY_VIOLATION = 1008
WRITER_REPLACED = 4009
ATTACHMENT_MARKER = "[attachment: "
TERMINALS = (TurnCompleted, TurnFailed, TurnCancelled, TurnInterrupted)


def _typed_response(kind: str, response: str) -> AskResponse:
    if kind == "choice":
        return ChoiceResponse(selected=response)
    if kind == "free_text":
        return FreeTextResponse(text=response)
    return ApprovalResponse(allow=response == "allow")


def _uuid(value: str) -> UUID:
    return UUID(hex=value)


@dataclass
class Subscription:
    task: asyncio.Task[None] | None = None
    connection: Connection | None = None
    entered: asyncio.Event = field(default_factory=asyncio.Event)


class SocketBridge:
    def __init__(self, websocket: WebSocket, resources: RuntimeResources, root_id: str, user_id: str) -> None:
        self.websocket = websocket
        self.resources = resources
        self.root_id = root_id
        self.user_id = user_id
        self.send_lock = asyncio.Lock()
        self.subscriptions: dict[str, Subscription] = {}
        self.asks: dict[str, tuple[str, str]] = {}

    async def send(self, frame: BaseModel) -> None:
        async with self.send_lock:
            await self.websocket.send_text(frame.model_dump_json())

    def track(self, item: LoggedEvent) -> None:
        event = item.event
        if isinstance(event, AskRaised):
            self.asks[event.ask_id] = (item.agent_id.hex, event.request.kind)
        elif isinstance(event, AskAnswered):
            self.asks.pop(event.ask_id, None)

    async def owns(self, agent_id: str) -> bool:
        header = await self.resources.store.header(agent_id)
        if header is None:
            return False
        return (header.root_id or header.id) == self.root_id and header.metadata.get("user") == self.user_id

    async def forward(self, item: LoggedEvent) -> None:
        self.track(item)
        await self.send(EventFrame(agent_id=item.agent_id.hex, seq=item.seq, event=item.event))

    async def stream(self, agent_id: str, after: int, writable: bool, subscription: Subscription) -> None:
        header = await self.resources.store.header(agent_id)
        assert header is not None
        watermark = header.last_seq
        ready = after >= watermark
        try:
            if agent_id == self.root_id:
                async with self.resources.runtime.connect(
                    _uuid(agent_id), after=after, writable=writable
                ) as connection:
                    subscription.connection = connection
                    subscription.entered.set()
                    if ready:
                        await self.send_ready(agent_id, watermark)
                    async for item in connection:
                        await self.forward(item)
                        if not ready and item.seq >= watermark:
                            ready = True
                            await self.send_ready(agent_id, watermark)
            else:
                subscription.entered.set()
                if ready:
                    await self.send_ready(agent_id, watermark)
                async for item in self.resources.runtime.events(_uuid(agent_id), after=after):
                    await self.forward(item)
                    if not ready and item.seq >= watermark:
                        ready = True
                        await self.send_ready(agent_id, watermark)
        except WriterReplaced:
            await self.websocket.close(code=WRITER_REPLACED, reason="writer_replaced")
        except asyncio.CancelledError:
            raise
        except (RuntimeError, WebSocketDisconnect):
            return
        except TantraError as exc:
            await self.send(ServerErrorFrame(message=str(exc)))
        finally:
            subscription.entered.set()

    async def send_ready(self, agent_id: str, watermark: int) -> None:
        task = self.resources.runtime.active.get(agent_id)
        active = task is not None and not task.done()
        await self.send(SubscriptionReadyFrame(agent_id=agent_id, seq=watermark, active=active))

    async def subscribe(self, frame: SubscribeFrame) -> None:
        if not await self.owns(frame.agent_id):
            await self.send(ServerErrorFrame(message="agent is not in this session"))
            return
        if frame.writable and frame.agent_id != self.root_id:
            await self.send(ServerErrorFrame(message="only the root subscription can be writable"))
            return
        await self.unsubscribe(frame.agent_id)
        subscription = Subscription()
        self.subscriptions[frame.agent_id] = subscription
        subscription.task = asyncio.create_task(self.stream(frame.agent_id, frame.after, frame.writable, subscription))

    async def unsubscribe(self, agent_id: str) -> None:
        subscription = self.subscriptions.pop(agent_id, None)
        if subscription is None or subscription.task is None:
            return
        subscription.task.cancel()
        await asyncio.gather(subscription.task, return_exceptions=True)

    async def writable(self) -> Connection:
        subscription = self.subscriptions.get(self.root_id)
        if subscription is None:
            raise WriterRequired("subscribe to the root with writable=true first")
        await subscription.entered.wait()
        if subscription.connection is None or not subscription.connection.writable:
            raise WriterRequired("subscribe to the root with writable=true first")
        return subscription.connection

    def owns_attachments(self, frame: UserMessageFrame) -> bool:
        root = (Path(get_settings().UPLOAD_DIR) / self.user_id).resolve()
        return all(Path(item.path).resolve().is_relative_to(root) for item in frame.attachments)

    async def user_message(self, frame: UserMessageFrame) -> None:
        if not self.owns_attachments(frame):
            await self.send(ServerErrorFrame(message="invalid attachment path"))
            return
        lines = [frame.text]
        lines.extend(f"{ATTACHMENT_MARKER}{item.name} path={item.path}]" for item in frame.attachments)
        connection = await self.writable()
        receipt = await connection.send("\n".join(lines), command_id=_uuid(frame.command_id))
        if not receipt.duplicate:
            self.start_title(frame.command_id, frame.text)

    async def ask_response(self, frame: AskResponseFrame) -> None:
        connection = await self.writable()
        agent_id, kind = self.asks.get(frame.ask_id, (self.root_id, "approval"))
        try:
            await connection.answer(
                _uuid(frame.ask_id),
                _typed_response(kind, frame.response),
                command_id=_uuid(frame.command_id),
            )
        except AskExpired as exc:
            await self.send(AskExpiredFrame(agent_id=agent_id, ask_id=frame.ask_id, message=str(exc)))

    async def cancel(self, frame: CancelFrame) -> None:
        connection = await self.writable()
        await connection.cancel(command_id=_uuid(frame.command_id))

    def start_title(self, command_id: str, text: str) -> None:
        if self.root_id in self.resources.title_started:
            return
        clean = "\n".join(line for line in text.splitlines() if not line.startswith(ATTACHMENT_MARKER)).strip()
        if not clean:
            return
        self.resources.title_started.add(self.root_id)
        task = asyncio.create_task(self.title_after_turn(command_id, clean))
        self.resources.title_tasks[self.root_id] = task
        task.add_done_callback(lambda done: self.resources.title_tasks.pop(self.root_id, None))

    async def title_after_turn(self, command_id: str, text: str) -> None:
        async for item in self.resources.runtime.events(_uuid(self.root_id)):
            event = item.event
            if isinstance(event, TERMINALS) and event.turn_id == command_id:
                break
        header = await self.resources.store.header(self.root_id)
        if header is None or header.title is not None:
            return
        title = await generate_title(self.resources.provider, header.model or "", text)
        if not title:
            return
        header = await self.resources.store.header(self.root_id)
        if header is None or header.title is not None:
            return
        await self.resources.store.patch_header(self.root_id, title=title)
        try:
            await self.send(TitleUpdatedFrame(title=title))
        except (RuntimeError, WebSocketDisconnect):
            return

    async def handle(self, frame: ClientFrame) -> None:
        if isinstance(frame, SubscribeFrame):
            await self.subscribe(frame)
        elif isinstance(frame, UnsubscribeFrame):
            await self.unsubscribe(frame.agent_id)
        elif isinstance(frame, UserMessageFrame):
            await self.user_message(frame)
        elif isinstance(frame, AskResponseFrame):
            await self.ask_response(frame)
        elif isinstance(frame, CancelFrame):
            await self.cancel(frame)

    async def run(self) -> None:
        try:
            while True:
                raw = await self.websocket.receive_text()
                try:
                    frame = CLIENT_FRAME_ADAPTER.validate_json(raw)
                    await self.handle(frame)
                except ValidationError:
                    await self.send(ServerErrorFrame(message="invalid client frame"))
                except WriterReplaced:
                    await self.websocket.close(code=WRITER_REPLACED, reason="writer_replaced")
                    return
                except (TantraError, ValueError) as exc:
                    await self.send(ServerErrorFrame(message=str(exc)))
        except WebSocketDisconnect:
            pass
        finally:
            for agent_id in list(self.subscriptions):
                await self.unsubscribe(agent_id)


@router.websocket("/sessions/{session_id}")
async def session_socket(
    websocket: WebSocket,
    session_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    token: Annotated[str | None, Query()] = None,
) -> None:
    await websocket.accept()
    try:
        root_id = _uuid(session_id).hex
        user = await resolve_user(token, db)
    except (HTTPException, ValueError):
        await websocket.close(code=POLICY_VIOLATION)
        return
    resources: RuntimeResources = websocket.app.state.resources
    header = await resources.store.header(root_id)
    if header is None or header.parent_id is not None or header.metadata.get("user") != str(user.id):
        await websocket.close(code=POLICY_VIOLATION)
        return
    await SocketBridge(websocket, resources, root_id, str(user.id)).run()
