import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from psycopg import Error as PostgresError
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
    HeaderUpdatedFrame,
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
    CommandTimeout,
    Connection,
    CoordinatorUnavailable,
    FreeTextResponse,
    InvalidCommandReuse,
    LeaseLost,
    LoggedEvent,
    RemoteExecutionError,
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
TRY_AGAIN_LATER = 1013
WRITER_REPLACED = 4009
ATTACHMENT_MARKER = "[attachment: "
TERMINALS = (TurnCompleted, TurnFailed, TurnCancelled, TurnInterrupted)
COMMAND_ERRORS: dict[type[BaseException], tuple[str, bool]] = {
    CommandTimeout: ("command_timeout", True),
    CoordinatorUnavailable: ("coordinator_unavailable", True),
    LeaseLost: ("lease_lost", True),
    RemoteExecutionError: ("remote_execution_error", False),
    InvalidCommandReuse: ("invalid_command_reuse", False),
    WriterRequired: ("writer_required", False),
}


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
    def __init__(
        self,
        websocket: WebSocket,
        resources: RuntimeResources,
        root_id: str,
        user_id: str,
        *,
        readonly: bool = False,
    ) -> None:
        self.websocket = websocket
        self.resources = resources
        self.root_id = root_id
        self.user_id = user_id
        self.readonly = readonly
        self.send_lock = asyncio.Lock()
        self.subscriptions: dict[str, Subscription] = {}
        self.asks: dict[str, str] = {}

    async def send(self, frame: BaseModel) -> None:
        async with self.send_lock:
            await self.websocket.send_text(frame.model_dump_json())

    def track(self, item: LoggedEvent) -> None:
        event = item.event
        if isinstance(event, AskRaised) and item.agent_id.hex == self.root_id:
            self.asks[event.ask_id] = event.request.kind
        elif isinstance(event, AskAnswered) and item.agent_id.hex == self.root_id:
            self.asks.pop(event.ask_id, None)

    async def owns(self, agent_id: str) -> bool:
        header = await self.resources.store.header(agent_id)
        if header is None:
            return False
        return (header.root_id or header.id) == self.root_id and header.metadata.get("user") == self.user_id

    async def forward(self, item: LoggedEvent) -> None:
        self.track(item)
        await self.send(EventFrame(agent_id=item.agent_id.hex, seq=item.seq, event=item.event))

    async def send_error(
        self,
        message: str,
        *,
        code: str,
        command_id: str | None = None,
        retryable: bool = False,
    ) -> None:
        await self.send(ServerErrorFrame(code=code, message=message, command_id=command_id, retryable=retryable))

    async def send_exception(self, exc: BaseException, frame: ClientFrame | None = None) -> None:
        code, retryable = COMMAND_ERRORS.get(type(exc), ("command_failed", False))
        command_id = getattr(frame, "command_id", None)
        await self.send_error(str(exc), code=code, command_id=command_id, retryable=retryable)

    async def watch_headers(self) -> None:
        coordinator = self.resources.runtime.coordinator
        if coordinator is None:
            return
        header = await self.resources.store.header(self.root_id)
        if header is None:
            return
        current = (header.title, header.model)
        try:
            async for notice in coordinator.watch(self.root_id):
                if notice.kind != "header" or notice.actor_id != self.root_id:
                    continue
                header = await self.resources.store.header(self.root_id)
                if header is None:
                    return
                changed = (header.title, header.model)
                if changed == current:
                    continue
                current = changed
                await self.send(HeaderUpdatedFrame(title=header.title, model=header.model))
        except asyncio.CancelledError:
            raise
        except TantraError as exc:
            await self.send_exception(exc)

    async def stream(self, agent_id: str, after: int, writable: bool, subscription: Subscription) -> None:
        try:
            header = await self.resources.store.header(agent_id)
            assert header is not None
            watermark = header.last_seq
            ready = after >= watermark
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
        except (TantraError, PostgresError):
            await self.websocket.close(code=TRY_AGAIN_LATER, reason="coordinator_unavailable")
        finally:
            subscription.entered.set()

    async def send_ready(self, agent_id: str, watermark: int) -> None:
        active = (await self.resources.runtime.status(_uuid(agent_id))).active
        await self.send(SubscriptionReadyFrame(agent_id=agent_id, seq=watermark, active=active))

    async def subscribe(self, frame: SubscribeFrame) -> None:
        if not await self.owns(frame.agent_id):
            await self.send_error("agent is not in this session", code="invalid_subscription")
            return
        if self.readonly and frame.writable:
            await self.send_error("read-only views cannot become writers", code="readonly")
            return
        if frame.writable and frame.agent_id != self.root_id:
            await self.send_error("only the root subscription can be writable", code="invalid_subscription")
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
            await self.send_error(
                "invalid attachment path",
                code="invalid_attachments",
                command_id=frame.command_id,
            )
            return
        lines = [frame.text]
        lines.extend(f"{ATTACHMENT_MARKER}{item.name} path={item.path}]" for item in frame.attachments)
        connection = await self.writable()
        receipt = await connection.send("\n".join(lines), command_id=_uuid(frame.command_id))
        if not receipt.duplicate:
            self.start_title(frame.command_id, frame.text)

    async def ask_response(self, frame: AskResponseFrame) -> None:
        connection = await self.writable()
        kind = self.asks.get(frame.ask_id, "approval")
        try:
            await connection.answer(
                _uuid(frame.ask_id),
                _typed_response(kind, frame.response),
                command_id=_uuid(frame.command_id),
            )
        except AskExpired as exc:
            await self.send(
                AskExpiredFrame(
                    agent_id=self.root_id,
                    ask_id=frame.ask_id,
                    command_id=frame.command_id,
                    message=str(exc),
                )
            )

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
        header_task = asyncio.create_task(self.watch_headers())
        try:
            while True:
                raw = await self.websocket.receive_text()
                try:
                    frame = CLIENT_FRAME_ADAPTER.validate_json(raw)
                    await self.handle(frame)
                except ValidationError:
                    await self.send_error("invalid client frame", code="invalid_frame")
                except WriterReplaced:
                    await self.websocket.close(code=WRITER_REPLACED, reason="writer_replaced")
                    return
                except (TantraError, ValueError) as exc:
                    await self.send_exception(exc, frame)
        except WebSocketDisconnect:
            pass
        finally:
            header_task.cancel()
            await asyncio.gather(header_task, return_exceptions=True)
            for agent_id in list(self.subscriptions):
                await self.unsubscribe(agent_id)


@router.websocket("/sessions/{session_id}")
async def session_socket(
    websocket: WebSocket,
    session_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    token: Annotated[str | None, Query()] = None,
    view: Annotated[Literal["readonly"] | None, Query()] = None,
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
    await SocketBridge(websocket, resources, root_id, str(user.id), readonly=view == "readonly").run()
