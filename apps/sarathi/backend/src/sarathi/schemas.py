from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Discriminator, Field

from tantra import SessionEvent


class SignupRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str


class UserOut(BaseModel):
    id: int
    email: str


class SessionOut(BaseModel):
    id: str
    title: str | None = None
    status: str
    model: str | None = None
    updated_at: datetime


class CreateSessionRequest(BaseModel):
    model: str | None = None


class PatchSessionRequest(BaseModel):
    model: str


class MemoryOut(BaseModel):
    id: str
    kind: str
    title: str
    body: str
    tags: list[str] = Field(default_factory=list)
    created_at: datetime


class Attachment(BaseModel):
    path: str
    name: str


WireId = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]


class SubscribeFrame(BaseModel):
    type: Literal["subscribe"] = "subscribe"
    agent_id: WireId
    after: int = Field(default=0, ge=0)
    writable: bool = False


class UnsubscribeFrame(BaseModel):
    type: Literal["unsubscribe"] = "unsubscribe"
    agent_id: WireId


class UserMessageFrame(BaseModel):
    type: Literal["user_message"] = "user_message"
    command_id: WireId
    text: str
    attachments: list[Attachment] = Field(default_factory=list)


class AskResponseFrame(BaseModel):
    type: Literal["ask_response"] = "ask_response"
    command_id: WireId
    ask_id: WireId
    response: str


class CancelFrame(BaseModel):
    type: Literal["cancel"] = "cancel"
    command_id: WireId


ClientFrame = Annotated[
    SubscribeFrame | UnsubscribeFrame | UserMessageFrame | AskResponseFrame | CancelFrame,
    Discriminator("type"),
]


class EventFrame(BaseModel):
    type: Literal["event"] = "event"
    agent_id: WireId
    seq: int = Field(ge=1)
    event: SessionEvent


class SubscriptionReadyFrame(BaseModel):
    type: Literal["subscription_ready"] = "subscription_ready"
    agent_id: WireId
    seq: int = Field(ge=0)
    active: bool


class AskExpiredFrame(BaseModel):
    type: Literal["ask_expired"] = "ask_expired"
    agent_id: WireId
    ask_id: WireId
    message: str


class TitleUpdatedFrame(BaseModel):
    type: Literal["title_updated"] = "title_updated"
    title: str


class ServerErrorFrame(BaseModel):
    type: Literal["server_error"] = "server_error"
    message: str


ServerFrame = EventFrame | SubscriptionReadyFrame | AskExpiredFrame | TitleUpdatedFrame | ServerErrorFrame
