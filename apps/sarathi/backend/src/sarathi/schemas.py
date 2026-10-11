from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


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
    model_config = ConfigDict(extra="forbid", strict=True)

    path: str
    name: str


WireId = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]


class TurnSummaryOut(BaseModel):
    turn_id: WireId
    outcome: Literal["completed", "failed", "cancelled", "interrupted"]
    stop_reason: str | None
    error: str | None


class ActorStatusOut(BaseModel):
    agent_id: WireId
    root_id: WireId
    parent_id: WireId | None
    agent: str
    name: str
    state: Literal[
        "queued",
        "running",
        "awaiting_input",
        "idle",
        "finished",
        "failed",
        "cancelled",
        "interrupted",
    ]
    active: bool
    current_turn_id: WireId | None
    last_turn: TurnSummaryOut | None
    last_seq: int
    updated_at: datetime


class SessionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    text: str
    attachments: list[Attachment]


class SocketTicketRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    mode: Literal["read", "write"] = "write"


class SocketTicketOut(BaseModel):
    ticket: str
    expires_at: datetime
    login_expires_at: datetime
    user_id: str


class HeaderNotice(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    type: Literal["header_updated"] = "header_updated"
    title: str | None
    model: str | None
