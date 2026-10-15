from datetime import datetime
from typing import Literal

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


class AcpTicketRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    mode: Literal["read", "write"] = "write"


class AcpTicketOut(BaseModel):
    ticket: str
    expires_at: datetime
