from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from tantra.ask import AskRequest, AskResponse


def _now() -> datetime:
    return datetime.now(UTC)


class EventBase(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int = 1


class Usage(BaseModel):
    model_config = ConfigDict(extra="allow")

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


class Lease(BaseModel):
    model_config = ConfigDict(extra="allow")

    holder: str
    expires_at: datetime


class SessionCreated(EventBase):
    type: Literal["session_created"] = "session_created"
    agent: str
    parent_id: str | None = None
    depth: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class TurnStarted(EventBase):
    type: Literal["turn_started"] = "turn_started"
    turn_id: str
    input: str


class SampleStarted(EventBase):
    type: Literal["sample_started"] = "sample_started"
    turn_id: str
    sample_id: str
    model: str


class TextPart(EventBase):
    type: Literal["text_part"] = "text_part"
    sample_id: str
    text: str


class ReasoningPart(EventBase):
    type: Literal["reasoning_part"] = "reasoning_part"
    sample_id: str
    text: str
    signature: str | None = None


class ToolCallRequested(EventBase):
    type: Literal["tool_call_requested"] = "tool_call_requested"
    sample_id: str
    call_id: str
    name: str
    args: dict[str, Any] = Field(default_factory=dict)


class ToolCallStarted(EventBase):
    type: Literal["tool_call_started"] = "tool_call_started"
    call_id: str


class ToolProgress(EventBase):
    type: Literal["tool_progress"] = "tool_progress"
    call_id: str
    message: str


class ToolCallCompleted(EventBase):
    type: Literal["tool_call_completed"] = "tool_call_completed"
    call_id: str
    result: Any = None
    is_error: bool = False


class ChildSessionSpawned(EventBase):
    type: Literal["child_session_spawned"] = "child_session_spawned"
    call_id: str
    child_session_id: str
    agent: str


class AskRaised(EventBase):
    type: Literal["ask_raised"] = "ask_raised"
    ask_id: str
    call_id: str | None = None
    request: AskRequest


class AskAnswered(EventBase):
    type: Literal["ask_answered"] = "ask_answered"
    ask_id: str
    response: AskResponse
    answered_by: str | None = None


class SampleCompleted(EventBase):
    type: Literal["sample_completed"] = "sample_completed"
    sample_id: str
    usage: Usage = Field(default_factory=Usage)
    finish_reason: str | None = None


class CompactionApplied(EventBase):
    type: Literal["compaction_applied"] = "compaction_applied"
    strategy: str
    tokens_before: int
    tokens_after: int
    summary: str
    floor_turn_id: str | None = None


class CancelRequested(EventBase):
    type: Literal["cancel_requested"] = "cancel_requested"
    turn_id: str


class TurnCompleted(EventBase):
    type: Literal["turn_completed"] = "turn_completed"
    turn_id: str
    stop_reason: str
    output: Any = None


class TurnFailed(EventBase):
    type: Literal["turn_failed"] = "turn_failed"
    turn_id: str
    error: str


SessionEvent = Annotated[
    SessionCreated
    | TurnStarted
    | SampleStarted
    | TextPart
    | ReasoningPart
    | ToolCallRequested
    | ToolCallStarted
    | ToolProgress
    | ToolCallCompleted
    | ChildSessionSpawned
    | AskRaised
    | AskAnswered
    | SampleCompleted
    | CompactionApplied
    | CancelRequested
    | TurnCompleted
    | TurnFailed,
    Field(discriminator="type"),
]

SESSION_EVENT_ADAPTER: TypeAdapter[SessionEvent] = TypeAdapter(SessionEvent)

SessionStatus = Literal["idle", "running", "awaiting_input", "failed"]


class Stamped(BaseModel):
    model_config = ConfigDict(extra="allow")

    seq: int
    event: SessionEvent


class SessionHeader(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    agent: str
    parent_id: str | None = None
    depth: int = 0
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)
    title: str | None = None
    status: SessionStatus = "idle"
    metadata: dict[str, Any] = Field(default_factory=dict)
    last_seq: int = 0
    usage: Usage = Field(default_factory=Usage)
    lease: Lease | None = None
    pending_ask: str | None = None
