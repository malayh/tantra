from __future__ import annotations

import os
import re
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

ContentBlock = dict[str, Any]
PROTOCOL_VERSION = 2
_CURSOR = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


class Wire(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="allow")

    meta: dict[str, Any] | None = Field(default=None, alias="_meta")


def dump(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json", by_alias=True, exclude_none=True)


def text_block(text: str) -> ContentBlock:
    return {"type": "text", "text": text}


def _absolute(value: str) -> str:
    if not os.path.isabs(value):
        raise ValueError("path must be absolute")
    return value


AbsolutePath = Annotated[str, AfterValidator(_absolute)]


class InitializeParams(Wire):
    protocol_version: int
    info: dict[str, Any] | None = None
    capabilities: dict[str, Any] = Field(default_factory=dict)


class NewSessionParams(Wire):
    cwd: AbsolutePath
    mcp_servers: list[Any] = Field(default_factory=list)


class ListSessionsParams(Wire):
    cwd: str | None = None
    cursor: str | None = None


class SessionParams(Wire):
    session_id: str


class ResumeSessionParams(SessionParams):
    cwd: AbsolutePath
    mcp_servers: list[Any] = Field(default_factory=list)
    replay_from: dict[str, Any] | None = None


class PromptParams(SessionParams):
    prompt: list[ContentBlock]

    @field_validator("prompt")
    @classmethod
    def _typed(cls, blocks: list[ContentBlock]) -> list[ContentBlock]:
        if not all(isinstance(block.get("type"), str) for block in blocks):
            raise ValueError("content blocks need a type")
        return blocks


class SetConfigOptionParams(SessionParams):
    config_id: str
    type: str = "id"
    value: Any = None


class PermissionOutcome(Wire):
    outcome: str
    option_id: str | None = None


class PermissionResult(Wire):
    outcome: PermissionOutcome


class ElicitationResult(Wire):
    action: str
    content: dict[str, Any] | None = None


class Implementation(Wire):
    name: str
    version: str


class InitializeResult(Wire):
    protocol_version: int = PROTOCOL_VERSION
    capabilities: dict[str, Any]
    info: Implementation
    auth_methods: list[Any] = Field(default_factory=list)


class ConfigValue(Wire):
    value: str
    name: str


class ConfigOption(Wire):
    config_id: str
    name: str
    category: str | None = None
    type: Literal["select"] = "select"
    current_value: str
    options: list[ConfigValue]


class NewSessionResult(Wire):
    session_id: str
    config_options: list[dict[str, Any]] | None = None


class ResumeSessionResult(Wire):
    config_options: list[dict[str, Any]] | None = None


class SessionInfo(Wire):
    session_id: str
    cwd: str
    title: str | None = None
    updated_at: str | None = None


class ListSessionsResult(Wire):
    sessions: list[SessionInfo]
    next_cursor: str | None = None


class PromptResult(Wire):
    message_id: str


class SetConfigOptionResult(Wire):
    config_options: list[dict[str, Any]]


class ErrorObject(Wire):
    code: int
    message: str


class State(Wire):
    state: Literal["running", "idle", "requires_action"]
    stop_reason: str | None = None
    error: ErrorObject | None = None


class MessageUpdate(Wire):
    session_update: Literal["user_message", "agent_message", "agent_thought"]
    message_id: str
    content: list[ContentBlock]


class MessageChunk(Wire):
    session_update: Literal["agent_message_chunk", "agent_thought_chunk"]
    message_id: str
    content: ContentBlock


class SessionMessage(Wire):
    session_update: Literal["session_message"] = "session_message"
    message_id: str
    sender_session_id: str | None = None
    recipient_session_id: str | None = None
    content: list[ContentBlock]


class ToolCallUpdate(Wire):
    session_update: Literal["tool_call_update"] = "tool_call_update"
    tool_call_id: str
    name: str | None = None
    title: str | None = None
    kind: str | None = None
    status: str | None = None
    content: list[dict[str, Any]] | None = None
    raw_input: Any = None
    raw_output: Any = None


class ToolCallContentChunk(Wire):
    session_update: Literal["tool_call_content_chunk"] = "tool_call_content_chunk"
    tool_call_id: str
    content: dict[str, Any]


class SubagentUpdate(Wire):
    session_update: Literal["subagent_update"] = "subagent_update"
    session_id: str
    title: str | None = None
    state: dict[str, Any] | None = None


class UsageUpdate(Wire):
    session_update: Literal["usage_update"] = "usage_update"
    used: int
    size: int


class CompactionUpdate(Wire):
    session_update: Literal["compaction_update"] = "compaction_update"
    compaction_id: str
    status: str
    summary: list[ContentBlock] | None = None


class CompactionSummaryChunk(Wire):
    session_update: Literal["compaction_summary_chunk"] = "compaction_summary_chunk"
    compaction_id: str
    content: ContentBlock


class SessionInfoUpdate(Wire):
    session_update: Literal["session_info_update"] = "session_info_update"
    title: str | None = None
    updated_at: str | None = None


class ConfigOptionUpdate(Wire):
    session_update: Literal["config_option_update"] = "config_option_update"
    config_options: list[dict[str, Any]]


class WriterUpdate(Wire):
    session_update: Literal["_tantra_writer"] = "_tantra_writer"
    writer: bool
    reason: Literal["claimed", "replaced", "lost"]


class QueueEntry(Wire):
    command_id: str
    content: list[ContentBlock]
    submitted_by: str | None = None


class QueueUpdate(Wire):
    session_update: Literal["_tantra_queue_update"] = "_tantra_queue_update"
    entries: list[dict[str, Any]]


class NoticeUpdate(Wire):
    session_update: Literal["_tantra_notice"] = "_tantra_notice"
    level: Literal["warning"] = "warning"
    code: str
    message: str


def parse_cursor(value: Any) -> tuple[int, int] | None:
    match = _CURSOR.fullmatch(value) if isinstance(value, str) else None
    return (int(match[1]), int(match[2])) if match is not None else None


class PermissionOption(Wire):
    option_id: str
    name: str
    kind: str


class RequestPermissionParams(Wire):
    session_id: str
    title: str
    description: str | None = None
    subject: dict[str, Any] | None = None
    options: list[PermissionOption]


class ElicitationParams(Wire):
    session_id: str
    tool_call_id: str | None = None
    mode: Literal["form"] = "form"
    message: str
    requested_schema: dict[str, Any]
