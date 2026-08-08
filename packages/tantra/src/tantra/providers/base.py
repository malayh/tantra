from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from tantra.events import Usage


class ModelLimits(BaseModel):
    model_config = ConfigDict(extra="allow")

    context_window: int
    max_output: int


class SystemBlock(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str
    cache: bool = False


class ReasoningBlock(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str
    signature: str | None = None


class ToolCall(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: Literal["tool_call"] = "tool_call"
    id: str
    name: str
    args: str = ""


class UserMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: Literal["user"] = "user"
    content: str


class AssistantMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: Literal["assistant"] = "assistant"
    text: str | None = None
    reasoning: list[ReasoningBlock] = Field(default_factory=list)
    tool_calls: list[ToolCall] = Field(default_factory=list)


class ToolResultMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: Literal["tool"] = "tool"
    call_id: str
    content: str
    is_error: bool = False


Message = Annotated[
    UserMessage | AssistantMessage | ToolResultMessage,
    Field(discriminator="role"),
]


class ToolSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str = ""
    parameters: dict[str, Any] = Field(default_factory=dict)


class SampleRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    model: str
    system: list[SystemBlock] = Field(default_factory=list)
    messages: list[Message] = Field(default_factory=list)
    tools: list[ToolSchema] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)


class TextDelta(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: Literal["text_delta"] = "text_delta"
    text: str


class ReasoningDelta(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: Literal["reasoning_delta"] = "reasoning_delta"
    text: str


class ToolCallDelta(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: Literal["tool_call_delta"] = "tool_call_delta"
    index: int
    id: str | None = None
    name: str | None = None
    args_fragment: str = ""


class StreamEnd(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: Literal["stream_end"] = "stream_end"
    text: str = ""
    reasoning: list[ReasoningBlock] = Field(default_factory=list)
    tool_calls: list[ToolCall] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    finish_reason: str | None = None


ProviderEvent = Annotated[
    TextDelta | ReasoningDelta | ToolCallDelta | ToolCall | StreamEnd,
    Field(discriminator="type"),
]


class Provider(Protocol):
    """One model vendor's transport. The model itself rides on `SampleRequest.model`."""

    def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        """Sample the model, yielding live fragments then one terminal `StreamEnd`.

        `TextDelta`, `ReasoningDelta` and `ToolCallDelta` are live-only and never persisted. Each
        complete `ToolCall` is yielded once its fragments are whole, before `StreamEnd`. `ToolCall.args`
        is the raw JSON string exactly as the vendor sent it — callers parse it.
        """

    def limits(self, model: str) -> ModelLimits:
        """Context window and max output for `model`. Unknown models get a conservative estimate."""


class Embedder(Protocol):
    """Text embeddings. Separate from `Provider` — a chat vendor need not serve embeddings."""

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one vector per input text, in input order."""
