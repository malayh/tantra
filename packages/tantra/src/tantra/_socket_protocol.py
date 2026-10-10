from __future__ import annotations

import json
import math
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, JsonValue, TypeAdapter, field_validator

from tantra.ask import AskResponse


def canonical_uuid(value: Any) -> UUID:
    if not isinstance(value, str):
        raise ValueError("UUID must be a string")
    parsed = UUID(value)
    if str(parsed) != value:
        raise ValueError("UUID must be canonical lowercase hyphenated form")
    return parsed


CanonicalUUID = Annotated[UUID, BeforeValidator(canonical_uuid)]
ServerUUID = Annotated[UUID, BeforeValidator(lambda value: value if isinstance(value, UUID) else canonical_uuid(value))]
Cursor = Annotated[int, Field(ge=0)]


class SocketFrame(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


class SubscribeFrame(SocketFrame):
    type: Literal["subscribe"] = "subscribe"
    subscription_id: CanonicalUUID
    actor_id: CanonicalUUID
    after: Cursor = 0
    mode: Literal["read", "write"] = "read"


class UnsubscribeFrame(SocketFrame):
    type: Literal["unsubscribe"] = "unsubscribe"
    subscription_id: CanonicalUUID


class AcquireWriterFrame(SocketFrame):
    type: Literal["acquire_writer"] = "acquire_writer"
    subscription_id: CanonicalUUID


class SendFrame(SocketFrame):
    type: Literal["send"] = "send"
    subscription_id: CanonicalUUID
    command_id: CanonicalUUID
    input: JsonValue


class AnswerFrame(SocketFrame):
    type: Literal["answer"] = "answer"
    subscription_id: CanonicalUUID
    command_id: CanonicalUUID
    ask_id: CanonicalUUID
    response: AskResponse

    @field_validator("response", mode="before")
    @classmethod
    def validate_response(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            raise ValueError("response must be an object")
        expected = {
            "approval": {"kind", "allow"},
            "choice": {"kind", "selected"},
            "free_text": {"kind", "text"},
        }.get(value.get("kind"))
        if expected is None or set(value) != expected:
            raise ValueError("response fields do not match kind")
        field = next(iter(expected - {"kind"}))
        expected_type = bool if field == "allow" else str
        if type(value[field]) is not expected_type:
            raise ValueError(f"response {field} must be {expected_type.__name__}")
        return value


class CancelFrame(SocketFrame):
    type: Literal["cancel"] = "cancel"
    subscription_id: CanonicalUUID
    command_id: CanonicalUUID


ClientFrame = Annotated[
    SubscribeFrame | UnsubscribeFrame | AcquireWriterFrame | SendFrame | AnswerFrame | CancelFrame,
    Field(discriminator="type"),
]


class EventFrame(SocketFrame):
    type: Literal["event"] = "event"
    subscription_id: ServerUUID
    actor_id: ServerUUID
    seq: Cursor
    body: JsonValue


class ReadyFrame(SocketFrame):
    type: Literal["ready"] = "ready"
    subscription_id: ServerUUID
    actor_id: ServerUUID
    cursor: Cursor
    mode: Literal["read", "write"]
    active: bool


class ReceiptFrame(SocketFrame):
    type: Literal["receipt"] = "receipt"
    subscription_id: ServerUUID
    command_id: ServerUUID
    operation: Literal["send", "answer", "cancel"]
    duplicate: bool


class WriterLostFrame(SocketFrame):
    type: Literal["writer_lost"] = "writer_lost"
    subscription_id: ServerUUID


class ErrorFrame(SocketFrame):
    type: Literal["error"] = "error"
    code: str
    message: str
    subscription_id: ServerUUID | None = None
    command_id: ServerUUID | None = None
    retryable: bool = False
    uncertain: bool = False


class NoticeFrame(SocketFrame):
    type: Literal["notice"] = "notice"
    body: JsonValue


class UnsubscribedFrame(SocketFrame):
    type: Literal["unsubscribed"] = "unsubscribed"
    subscription_id: ServerUUID


ServerFrame = Annotated[
    EventFrame | ReadyFrame | ReceiptFrame | WriterLostFrame | ErrorFrame | NoticeFrame | UnsubscribedFrame,
    Field(discriminator="type"),
]


_CLIENT_FRAME_ADAPTER: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON number must be finite")
    return parsed


def parse_frame(text: str) -> ClientFrame:
    value = json.loads(
        text, object_pairs_hook=_reject_duplicate_keys, parse_constant=_reject_constant, parse_float=_finite_float
    )
    return _CLIENT_FRAME_ADAPTER.validate_python(value, strict=True)
