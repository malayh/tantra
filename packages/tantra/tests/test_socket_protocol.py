from __future__ import annotations

import json
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError

from tantra._socket_protocol import (
    AnswerFrame,
    ErrorFrame,
    EventFrame,
    NoticeFrame,
    ReadyFrame,
    ReceiptFrame,
    ServerFrame,
    SubscribeFrame,
    UnsubscribedFrame,
    WriterLostFrame,
    parse_frame,
)

SUBSCRIPTION_ID = "12345678-1234-1234-1234-123456789abc"
ACTOR_ID = "22345678-1234-1234-1234-123456789abc"
COMMAND_ID = "32345678-1234-1234-1234-123456789abc"
ASK_ID = "42345678-1234-1234-1234-123456789abc"


@pytest.mark.parametrize(
    "text",
    [
        "{",
        '{"type":"unsubscribe","subscription_id":"12345678-1234-1234-1234-123456789abc","type":"unsubscribe"}',
        '{"type":"send","subscription_id":"12345678-1234-1234-1234-123456789abc","command_id":"32345678-1234-1234-1234-123456789abc","input":NaN}',
        '{"type":"send","subscription_id":"12345678-1234-1234-1234-123456789abc","command_id":"32345678-1234-1234-1234-123456789abc","input":Infinity}',
        '{"type":"send","subscription_id":"12345678-1234-1234-1234-123456789abc","command_id":"32345678-1234-1234-1234-123456789abc","input":-Infinity}',
    ],
)
def test_parse_frame_rejects_invalid_json(text: str) -> None:
    with pytest.raises(ValueError):
        parse_frame(text)


def test_parse_frame_rejects_bool_cursor_and_extra_fields() -> None:
    base = {"type": "subscribe", "subscription_id": SUBSCRIPTION_ID, "actor_id": ACTOR_ID}
    with pytest.raises(ValidationError):
        parse_frame(json.dumps({**base, "after": True}))
    with pytest.raises(ValidationError):
        parse_frame(json.dumps({**base, "extra": "forged"}))


@pytest.mark.parametrize(
    "value",
    [
        "12345678123412341234123456789abc",
        "12345678-1234-1234-1234-123456789ABC",
        "{12345678-1234-1234-1234-123456789abc}",
        "12345678-1234-1234-1234-123456789ab",
    ],
)
def test_parse_frame_rejects_noncanonical_uuid(value: str) -> None:
    with pytest.raises((ValueError, ValidationError)):
        parse_frame(json.dumps({"type": "unsubscribe", "subscription_id": value}))


def test_parse_frame_uses_uuid_values_and_subscribe_defaults() -> None:
    frame = parse_frame(json.dumps({"type": "subscribe", "subscription_id": SUBSCRIPTION_ID, "actor_id": ACTOR_ID}))
    assert isinstance(frame, SubscribeFrame)
    assert frame.subscription_id == UUID(SUBSCRIPTION_ID)
    assert frame.after == 0
    assert frame.mode == "read"


@pytest.mark.parametrize(
    "response",
    [
        {"kind": "approval", "allow": 1},
        {"kind": "approval", "allow": "true"},
        {"kind": "approval", "allow": True, "title": "forged"},
        {"kind": "choice", "selected": "yes", "options": ["yes"]},
        {"kind": "free_text", "text": "ok", "prompt": "forged"},
    ],
)
def test_answer_rejects_coercion_and_extra_response_fields(response: object) -> None:
    with pytest.raises(ValidationError):
        parse_frame(
            json.dumps(
                {
                    "type": "answer",
                    "subscription_id": SUBSCRIPTION_ID,
                    "command_id": COMMAND_ID,
                    "ask_id": ASK_ID,
                    "response": response,
                }
            )
        )


def test_answer_parses_existing_response_model_strictly() -> None:
    frame = parse_frame(
        json.dumps(
            {
                "type": "answer",
                "subscription_id": SUBSCRIPTION_ID,
                "command_id": COMMAND_ID,
                "ask_id": ASK_ID,
                "response": {"kind": "approval", "allow": True},
            }
        )
    )
    assert isinstance(frame, AnswerFrame)
    assert frame.response.kind == "approval"
    assert frame.response.allow is True


@pytest.mark.parametrize(
    "payload",
    [
        {
            "type": "subscribe",
            "subscription_id": SUBSCRIPTION_ID,
            "actor_id": ACTOR_ID,
            "after": 3,
            "mode": "write",
        },
        {"type": "unsubscribe", "subscription_id": SUBSCRIPTION_ID},
        {"type": "acquire_writer", "subscription_id": SUBSCRIPTION_ID},
        {
            "type": "send",
            "subscription_id": SUBSCRIPTION_ID,
            "command_id": COMMAND_ID,
            "input": {"parts": ["hello", None, True, 1]},
        },
        {
            "type": "answer",
            "subscription_id": SUBSCRIPTION_ID,
            "command_id": COMMAND_ID,
            "ask_id": ASK_ID,
            "response": {"kind": "choice", "selected": "yes"},
        },
        {
            "type": "cancel",
            "subscription_id": SUBSCRIPTION_ID,
            "command_id": COMMAND_ID,
        },
    ],
)
def test_client_frame_round_trip(payload: dict[str, object]) -> None:
    frame = parse_frame(json.dumps(payload))
    assert parse_frame(frame.model_dump_json()) == frame


@pytest.mark.parametrize(
    "frame",
    [
        EventFrame(subscription_id=SUBSCRIPTION_ID, actor_id=ACTOR_ID, seq=1, body={"part": None}),
        ReadyFrame(subscription_id=SUBSCRIPTION_ID, actor_id=ACTOR_ID, cursor=2, mode="write", active=True),
        ReceiptFrame(
            subscription_id=SUBSCRIPTION_ID,
            command_id=COMMAND_ID,
            operation="send",
            duplicate=False,
        ),
        WriterLostFrame(subscription_id=SUBSCRIPTION_ID),
        ErrorFrame(
            code="busy",
            message="writer busy",
            subscription_id=SUBSCRIPTION_ID,
            command_id=COMMAND_ID,
            retryable=True,
        ),
        NoticeFrame(body=["maintenance", 5]),
        UnsubscribedFrame(subscription_id=SUBSCRIPTION_ID),
    ],
)
def test_server_frame_round_trip(frame: object) -> None:
    adapter = TypeAdapter(ServerFrame)
    encoded = adapter.dump_json(frame)
    decoded = adapter.validate_json(encoded, strict=True)
    assert decoded == frame
    payload = json.loads(encoded)
    for key, expected in (
        ("subscription_id", SUBSCRIPTION_ID),
        ("actor_id", ACTOR_ID),
        ("command_id", COMMAND_ID),
    ):
        if key in payload:
            assert payload[key] == expected


def test_json_float_overflow_and_nonfinite_server_values_are_rejected() -> None:
    with pytest.raises(ValueError, match="finite"):
        parse_frame(
            '{"type":"send","subscription_id":"'
            + SUBSCRIPTION_ID
            + '","command_id":"'
            + COMMAND_ID
            + '","input":1e999}'
        )
    with pytest.raises(ValidationError):
        NoticeFrame(body={"value": float("nan")})
