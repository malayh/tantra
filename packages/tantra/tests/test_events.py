from typing import get_args

import pytest
from pydantic import BaseModel, Json, ValidationError, computed_field

from tantra.events import (
    SESSION_EVENT_ADAPTER,
    AgentMessageQueued,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextPart,
    ToolCallCompleted,
)

PERSISTED = {
    "SessionCreated",
    "TurnStarted",
    "SampleStarted",
    "TextPart",
    "ReasoningPart",
    "ToolCallRequested",
    "ToolCallStarted",
    "ToolProgress",
    "ToolCallCompleted",
    "ChildSessionSpawned",
    "AskRaised",
    "AskAnswered",
    "SampleCompleted",
    "CompactionApplied",
    "CancelRequested",
    "AgentMessageQueued",
    "TaskNoticeQueued",
    "KillRequested",
    "TurnCompleted",
    "TurnFailed",
}


class NestedPayload(BaseModel):
    raw: Json[list[str]]
    text: str

    @computed_field
    @property
    def computed(self) -> str:
        return f"computed:{self.text}"


def test_union_covers_every_persisted_event() -> None:
    members = get_args(get_args(SessionEvent)[0])
    assert {member.__name__ for member in members} == PERSISTED


def test_version_defaults_to_one() -> None:
    assert TextPart(sample_id="s1", text="hi").version == 1


def test_unknown_fields_survive_a_round_trip() -> None:
    raw = '{"type":"text_part","version":7,"sample_id":"s1","text":"hi","future_field":{"a":1}}'
    event = SESSION_EVENT_ADAPTER.validate_json(raw)

    assert isinstance(event, TextPart)
    assert event.version == 7
    assert event.model_extra == {"future_field": {"a": 1}}
    assert "future_field" in event.model_dump_json()


def test_event_payloads_replace_nuls_recursively() -> None:
    event = ToolCallCompleted(
        call_id="c1",
        result={"text": "a\0b", "nested": ["\0", ("c\0d",)]},
        future_field={"key\0": "value\0"},
    )

    assert event.result == {"text": "a\ufffdb", "nested": ["\ufffd", ["c\ufffdd"]]}
    assert event.model_extra == {"future_field": {"key\ufffd": "value\ufffd"}}


def test_parsed_event_payloads_replace_nuls_recursively() -> None:
    event = SESSION_EVENT_ADAPTER.validate_json(
        '{"type":"tool_call_completed","call_id":"c1","result":{"text":"a\\u0000b"},"future_field":["\\u0000"]}'
    )

    assert isinstance(event, ToolCallCompleted)
    assert event.result == {"text": "a\ufffdb"}
    assert event.model_extra == {"future_field": ["\ufffd"]}


def test_nested_models_keep_their_durable_serialization_shape() -> None:
    event = ToolCallCompleted(
        call_id="c1",
        result=NestedPayload(raw='["a\\u0000b"]', text="x\0y"),
    )

    assert event.result == {
        "raw": ["a\ufffdb"],
        "text": "x\ufffdy",
        "computed": "computed:x\ufffdy",
    }


def test_stamped_round_trip_restores_the_event_class() -> None:
    stamped = Stamped(seq=3, event=ToolCallCompleted(call_id="c1", result={"rows": [1]}, is_error=True))
    parsed = Stamped.model_validate_json(stamped.model_dump_json())

    assert parsed == stamped
    assert isinstance(parsed.event, ToolCallCompleted)


def test_header_defaults() -> None:
    header = SessionHeader(id="s1", agent="build")

    assert header.status == "idle"
    assert header.last_seq == 0
    assert header.lease is None
    assert header.pending_ask is None
    assert header.metadata == {}
    assert header.usage.input_tokens == 0
    assert header.created_at.tzinfo is not None


def test_agent_message_source_and_sender_must_match() -> None:
    assert (
        AgentMessageQueued(
            message_id="m1",
            sender_session_id=None,
            source="user",
            text="hello",
        ).source
        == "user"
    )
    assert (
        AgentMessageQueued(
            message_id="m2",
            sender_session_id="parent-1",
            source="parent",
            text="hello",
        ).sender_session_id
        == "parent-1"
    )

    with pytest.raises(ValidationError, match="sender_session_id"):
        AgentMessageQueued(
            message_id="m3",
            sender_session_id="root-1",
            source="user",
            text="hello",
        )
    with pytest.raises(ValidationError, match="sender_session_id"):
        AgentMessageQueued(
            message_id="m4",
            sender_session_id=None,
            source="child",
            text="hello",
        )
