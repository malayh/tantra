from typing import get_args

from tantra.events import (
    SESSION_EVENT_ADAPTER,
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
    "TurnCompleted",
    "TurnFailed",
}


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
