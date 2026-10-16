from typing import get_args

from tantra.events import (
    SESSION_EVENT_ADAPTER,
    ChildCreated,
    InputQueued,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextPart,
    ToolCallCompleted,
)

PERSISTED = {
    "SessionCreated",
    "InputQueued",
    "TurnStarted",
    "SampleStarted",
    "TextDelta",
    "ReasoningDelta",
    "ToolCallDelta",
    "TextPart",
    "ReasoningPart",
    "ToolCallRequested",
    "ToolCallStarted",
    "ToolProgress",
    "ToolCallCompleted",
    "ChildCreated",
    "AgentFinished",
    "AskRaised",
    "AskAnswered",
    "SampleCompleted",
    "CompactionApplied",
    "CancellationRequested",
    "TurnCompleted",
    "TurnFailed",
    "TurnCancelled",
    "TurnInterrupted",
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


def test_legacy_name_fields_default_to_none() -> None:
    header = SessionHeader.model_validate({"id": "s1", "agent": "build"})
    created = SESSION_EVENT_ADAPTER.validate_python({"type": "session_created", "agent": "build"})
    child = SESSION_EVENT_ADAPTER.validate_python(
        {
            "type": "child_created",
            "child_id": "s2",
            "agent": "worker",
            "turn_id": "turn",
            "call_id": "call",
        }
    )

    assert header.name is None
    assert isinstance(created, SessionCreated)
    assert created.name is None
    assert isinstance(child, ChildCreated)
    assert child.name is None


def test_header_defaults() -> None:
    header = SessionHeader(id="s1", agent="build")

    assert header.status == "idle"
    assert header.last_seq == 0
    assert header.pending_ask is None
    assert header.finished is False
    assert header.metadata == {}
    assert header.usage.input_tokens == 0
    assert header.created_at.tzinfo is not None


def test_a_legacy_input_equals_its_structured_retry_in_both_directions() -> None:
    legacy = InputQueued(command_id="c1", input="hi", submitted_by="u")
    structured = InputQueued(command_id="c1", input="hi", submitted_by="u", sender_id="a1", origin="message")
    user = structured.model_copy(update={"sender_id": None, "origin": "user"})

    assert legacy == structured and structured == legacy
    assert not legacy != structured and not structured != legacy
    assert ("a1", legacy) == ("a1", structured)
    assert Stamped(seq=1, event=legacy) == Stamped(seq=1, event=structured)
    assert structured != user and user != structured
    assert legacy != InputQueued(command_id="c1", input="changed", origin="user")
    assert legacy != InputQueued(command_id="c1", input="hi", submitted_by="v", origin="user")
    assert InputQueued(command_id="c1", input="hi", submitted_by="u", note=1) != structured
    assert structured != InputQueued(command_id="c1", input="hi", submitted_by="u", sender_id="a2", origin="message")


def test_structured_inputs_round_trip_and_legacy_json_reads_as_none() -> None:
    structured = InputQueued(command_id="c1", input="hi", sender_id="a1", origin="spawn")
    parsed = SESSION_EVENT_ADAPTER.validate_json(structured.model_dump_json())
    legacy = SESSION_EVENT_ADAPTER.validate_json('{"type":"input_queued","command_id":"c1","input":"hi"}')

    assert isinstance(parsed, InputQueued) and isinstance(legacy, InputQueued)
    assert (parsed.sender_id, parsed.origin) == ("a1", "spawn")
    assert (legacy.sender_id, legacy.origin, legacy.model_extra) == (None, None, {})
    assert parsed.model_dump() == structured.model_dump()
