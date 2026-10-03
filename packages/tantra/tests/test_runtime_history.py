from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from tantra import Runtime
from tantra.agent import Agent
from tantra.context import TurnContext, build_sample_request, compacted_history
from tantra.events import (
    CompactionApplied,
    InputQueued,
    LoggedEvent,
    SessionEvent,
    SessionHeader,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.loop import RetryConfig, TurnEngine
from tantra.providers.base import AssistantMessage, ToolResultMessage, ToolSchema
from tantra.providers.fake import FakeProvider, Sample
from tantra.skills import SkillInfo
from tantra.stores.memory import MemoryStore


class Bot(Agent):
    pass


def marker(summary: str, floor: str | None = None) -> CompactionApplied:
    return CompactionApplied(
        strategy="test",
        tokens_before=100,
        tokens_after=10,
        summary=summary,
        floor_turn_id=floor,
    )


async def engine_for(
    history: list[SessionEvent],
    *,
    history_mode: str = "full",
    hooks: list[Hook] | None = None,
    compactor: Any = None,
) -> tuple[TurnEngine, MemoryStore, InputQueued, FakeProvider]:
    store = MemoryStore()
    await store.setup()
    header = SessionHeader(id=uuid4().hex, agent="bot")
    await store.create(header)
    await store.append(header.id, history)
    queued = InputQueued(command_id=uuid4().hex, input="current input")
    await store.enqueue(header.id, queued)
    loaded = await store.header(header.id)
    assert loaded is not None
    items = await store.read_page(header.id)
    provider = FakeProvider([Sample(text="done")])
    engine = TurnEngine(
        store=store,
        provider=provider,
        header=loaded,
        agent=Bot,
        tools={},
        model="fake/model",
        history=[item.event for item in items],
        history_mode=history_mode,
        hooks=hooks or [],
        compactor=compactor,
        retry=RetryConfig(max_attempts=1),
    )
    return engine, store, queued, provider


def test_compacted_history_keeps_the_latest_marker_and_its_window() -> None:
    first = marker("first", "old")
    second = marker("second", "kept")
    events: list[SessionEvent] = [
        TurnStarted(turn_id="old", input="old input"),
        TextPart(sample_id="old-sample", text="old answer"),
        first,
        TurnStarted(turn_id="kept", input="kept input"),
        TextPart(sample_id="kept-sample", text="kept answer"),
        second,
        TurnStarted(turn_id="latest", input="latest input"),
    ]

    assert compacted_history(events) == events[3:]

    missing = marker("missing", "absent")
    with_missing_floor = [*events, missing, TurnStarted(turn_id="after", input="after")]
    assert compacted_history(with_missing_floor) == with_missing_floor[-2:]


def test_compacted_request_matches_full_request_with_tools_and_skills() -> None:
    events: list[SessionEvent] = [
        TurnStarted(turn_id="old", input="old input"),
        TextPart(sample_id="old-sample", text="old answer"),
        marker("first", "old"),
        TurnStarted(turn_id="kept", input="kept input"),
        ToolCallRequested(
            sample_id="kept-sample",
            call_id="skill-call",
            name="skill",
            args={"name": "release"},
        ),
        ToolCallCompleted(call_id="skill-call", result="Read the release checklist."),
        marker("second", "kept"),
        TurnStarted(turn_id="latest", input="latest input"),
    ]
    kwargs = {
        "model": "fake/model",
        "prompt": "system prompt",
        "tools": [ToolSchema(name="skill")],
        "skills": [SkillInfo(name="release", description="Ship safely.")],
    }

    full = build_sample_request(events=events, **kwargs)
    compacted = build_sample_request(events=compacted_history(events), **kwargs)

    assert compacted == full
    assistant = next(message for message in compacted.messages if isinstance(message, AssistantMessage))
    result = next(message for message in compacted.messages if isinstance(message, ToolResultMessage))
    assert [call.id for call in assistant.tool_calls] == ["skill-call"]
    assert result.content == "Read the release checklist."
    assert "release: Ship safely." in compacted.system[1].text


async def test_full_history_remains_the_default_for_hooks_and_compactors() -> None:
    snapshots: list[list[SessionEvent]] = []

    class CaptureHook(Hook):
        async def before_turn(self, turn: TurnContext) -> None:
            snapshots.append(list(turn.history or []))

    class CaptureCompactor:
        async def compact(self, turn: TurnContext) -> list[SessionEvent]:
            snapshots.append(list(turn.history or []))
            return []

    old = TurnStarted(turn_id="old", input="old input")
    history: list[SessionEvent] = [
        old,
        TextPart(sample_id="old-sample", text="old answer"),
        marker("brief"),
        TurnStarted(turn_id="kept", input="kept input"),
    ]
    engine, _, queued, _ = await engine_for(
        history,
        hooks=[CaptureHook()],
        compactor=CaptureCompactor(),
    )

    await engine.run(queued)

    assert len(snapshots) == 2
    assert all(old in snapshot for snapshot in snapshots)
    assert engine.history is not None and old in engine.history


async def test_compacted_history_trims_in_place_after_commit_and_callbacks() -> None:
    old = TurnStarted(turn_id="old", input="old input")
    observed: list[list[SessionEvent]] = []
    durable: list[bool] = []
    engine: TurnEngine
    store: MemoryStore

    class CaptureHook(Hook):
        async def on_event(self, emitted: LoggedEvent) -> None:
            if isinstance(emitted.event, CompactionApplied):
                assert engine.turn is not None
                observed.append(list(engine.turn.history or []))
                stored = await store.read_page(emitted.agent_id.hex)
                durable.append(any(item.event == emitted.event for item in stored))

    class ApplyCompactor:
        async def compact(self, turn: TurnContext) -> list[SessionEvent]:
            return [marker("brief", turn.turn_id)]

    engine, store, queued, _ = await engine_for(
        [old, TextPart(sample_id="old-sample", text="old answer")],
        history_mode="compacted",
        hooks=[CaptureHook()],
        compactor=ApplyCompactor(),
    )

    await engine.run(queued)

    assert engine.turn is not None
    assert engine.turn.history is engine.history
    assert durable == [True]
    assert old in observed[0]
    assert old not in engine.history
    assert any(isinstance(event, CompactionApplied) for event in engine.history)
    assert any(isinstance(event, TurnStarted) and event.turn_id == queued.command_id for event in engine.history)


@pytest.mark.parametrize("history_mode", ["", "summary", None])
def test_runtime_rejects_unknown_history_mode(history_mode):
    with pytest.raises(ValueError, match="history_mode"):
        Runtime(FakeProvider([]), MemoryStore(), [Bot], history_mode=history_mode)


async def test_runtime_custom_store_fallback_preserves_requests_and_public_replay():
    requests = []
    old_turn, retained_turn = uuid4().hex, uuid4().hex
    old = TurnStarted(turn_id=old_turn, input="old input")
    kept = TurnStarted(turn_id=retained_turn, input="kept input")
    history = [
        old,
        TextPart(sample_id="old", text="old answer"),
        TurnCompleted(turn_id=old_turn, stop_reason="done"),
        kept,
        TextPart(sample_id="kept", text="retained answer"),
        TurnCompleted(turn_id=retained_turn, stop_reason="done"),
        marker("summary", retained_turn),
    ]
    for mode in ("full", "compacted"):
        observed = []

        def prompt(turn, observed=observed):
            observed.append(list(turn.history))
            return "system"

        class CaptureBot(Agent):
            pass

        CaptureBot.prompt = prompt
        provider = FakeProvider([Sample(text="done")])
        store = MemoryStore()
        runtime = Runtime(provider, store, [CaptureBot], default_model="m", history_mode=mode)
        sid = await runtime.create(CaptureBot)
        await store.append(sid.hex, history)
        snapshot = await runtime._history(sid.hex)
        assert snapshot.last_seq == len(history) + 1
        assert (old in [item.event for item in snapshot.items]) is (mode == "full")
        assert kept in [item.event for item in snapshot.items]
        assert await runtime._result(sid.hex, uuid4()) is None
        async with runtime.connect(sid, writable=True) as connection:
            result = await connection.prompt("current", command_id=uuid4())
            assert result.text == "done"
        assert (old in observed[0]) is (mode == "full")
        requests.append(provider.requests[0])
        await runtime.aclose()
        replay = [item async for item in runtime.events(sid)]
        assert [item.event for item in replay][1 : len(history) + 1] == history
        cursor = snapshot.last_seq
        suffix = [item async for item in runtime.events(sid, after=cursor)]
        assert [item.seq for item in suffix] == list(range(cursor + 1, replay[-1].seq + 1))
    assert requests[0] == requests[1]


async def test_compacted_engine_absorbs_after_loaded_watermark_without_duplicates():
    old = TurnStarted(turn_id="old", input="old")
    engine, store, _, _ = await engine_for([old, marker("first")], history_mode="compacted")
    loaded_seq = engine.header.last_seq
    later = [TurnStarted(turn_id="retained", input="new"), marker("second", "retained")]
    await store.append(engine.header.id, later)
    await engine._absorb()
    assert engine.header.last_seq == loaded_seq + len(later)
    assert engine.history == later
    await engine._absorb()
    assert engine.history == later
