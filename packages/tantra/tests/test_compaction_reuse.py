from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

import tantra.compaction as compaction_module
from tantra.agent import Agent
from tantra.compaction import CompactionConfig, PruneThenSummarize
from tantra.context import TurnContext, build_messages, build_sample_request
from tantra.events import (
    CompactionApplied,
    SampleCompleted,
    SessionEvent,
    SessionHeader,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
    TurnStarted,
    Usage,
)
from tantra.loop import TurnEngine
from tantra.providers.base import ModelLimits, SampleRequest, SystemBlock, ToolSchema, UserMessage
from tantra.providers.fake import FakeProvider, Sample
from tantra.skills import SkillInfo
from tantra.stores.memory import MemoryStore

MODEL = "fake/model"


def turn_context(
    history: list[SessionEvent],
    request: SampleRequest,
    provider: FakeProvider | None = None,
    *,
    context_window: int = 100_000,
) -> TurnContext:
    return TurnContext(
        session_id="session",
        turn_id="now",
        agent="agent",
        depth=0,
        input="fresh",
        history=history,
        model=MODEL,
        limits=ModelLimits(context_window=context_window, max_output=0),
        provider=provider,
        sample_request=request,
    )


def prepared_request(history: list[SessionEvent]) -> SampleRequest:
    return build_sample_request(
        model=MODEL,
        prompt="system prompt",
        events=history,
        tools=[ToolSchema(name="search")],
        params={"temperature": 0},
        skills=[SkillInfo(name="release", description="Ship safely.")],
    )


async def test_prepared_path_reuses_request_messages_and_estimate(monkeypatch: pytest.MonkeyPatch) -> None:
    history: list[SessionEvent] = [TurnStarted(turn_id="now", input="fresh")]
    request = prepared_request(history)
    context = turn_context(history, request)
    estimated: list[SampleRequest] = []
    estimate = compaction_module.estimate_request_tokens

    def capture(candidate: SampleRequest) -> int:
        estimated.append(candidate)
        return estimate(candidate)

    def reject_assembly(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("unchanged messages were reassembled")

    monkeypatch.setattr(compaction_module, "estimate_request_tokens", capture)
    monkeypatch.setattr(compaction_module, "assemble_messages", reject_assembly)

    assert await PruneThenSummarize()._compact(context, forced=False, prepared=request) == []
    assert estimated == [request]
    assert estimated[0] is request
    assert estimated[0].system[1].text.endswith("release: Ship safely.")
    assert estimated[0].tools == [ToolSchema(name="search")]
    assert estimated[0].params == {"temperature": 0}


async def test_direct_compaction_reassembles_instead_of_trusting_sample_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history: list[SessionEvent] = [TurnStarted(turn_id="now", input="fresh")]
    stale = SampleRequest(
        model=MODEL,
        system=[SystemBlock(text="fixed")],
        messages=[UserMessage(content="stale")],
    )
    context = turn_context(history, stale)
    estimated: list[SampleRequest] = []
    estimate = compaction_module.estimate_request_tokens

    def capture(candidate: SampleRequest) -> int:
        estimated.append(candidate)
        return estimate(candidate)

    monkeypatch.setattr(compaction_module, "estimate_request_tokens", capture)

    assert await PruneThenSummarize().compact(context) == []
    assert estimated[0] is not stale
    assert estimated[0].messages == build_messages(history)
    assert estimated[0].system == stale.system


async def test_changed_summary_tail_rebuilds_without_reprojecting_the_unchanged_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history: list[SessionEvent] = [
        TurnStarted(turn_id="old", input="old"),
        TextPart(sample_id="old-sample", text="x" * 1_000),
        SampleCompleted(sample_id="old-sample", usage=Usage(input_tokens=2_000)),
        TurnCompleted(turn_id="old", stop_reason="completed"),
        TurnStarted(turn_id="now", input="fresh"),
    ]
    request = prepared_request(history)
    provider = FakeProvider([Sample(text="brief")])
    context = turn_context(history, request, provider, context_window=4_000)
    projected: list[tuple[str, list[SessionEvent]]] = []
    project = compaction_module._projected

    def capture(ctx: TurnContext, summary: str, events: list[SessionEvent]) -> int:
        projected.append((summary, list(events)))
        return project(ctx, summary, events)

    monkeypatch.setattr(compaction_module, "_projected", capture)

    events = await PruneThenSummarize(CompactionConfig(buffer=0))._compact(
        context,
        forced=True,
        prepared=request,
    )

    assert len([event for event in events if isinstance(event, CompactionApplied)]) == 1
    assert len(projected) == 3
    assert not any(summary == "" and window == history for summary, window in projected)
    assert projected[-1][0] == "brief"
    assert projected[-1][1] == [history[-1]]


async def test_changed_pruned_window_is_rebuilt(monkeypatch: pytest.MonkeyPatch) -> None:
    history: list[SessionEvent] = [
        TurnStarted(turn_id="old", input="old"),
        ToolCallRequested(sample_id="sample", call_id="call", name="search", args={}),
        ToolCallCompleted(call_id="call", result="r" * 8_000),
        SampleCompleted(sample_id="sample", usage=Usage(input_tokens=2_500)),
        TurnCompleted(turn_id="old", stop_reason="completed"),
        TurnStarted(turn_id="now", input="fresh"),
    ]
    request = prepared_request(history)
    context = turn_context(history, request, FakeProvider([]), context_window=3_000)
    projected: list[list[SessionEvent]] = []
    project = compaction_module._projected

    def capture(ctx: TurnContext, summary: str, events: list[SessionEvent]) -> int:
        projected.append(list(events))
        return project(ctx, summary, events)

    monkeypatch.setattr(compaction_module, "_projected", capture)
    config = CompactionConfig(
        buffer=0,
        prune_pool_min=1,
        prune_gain_min=1,
        tail_turns=1,
        trigger_at=1,
        recent_tokens=1_000,
    )

    events = await PruneThenSummarize(config)._compact(context, forced=True, prepared=request)

    assert len(events) == 1
    assert isinstance(events[0], ToolCallCompleted)
    assert any(
        isinstance(event, ToolCallCompleted) and str(event.result).startswith("[pruned:")
        for window in projected
        for event in window
    )


async def make_engine(compactor: Any) -> TurnEngine:
    store = MemoryStore()
    await store.setup()
    header = SessionHeader(id=uuid4().hex, agent="agent")
    await store.create(header)

    class Bot(Agent):
        pass

    return TurnEngine(
        store=store,
        provider=FakeProvider([]),
        header=header,
        agent=Bot,
        tools={},
        model=MODEL,
        history=[],
        compactor=compactor,
    )


async def test_engine_uses_private_prepared_path_only_for_the_exact_builtin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history: list[SessionEvent] = [TurnStarted(turn_id="now", input="fresh")]
    request = prepared_request(history)
    received: list[SampleRequest | None] = []
    compact = PruneThenSummarize._compact

    async def capture(
        self: PruneThenSummarize,
        ctx: TurnContext,
        *,
        forced: bool,
        prepared: SampleRequest | None = None,
    ) -> list[SessionEvent]:
        received.append(prepared)
        return await compact(self, ctx, forced=forced, prepared=prepared)

    monkeypatch.setattr(PruneThenSummarize, "_compact", capture)
    engine = await make_engine(PruneThenSummarize())
    engine.history = history
    engine.turn = turn_context(history, request)

    assert await engine._compact(prepared=request) is False
    assert received == [request]

    calls: list[SampleRequest | None] = []

    class Derived(PruneThenSummarize):
        async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
            calls.append(ctx.sample_request)
            return []

    derived = await make_engine(Derived())
    derived.history = history
    derived.turn = turn_context(history, request)

    assert await derived._compact(prepared=request) is False
    assert calls == [request]
    assert received == [request]
