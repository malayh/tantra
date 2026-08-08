from __future__ import annotations

from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import ApprovalResponse
from tantra.compaction import (
    MIN_RESULT_CHARS,
    STRATEGY,
    SUMMARIZE_INSTRUCTION,
    CompactionConfig,
    PruneThenSummarize,
    estimate_tokens,
)
from tantra.context import TurnContext, assemble_messages, build_messages, compaction_window
from tantra.errors import ProviderError
from tantra.events import (
    AskRaised,
    CompactionApplied,
    SampleCompleted,
    SampleStarted,
    SessionCreated,
    SessionEvent,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
    Usage,
)
from tantra.harness import Harness
from tantra.hooks import Hook
from tantra.providers.base import (
    AssistantMessage,
    Message,
    ModelLimits,
    SampleRequest,
    ToolCall,
    ToolResultMessage,
    UserMessage,
)
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import tool

MODEL = "fake/model"

BRIEF = """## Goal
Ship the p99 panel.

## Constraints
No new dependencies.

## Progress
Three metrics located.

## Key Decisions
Use histogram_quantile.

## Next Steps
Write the query.

## Critical Context
The dashboard id is 42."""

TAIL_TURN = "t-tail"


class TinyProvider(FakeProvider):
    def __init__(self, samples: list[Sample], *, context_window: int = 150_000, max_output: int = 30_000) -> None:
        super().__init__(samples)
        self._limits = ModelLimits(context_window=context_window, max_output=max_output)

    def limits(self, model: str) -> ModelLimits:
        return self._limits


TINY_LIMITS = TinyProvider([]).limits(MODEL)
USABLE = CompactionConfig().usable(TINY_LIMITS)


def tool_turn(
    turn: str,
    *,
    input: str,
    call: str,
    result: str,
    text: str,
    name: str = "search",
) -> list[SessionEvent]:
    first, second = f"{turn}-s1", f"{turn}-s2"
    return [
        TurnStarted(turn_id=turn, input=input),
        SampleStarted(turn_id=turn, sample_id=first, model=MODEL),
        ToolCallRequested(sample_id=first, call_id=call, name=name, args={"q": turn}),
        SampleCompleted(sample_id=first),
        ToolCallStarted(call_id=call),
        ToolCallCompleted(call_id=call, result=result),
        SampleStarted(turn_id=turn, sample_id=second, model=MODEL),
        TextPart(sample_id=second, text=text),
        SampleCompleted(sample_id=second),
        TurnCompleted(turn_id=turn, stop_reason="completed"),
    ]


def chat_turn(turn: str, *, input: str, text: str) -> list[SessionEvent]:
    sample = f"{turn}-s1"
    return [
        TurnStarted(turn_id=turn, input=input),
        SampleStarted(turn_id=turn, sample_id=sample, model=MODEL),
        TextPart(sample_id=sample, text=text),
        SampleCompleted(sample_id=sample),
        TurnCompleted(turn_id=turn, stop_reason="completed"),
    ]


def report_usage(events: list[SessionEvent]) -> None:
    total = 0
    for event in events:
        if isinstance(event, TurnStarted):
            total += len(event.input)
        elif isinstance(event, TextPart):
            total += len(event.text)
        elif isinstance(event, ToolCallCompleted):
            total += len(str(event.result))
    last = [event for event in events if isinstance(event, SampleCompleted)][-1]
    last.usage = Usage(input_tokens=total // 4)


def session(*, result_chars: int, text_chars: int, turns: int = 5, name: str = "search") -> list[SessionEvent]:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    for index in range(1, turns + 1):
        events += tool_turn(
            f"t{index}",
            input=f"question {index}",
            call=f"c{index}",
            result="r" * result_chars,
            text="w" * text_chars,
            name=name,
        )
    events += tool_turn(TAIL_TURN, input="one more", call="c-tail", result="s" * 400, text="short")
    report_usage(events)
    events.append(TurnStarted(turn_id="now", input="what next?"))
    return events


def context_for(history: list[SessionEvent], provider: FakeProvider | None = None) -> TurnContext:
    return TurnContext(
        session_id="s1",
        turn_id="now",
        agent="analyst",
        depth=0,
        input="what next?",
        history=history,
        model=MODEL,
        limits=TINY_LIMITS,
        provider=provider,
    )


def stubs_in(events: list[SessionEvent]) -> list[ToolCallCompleted]:
    return [event for event in events if isinstance(event, ToolCallCompleted)]


def applied_in(events: list[SessionEvent]) -> list[CompactionApplied]:
    return [event for event in events if isinstance(event, CompactionApplied)]


def dumps(messages: list[Message]) -> list[dict[str, Any]]:
    return [message.model_dump() for message in messages]


def tail_messages(history: list[SessionEvent], turn: str = TAIL_TURN) -> list[Message]:
    _, window = compaction_window(history)
    starts = [index for index, event in enumerate(window) if isinstance(event, TurnStarted) and event.turn_id == turn]
    return assemble_messages("", window[starts[0] :])


def pairs(messages: list[Message]) -> tuple[list[str], list[str]]:
    calls = [call.id for message in messages if isinstance(message, AssistantMessage) for call in message.tool_calls]
    results = [message.call_id for message in messages if isinstance(message, ToolResultMessage)]
    return calls, results


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [emitted.event for emitted in events if isinstance(emitted.event, kind)]


async def test_a_session_with_200k_tokens_of_tool_output_compacts_below_usable() -> None:
    history = session(result_chars=160_000, text_chars=100_000)
    provider = TinyProvider([Sample(text=BRIEF)])
    summary, window = compaction_window(history)
    assert estimate_tokens(window, summary) > USABLE

    events = await PruneThenSummarize().compact(context_for(history, provider))

    assert len(applied_in(events)) == 1
    summary, window = compaction_window(history + events)
    assert summary == BRIEF
    assert estimate_tokens(window, summary) < USABLE


async def test_the_protected_tail_turns_are_byte_identical_before_and_after_compaction() -> None:
    history = session(result_chars=160_000, text_chars=100_000)
    expected = dumps(tail_messages(history))
    before = dumps(build_messages(history))

    events = await PruneThenSummarize().compact(context_for(history, TinyProvider([Sample(text=BRIEF)])))

    after = dumps(build_messages(history + events))
    assert before[-len(expected) :] == expected
    assert after[-len(expected) :] == expected
    assert len(after) < len(before)


async def test_every_tool_call_in_the_compacted_messages_still_has_exactly_one_result() -> None:
    history = session(result_chars=160_000, text_chars=100_000)

    events = await PruneThenSummarize().compact(context_for(history, TinyProvider([Sample(text=BRIEF)])))

    messages = build_messages(history + events)
    calls, results = pairs(messages)
    assert calls == ["c-tail"]
    assert sorted(calls) == sorted(results)
    assert len(set(calls)) == len(calls)
    assert len(set(results)) == len(results)


async def test_a_session_of_pure_conversation_skips_the_prune_and_goes_straight_to_the_brief() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    for index in range(1, 7):
        events += chat_turn(f"t{index}", input=f"question {index}", text="w" * 100_000)
    events += chat_turn(TAIL_TURN, input="one more", text="short")
    report_usage(events)
    events.append(TurnStarted(turn_id="now", input="what next?"))
    provider = TinyProvider([Sample(text=BRIEF)])

    compacted = await PruneThenSummarize().compact(context_for(events, provider))

    assert stubs_in(compacted) == []
    assert len(compacted) == 1
    applied = applied_in(compacted)[0]
    assert applied.strategy == STRATEGY
    assert applied.summary == BRIEF
    assert applied.floor_turn_id == TAIL_TURN
    assert applied.tokens_after < applied.tokens_before
    assert len(provider.requests) == 1


async def test_pruning_alone_stubs_the_newest_results_and_emits_no_compaction_event() -> None:
    history = session(result_chars=160_000, text_chars=4_000)
    provider = TinyProvider([])

    events = await PruneThenSummarize().compact(context_for(history, provider))

    assert applied_in(events) == []
    assert [event.call_id for event in stubs_in(events)] == ["c5", "c4", "c3"]
    assert provider.requests == []

    messages = build_messages(history + events)
    contents = {message.call_id: message.content for message in messages if isinstance(message, ToolResultMessage)}
    assert contents["c3"] == f"[pruned: search output, {160_000} chars omitted]"
    assert contents["c5"] == f"[pruned: search output, {160_000} chars omitted]"
    assert contents["c2"] == "r" * 160_000
    assert contents["c-tail"] == "s" * 400
    assert len(build_messages(history)) == len(messages)


async def test_a_stubbed_result_replaces_content_in_place_without_a_second_message() -> None:
    history = session(result_chars=160_000, text_chars=4_000)
    stub = ToolCallCompleted(call_id="c1", result="[pruned]", is_error=False)

    messages = build_messages(history + [stub])

    positions = [index for index, message in enumerate(messages) if isinstance(message, ToolResultMessage)]
    assert len(positions) == 6
    assert messages[positions[0]].call_id == "c1"
    assert messages[positions[0]].content == "[pruned]"
    assert dumps(build_messages(history))[: positions[0]] == dumps(messages)[: positions[0]]


async def test_skill_output_is_never_stubbed() -> None:
    history = session(result_chars=160_000, text_chars=100_000, name="skill")
    provider = TinyProvider([Sample(text=BRIEF)])

    events = await PruneThenSummarize().compact(context_for(history, provider))

    assert stubs_in(events) == []
    assert len(applied_in(events)) == 1
    assert "r" * 160_000 in provider.requests[0].messages[2].content


async def test_results_inside_the_protected_tail_are_never_stubbed() -> None:
    history = session(result_chars=160_000, text_chars=4_000)
    tail_result = next(event for event in history if isinstance(event, ToolCallCompleted) and event.call_id == "c-tail")
    assert len(tail_result.result) > MIN_RESULT_CHARS

    events = await PruneThenSummarize().compact(context_for(history, TinyProvider([])))

    assert stubs_in(events) != []
    assert "c-tail" not in [event.call_id for event in stubs_in(events)]
    assert dumps(tail_messages(history + events)) == dumps(tail_messages(history))


async def test_a_candidate_pool_below_prune_pool_min_leaves_every_result_intact() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    events += tool_turn("t1", input="question 1", call="c1", result="r" * 120_000, text="w" * 400_000)
    events += chat_turn(TAIL_TURN, input="one more", text="short")
    report_usage(events)
    events.append(TurnStarted(turn_id="now", input="what next?"))
    provider = TinyProvider([Sample(text=BRIEF)])

    compacted = await PruneThenSummarize().compact(context_for(events, provider))

    assert stubs_in(compacted) == []
    assert len(applied_in(compacted)) == 1
    assert "r" * 120_000 in provider.requests[0].messages[2].content


async def test_a_result_shorter_than_the_floor_survives_while_the_bulky_ones_are_stubbed() -> None:
    assert 200 < MIN_RESULT_CHARS
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    for index in range(1, 6):
        events += tool_turn(
            f"t{index}", input=f"question {index}", call=f"c{index}", result="r" * 160_000, text="w" * 4_000
        )
        events += tool_turn(f"s{index}", input=f"aside {index}", call=f"k{index}", result="t" * 200, text="short")
    events += tool_turn(TAIL_TURN, input="one more", call="c-tail", result="s" * 400, text="short")
    report_usage(events)
    events.append(TurnStarted(turn_id="now", input="what next?"))
    provider = TinyProvider([])

    compacted = await PruneThenSummarize().compact(context_for(events, provider))

    assert applied_in(compacted) == []
    assert provider.requests == []
    stubbed = [event.call_id for event in stubs_in(compacted)]
    assert stubbed and all(call_id.startswith("c") for call_id in stubbed)
    messages = build_messages(events + compacted)
    contents = {message.call_id: message.content for message in messages if isinstance(message, ToolResultMessage)}
    assert [contents[f"k{index}"] for index in range(1, 6)] == ["t" * 200] * 5


async def test_a_prune_that_cannot_reclaim_prune_gain_min_is_skipped_entirely() -> None:
    history = session(result_chars=160_000, text_chars=4_000)
    provider = TinyProvider([Sample(text=BRIEF)])
    config = CompactionConfig(prune_gain_min=250_000)

    compacted = await PruneThenSummarize(config).compact(context_for(history, provider))

    assert stubs_in(compacted) == []
    assert len(applied_in(compacted)) == 1
    contents = [message.content for message in provider.requests[0].messages if isinstance(message, ToolResultMessage)]
    assert contents == ["r" * 160_000] * 5


async def test_cache_read_tokens_count_towards_the_reported_context_size() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    events += chat_turn("t1", input="question 1", text="short")
    last = [event for event in events if isinstance(event, SampleCompleted)][-1]
    last.usage = Usage(input_tokens=1_000, cache_read_tokens=140_000, cache_write_tokens=500, output_tokens=200)
    events.append(TurnStarted(turn_id="now", input="what next?"))

    summary, window = compaction_window(events)

    assert estimate_tokens(window, summary) >= 141_700
    assert estimate_tokens(window, summary) > USABLE


async def test_a_log_reporting_no_usage_at_all_falls_back_to_the_assembled_size() -> None:
    history = session(result_chars=160_000, text_chars=4_000)
    for event in history:
        if isinstance(event, SampleCompleted):
            event.usage = Usage()
    summary, window = compaction_window(history)
    assert estimate_tokens(window, summary) > USABLE

    compacted = await PruneThenSummarize().compact(context_for(history, TinyProvider([])))

    assert stubs_in(compacted) != []


async def test_a_prune_only_compaction_lowers_the_estimate_it_reports_on() -> None:
    history = session(result_chars=160_000, text_chars=4_000)
    summary, window = compaction_window(history)
    before = estimate_tokens(window, summary)

    compacted = await PruneThenSummarize().compact(context_for(history, TinyProvider([])))
    history += compacted
    summary, window = compaction_window(history)
    after = estimate_tokens(window, summary)

    assert stubs_in(compacted) != []
    assert after < before

    summary, window = compaction_window(history + [ToolCallCompleted(call_id="c5", result="[pruned again]")])
    assert after - 100 <= estimate_tokens(window, summary) <= after


async def test_an_empty_brief_raises_rather_than_deleting_the_prefix() -> None:
    history = session(result_chars=160_000, text_chars=100_000)

    with pytest.raises(ProviderError, match="empty"):
        await PruneThenSummarize().compact(context_for(history, TinyProvider([Sample(text="")])))


async def test_a_second_compaction_carries_the_first_summary_into_the_new_brief() -> None:
    history = session(result_chars=160_000, text_chars=100_000)
    history += await PruneThenSummarize().compact(context_for(history, TinyProvider([Sample(text=BRIEF)])))
    history += [
        SampleStarted(turn_id="now", sample_id="now-s1", model=MODEL),
        TextPart(sample_id="now-s1", text="w" * 300_000),
        SampleCompleted(sample_id="now-s1"),
        TurnCompleted(turn_id="now", stop_reason="completed"),
    ]
    history += chat_turn("t7", input="question 7", text="w" * 300_000)
    history.append(TurnStarted(turn_id="later", input="and now?"))
    provider = TinyProvider([Sample(text="the second brief")])

    compacted = await PruneThenSummarize().compact(context_for(history, provider))

    assert applied_in(compacted)[0].summary == "the second brief"
    assert applied_in(compacted)[0].floor_turn_id == "t7"
    assert provider.requests[0].messages[0].content == BRIEF


async def test_an_estimate_below_usable_compacts_nothing_and_never_samples() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    events += tool_turn("t1", input="question 1", call="c1", result="r" * 400, text="short")
    events += tool_turn("t2", input="question 2", call="c2", result="r" * 400, text="short")
    events += chat_turn(TAIL_TURN, input="one more", text="short")
    report_usage(events)
    events.append(TurnStarted(turn_id="now", input="what next?"))
    provider = TinyProvider([])

    assert await PruneThenSummarize().compact(context_for(events, provider)) == []
    assert provider.requests == []


async def test_the_brief_request_carries_the_stubbed_prefix_and_no_tools() -> None:
    history = session(result_chars=160_000, text_chars=100_000)
    provider = TinyProvider([Sample(text=BRIEF)])

    await PruneThenSummarize().compact(context_for(history, provider))

    request = provider.requests[0]
    assert request.model == MODEL
    assert request.tools == []
    assert request.messages[-1].content == SUMMARIZE_INSTRUCTION
    assert not any("r" * 160_000 == getattr(message, "content", None) for message in request.messages)
    contents = [message.content for message in request.messages if isinstance(message, ToolResultMessage)]
    assert contents == [f"[pruned: search output, {160_000} chars omitted]"] * 5
    calls, results = pairs(request.messages)
    assert sorted(calls) == sorted(results) == ["c1", "c2", "c3", "c4", "c5"]
    assert "one more" not in [message.content for message in request.messages if isinstance(message, UserMessage)]


async def test_a_configured_model_overrides_the_turn_model_for_the_brief() -> None:
    history = session(result_chars=160_000, text_chars=100_000)
    provider = TinyProvider([Sample(text=BRIEF)])

    await PruneThenSummarize(model="fake/cheap").compact(context_for(history, provider))

    assert provider.requests[0].model == "fake/cheap"


async def test_a_legacy_compaction_event_floors_the_window_at_its_own_position() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    events += chat_turn("t1", input="question 1", text="long answer")
    events.append(CompactionApplied(strategy=STRATEGY, tokens_before=100, tokens_after=10, summary="the brief"))
    events += chat_turn("t2", input="question 2", text="second answer")

    summary, window = compaction_window(events)

    assert summary == "the brief"
    assert [event for event in window if isinstance(event, TurnStarted)] == [
        event for event in events if isinstance(event, TurnStarted) and event.turn_id == "t2"
    ]
    messages = build_messages(events)
    assert isinstance(messages[0], UserMessage)
    assert messages[0].content == "the brief"
    assert "long answer" not in [message.text for message in messages if isinstance(message, AssistantMessage)]


async def test_a_stub_for_a_call_requested_before_the_floor_never_becomes_an_orphan_result() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    events += tool_turn("t1", input="question 1", call="c1", result="r" * 4_000, text="answer")
    events += chat_turn("t2", input="question 2", text="second answer")
    events.append(
        CompactionApplied(
            strategy=STRATEGY, tokens_before=100, tokens_after=10, summary="the brief", floor_turn_id="t2"
        )
    )
    events.append(ToolCallCompleted(call_id="c1", result="[pruned: search output, 4000 chars omitted]"))

    messages = build_messages(events)

    calls, results = pairs(messages)
    assert calls == results == []
    assert not any(isinstance(message, ToolResultMessage) for message in messages)


async def test_the_estimate_falls_back_to_the_assembled_view_right_after_a_compaction() -> None:
    events: list[SessionEvent] = [SessionCreated(agent="analyst")]
    events += chat_turn("t1", input="question 1", text="w" * 4_000)
    report_usage(events)
    events += [TurnStarted(turn_id="t2", input="question 2")]
    summary, window = compaction_window(events)
    reported = estimate_tokens(window, summary)

    events.append(
        CompactionApplied(
            strategy=STRATEGY, tokens_before=reported, tokens_after=5, summary="the brief", floor_turn_id="t2"
        )
    )
    summary, window = compaction_window(events)

    assert reported > 1_000
    assert estimate_tokens(window, summary) < 100


class Analyst(Agent):
    prompt = "You analyse."


@tool
async def fetch(path: str) -> str:
    """Fetch a document."""
    return "d" * 500_000


@tool
async def note(text: str) -> str:
    """Write a note."""
    return f"noted {text}"


class Fetcher(Agent):
    prompt = "You fetch."
    tools = [fetch]


class Noter(Agent):
    prompt = "You note."
    tools = [note]
    permissions = {"note": "ask"}


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


async def test_a_harness_without_a_compactor_builds_the_requests_an_idle_compactor_also_builds() -> None:
    script = [
        Sample(tool_calls=[call("fetch", '{"path": "a.md"}')]),
        Sample(text="done"),
    ]
    plain = Harness(FakeProvider(list(script)), MemoryStore(), [Fetcher], default_model=MODEL)
    plain_sid = (await plain.create_session(Fetcher)).id
    plain_events = await collect(plain.run(plain_sid, "read it"))

    guarded_store = MemoryStore()
    guarded = Harness(
        FakeProvider(list(script)),
        guarded_store,
        [Fetcher],
        default_model=MODEL,
        compactor=PruneThenSummarize(),
    )
    guarded_sid = (await guarded.create_session(Fetcher)).id
    guarded_events = await collect(guarded.run(guarded_sid, "read it"))

    def bodies(requests: list[SampleRequest]) -> list[dict[str, Any]]:
        return [request.model_dump() for request in requests]

    assert bodies(plain.provider.requests) == bodies(guarded.provider.requests)
    assert [type(emitted.event) for emitted in plain_events] == [type(emitted.event) for emitted in guarded_events]
    assert applied_in([emitted.event for emitted in guarded_events]) == []
    assert dumps(plain.provider.requests[-1].messages) == dumps(
        [
            UserMessage(content="read it"),
            AssistantMessage(tool_calls=[ToolCall(id="c1", name="fetch", args='{"path": "a.md"}')]),
            ToolResultMessage(call_id="c1", content="d" * 500_000),
        ]
    )


async def test_a_harness_run_prunes_between_two_samples_of_a_turn_that_then_completes() -> None:
    provider = TinyProvider(
        [
            Sample(tool_calls=[call("fetch", '{"path": "a.md"}')]),
            Sample(text="one done"),
            Sample(text="two done"),
            Sample(tool_calls=[call("fetch", '{"path": "b.md"}', cid="c2")]),
            Sample(text="three done"),
        ],
        context_window=220_000,
    )
    store = MemoryStore()
    harness = Harness(provider, store, [Fetcher], default_model=MODEL, compactor=PruneThenSummarize())
    sid = (await harness.create_session(Fetcher)).id

    await collect(harness.run(sid, "one"))
    await collect(harness.run(sid, "two"))
    events = await collect(harness.run(sid, "three"))

    assert picks(events, TurnCompleted)[0].stop_reason == "completed"
    assert picks(events, CompactionApplied) == []
    assert len(provider.requests) == 5

    logged = [stamped.event async for stamped in store.read(sid)]
    stubbed = [event for event in logged if isinstance(event, ToolCallCompleted) and event.call_id == "c1"]
    assert [event.result for event in stubbed] == [
        "d" * 500_000,
        "[pruned: fetch output, 500000 chars omitted]",
    ]

    opening = [message.content for message in provider.requests[3].messages if isinstance(message, ToolResultMessage)]
    final = [message.content for message in provider.requests[4].messages if isinstance(message, ToolResultMessage)]
    assert opening == ["d" * 500_000]
    assert final == ["[pruned: fetch output, 500000 chars omitted]", "d" * 500_000]


class Terminals(Hook):
    def __init__(self) -> None:
        self.seen: list[SessionEvent] = []

    async def after_turn(self, turn: TurnContext, event: SessionEvent) -> None:
        self.seen.append(event)


async def test_a_failing_summarizer_fails_the_turn_and_the_session_recovers_on_the_next_run() -> None:
    terminals = Terminals()
    provider = TinyProvider(
        [
            Sample(text="x" * 500_000),
            Sample(text=""),
            Sample(text=BRIEF),
            Sample(text="done"),
        ]
    )
    store = MemoryStore()
    harness = Harness(
        provider,
        store,
        [Analyst],
        default_model=MODEL,
        hooks=[terminals],
        compactor=PruneThenSummarize(CompactionConfig(tail_turns=1)),
    )
    sid = (await harness.create_session(Analyst)).id
    await collect(harness.run(sid, "one"))

    failed = await collect(harness.run(sid, "two"))

    assert picks(failed, TurnFailed)[0].error.startswith("compaction summary came back empty")
    assert isinstance(terminals.seen[-1], TurnFailed)
    assert (await store.header(sid)).status == "failed"

    recovered = await collect(harness.run(sid, "three"))

    assert picks(recovered, CompactionApplied)[0].summary == BRIEF
    assert picks(recovered, TurnCompleted)[0].stop_reason == "completed"


async def test_a_compacted_turn_resumes_from_a_fresh_harness_over_the_summary() -> None:
    provider = TinyProvider(
        [
            Sample(text="x" * 500_000, usage=Usage(input_tokens=126_000)),
            Sample(text=BRIEF),
            Sample(tool_calls=[call("note", '{"text": "p99"}', cid="n1")]),
        ]
    )
    store = MemoryStore()
    first = Harness(
        provider,
        store,
        [Noter],
        default_model=MODEL,
        compactor=PruneThenSummarize(CompactionConfig(tail_turns=1)),
    )
    sid = (await first.create_session(Noter)).id
    await collect(first.run(sid, "one"))
    opening = await collect(first.run(sid, "two"))

    applied = picks(opening, CompactionApplied)[0]
    raised = picks(opening, AskRaised)[0]
    assert applied.summary == BRIEF
    assert applied.floor_turn_id == picks(opening, TurnStarted)[0].turn_id
    assert provider.requests[1].messages[-1].content == SUMMARIZE_INSTRUCTION

    del first
    second = Harness(
        TinyProvider([Sample(text="done")]),
        store,
        [Noter],
        default_model=MODEL,
        compactor=PruneThenSummarize(CompactionConfig(tail_turns=1)),
    )

    resumed = await collect(second.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"
    request = second.provider.requests[0]
    assert isinstance(request.messages[0], UserMessage)
    assert request.messages[0].content == BRIEF
    assert not any("x" * 500_000 == getattr(message, "text", None) for message in request.messages)
    calls, results = pairs(request.messages)
    assert sorted(calls) == sorted(results) == ["n1"]

    replayed = [emitted.event async for emitted in second.replay(sid)]
    assert len([event for event in replayed if isinstance(event, CompactionApplied)]) == 1
    assert any(isinstance(event, TextPart) and event.text == "x" * 500_000 for event in replayed)
    assert len([event for event in replayed if isinstance(event, TurnStarted)]) == 2
