from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.events import (
    ReasoningPart,
    SampleCompleted,
    SampleStarted,
    SessionCreated,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    ToolProgress,
    TurnCompleted,
    TurnStarted,
    Usage,
)
from tantra.harness import Harness
from tantra.loop import Emitted
from tantra.providers.base import SystemBlock, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool


@tool
async def search_metrics(query: str, ctx: Context) -> list[str]:
    """Search for relevant metrics based on query."""
    return [f"metric:{query}"]


@tool
def boom(query: str) -> str:
    """Always raises."""
    raise RuntimeError("tool exploded")


@tool
async def noisy(query: str, ctx: Context) -> str:
    """Emits progress while working."""
    await ctx.emit(f"searching for {query}")
    await ctx.emit("done searching")
    return "ok"


class Bot(Agent):
    prompt = "You are helpful."
    tools = [search_metrics, boom, noisy]


class Capped(Agent):
    tools = [search_metrics]
    max_steps = 1


class Dashboard(BaseModel):
    title: str
    panels: int


class Structured(Agent):
    tools = [search_metrics]
    output_schema = Dashboard


class CappedStructured(Agent):
    tools = [search_metrics]
    output_schema = Dashboard
    max_steps = 1


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def persisted(events: list[Emitted]) -> list[Emitted]:
    return [event for event in events if event.seq is not None]


def kinds(events: list[Emitted]) -> list[str]:
    return [type(event.event).__name__ for event in persisted(events)]


def picks(events: list[Emitted], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


async def build(samples: list[Sample], agent: type[Agent] = Bot, **kwargs: Any) -> tuple[Harness, MemoryStore, str]:
    store = MemoryStore()
    harness = Harness(FakeProvider(samples), store, [agent], default_model="fake/model", **kwargs)
    header = await harness.create_session(agent)
    return harness, store, header.id


async def test_tool_then_answer_yields_the_spec_ordering() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("search_metrics", '{"query": "p99"}')]),
            Sample(text="p99 is fine."),
        ]
    )

    events = await collect(harness.run(sid, "how is p99?"))

    assert kinds(events) == [
        "TurnStarted",
        "SampleStarted",
        "ToolCallRequested",
        "SampleCompleted",
        "ToolCallStarted",
        "ToolCallCompleted",
        "SampleStarted",
        "TextPart",
        "SampleCompleted",
        "TurnCompleted",
    ]
    spine = (ToolCallRequested, ToolCallStarted, ToolCallCompleted, TextPart, TurnCompleted)
    assert [type(event.event).__name__ for event in events if isinstance(event.event, spine)] == [
        "ToolCallRequested",
        "ToolCallStarted",
        "ToolCallCompleted",
        "TextPart",
        "TurnCompleted",
    ]
    assert picks(events, ToolCallCompleted)[0].result == ["metric:p99"]
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_a_fresh_harness_replays_identical_persisted_events() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("search_metrics", '{"query": "p99"}')]),
            Sample(text="p99 is fine."),
        ]
    )
    live = persisted(await collect(harness.run(sid, "how is p99?")))

    fresh = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")
    replayed = await collect(fresh.replay(sid))

    assert isinstance(replayed[0].event, SessionCreated)
    assert [(event.seq, event.event) for event in replayed[1:]] == [(event.seq, event.event) for event in live]
    assert [event.seq for event in replayed] == list(range(1, len(replayed) + 1))


async def test_deltas_are_emitted_without_a_seq_and_never_persisted() -> None:
    harness, store, sid = await build([Sample(text="hello there")])

    events = await collect(harness.run(sid, "hi"))

    assert [event for event in events if event.seq is None]
    assert all(event.session_id == sid and event.depth == 0 for event in events)
    assert len(picks(persisted(events), TextPart)) == 1


async def test_a_raising_tool_becomes_an_error_result_and_the_loop_continues() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("boom", '{"query": "x"}')]),
            Sample(text="recovered"),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    completed = picks(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result == "tool exploded"
    assert len(picks(events, SampleStarted)) == 2
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_a_tool_with_bad_arguments_fails_the_call_not_the_turn() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("search_metrics", '{"query": {"nested": 1}}')]),
            Sample(text="recovered"),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    completed = picks(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_unknown_tool_names_never_crash_the_loop() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("nope", "{}")]),
            Sample(text="recovered"),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    completed = picks(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result == "unknown tool 'nope'"
    assert not picks(events, ToolCallStarted)


async def test_invalid_json_arguments_are_requested_empty_and_never_executed() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("search_metrics", "{not json")]),
            Sample(text="recovered"),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    assert picks(events, ToolCallRequested)[0].args == {}
    completed = picks(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result.startswith("invalid JSON arguments")
    assert not picks(events, ToolCallStarted)


async def test_max_steps_synthesizes_results_for_every_orphaned_call() -> None:
    harness, store, sid = await build(
        [
            Sample(
                tool_calls=[
                    call("search_metrics", '{"query": "a"}', cid="c1"),
                    call("search_metrics", '{"query": "b"}', cid="c2"),
                ]
            )
        ],
        agent=Capped,
    )

    events = await collect(harness.run(sid, "go"))

    completed = picks(events, ToolCallCompleted)
    assert [c.call_id for c in completed] == ["c1", "c2"]
    assert all(c.is_error and c.result == "not executed: max steps reached" for c in completed)
    assert not picks(events, ToolCallStarted)
    assert picks(events, TurnCompleted)[0].stop_reason == "max_steps"


async def test_submit_output_on_the_final_allowed_sample_beats_the_cap() -> None:
    harness, store, sid = await build(
        [
            Sample(
                tool_calls=[
                    call("search_metrics", '{"query": "x"}', cid="c1"),
                    call("submit_output", '{"title": "p99", "panels": 2}', cid="c2"),
                ]
            )
        ],
        agent=CappedStructured,
    )

    events = await collect(harness.run(sid, "go"))

    orphan, submitted = picks(events, ToolCallCompleted)
    assert orphan.call_id == "c1"
    assert orphan.is_error
    assert orphan.result == "not executed: max steps reached"
    assert submitted.result == {"title": "p99", "panels": 2}
    turn = picks(events, TurnCompleted)[0]
    assert turn.stop_reason == "output"
    assert turn.output == {"title": "p99", "panels": 2}


async def test_reasoning_is_persisted_before_the_text_of_the_same_sample() -> None:
    harness, store, sid = await build([Sample(text="the answer", reasoning="the thinking")])

    events = await collect(harness.run(sid, "go"))

    assert kinds(events) == [
        "TurnStarted",
        "SampleStarted",
        "ReasoningPart",
        "TextPart",
        "SampleCompleted",
        "TurnCompleted",
    ]
    assert picks(events, ReasoningPart)[0].text == "the thinking"


async def test_ctx_emit_persists_tool_progress_between_start_and_completion() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("noisy", '{"query": "p99"}')]),
            Sample(text="done"),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    assert kinds(events)[4:8] == ["ToolCallStarted", "ToolProgress", "ToolProgress", "ToolCallCompleted"]
    assert [p.message for p in picks(events, ToolProgress)] == ["searching for p99", "done searching"]

    fresh = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")
    replayed = await collect(fresh.replay(sid))
    assert [p.message for p in picks(replayed, ToolProgress)] == ["searching for p99", "done searching"]


async def test_submit_output_ends_the_turn_with_the_parsed_output() -> None:
    harness, store, sid = await build(
        [Sample(tool_calls=[call("submit_output", '{"title": "p99", "panels": 2}')])],
        agent=Structured,
    )

    events = await collect(harness.run(sid, "build it"))

    completed = picks(events, ToolCallCompleted)[0]
    assert completed.result == {"title": "p99", "panels": 2}
    assert not completed.is_error
    turn = picks(events, TurnCompleted)[0]
    assert turn.stop_reason == "output"
    assert turn.output == {"title": "p99", "panels": 2}
    assert not picks(events, ToolCallStarted)

    offered = harness.provider.requests[0].tools
    assert [t.name for t in offered] == ["search_metrics", "submit_output"]
    assert offered[-1].parameters["type"] == "object"


async def test_invalid_submit_output_is_an_error_result_and_the_turn_continues() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("submit_output", '{"title": "p99"}', cid="c1")]),
            Sample(tool_calls=[call("submit_output", '{"title": "p99", "panels": 2}', cid="c2")]),
        ],
        agent=Structured,
    )

    events = await collect(harness.run(sid, "build it"))

    first, second = picks(events, ToolCallCompleted)
    assert first.is_error
    assert first.result.startswith("invalid output:")
    assert second.result == {"title": "p99", "panels": 2}
    assert picks(events, TurnCompleted)[0].stop_reason == "output"


async def test_calls_after_submit_output_get_synthesized_results() -> None:
    harness, store, sid = await build(
        [
            Sample(
                tool_calls=[
                    call("submit_output", '{"title": "p99", "panels": 1}', cid="c1"),
                    call("search_metrics", '{"query": "x"}', cid="c2"),
                ]
            )
        ],
        agent=Structured,
    )

    events = await collect(harness.run(sid, "build it"))

    submitted, orphan = picks(events, ToolCallCompleted)
    assert not submitted.is_error
    assert orphan.call_id == "c2"
    assert orphan.is_error
    assert orphan.result == "not executed: turn completed"
    assert picks(events, TurnCompleted)[0].stop_reason == "output"


async def test_the_second_sample_sees_the_system_prompt_and_the_tool_result() -> None:
    harness, store, sid = await build(
        [
            Sample(reasoning="thinking", tool_calls=[call("search_metrics", '{"query": "p99"}')]),
            Sample(text="p99 is fine."),
        ]
    )

    await collect(harness.run(sid, "how is p99?"))

    request = harness.provider.requests[1]
    assert request.model == "fake/model"
    assert request.system == [SystemBlock(text="You are helpful.")]
    assert [message.role for message in request.messages] == ["user", "assistant", "tool"]
    assistant = request.messages[1]
    assert assistant.reasoning[0].text == "thinking"
    assert [(c.id, c.name, c.args) for c in assistant.tool_calls] == [("c1", "search_metrics", '{"query": "p99"}')]
    assert request.messages[2].call_id == "c1"
    assert request.messages[2].content == '["metric:p99"]'
    assert sorted(t.name for t in request.tools) == ["boom", "noisy", "search_metrics"]


async def test_one_tool_call_produces_exactly_one_requested_event() -> None:
    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("search_metrics", '{"query": "p99"}')]),
            Sample(text="done"),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    assert len(picks(persisted(events), ToolCallRequested)) == 1


async def test_usage_accumulates_onto_the_header() -> None:
    harness, store, sid = await build(
        [
            Sample(
                tool_calls=[call("search_metrics", '{"query": "x"}')],
                usage=Usage(input_tokens=10, output_tokens=2),
            ),
            Sample(text="done", usage=Usage(input_tokens=20, output_tokens=5, cache_read_tokens=3)),
        ]
    )

    events = await collect(harness.run(sid, "go"))

    assert [s.usage.input_tokens for s in picks(events, SampleCompleted)] == [10, 20]
    header = await store.header(sid)
    assert header.usage == Usage(input_tokens=30, output_tokens=7, cache_read_tokens=3)
    assert header.status == "idle"


async def test_a_callable_prompt_is_resolved_per_sample() -> None:
    seen: list[str] = []

    def dynamic_prompt(turn: Any) -> str:
        seen.append(turn.turn_id)
        return f"input was {turn.input}"

    class Dynamic(Agent):
        prompt = dynamic_prompt
        tools = [search_metrics]

    harness, store, sid = await build(
        [
            Sample(tool_calls=[call("search_metrics", '{"query": "x"}')]),
            Sample(text="done"),
        ],
        agent=Dynamic,
    )

    await collect(harness.run(sid, "how is p99?"))

    assert harness.provider.requests[0].system == [SystemBlock(text="input was how is p99?")]
    assert len(seen) == 2
    assert len(set(seen)) == 1


async def test_an_async_callable_prompt_is_awaited() -> None:
    async def dynamic_prompt(turn: Any) -> str:
        return f"agent {turn.agent}"

    class AsyncPrompt(Agent):
        prompt = dynamic_prompt

    harness, store, sid = await build([Sample(text="done")], agent=AsyncPrompt)

    await collect(harness.run(sid, "hi"))

    assert harness.provider.requests[0].system == [SystemBlock(text="agent async_prompt")]


async def test_turn_started_is_persisted_and_yielded_first() -> None:
    harness, store, sid = await build([Sample(text="done")])

    events = await collect(harness.run(sid, "hello"))

    first = events[0]
    assert isinstance(first.event, TurnStarted)
    assert first.seq == 2
    assert first.event.input == "hello"
