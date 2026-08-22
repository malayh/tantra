from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import Approval, ApprovalResponse, FreeText, FreeTextResponse
from tantra.compaction import CompactionConfig, PruneThenSummarize
from tantra.context import TurnContext
from tantra.errors import ProviderError, TantraError
from tantra.events import AskRaised, CompactionApplied, SessionEvent, ToolCallRequested, ToolProgress, Usage
from tantra.harness import Harness
from tantra.hooks import Denial, Hook
from tantra.loop import RetryConfig
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool
from tantra.tracing import NULL_TRACER, current_span

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


class RecordingTracer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._handles = 0

    def _open(self, method: str, **kwargs: Any) -> int:
        self._handles += 1
        self.calls.append((method, {**kwargs, "handle": self._handles}))
        return self._handles

    def _close(self, method: str, **kwargs: Any) -> None:
        self.calls.append((method, kwargs))

    def start_turn(self, turn: TurnContext, *, resumed: bool, ask_id: str | None, parent: Any) -> Any:
        return self._open("start_turn", turn=turn, resumed=resumed, ask_id=ask_id, parent=parent)

    def end_turn(
        self,
        span: Any,
        *,
        outcome: str,
        stop_reason: str | None,
        output: Any,
        final_text: str | None,
        error: BaseException | str | None,
        ask_id: str | None,
    ) -> None:
        self._close(
            "end_turn",
            span=span,
            outcome=outcome,
            stop_reason=stop_reason,
            output=output,
            final_text=final_text,
            error=error,
            ask_id=ask_id,
        )

    def start_sample(
        self, parent: Any, req: SampleRequest, *, sample_id: str | None, provider: Any, compacted: bool
    ) -> Any:
        return self._open(
            "start_sample", parent=parent, req=req, sample_id=sample_id, provider=provider, compacted=compacted
        )

    def end_sample(self, span: Any, *, end: StreamEnd | None, error: BaseException | None, attempts: int) -> None:
        self._close("end_sample", span=span, end=end, error=error, attempts=attempts)

    def start_tool(
        self, parent: Any, call: ToolCallRequested, *, args: dict[str, Any], tool: Any, replayed: bool
    ) -> Any:
        return self._open("start_tool", parent=parent, call=call, args=args, tool=tool, replayed=replayed)

    def end_tool(
        self,
        span: Any,
        *,
        result: Any,
        is_error: bool,
        outcome: str,
        error_type: str | None,
        ask_id: str | None,
    ) -> None:
        self._close(
            "end_tool",
            span=span,
            result=result,
            is_error=is_error,
            outcome=outcome,
            error_type=error_type,
            ask_id=ask_id,
        )

    def start_compaction(self, parent: Any) -> Any:
        return self._open("start_compaction", parent=parent)

    def end_compaction(self, span: Any, *, applied: CompactionApplied | None, error: BaseException | None) -> None:
        self._close("end_compaction", span=span, applied=applied, error=error)


class TinyProvider(FakeProvider):
    def __init__(self, samples: list[Sample]) -> None:
        super().__init__(samples)
        self._limits = ModelLimits(context_window=150_000, max_output=30_000)

    def limits(self, model: str) -> ModelLimits:
        return self._limits


class FlakyProvider(FakeProvider):
    def __init__(self, samples: list[Sample], *, failures: int, status_code: int) -> None:
        super().__init__(samples)
        self.failures = failures
        self.status_code = status_code

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if self.failures:
            self.failures -= 1
            raise ProviderError("provider is unwell", status_code=self.status_code)
        async for event in super().stream(req):
            yield event


class LosingStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.grants = 0

    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool:
        self.grants += 1
        if self.grants > 1:
            return False
        return await super().acquire_lease(sid, holder, ttl)


@tool
async def search(query: str) -> str:
    """Search for something."""
    return f"found {query}"


@tool
async def explode(query: str) -> str:
    """Always raises."""
    raise ValueError("tool exploded")


@tool
async def confirm(ctx: Context) -> str:
    """Asks the human."""
    reply = await ctx.ask(FreeText(prompt="ok?"))
    return f"heard {reply.text}"


@tool
async def stall(ctx: Context) -> str:
    """Reports progress, then never finishes."""
    await ctx.emit("working")
    await asyncio.sleep(5.0)
    return "done"


@tool
async def look(q: str) -> str:
    """Look something up."""
    return f"found {q}"


class Bot(Agent):
    prompt = "You are helpful."
    tools = [search, explode]


class Asker(Agent):
    tools = [confirm]


class Guarded(Agent):
    tools = [search]
    permissions = {"search": "ask"}


class Staller(Agent):
    tools = [stall]


class Researcher(Agent):
    tools = [look]


class Boss(Agent):
    subagents = [Researcher]


class Analyst(Agent):
    prompt = "You analyse."


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def names(tracer: RecordingTracer, start: int = 0) -> list[str]:
    return [name for name, _ in tracer.calls[start:]]


def only(tracer: RecordingTracer, method: str, start: int = 0) -> list[dict[str, Any]]:
    return [kwargs for name, kwargs in tracer.calls[start:] if name == method]


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [emitted.event for emitted in events if isinstance(emitted.event, kind)]


async def build(
    provider: Any, agent: type[Agent] = Bot, **kwargs: Any
) -> tuple[Harness, RecordingTracer, MemoryStore, str]:
    tracer = RecordingTracer()
    store = kwargs.pop("store", None) or MemoryStore()
    harness = Harness(provider, store, [agent], default_model=MODEL, telemetry=tracer, **kwargs)
    header = await harness.create_session(agent)
    return harness, tracer, store, header.id


async def test_a_text_only_turn_wraps_one_sample_span_in_a_turn_span() -> None:
    harness, tracer, _, sid = await build(FakeProvider([Sample(text="p99 is fine.")]))

    await collect(harness.run(sid, "how is p99?"))

    assert names(tracer) == ["start_turn", "start_sample", "end_sample", "end_turn"]
    opened = only(tracer, "start_turn")[0]
    assert opened["resumed"] is False
    assert opened["ask_id"] is None
    assert opened["parent"] is None
    assert opened["turn"].input == "how is p99?"
    sample = only(tracer, "start_sample")[0]
    assert sample["parent"] == opened["handle"]
    assert sample["compacted"] is False
    assert sample["sample_id"]
    assert sample["provider"] is harness.provider
    assert isinstance(sample["req"], SampleRequest)
    ended = only(tracer, "end_sample")[0]
    assert ended["span"] == sample["handle"]
    assert ended["attempts"] == 1
    assert isinstance(ended["end"], StreamEnd)
    assert ended["error"] is None
    finished = only(tracer, "end_turn")[0]
    assert finished["span"] == opened["handle"]
    assert finished["outcome"] == "completed"
    assert finished["stop_reason"] == "completed"
    assert finished["final_text"] == "p99 is fine."
    assert finished["error"] is None
    assert finished["ask_id"] is None


async def test_a_tool_span_parents_on_the_turn_and_closes_before_the_next_sample() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", '{"query": "p99"}')]),
                Sample(text="done"),
            ]
        )
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "end_tool",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    turn = only(tracer, "start_turn")[0]
    opened = only(tracer, "start_tool")[0]
    assert opened["parent"] == turn["handle"]
    assert opened["args"] == {"query": "p99"}
    assert opened["replayed"] is False
    assert opened["tool"] is harness.tools["bot"]["search"]
    assert opened["call"].call_id == "c1"
    closed = only(tracer, "end_tool")[0]
    assert closed["span"] == opened["handle"]
    assert closed["outcome"] == "completed"
    assert closed["is_error"] is False
    assert closed["error_type"] is None
    assert closed["result"] == "found p99"


async def test_a_raising_tool_ends_its_span_with_the_exception_type() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("explode", '{"query": "x"}')]),
                Sample(text="recovered"),
            ]
        )
    )

    await collect(harness.run(sid, "go"))

    closed = only(tracer, "end_tool")[0]
    assert closed["is_error"] is True
    assert closed["error_type"] == "ValueError"
    assert closed["outcome"] == "error"
    assert closed["result"] == "tool exploded"


async def test_a_hook_denied_tool_still_gets_a_span_pair() -> None:
    class Denier(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial:
            return Denial("not allowed")

    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", '{"query": "p99"}')]),
                Sample(text="recovered"),
            ]
        ),
        hooks=[Denier()],
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "end_tool",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    closed = only(tracer, "end_tool")[0]
    assert closed["span"] == only(tracer, "start_tool")[0]["handle"]
    assert closed["is_error"] is True
    assert closed["error_type"] == "_OTHER"
    assert closed["outcome"] == "error"


async def test_hook_rewritten_arguments_are_the_ones_on_the_tool_span() -> None:
    class Rewriter(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> ToolCallRequested:
            return call.model_copy(update={"args": {"query": "rewritten"}})

    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", '{"query": "p99"}')]),
                Sample(text="done"),
            ]
        ),
        hooks=[Rewriter()],
    )

    await collect(harness.run(sid, "go"))

    opened = only(tracer, "start_tool")[0]
    assert opened["args"] == {"query": "rewritten"}
    assert opened["call"].args == {"query": "p99"}
    assert only(tracer, "end_tool")[0]["result"] == "found rewritten"


async def test_a_retried_sample_stays_one_span_and_reports_both_attempts() -> None:
    harness, tracer, _, sid = await build(
        FlakyProvider([Sample(text="recovered")], failures=1, status_code=500),
        retry=RetryConfig(max_attempts=3, base_delay=0.0),
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == ["start_turn", "start_sample", "end_sample", "end_turn"]
    ended = only(tracer, "end_sample")[0]
    assert ended["attempts"] == 2
    assert ended["error"] is None
    assert isinstance(ended["end"], StreamEnd)
    assert only(tracer, "end_turn")[0]["outcome"] == "completed"


async def test_a_non_retryable_provider_error_fails_both_the_sample_and_the_turn() -> None:
    harness, tracer, _, sid = await build(
        FlakyProvider([Sample(text="never reached")], failures=1, status_code=400),
        retry=RetryConfig(max_attempts=3, base_delay=0.0),
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == ["start_turn", "start_sample", "end_sample", "end_turn"]
    ended = only(tracer, "end_sample")[0]
    assert isinstance(ended["error"], ProviderError)
    assert ended["end"] is None
    assert ended["attempts"] == 1
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "failed"
    assert finished["stop_reason"] is None
    assert "provider is unwell" in finished["error"]


async def test_a_ctx_ask_suspends_the_open_tool_span_and_the_turn_span() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("confirm", "{}")]), Sample(text="done")]),
        agent=Asker,
    )

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]

    assert names(tracer) == ["start_turn", "start_sample", "end_sample", "start_tool", "end_tool", "end_turn"]
    suspended = only(tracer, "end_tool")[0]
    assert suspended["outcome"] == "suspended"
    assert suspended["ask_id"] == raised.ask_id
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "suspended"
    assert finished["ask_id"] == raised.ask_id

    mark = len(tracer.calls)
    await collect(harness.resume(sid, raised.ask_id, FreeTextResponse(text="yes")))

    assert names(tracer, mark) == [
        "start_turn",
        "start_tool",
        "end_tool",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    resumed = only(tracer, "start_turn", mark)[0]
    assert resumed["resumed"] is True
    assert resumed["ask_id"] == raised.ask_id
    assert only(tracer, "start_tool", mark)[0]["replayed"] is True
    assert only(tracer, "end_tool", mark)[0]["outcome"] == "completed"
    assert only(tracer, "end_turn", mark)[0]["outcome"] == "completed"


async def test_a_permission_ask_suspends_the_turn_without_opening_a_tool_span() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("search", '{"query": "p99"}')]), Sample(text="done")]),
        agent=Guarded,
    )

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]

    assert names(tracer) == ["start_turn", "start_sample", "end_sample", "end_turn"]
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "suspended"
    assert finished["ask_id"] == raised.ask_id

    mark = len(tracer.calls)
    await collect(harness.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    assert names(tracer, mark) == [
        "start_turn",
        "start_tool",
        "end_tool",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    assert only(tracer, "start_tool", mark)[0]["replayed"] is False
    assert only(tracer, "end_turn", mark)[0]["outcome"] == "completed"


async def test_a_spawned_child_turn_parents_on_the_spawning_tool_span() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("researcher", '{"task": "dig"}', cid="p1")]),
                Sample(text="child answer"),
                Sample(text="parent answer"),
            ]
        ),
        agent=Boss,
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "start_turn",
        "start_sample",
        "end_sample",
        "end_turn",
        "end_tool",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    parent_turn, child_turn = only(tracer, "start_turn")
    delegated = only(tracer, "start_tool")[0]
    assert parent_turn["parent"] is None
    assert delegated["parent"] == parent_turn["handle"]
    assert child_turn["parent"] == delegated["handle"]
    assert only(tracer, "end_turn")[0]["span"] == child_turn["handle"]
    assert current_span.get() is None


async def test_fan_out_parents_every_child_turn_on_the_same_tool_span() -> None:
    @tool
    async def survey(ctx: Context) -> list[Any]:
        """Fans out to two researchers."""
        return await ctx.fan_out([(Researcher, "a"), (Researcher, "b")])

    class Chief(Agent):
        tools = [survey]
        subagents = [Researcher]

    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("survey", "{}", cid="p1")]),
                Sample(text="first child"),
                Sample(text="second child"),
                Sample(text="parent answer"),
            ]
        ),
        agent=Chief,
    )

    await collect(harness.run(sid, "go"))

    delegated = only(tracer, "start_tool")[0]
    children = only(tracer, "start_turn")[1:]
    assert len(children) == 2
    assert [child["parent"] for child in children] == [delegated["handle"]] * 2
    assert [record["outcome"] for record in only(tracer, "end_turn")[:2]] == ["completed", "completed"]
    assert only(tracer, "end_tool")[0]["is_error"] is False
    assert current_span.get() is None


async def test_a_compaction_span_wraps_the_summarizer_call_and_marks_the_next_sample() -> None:
    tracer = RecordingTracer()
    store = MemoryStore()
    provider = TinyProvider(
        [
            Sample(text="x" * 500_000, usage=Usage(input_tokens=126_000)),
            Sample(text=BRIEF),
            Sample(text="done"),
        ]
    )
    harness = Harness(
        provider,
        store,
        [Analyst],
        default_model=MODEL,
        compactor=PruneThenSummarize(CompactionConfig(tail_turns=1)),
        telemetry=tracer,
    )
    sid = (await harness.create_session(Analyst)).id

    await collect(harness.run(sid, "one"))
    mark = len(tracer.calls)
    events = await collect(harness.run(sid, "two"))

    assert picks(events, CompactionApplied)
    assert names(tracer, mark) == [
        "start_turn",
        "start_compaction",
        "start_sample",
        "end_sample",
        "end_compaction",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    compaction = only(tracer, "start_compaction", mark)[0]
    assert compaction["parent"] == only(tracer, "start_turn", mark)[0]["handle"]
    brief, following = only(tracer, "start_sample", mark)
    assert brief["sample_id"] is None
    assert brief["parent"] == compaction["handle"]
    assert brief["compacted"] is False
    assert following["sample_id"]
    assert following["compacted"] is True
    closed = only(tracer, "end_compaction", mark)[0]
    assert isinstance(closed["applied"], CompactionApplied)
    assert closed["applied"].summary == BRIEF
    assert closed["error"] is None
    assert only(tracer, "end_sample", mark)[0]["end"].text == BRIEF
    assert current_span.get() is None


async def test_a_compactor_that_changes_nothing_still_reports_an_empty_compaction() -> None:
    harness, tracer, _, sid = await build(
        TinyProvider([Sample(text="short answer")]),
        agent=Analyst,
        compactor=PruneThenSummarize(),
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == [
        "start_turn",
        "start_compaction",
        "end_compaction",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    assert only(tracer, "end_compaction")[0]["applied"] is None
    assert only(tracer, "start_sample")[0]["compacted"] is False


async def test_a_compactor_that_raises_ends_its_span_with_the_error_and_fails_the_turn() -> None:
    class FailingCompactor:
        async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
            raise ProviderError("compaction summary came back empty")

    harness, tracer, _, sid = await build(
        FakeProvider([Sample(text="never reached")]),
        compactor=FailingCompactor(),
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == ["start_turn", "start_compaction", "end_compaction", "end_turn"]
    closed = only(tracer, "end_compaction")[0]
    assert closed["span"] == only(tracer, "start_compaction")[0]["handle"]
    assert closed["applied"] is None
    assert isinstance(closed["error"], ProviderError)
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "failed"
    assert "compaction summary came back empty" in finished["error"]
    assert current_span.get() is None


async def test_an_exception_the_caller_is_handling_never_lands_on_a_span() -> None:
    harness, tracer, _, sid = await build(FakeProvider([Sample(text="p99 is fine.")]))

    try:
        raise ValueError("the caller's own problem")
    except ValueError:
        await collect(harness.run(sid, "go"))

    assert names(tracer) == ["start_turn", "start_sample", "end_sample", "end_turn"]
    assert only(tracer, "end_sample")[0]["error"] is None
    assert only(tracer, "end_turn")[0]["error"] is None
    assert only(tracer, "end_turn")[0]["outcome"] == "completed"


async def test_closing_the_stream_mid_tool_aborts_the_open_tool_and_turn_spans() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("stall", "{}")]), Sample(text="never reached")]),
        agent=Staller,
    )

    stream = harness.run(sid, "go")
    async for emitted in stream:
        if isinstance(emitted.event, ToolProgress):
            break
    await stream.aclose()

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "end_tool",
        "end_turn",
    ]
    aborted = only(tracer, "end_tool")[0]
    assert aborted["span"] == only(tracer, "start_tool")[0]["handle"]
    assert aborted["outcome"] == "aborted"
    assert aborted["ask_id"] is None
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "aborted"
    assert isinstance(finished["error"], GeneratorExit)


async def test_a_lost_lease_ends_the_turn_span_as_aborted_with_the_error() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider([Sample(text="never reached")]),
        store=LosingStore(),
    )

    with pytest.raises(TantraError, match="lease lost"):
        await collect(harness.run(sid, "go"))

    assert names(tracer) == ["start_turn", "end_turn"]
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "aborted"
    assert isinstance(finished["error"], TantraError)
    assert "lease lost" in str(finished["error"])


async def test_a_harness_without_telemetry_hands_the_turn_the_null_tracer() -> None:
    class Capture(Hook):
        def __init__(self) -> None:
            self.turns: list[TurnContext] = []

        async def before_sample(self, turn: TurnContext) -> None:
            self.turns.append(turn)

    seen = Capture()
    store = MemoryStore()
    plain = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model=MODEL, hooks=[seen])
    sid = (await plain.create_session(Bot)).id

    await collect(plain.run(sid, "go"))

    assert plain.tracer is NULL_TRACER
    assert seen.turns[0].tracer is NULL_TRACER

    tracer = RecordingTracer()
    traced = Harness(
        FakeProvider([Sample(text="hi")]),
        MemoryStore(),
        [Bot],
        default_model=MODEL,
        hooks=[seen],
        telemetry=tracer,
    )
    traced_sid = (await traced.create_session(Bot)).id

    await collect(traced.run(traced_sid, "go"))

    assert seen.turns[1].tracer is tracer


async def test_a_cancelled_turn_reports_its_stop_reason_as_the_turn_outcome() -> None:
    watcher: dict[str, Harness] = {}

    @tool
    async def touch(ctx: Context) -> str:
        """Cancels its own session from another harness."""
        await watcher["other"].cancel(ctx.session_id)
        return "done"

    class Worker(Agent):
        tools = [touch]

    harness, tracer, store, sid = await build(
        FakeProvider([Sample(tool_calls=[call("touch", "{}")]), Sample(text="never reached")]),
        agent=Worker,
    )
    watcher["other"] = Harness(FakeProvider([]), store, [Worker], default_model=MODEL)

    await collect(harness.run(sid, "go"))

    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "cancelled"
    assert finished["stop_reason"] == "cancelled"
    assert only(tracer, "end_tool")[0]["outcome"] == "completed"


async def test_an_unanswered_call_synthesized_at_the_cap_gets_its_own_span_pair() -> None:
    class Capped(Agent):
        tools = [search]
        max_steps = 1

    harness, tracer, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("search", '{"query": "p99"}')])]),
        agent=Capped,
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "end_tool",
        "end_turn",
    ]
    closed = only(tracer, "end_tool")[0]
    assert closed["span"] == only(tracer, "start_tool")[0]["handle"]
    assert closed["is_error"] is True
    assert closed["error_type"] == "_OTHER"
    assert closed["result"] == "not executed: max steps reached"
    assert only(tracer, "end_turn")[0]["outcome"] == "max_steps"


async def test_invalid_json_arguments_get_a_span_pair_without_executing_the_tool() -> None:
    harness, tracer, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", "{not json")]),
                Sample(text="recovered"),
            ]
        )
    )

    await collect(harness.run(sid, "go"))

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "end_tool",
        "start_sample",
        "end_sample",
        "end_turn",
    ]
    opened = only(tracer, "start_tool")[0]
    assert opened["args"] == {}
    assert opened["parent"] == only(tracer, "start_turn")[0]["handle"]
    closed = only(tracer, "end_tool")[0]
    assert closed["is_error"] is True
    assert closed["error_type"] == "_OTHER"
    assert str(closed["result"]).startswith("invalid JSON arguments")


async def test_a_submitted_output_closes_its_tool_span_and_names_the_turn_outcome() -> None:
    from pydantic import BaseModel

    class Dashboard(BaseModel):
        title: str
        panels: int

    class Structured(Agent):
        tools = [search]
        output_schema = Dashboard

    harness, tracer, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("submit_output", '{"title": "p99", "panels": 2}')])]),
        agent=Structured,
    )

    await collect(harness.run(sid, "build it"))

    assert names(tracer) == [
        "start_turn",
        "start_sample",
        "end_sample",
        "start_tool",
        "end_tool",
        "end_turn",
    ]
    closed = only(tracer, "end_tool")[0]
    assert closed["is_error"] is False
    assert closed["outcome"] == "completed"
    assert closed["result"] == {"title": "p99", "panels": 2}
    finished = only(tracer, "end_turn")[0]
    assert finished["outcome"] == "output"
    assert finished["output"] == {"title": "p99", "panels": 2}


async def test_a_custom_compactor_reaches_the_tracer_through_the_turn_context() -> None:
    seen: list[TurnContext] = []

    class Watcher:
        async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
            seen.append(ctx)
            return []

    harness, tracer, _, sid = await build(FakeProvider([Sample(text="hi")]), compactor=Watcher())

    await collect(harness.run(sid, "go"))

    assert seen[0].tracer is tracer


async def test_an_ask_answered_with_a_denial_still_closes_the_stub_tool_span() -> None:
    @tool
    async def write(path: str) -> str:
        """Write a file."""
        return f"wrote {path}"

    class Editor(Agent):
        tools = [write]
        permissions = {"write": "ask"}

    harness, tracer, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("write", '{"path": "a.json"}')]), Sample(text="ok")]),
        agent=Editor,
    )

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]
    assert isinstance(raised.request, Approval)

    mark = len(tracer.calls)
    await collect(harness.resume(sid, raised.ask_id, ApprovalResponse(allow=False)))

    assert names(tracer, mark) == ["start_turn", "start_tool", "end_tool", "start_sample", "end_sample", "end_turn"]
    closed = only(tracer, "end_tool", mark)[0]
    assert closed["is_error"] is True
    assert closed["error_type"] == "_OTHER"
    assert closed["result"] == "denied by user"
