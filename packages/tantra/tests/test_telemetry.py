from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from pydantic import BaseModel

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import FreeText, FreeTextResponse
from tantra.compaction import CompactionConfig, PruneThenSummarize
from tantra.context import TurnContext
from tantra.errors import ProviderError
from tantra.events import AskRaised, ToolCallCompleted, ToolCallRequested, ToolProgress, Usage
from tantra.harness import Harness
from tantra.hooks import Hook
from tantra.loop import RetryConfig
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.telemetry import Telemetry
from tantra.tools import Context, tool

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


class TinyProvider(FakeProvider):
    def __init__(self, samples: list[Sample]) -> None:
        super().__init__(samples)
        self._limits = ModelLimits(context_window=150_000, max_output=30_000)

    def limits(self, model: str) -> ModelLimits:
        return self._limits


class HostedProvider(FakeProvider):
    def __init__(self, samples: list[Sample], base_url: str) -> None:
        super().__init__(samples)
        self.base_url = base_url


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
async def dump(size: int) -> str:
    """Returns a lot of text."""
    return "x" * size


@tool
async def weird() -> dict[Any, Any]:
    """Returns something JSON cannot express."""
    return {(1, 2): "x"}


@tool
async def look(q: str) -> str:
    """Look something up."""
    return f"found {q}"


class Bot(Agent):
    prompt = "You are helpful."
    tools = [search, explode]


class Asker(Agent):
    tools = [confirm]


class Staller(Agent):
    tools = [stall]


class Hoarder(Agent):
    tools = [dump]


class Oddball(Agent):
    tools = [weird]


class Researcher(Agent):
    tools = [look]


class Boss(Agent):
    subagents = [Researcher]


class Analyst(Agent):
    prompt = "You analyse."


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def make(capture: bool = False) -> tuple[Telemetry, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return Telemetry(provider, capture_content=capture), exporter


async def build(
    provider: Any, agent: type[Agent] = Bot, *, capture: bool = False, **kwargs: Any
) -> tuple[Harness, InMemorySpanExporter, MemoryStore, str]:
    telemetry, exporter = make(capture)
    store = kwargs.pop("store", None) or MemoryStore()
    harness = Harness(provider, store, [agent], default_model=MODEL, telemetry=telemetry, **kwargs)
    header = await harness.create_session(agent)
    return harness, exporter, store, header.id


def named(exporter: InMemorySpanExporter, prefix: str) -> list[ReadableSpan]:
    return [span for span in exporter.get_finished_spans() if span.name.startswith(prefix)]


def one(exporter: InMemorySpanExporter, prefix: str) -> ReadableSpan:
    found = named(exporter, prefix)
    assert len(found) == 1, [span.name for span in exporter.get_finished_spans()]
    return found[0]


def parents(child: ReadableSpan, parent: ReadableSpan) -> bool:
    return child.parent is not None and child.parent.span_id == parent.context.span_id


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [emitted.event for emitted in events if isinstance(emitted.event, kind)]


async def test_a_text_turn_exports_a_turn_span_and_a_chat_span() -> None:
    harness, exporter, _, sid = await build(FakeProvider([Sample(text="p99 is fine.", usage=Usage(input_tokens=11))]))

    await collect(harness.run(sid, "how is p99?"))

    assert len(exporter.get_finished_spans()) == 2
    root = one(exporter, "invoke_agent ")
    assert root.name == "invoke_agent bot"
    assert root.parent is None
    assert root.kind is SpanKind.INTERNAL
    assert root.attributes["gen_ai.operation.name"] == "invoke_agent"
    assert root.attributes["gen_ai.agent.name"] == "bot"
    assert root.attributes["gen_ai.conversation.id"] == sid
    assert root.attributes["tantra.turn_id"]
    assert root.attributes["tantra.depth"] == 0
    assert root.attributes["tantra.turn.resumed"] is False
    assert root.attributes["tantra.turn.outcome"] == "completed"
    assert root.attributes["tantra.stop_reason"] == "completed"
    assert root.attributes["gen_ai.usage.input_tokens"] == 11

    chat = one(exporter, "chat ")
    assert chat.name == f"chat {MODEL}"
    assert chat.kind is SpanKind.CLIENT
    assert parents(chat, root)
    assert chat.attributes["gen_ai.operation.name"] == "chat"
    assert chat.attributes["gen_ai.provider.name"] == "fake"
    assert chat.attributes["gen_ai.request.model"] == MODEL
    assert chat.attributes["gen_ai.conversation.id"] == sid
    assert chat.attributes["tantra.turn_id"] == root.attributes["tantra.turn_id"]
    assert chat.attributes["tantra.sample_id"]
    assert chat.attributes["tantra.sample.attempts"] == 1
    assert chat.attributes["gen_ai.response.finish_reasons"] == ("stop",)
    assert chat.attributes["gen_ai.usage.input_tokens"] == 11
    assert "server.address" not in chat.attributes
    assert "gen_ai.input.messages" not in chat.attributes
    assert "gen_ai.system_instructions" not in chat.attributes
    assert "gen_ai.output.messages" not in root.attributes


async def test_capture_content_records_the_prompt_and_the_completion() -> None:
    harness, exporter, _, sid = await build(FakeProvider([Sample(text="p99 is fine.")]), capture=True)

    await collect(harness.run(sid, "how is p99?"))

    chat = one(exporter, "chat ")
    assert json.loads(chat.attributes["gen_ai.input.messages"]) == [
        {"role": "user", "parts": [{"type": "text", "content": "how is p99?"}]}
    ]
    assert json.loads(chat.attributes["gen_ai.system_instructions"]) == [
        {"type": "text", "content": "You are helpful."}
    ]
    output = json.loads(chat.attributes["gen_ai.output.messages"])
    assert output[0]["parts"] == [{"type": "text", "content": "p99 is fine."}]
    assert output[0]["finish_reason"] == "stop"
    tools = json.loads(chat.attributes["gen_ai.tool.definitions"])
    assert {schema["name"] for schema in tools} == {"search", "explode"}

    root = one(exporter, "invoke_agent ")
    assert json.loads(root.attributes["gen_ai.input.messages"])[0]["parts"][0]["content"] == "how is p99?"
    assert json.loads(root.attributes["gen_ai.output.messages"]) == [
        {"role": "assistant", "parts": [{"type": "text", "content": "p99 is fine."}], "finish_reason": "completed"}
    ]


async def test_capture_content_records_a_tool_round_trip_in_the_next_prompt() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", '{"query": "p99"}')]),
                Sample(text="done"),
            ]
        ),
        capture=True,
    )

    await collect(harness.run(sid, "go"))

    first, second = named(exporter, "chat ")
    assert json.loads(first.attributes["gen_ai.output.messages"])[0]["parts"] == [
        {"type": "tool_call", "id": "c1", "name": "search", "arguments": {"query": "p99"}}
    ]
    messages = json.loads(second.attributes["gen_ai.input.messages"])
    assert messages[0] == {"role": "user", "parts": [{"type": "text", "content": "go"}]}
    assert messages[1] == {
        "role": "assistant",
        "parts": [{"type": "tool_call", "id": "c1", "name": "search", "arguments": {"query": "p99"}}],
    }
    assert messages[2] == {
        "role": "tool",
        "parts": [{"type": "tool_call_response", "id": "c1", "response": "found p99"}],
    }


async def test_a_tool_span_parents_on_the_turn_span_not_the_chat_span() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", '{"query": "p99"}')]),
                Sample(text="done"),
            ]
        ),
        capture=True,
    )

    await collect(harness.run(sid, "go"))

    root = one(exporter, "invoke_agent ")
    executed = one(exporter, "execute_tool ")
    assert executed.name == "execute_tool search"
    assert parents(executed, root)
    assert executed.kind is SpanKind.INTERNAL
    assert executed.attributes["gen_ai.operation.name"] == "execute_tool"
    assert executed.attributes["gen_ai.tool.name"] == "search"
    assert executed.attributes["gen_ai.tool.call.id"] == "c1"
    assert executed.attributes["gen_ai.tool.type"] == "function"
    assert executed.attributes["gen_ai.tool.description"] == "Search for something."
    assert executed.attributes["gen_ai.agent.name"] == "bot"
    assert executed.attributes["gen_ai.conversation.id"] == sid
    assert executed.attributes["tantra.tool.outcome"] == "completed"
    assert executed.attributes["tantra.tool.replayed"] is False
    assert json.loads(executed.attributes["gen_ai.tool.call.arguments"]) == {"query": "p99"}
    assert executed.attributes["gen_ai.tool.call.result"] == "found p99"
    assert "tantra.tool.original_arguments" not in executed.attributes


async def test_a_raising_tool_marks_its_span_as_an_error() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("explode", '{"query": "x"}')]),
                Sample(text="recovered"),
            ]
        ),
        capture=True,
    )

    await collect(harness.run(sid, "go"))

    executed = one(exporter, "execute_tool ")
    assert executed.status.status_code is StatusCode.ERROR
    assert executed.attributes["error.type"] == "ValueError"
    assert executed.attributes["tantra.tool.outcome"] == "error"
    assert executed.attributes["gen_ai.tool.call.result"] == "tool exploded"
    assert one(exporter, "invoke_agent ").attributes["tantra.turn.outcome"] == "completed"


async def test_hook_rewritten_arguments_record_the_original_alongside_them() -> None:
    class Rewriter(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> ToolCallRequested:
            return call.model_copy(update={"args": {"query": "rewritten"}})

    harness, exporter, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", '{"query": "p99"}')]),
                Sample(text="done"),
            ]
        ),
        capture=True,
        hooks=[Rewriter()],
    )

    await collect(harness.run(sid, "go"))

    executed = one(exporter, "execute_tool ")
    assert json.loads(executed.attributes["gen_ai.tool.call.arguments"]) == {"query": "rewritten"}
    assert json.loads(executed.attributes["tantra.tool.original_arguments"]) == {"query": "p99"}


async def test_a_retried_sample_stays_one_span_and_counts_both_attempts() -> None:
    harness, exporter, _, sid = await build(
        FlakyProvider([Sample(text="recovered")], failures=1, status_code=500),
        retry=RetryConfig(max_attempts=3, base_delay=0.0),
    )

    await collect(harness.run(sid, "go"))

    chat = one(exporter, "chat ")
    assert chat.attributes["tantra.sample.attempts"] == 2
    assert chat.status.status_code is not StatusCode.ERROR


async def test_a_failed_sample_marks_the_chat_span_and_the_turn_span() -> None:
    harness, exporter, _, sid = await build(
        FlakyProvider([Sample(text="never reached")], failures=1, status_code=400),
        retry=RetryConfig(max_attempts=3, base_delay=0.0),
    )

    await collect(harness.run(sid, "go"))

    chat = one(exporter, "chat ")
    assert chat.status.status_code is StatusCode.ERROR
    assert chat.attributes["error.type"] == "ProviderError"
    assert chat.attributes["tantra.provider.status_code"] == 400
    assert "gen_ai.response.finish_reasons" not in chat.attributes
    root = one(exporter, "invoke_agent ")
    assert root.status.status_code is StatusCode.ERROR
    assert root.attributes["tantra.turn.outcome"] == "failed"
    assert root.attributes["error.type"] == "_OTHER"


async def test_a_suspended_turn_and_its_resume_are_two_traces_sharing_the_turn_id() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("confirm", "{}")]), Sample(text="done")]),
        agent=Asker,
    )

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]
    suspended = one(exporter, "invoke_agent ")

    assert suspended.attributes["tantra.turn.outcome"] == "suspended"
    assert suspended.attributes["tantra.ask_id"] == raised.ask_id
    assert one(exporter, "execute_tool ").attributes["tantra.tool.outcome"] == "suspended"

    await collect(harness.resume(sid, raised.ask_id, FreeTextResponse(text="yes")))

    first, second = named(exporter, "invoke_agent ")
    assert first.context.trace_id != second.context.trace_id
    assert first.attributes["tantra.turn_id"] == second.attributes["tantra.turn_id"]
    assert second.attributes["tantra.turn.resumed"] is True
    assert second.attributes["tantra.ask_id"] == raised.ask_id
    assert second.attributes["tantra.turn.outcome"] == "completed"
    replayed = named(exporter, "execute_tool ")[1]
    assert parents(replayed, second)
    assert replayed.attributes["tantra.tool.replayed"] is True
    assert replayed.attributes["tantra.tool.outcome"] == "completed"


async def test_a_spawned_child_turn_nests_under_the_spawning_tool_span() -> None:
    harness, exporter, _, sid = await build(
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

    child, parent = named(exporter, "invoke_agent ")
    assert child.name == "invoke_agent researcher"
    assert parent.name == "invoke_agent boss"
    delegated = one(exporter, "execute_tool ")
    assert parents(delegated, parent)
    assert parents(child, delegated)
    assert child.context.trace_id == parent.context.trace_id
    assert child.attributes["tantra.depth"] == 1


async def test_fan_out_nests_both_children_under_the_same_tool_span() -> None:
    @tool
    async def survey(ctx: Context) -> list[Any]:
        """Fans out to two researchers."""
        return await ctx.fan_out([(Researcher, "a"), (Researcher, "b")])

    class Chief(Agent):
        tools = [survey]
        subagents = [Researcher]

    harness, exporter, _, sid = await build(
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

    delegated = one(exporter, "execute_tool ")
    children = named(exporter, "invoke_agent researcher")
    assert len(children) == 2
    assert all(parents(child, delegated) for child in children)
    assert {child.context.trace_id for child in children} == {delegated.context.trace_id}


async def test_a_compaction_span_carries_the_summarizer_call_and_marks_the_next_chat() -> None:
    telemetry, exporter = make(capture=True)
    provider = TinyProvider(
        [
            Sample(text="x" * 500_000, usage=Usage(input_tokens=126_000)),
            Sample(text=BRIEF, usage=Usage(input_tokens=7, output_tokens=3)),
            Sample(text="done"),
        ]
    )
    harness = Harness(
        provider,
        MemoryStore(),
        [Analyst],
        default_model=MODEL,
        compactor=PruneThenSummarize(CompactionConfig(tail_turns=1)),
        telemetry=telemetry,
    )
    sid = (await harness.create_session(Analyst)).id

    await collect(harness.run(sid, "one"))
    exporter.clear()
    await collect(harness.run(sid, "two"))

    root = one(exporter, "invoke_agent ")
    compact = one(exporter, "compact")
    assert parents(compact, root)
    assert compact.attributes["gen_ai.operation.name"] == "compact"
    assert compact.attributes["gen_ai.conversation.id"] == sid
    assert compact.attributes["tantra.compaction.applied"] is True
    assert compact.attributes["tantra.compaction.strategy"]
    assert compact.attributes["tantra.compaction.tokens_before"] > compact.attributes["tantra.compaction.tokens_after"]
    assert compact.attributes["tantra.compaction.summary"] == BRIEF

    brief, following = named(exporter, "chat ")
    assert parents(brief, compact)
    assert "tantra.sample_id" not in brief.attributes
    assert "gen_ai.conversation.compacted" not in brief.attributes
    assert parents(following, root)
    assert following.attributes["gen_ai.conversation.compacted"] is True
    assert compact.attributes["gen_ai.usage.input_tokens"] == brief.attributes["gen_ai.usage.input_tokens"] == 7
    assert compact.attributes["gen_ai.usage.output_tokens"] == brief.attributes["gen_ai.usage.output_tokens"] == 3


async def test_a_large_tool_result_is_truncated_with_a_marker() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("dump", '{"size": 51200}')]),
                Sample(text="ok"),
            ]
        ),
        agent=Hoarder,
        capture=True,
    )

    await collect(harness.run(sid, "go"))

    captured = one(exporter, "execute_tool ").attributes["gen_ai.tool.call.result"]
    marker = f"…[truncated {51_200 - 32_768} chars]"
    assert captured.endswith(marker)
    assert len(captured) == 32_768 + len(marker)


async def test_closing_the_stream_mid_tool_still_ends_every_span() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("stall", "{}")]), Sample(text="never reached")]),
        agent=Staller,
    )

    stream = harness.run(sid, "go")
    async for emitted in stream:
        if isinstance(emitted.event, ToolProgress):
            break
    await stream.aclose()

    finished = exporter.get_finished_spans()
    assert len(finished) == 3
    assert all(span.end_time is not None for span in finished)
    root = one(exporter, "invoke_agent ")
    assert root.attributes["tantra.turn.outcome"] == "aborted"
    assert root.status.status_code is not StatusCode.ERROR
    assert one(exporter, "execute_tool ").attributes["tantra.tool.outcome"] == "aborted"


async def test_a_provider_base_url_becomes_the_server_attributes() -> None:
    harness, exporter, _, sid = await build(HostedProvider([Sample(text="hi")], "http://localhost:8080/v1"))

    await collect(harness.run(sid, "go"))

    chat = one(exporter, "chat ")
    assert chat.attributes["server.address"] == "localhost"
    assert chat.attributes["server.port"] == 8080


async def test_a_malformed_base_url_leaves_the_server_attributes_off() -> None:
    harness, exporter, _, sid = await build(HostedProvider([Sample(text="hi")], "http://[::1"))

    await collect(harness.run(sid, "go"))

    chat = one(exporter, "chat ")
    assert "server.address" not in chat.attributes
    assert "server.port" not in chat.attributes
    assert one(exporter, "invoke_agent ").attributes["tantra.turn.outcome"] == "completed"


async def test_an_unserializable_tool_result_never_raises_out_of_the_tool_span() -> None:
    harness, exporter, store, sid = await build(
        FakeProvider([Sample(tool_calls=[call("weird", "{}")]), Sample(text="ok")]),
        agent=Oddball,
        capture=True,
    )

    with pytest.raises(TypeError, match="keys must be str"):
        await collect(harness.run(sid, "go"))

    persisted = [stamped.event async for stamped in store.read(sid)]
    assert [event for event in persisted if isinstance(event, ToolCallCompleted)]
    executed = one(exporter, "execute_tool ")
    assert executed.attributes["gen_ai.tool.call.result"] == str({(1, 2): "x"})
    assert executed.attributes["tantra.tool.outcome"] == "completed"


async def test_a_submitted_output_becomes_the_root_output_message() -> None:
    class Dashboard(BaseModel):
        title: str
        panels: int

    class Structured(Agent):
        tools = [search]
        output_schema = Dashboard

    harness, exporter, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("submit_output", '{"title": "p99", "panels": 2}')])]),
        agent=Structured,
        capture=True,
    )

    await collect(harness.run(sid, "build it"))

    root = one(exporter, "invoke_agent ")
    assert root.attributes["tantra.turn.outcome"] == "output"
    message = json.loads(root.attributes["gen_ai.output.messages"])[0]
    assert message["finish_reason"] == "output"
    assert json.loads(message["parts"][0]["content"]) == {"title": "p99", "panels": 2}


async def test_a_large_argument_payload_is_truncated_in_the_json_attributes() -> None:
    payload = "y" * 50_000
    harness, exporter, _, sid = await build(
        FakeProvider(
            [
                Sample(tool_calls=[call("search", json.dumps({"query": payload}))]),
                Sample(text="ok"),
            ]
        ),
        capture=True,
    )

    await collect(harness.run(sid, "go"))

    marker = f"…[truncated {len(json.dumps({'query': payload})) - 32_768} chars]"
    arguments = one(exporter, "execute_tool ").attributes["gen_ai.tool.call.arguments"]
    assert arguments.endswith(marker)
    assert len(arguments) == 32_768 + len(marker)
    messages = named(exporter, "chat ")[1].attributes["gen_ai.input.messages"]
    assert messages.endswith(" chars]")
    assert len(messages) <= 32_768 + len(marker)


async def test_a_swept_tool_span_records_no_result_under_capture() -> None:
    harness, exporter, _, sid = await build(
        FakeProvider([Sample(tool_calls=[call("stall", "{}")]), Sample(text="never reached")]),
        agent=Staller,
        capture=True,
    )

    stream = harness.run(sid, "go")
    async for emitted in stream:
        if isinstance(emitted.event, ToolProgress):
            break
    await stream.aclose()

    executed = one(exporter, "execute_tool ")
    assert executed.attributes["tantra.tool.outcome"] == "aborted"
    assert "gen_ai.tool.call.result" not in executed.attributes


def test_from_env_returns_nothing_without_an_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    installed: list[Any] = []
    monkeypatch.setattr("tantra.telemetry.trace.set_tracer_provider", installed.append)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)

    assert Telemetry.from_env() is None
    assert installed == []


async def test_from_env_builds_an_env_configured_provider_and_installs_it(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[dict[str, Any]] = []
    installed: list[Any] = []

    class Recorder(SpanExporter):
        def __init__(self, **kwargs: Any) -> None:
            built.append(kwargs)

        def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            return None

    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter", Recorder, raising=False
    )
    monkeypatch.setattr("tantra.telemetry.trace.set_tracer_provider", installed.append)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.invalid")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "from-env-agent")

    telemetry = Telemetry.from_env(capture_content=True)

    assert isinstance(telemetry, Telemetry)
    assert telemetry.capture_content is True
    assert built == [{}]
    assert len(installed) == 1
    assert installed[0].resource.attributes["service.name"] == "from-env-agent"

    exporter = InMemorySpanExporter()
    installed[0].add_span_processor(SimpleSpanProcessor(exporter))
    harness = Harness(FakeProvider([Sample(text="hi")]), MemoryStore(), [Bot], default_model=MODEL, telemetry=telemetry)
    sid = (await harness.create_session(Bot)).id

    await collect(harness.run(sid, "go"))

    assert {span.name for span in exporter.get_finished_spans()} == {"invoke_agent bot", f"chat {MODEL}"}
    telemetry.shutdown()


def test_shutdown_is_a_noop_without_a_provider() -> None:
    Telemetry().shutdown()


def test_shutdown_closes_the_provider_it_was_given() -> None:
    class StubProvider:
        def __init__(self) -> None:
            self.closed = 0

        def shutdown(self) -> None:
            self.closed += 1

    provider = StubProvider()

    Telemetry(provider).shutdown()

    assert provider.closed == 1


async def test_a_provider_set_globally_after_construction_still_receives_the_spans() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    harness = Harness(
        FakeProvider([Sample(text="hi")]),
        MemoryStore(),
        [Bot],
        default_model=MODEL,
        telemetry=Telemetry(),
    )
    sid = (await harness.create_session(Bot)).id
    trace.set_tracer_provider(provider)

    await collect(harness.run(sid, "go"))

    assert {span.name for span in exporter.get_finished_spans()} == {"invoke_agent bot", f"chat {MODEL}"}
