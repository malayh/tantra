"""Manual live smoke for OTel telemetry — run by a human, never collected by pytest.

    OTEL_EXPORTER_OTLP_ENDPOINT=… OTEL_EXPORTER_OTLP_HEADERS=… uv run python stress/live_telemetry.py

Drives one scripted turn — tool call, subagent spawn, final answer — through a real OTLP batch exporter
with `capture_content=True`, prints the trace id and every span it emitted, then flushes. The exporter
and the resource are built with no arguments, so the OTel SDK reads the standard `OTEL_*` environment
itself: `OTEL_EXPORTER_OTLP_ENDPOINT` is required, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME`
(defaulted to `tantra-smoke`) and `OTEL_RESOURCE_ATTRIBUTES` are optional. The model is faked, so
nothing but the collector is contacted. Check the backend for an agent observation with input/output,
a generation per model call, a tool observation with arguments and result, and a nested agent under
the spawning tool.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from contextlib import aclosing
from typing import Any

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

from tantra import Agent, FakeProvider, Harness, MemoryStore, Sample, tool
from tantra.providers.base import ToolCall
from tantra.telemetry import Telemetry

MODEL = "fake/model"

TASK = "Find out how the p99 panel is doing and have the scribe count the words in what you found."

FINDING = "the p99 panel reads four hundred milliseconds at the ninety ninth percentile"


@tool
async def lookup(query: str) -> str:
    """Look a metric up in the dashboard."""
    return FINDING


@tool
async def word_count(text: str) -> int:
    """Count the whitespace-separated words in `text`."""
    return len(text.split())


class Scribe(Agent):
    """Counts the words in a passage and replies with the number alone."""

    prompt = "You count words. Reply with the number alone."
    tools = [word_count]


class Lead(Agent):
    prompt = "You are a terse research desk lead. Use your tools; never guess a number you can count."
    tools = [lookup]
    subagents = [Scribe]


SCRIPT = [
    Sample(tool_calls=[ToolCall(id="c1", name="lookup", args='{"query": "p99 panel"}')]),
    Sample(tool_calls=[ToolCall(id="c2", name="scribe", args=f'{{"task": "count the words in: {FINDING}"}}')]),
    Sample(tool_calls=[ToolCall(id="c3", name="word_count", args=f'{{"text": "{FINDING}"}}')]),
    Sample(text="11"),
    Sample(text=f"{FINDING} — 11 words."),
]


class Watcher(SpanProcessor):
    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def on_end(self, span: ReadableSpan) -> None:
        self.spans.append(span)


class Checked(SpanExporter):
    def __init__(self, inner: SpanExporter) -> None:
        self.inner = inner
        self.rejected = 0

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        result = self.inner.export(spans)
        if result is not SpanExportResult.SUCCESS:
            self.rejected += len(spans)
        return result

    def shutdown(self) -> None:
        self.inner.shutdown()


def assemble() -> tuple[TracerProvider, Watcher, Checked]:
    os.environ.setdefault("OTEL_SERVICE_NAME", "tantra-smoke")
    provider = TracerProvider(resource=Resource.create())
    exporter = Checked(OTLPSpanExporter())
    provider.add_span_processor(BatchSpanProcessor(exporter))
    watcher = Watcher()
    provider.add_span_processor(watcher)
    return provider, watcher, exporter


async def drive(stream: Any) -> None:
    async with aclosing(stream) as live:
        async for _ in live:
            pass


def depth(span: ReadableSpan, by_id: dict[int, ReadableSpan]) -> int:
    seen = 0
    while span.parent is not None and span.parent.span_id in by_id:
        span = by_id[span.parent.span_id]
        seen += 1
    return seen


def report(watcher: Watcher) -> None:
    by_id = {span.context.span_id: span for span in watcher.spans}
    for span in sorted(watcher.spans, key=lambda item: item.start_time or 0):
        print(f"{'  ' * depth(span, by_id)}{span.name}")


async def main() -> int:
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        print("live_telemetry needs OTEL_EXPORTER_OTLP_ENDPOINT exported; refusing to run.")
        return 2

    provider, watcher, exporter = assemble()
    harness = Harness(
        FakeProvider(SCRIPT),
        MemoryStore(),
        [Lead],
        default_model=MODEL,
        telemetry=Telemetry(provider, capture_content=True),
    )
    session = await harness.create_session(Lead)
    print(f"session {session.id} → {endpoint}\n")

    await drive(harness.run(session.id, TASK))

    report(watcher)
    roots = [span for span in watcher.spans if span.parent is None]
    if not roots:
        print("\nno root span was recorded; nothing to export.")
        return 1
    outcome = roots[0].attributes.get("tantra.turn.outcome")
    if outcome != "completed":
        print(f"\nthe turn ended {outcome}, not completed; the scripted samples no longer match the agents.")
        return 1
    print(f"\ntrace id: {format(roots[0].context.trace_id, '032x')}  spans: {len(watcher.spans)}")

    flushed = provider.force_flush()
    provider.shutdown()
    if not flushed or exporter.rejected:
        print(f"export failed: flushed={flushed} rejected={exporter.rejected} spans.")
        return 1
    print("exported.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
