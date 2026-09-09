"""Manual OpenTelemetry smoke using Runtime and independent actor turns.

OTEL_EXPORTER_OTLP_ENDPOINT=… uv run python stress/live_telemetry.py
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from uuid import uuid4

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

from stress.driver import PolicyState, SyntheticProvider, by_model, last_user, turn_step
from tantra import Agent, MemoryStore, Runtime, Sample, SampleRequest, tool
from tantra.providers.base import ToolCall
from tantra.telemetry import Telemetry

MODEL_LEAD = "fake/lead"
MODEL_SCRIBE = "fake/scribe"
TASK = "Look up p99 and spawn the scribe to count the words."
FINDING = "the p99 panel reads four hundred milliseconds at the ninety ninth percentile"


@tool
async def lookup(query: str) -> str:
    """Look a metric up in the dashboard."""
    return FINDING


@tool
async def word_count(text: str) -> int:
    """Count whitespace-separated words."""
    return len(text.split())


class Scribe(Agent):
    model = MODEL_SCRIBE
    tools = [word_count]
    permissions = {"word_count": "allow", "finish": "allow"}


class Lead(Agent):
    model = MODEL_LEAD
    tools = [lookup]
    subagents = [Scribe]
    permissions = {"lookup": "allow", "spawn": "allow"}


def lead(req: SampleRequest, state: PolicyState) -> Sample:
    if last_user(req) == TASK:
        if turn_step(req) == 0:
            return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name="lookup", args='{"query":"p99"}')])
        if turn_step(req) == 1:
            return Sample(
                tool_calls=[
                    ToolCall(
                        id=state.next_call_id(),
                        name="spawn",
                        args=f'{{"agent_name":"scribe","input":"count: {FINDING}"}}',
                    )
                ]
            )
        return Sample(text="scribe started")
    return Sample(text=f"{FINDING} — 11 words.")


def scribe(req: SampleRequest, state: PolicyState) -> Sample:
    if turn_step(req) == 0:
        return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name="word_count", args=f'{{"text":"{FINDING}"}}')])
    return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name="finish", args='{"result":11}')])


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


def report(watcher: Watcher) -> None:
    for span in sorted(watcher.spans, key=lambda item: item.start_time or 0):
        print(span.name)


async def idle(runtime: Runtime) -> None:
    while runtime.active:
        await asyncio.sleep(0)


async def main() -> int:
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        print("live_telemetry needs OTEL_EXPORTER_OTLP_ENDPOINT exported; refusing to start.")
        return 2

    provider, watcher, exporter = assemble()
    runtime = Runtime(
        SyntheticProvider(by_model({MODEL_LEAD: lead, MODEL_SCRIBE: scribe})),
        MemoryStore(),
        [Lead],
        telemetry=Telemetry(provider, capture_content=True),
    )
    root_id = await runtime.create(Lead)
    print(f"root {root_id} → {endpoint}")
    print()
    async with runtime.connect(root_id, writable=True) as connection:
        result = await connection.prompt(TASK, command_id=uuid4())
    await idle(runtime)

    report(watcher)
    roots = [span for span in watcher.spans if span.parent is None]
    if result.outcome != "completed" or not roots:
        print()
        print("scripted actor turns did not complete or produced no root span.")
        return 1
    print()
    print(f"trace id: {format(roots[0].context.trace_id, '032x')}  spans: {len(watcher.spans)}")

    await runtime.aclose()
    flushed = provider.force_flush()
    provider.shutdown()
    if not flushed or exporter.rejected:
        print(f"export failed: flushed={flushed} rejected={exporter.rejected} spans.")
        return 1
    print("exported.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
