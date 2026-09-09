from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

from tantra import Agent, FakeProvider, MemoryStore, RetryConfig, Runtime, Sample, tool
from tantra.context import TurnContext
from tantra.errors import ProviderError
from tantra.events import CompactionApplied, SessionEvent, ToolCallRequested
from tantra.providers.base import ProviderEvent, SampleRequest, StreamEnd, ToolCall


class RecordingTracer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.handle = 0

    def open(self, name: str, **values: Any) -> int:
        self.handle += 1
        self.calls.append((name, {**values, "handle": self.handle}))
        return self.handle

    def start_turn(self, turn: TurnContext, *, resumed: bool, ask_id: str | None, parent: Any) -> Any:
        return self.open("start_turn", turn=turn, resumed=resumed, ask_id=ask_id, parent=parent)

    def end_turn(self, span: Any, **values: Any) -> None:
        self.calls.append(("end_turn", {"span": span, **values}))

    def start_sample(
        self,
        parent: Any,
        req: SampleRequest,
        *,
        sample_id: str | None,
        provider: Any,
        compacted: bool,
    ) -> Any:
        return self.open(
            "start_sample",
            parent=parent,
            req=req,
            sample_id=sample_id,
            provider=provider,
            compacted=compacted,
        )

    def end_sample(self, span: Any, *, end: StreamEnd | None, error: BaseException | None, attempts: int) -> None:
        self.calls.append(("end_sample", {"span": span, "end": end, "error": error, "attempts": attempts}))

    def start_tool(
        self,
        parent: Any,
        call: ToolCallRequested,
        *,
        args: dict[str, Any],
        tool: Any,
        replayed: bool,
    ) -> Any:
        return self.open("start_tool", parent=parent, call=call, args=args, tool=tool, replayed=replayed)

    def end_tool(self, span: Any, **values: Any) -> None:
        self.calls.append(("end_tool", {"span": span, **values}))

    def start_compaction(self, parent: Any) -> Any:
        return self.open("start_compaction", parent=parent)

    def end_compaction(
        self,
        span: Any,
        *,
        applied: CompactionApplied | None,
        error: BaseException | None,
    ) -> None:
        self.calls.append(("end_compaction", {"span": span, "applied": applied, "error": error}))


class FlakyProvider(FakeProvider):
    def __init__(self, samples: list[Sample]) -> None:
        super().__init__(samples)
        self.failures = 1

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if self.failures:
            self.failures -= 1
            raise ProviderError("retry", status_code=500)
        async for item in super().stream(req):
            yield item


class OneCompactor:
    def __init__(self) -> None:
        self.applied = False

    async def compact(self, turn: TurnContext) -> list[SessionEvent]:
        if self.applied:
            return []
        self.applied = True
        return [
            CompactionApplied(
                strategy="test",
                tokens_before=10,
                tokens_after=3,
                summary="summary",
            )
        ]


@tool(description="Echo a value.")
async def echo(value: str) -> str:
    return value


class Bot(Agent):
    tools = [echo]


def calls(tracer: RecordingTracer, name: str) -> list[dict[str, Any]]:
    return [values for method, values in tracer.calls if method == name]


async def test_runtime_trace_parenting_and_configured_provider_retry() -> None:
    tracer = RecordingTracer()
    provider = FlakyProvider(
        [
            Sample(tool_calls=[ToolCall(id="echo", name="echo", args='{"value":"yes"}')]),
            Sample(text="done"),
        ]
    )
    runtime = Runtime(
        provider,
        MemoryStore(),
        [Bot],
        default_model="m",
        retry=RetryConfig(max_attempts=3, base_delay=0),
        compactor=OneCompactor(),
        telemetry=tracer,
    )
    sid = await runtime.create(Bot)
    try:
        async with runtime.connect(sid, writable=True) as connection:
            result = await connection.prompt("go", command_id=uuid4())
        turn = calls(tracer, "start_turn")[0]
        samples = calls(tracer, "start_sample")
        tools = calls(tracer, "start_tool")
        compactions = calls(tracer, "start_compaction")
        assert result.outcome == "completed"
        assert [item["parent"] for item in samples] == [turn["handle"], turn["handle"]]
        assert tools[0]["parent"] == turn["handle"]
        assert all(item["parent"] == turn["handle"] for item in compactions)
        assert calls(tracer, "end_sample")[0]["attempts"] == 2
        assert calls(tracer, "end_sample")[0]["error"] is None
        assert calls(tracer, "end_compaction")[0]["applied"].summary == "summary"
        assert calls(tracer, "end_turn")[0]["outcome"] == "completed"
    finally:
        await runtime.aclose()
