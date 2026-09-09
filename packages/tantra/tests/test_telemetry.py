from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from tantra import Agent, FakeProvider, MemoryStore, Runtime, Sample, tool
from tantra.context import TurnContext
from tantra.events import CompactionApplied, SessionEvent, Usage
from tantra.providers.base import ToolCall
from tantra.telemetry import Telemetry

MODEL = "fake/model"


@tool(description="Echo a value.")
async def echo(value: str) -> str:
    return value


class Bot(Agent):
    prompt = "You are helpful."
    tools = [echo]


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


def spans(exporter: InMemorySpanExporter, prefix: str) -> list[ReadableSpan]:
    return [span for span in exporter.get_finished_spans() if span.name.startswith(prefix)]


def one(exporter: InMemorySpanExporter, prefix: str) -> ReadableSpan:
    found = spans(exporter, prefix)
    assert len(found) == 1
    return found[0]


def parented(child: ReadableSpan, parent: ReadableSpan) -> bool:
    return child.parent is not None and child.parent.span_id == parent.context.span_id


async def run(
    provider: FakeProvider,
    *,
    text: str = "go",
    capture_content: bool | None = None,
    max_content_chars: int = 32_768,
    compactor: Any = None,
) -> tuple[InMemorySpanExporter, str]:
    exporter = InMemorySpanExporter()
    provider_impl = TracerProvider()
    provider_impl.add_span_processor(SimpleSpanProcessor(exporter))
    telemetry = (
        Telemetry(provider_impl)
        if capture_content is None
        else Telemetry(
            provider_impl,
            capture_content=capture_content,
            max_content_chars=max_content_chars,
        )
    )
    runtime = Runtime(
        provider,
        MemoryStore(),
        [Bot],
        default_model=MODEL,
        compactor=compactor,
        telemetry=telemetry,
    )
    sid = await runtime.create(Bot)
    try:
        async with runtime.connect(sid, writable=True) as connection:
            await connection.prompt(text, command_id=uuid4())
    finally:
        await runtime.aclose()
        telemetry.shutdown()
    return exporter, sid.hex


async def test_semantic_attributes_and_turn_sample_tool_compaction_parenting() -> None:
    exporter, sid = await run(
        FakeProvider(
            [
                Sample(
                    tool_calls=[ToolCall(id="echo", name="echo", args='{"value":"yes"}')],
                    usage=Usage(input_tokens=7),
                ),
                Sample(text="done", usage=Usage(output_tokens=3)),
            ]
        ),
        compactor=OneCompactor(),
    )

    root = one(exporter, "invoke_agent ")
    chats = spans(exporter, "chat ")
    executed = one(exporter, "execute_tool ")
    compactions = spans(exporter, "compact")
    assert root.parent is None
    assert root.kind is SpanKind.INTERNAL
    assert root.attributes["gen_ai.operation.name"] == "invoke_agent"
    assert root.attributes["gen_ai.agent.name"] == "bot"
    assert root.attributes["gen_ai.conversation.id"] == sid
    assert root.attributes["tantra.turn.outcome"] == "completed"
    assert root.attributes["gen_ai.usage.input_tokens"] == 7
    assert root.attributes["gen_ai.usage.output_tokens"] == 3
    assert all(parented(chat, root) for chat in chats)
    assert all(chat.kind is SpanKind.CLIENT for chat in chats)
    assert all(chat.attributes["gen_ai.operation.name"] == "chat" for chat in chats)
    assert all(chat.attributes["gen_ai.provider.name"] == "fake" for chat in chats)
    assert all(chat.attributes["gen_ai.request.model"] == MODEL for chat in chats)
    assert parented(executed, root)
    assert executed.attributes["gen_ai.operation.name"] == "execute_tool"
    assert executed.attributes["gen_ai.tool.name"] == "echo"
    assert executed.attributes["gen_ai.tool.call.id"] == "echo"
    assert executed.attributes["tantra.tool.outcome"] == "completed"
    assert all(parented(compact, root) for compact in compactions)
    assert compactions[0].attributes["gen_ai.operation.name"] == "compact"
    assert compactions[0].attributes["tantra.compaction.applied"] is True
    assert compactions[0].attributes["tantra.compaction.strategy"] == "test"
    assert "gen_ai.input.messages" not in chats[0].attributes
    assert "gen_ai.tool.call.arguments" not in executed.attributes


async def test_content_capture_is_opt_in_and_records_messages() -> None:
    off, _ = await run(FakeProvider([Sample(text="answer")]), text="question", capture_content=False)
    assert "gen_ai.input.messages" not in one(off, "invoke_agent ").attributes
    assert "gen_ai.input.messages" not in one(off, "chat ").attributes

    exporter, _ = await run(FakeProvider([Sample(text="answer")]), text="question", capture_content=True)
    root = one(exporter, "invoke_agent ")
    chat = one(exporter, "chat ")
    assert json.loads(root.attributes["gen_ai.input.messages"])[0]["parts"][0]["content"] == "question"
    assert json.loads(root.attributes["gen_ai.output.messages"])[0]["parts"][0]["content"] == "answer"
    assert json.loads(chat.attributes["gen_ai.system_instructions"]) == [
        {"type": "text", "content": "You are helpful."}
    ]
    assert json.loads(chat.attributes["gen_ai.input.messages"]) == [
        {"role": "user", "parts": [{"type": "text", "content": "question"}]}
    ]
    assert json.loads(chat.attributes["gen_ai.output.messages"])[0]["parts"][0]["content"] == "answer"
    assert {item["name"] for item in json.loads(chat.attributes["gen_ai.tool.definitions"])} == {"echo"}


async def test_content_capture_truncates_tool_arguments_and_results() -> None:
    value = "x" * 100
    exporter, _ = await run(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="echo", name="echo", args=json.dumps({"value": value}))]),
                Sample(text="done"),
            ]
        ),
        capture_content=True,
        max_content_chars=32,
    )

    executed = one(exporter, "execute_tool ")
    arguments = executed.attributes["gen_ai.tool.call.arguments"]
    result = executed.attributes["gen_ai.tool.call.result"]
    assert arguments.endswith("…[truncated 81 chars]")
    assert len(arguments) == 32 + len("…[truncated 81 chars]")
    assert result.endswith("…[truncated 68 chars]")
    assert len(result) == 32 + len("…[truncated 68 chars]")
