from __future__ import annotations

import json
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from tantra.context import _as_content
from tantra.errors import ProviderError
from tantra.events import Usage

try:
    from opentelemetry import trace
    from opentelemetry.trace import SpanKind, Status, StatusCode, set_span_in_context
except ImportError as exc:
    raise ImportError("opentelemetry not installed: install tantra-harness[telemetry]") from exc

if TYPE_CHECKING:
    from tantra.context import TurnContext
    from tantra.events import CompactionApplied, ToolCallRequested
    from tantra.providers.base import Message, Provider, SampleRequest, StreamEnd, ToolCall, ToolSchema
    from tantra.tools import Tool

_VERSION = version("tantra-harness")
_DEFAULT_PORTS = {"http": 80, "https": 443}


@dataclass
class _Handle:
    span: Any
    ctx: Any
    conversation_id: str | None = None
    turn_id: str | None = None
    agent: str | None = None
    usage: Usage = field(default_factory=Usage)
    parent: _Handle | None = None


def _clean(attributes: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in attributes.items() if value is not None}


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return str(value)


def _text(value: Any) -> str:
    try:
        return _as_content(value)
    except (TypeError, ValueError):
        return str(value)


def _usage_attrs(usage: Usage) -> dict[str, Any]:
    return {
        "gen_ai.usage.input_tokens": usage.input_tokens,
        "gen_ai.usage.output_tokens": usage.output_tokens,
        "gen_ai.usage.cache_read.input_tokens": usage.cache_read_tokens,
        "gen_ai.usage.cache_creation.input_tokens": usage.cache_write_tokens,
    }


def _accumulate(total: Usage, delta: Usage) -> None:
    total.input_tokens += delta.input_tokens
    total.output_tokens += delta.output_tokens
    total.cache_read_tokens += delta.cache_read_tokens
    total.cache_write_tokens += delta.cache_write_tokens


def _arguments(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def _tool_call_part(call: ToolCall) -> dict[str, Any]:
    return {"type": "tool_call", "id": call.id, "name": call.name, "arguments": _arguments(call.args)}


def _input_messages(messages: list[Message]) -> list[dict[str, Any]]:
    built: list[dict[str, Any]] = []
    for message in messages:
        if message.role == "user":
            built.append({"role": "user", "parts": [{"type": "text", "content": message.content}]})
        elif message.role == "assistant":
            parts: list[dict[str, Any]] = []
            if message.text:
                parts.append({"type": "text", "content": message.text})
            parts.extend(_tool_call_part(call) for call in message.tool_calls)
            built.append({"role": "assistant", "parts": parts})
        else:
            built.append(
                {
                    "role": "tool",
                    "parts": [
                        {"type": "tool_call_response", "id": message.call_id, "response": message.content},
                    ],
                }
            )
    return built


def _output_messages(end: StreamEnd) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    if end.text:
        parts.append({"type": "text", "content": end.text})
    parts.extend(_tool_call_part(call) for call in end.tool_calls)
    return [{"role": "assistant", "parts": parts, "finish_reason": end.finish_reason}]


def _tool_definitions(tools: list[ToolSchema]) -> list[dict[str, Any]]:
    return [
        {"type": "function", "name": schema.name, "description": schema.description, "parameters": schema.parameters}
        for schema in tools
    ]


def _server(provider: Provider) -> tuple[str | None, int | None]:
    base_url = getattr(provider, "base_url", None)
    if not base_url:
        return None, None
    try:
        parts = urlsplit(str(base_url))
        host, port = parts.hostname, parts.port
    except ValueError:
        return None, None
    if not host:
        return None, None
    return host, port if port is not None else _DEFAULT_PORTS.get(parts.scheme)


class Telemetry:
    def __init__(
        self,
        tracer_provider: Any = None,
        *,
        capture_content: bool = False,
        max_content_chars: int = 32_768,
    ) -> None:
        self.capture_content = capture_content
        self.max_content_chars = max_content_chars
        self._provider = tracer_provider
        self._tracer: Any = None

    def _cut(self, text: str) -> str:
        if len(text) <= self.max_content_chars:
            return text
        return f"{text[: self.max_content_chars]}…[truncated {len(text) - self.max_content_chars} chars]"

    def _content(self, value: Any) -> str:
        return self._cut(_dumps(value))

    def _open(self, name: str, parent: Any, kind: Any, attributes: dict[str, Any]) -> _Handle:
        if self._tracer is None:
            self._tracer = (self._provider or trace.get_tracer_provider()).get_tracer("tantra", _VERSION)
        outer = parent if isinstance(parent, _Handle) else None
        if outer is not None:
            attributes.setdefault("gen_ai.conversation.id", outer.conversation_id)
            attributes.setdefault("tantra.turn_id", outer.turn_id)
        span = self._tracer.start_span(
            name,
            context=outer.ctx if outer is not None else None,
            kind=kind,
            attributes=_clean(attributes),
        )
        handle = _Handle(span=span, ctx=set_span_in_context(span), parent=outer)
        if outer is not None:
            handle.conversation_id = outer.conversation_id
            handle.turn_id = outer.turn_id
            handle.agent = outer.agent
        return handle

    def _close(self, handle: _Handle, attributes: dict[str, Any], status: Any = None) -> None:
        handle.span.set_attributes(_clean(attributes))
        if status is not None:
            handle.span.set_status(status)
        handle.span.end()

    def start_turn(self, turn: TurnContext, *, resumed: bool, ask_id: str | None, parent: Any) -> Any:
        attributes: dict[str, Any] = {
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": turn.agent,
            "gen_ai.conversation.id": turn.session_id,
            "tantra.turn_id": turn.turn_id,
            "tantra.depth": turn.depth,
            "tantra.turn.resumed": resumed,
        }
        if resumed and ask_id is not None:
            attributes["tantra.ask_id"] = ask_id
        for key, value in turn.metadata.items():
            if isinstance(value, str | int | float | bool):
                attributes[f"tantra.metadata.{key}"] = value
        if self.capture_content:
            attributes["gen_ai.input.messages"] = self._content(
                [{"role": "user", "parts": [{"type": "text", "content": turn.input}]}]
            )
        handle = self._open(f"invoke_agent {turn.agent}", parent, SpanKind.INTERNAL, attributes)
        handle.conversation_id = turn.session_id
        handle.turn_id = turn.turn_id
        handle.agent = turn.agent
        return handle

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
        if not isinstance(span, _Handle):
            return
        attributes: dict[str, Any] = {
            "tantra.turn.outcome": outcome,
            "tantra.stop_reason": stop_reason,
            "tantra.ask_id": ask_id,
            **_usage_attrs(span.usage),
        }
        status = None
        if isinstance(error, BaseException):
            if not isinstance(error, GeneratorExit):
                span.span.record_exception(error)
                attributes["error.type"] = type(error).__qualname__
                status = Status(StatusCode.ERROR, str(error))
        elif isinstance(error, str):
            attributes["error.type"] = "_OTHER"
            status = Status(StatusCode.ERROR, error)
        if self.capture_content:
            text = final_text or (_dumps(output) if output is not None else None)
            if text is not None:
                attributes["gen_ai.output.messages"] = self._content(
                    [{"role": "assistant", "parts": [{"type": "text", "content": text}], "finish_reason": stop_reason}]
                )
        self._close(span, attributes, status)

    def start_sample(
        self, parent: Any, req: SampleRequest, *, sample_id: str | None, provider: Provider, compacted: bool
    ) -> Any:
        address, port = _server(provider)
        attributes: dict[str, Any] = {
            "gen_ai.operation.name": "chat",
            "gen_ai.provider.name": getattr(provider, "provider_name", "unknown"),
            "gen_ai.request.model": req.model,
            "server.address": address,
            "server.port": port,
            "tantra.sample_id": sample_id,
        }
        if compacted:
            attributes["gen_ai.conversation.compacted"] = True
        if self.capture_content:
            if req.system:
                attributes["gen_ai.system_instructions"] = self._content(
                    [{"type": "text", "content": block.text} for block in req.system]
                )
            attributes["gen_ai.input.messages"] = self._content(_input_messages(req.messages))
            if req.tools:
                attributes["gen_ai.tool.definitions"] = self._content(_tool_definitions(req.tools))
        return self._open(f"chat {req.model}", parent, SpanKind.CLIENT, attributes)

    def end_sample(self, span: Any, *, end: StreamEnd | None, error: BaseException | None, attempts: int) -> None:
        if not isinstance(span, _Handle):
            return
        attributes: dict[str, Any] = {"tantra.sample.attempts": attempts}
        status = None
        if end is not None:
            if end.finish_reason is not None:
                attributes["gen_ai.response.finish_reasons"] = [end.finish_reason]
            attributes.update(_usage_attrs(end.usage))
            if span.parent is not None:
                _accumulate(span.parent.usage, end.usage)
            if self.capture_content:
                attributes["gen_ai.output.messages"] = self._content(_output_messages(end))
        if error is not None:
            span.span.record_exception(error)
            attributes["error.type"] = type(error).__qualname__
            if isinstance(error, ProviderError):
                attributes["tantra.provider.status_code"] = error.status_code
            status = Status(StatusCode.ERROR, str(error))
        self._close(span, attributes, status)

    def start_tool(
        self, parent: Any, call: ToolCallRequested, *, args: dict[str, Any], tool: Tool | None, replayed: bool
    ) -> Any:
        attributes: dict[str, Any] = {
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": call.name,
            "gen_ai.tool.call.id": call.call_id,
            "gen_ai.tool.type": "function",
            "gen_ai.tool.description": tool.description if tool is not None else None,
            "gen_ai.agent.name": parent.agent if isinstance(parent, _Handle) else None,
            "tantra.tool.replayed": replayed,
        }
        if self.capture_content:
            attributes["gen_ai.tool.call.arguments"] = self._content(args)
            if args != call.args:
                attributes["tantra.tool.original_arguments"] = self._content(call.args)
        return self._open(f"execute_tool {call.name}", parent, SpanKind.INTERNAL, attributes)

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
        if not isinstance(span, _Handle):
            return
        attributes: dict[str, Any] = {"tantra.tool.outcome": outcome, "tantra.ask_id": ask_id}
        status = None
        if is_error:
            attributes["error.type"] = error_type or "_OTHER"
            status = Status(StatusCode.ERROR, error_type or "_OTHER")
        if self.capture_content and result is not None:
            attributes["gen_ai.tool.call.result"] = self._cut(_text(result))
        self._close(span, attributes, status)

    def start_compaction(self, parent: Any) -> Any:
        return self._open("compact", parent, SpanKind.INTERNAL, {"gen_ai.operation.name": "compact"})

    def end_compaction(self, span: Any, *, applied: CompactionApplied | None, error: BaseException | None) -> None:
        if not isinstance(span, _Handle):
            return
        attributes: dict[str, Any] = {
            "tantra.compaction.applied": applied is not None,
            **_usage_attrs(span.usage),
        }
        status = None
        if applied is not None:
            attributes["tantra.compaction.strategy"] = applied.strategy
            attributes["tantra.compaction.tokens_before"] = applied.tokens_before
            attributes["tantra.compaction.tokens_after"] = applied.tokens_after
            attributes["tantra.compaction.floor_turn_id"] = applied.floor_turn_id
            if self.capture_content:
                attributes["tantra.compaction.summary"] = self._cut(applied.summary)
        if error is not None:
            span.span.record_exception(error)
            attributes["error.type"] = type(error).__qualname__
            status = Status(StatusCode.ERROR, str(error))
        self._close(span, attributes, status)
