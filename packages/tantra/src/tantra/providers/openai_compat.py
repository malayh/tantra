from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import openai
from openai import AsyncOpenAI
from openai.lib.streaming.chat import ChatCompletionStreamState

from tantra.errors import ProviderError
from tantra.events import Usage
from tantra.providers.base import (
    Message,
    ModelLimits,
    ProviderEvent,
    ReasoningBlock,
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    TextDelta,
    ToolCall,
    ToolCallDelta,
    ToolResultMessage,
    UserMessage,
)

FALLBACK_LIMITS = ModelLimits(context_window=128_000, max_output=4_096)
RESERVED_KEYS = frozenset({"model", "messages", "stream", "stream_options", "tools"})
CONTEXT_OVERFLOW_CODE = "context_length_exceeded"


def _message_payload(message: Message) -> dict[str, Any]:
    if isinstance(message, UserMessage):
        return {"role": "user", "content": message.content}
    if isinstance(message, ToolResultMessage):
        return {"role": "tool", "tool_call_id": message.call_id, "content": message.content}
    content = message.text if message.tool_calls else message.text or ""
    payload: dict[str, Any] = {"role": "assistant", "content": content}
    if message.tool_calls:
        payload["tool_calls"] = [
            {"id": call.id, "type": "function", "function": {"name": call.name, "arguments": call.args}}
            for call in message.tool_calls
        ]
    return payload


def _usage_payload(raw: dict[str, Any]) -> Usage:
    details = raw.get("prompt_tokens_details") or {}
    cached = details.get("cached_tokens") or 0
    return Usage(
        input_tokens=max((raw.get("prompt_tokens") or 0) - cached, 0),
        output_tokens=raw.get("completion_tokens") or 0,
        cache_read_tokens=cached,
    )


def _positive_int(*values: Any, fallback: int) -> int:
    return next(
        (value for value in values if isinstance(value, int) and not isinstance(value, bool) and value > 0),
        fallback,
    )


def _metadata_limits(raw: dict[str, Any]) -> ModelLimits:
    nested = raw.get("top_provider")
    top_provider = nested if isinstance(nested, dict) else {}
    return ModelLimits(
        context_window=_positive_int(
            raw.get("context_length"),
            top_provider.get("context_length"),
            fallback=FALLBACK_LIMITS.context_window,
        ),
        max_output=_positive_int(
            raw.get("max_completion_tokens"),
            top_provider.get("max_completion_tokens"),
            fallback=FALLBACK_LIMITS.max_output,
        ),
    )


def _context_overflow(exc: openai.OpenAIError) -> bool:
    body = getattr(exc, "body", None)
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        body = body["error"]
    structured = body if isinstance(body, dict) else {}
    metadata = structured.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    if any(
        value == CONTEXT_OVERFLOW_CODE
        for value in (structured.get("code"), metadata.get("error_type"), metadata.get("provider_error_code"))
    ):
        return True
    message = structured.get("message")
    text = message if isinstance(message, str) else str(exc)
    text = text.lower()
    has_context = "context length" in text or "context window" in text
    has_tokens = "token" in text
    has_overflow = any(phrase in text for phrase in ("exceed", "requested", "resulted", "too large", "too long"))
    return has_context and has_tokens and has_overflow


class OpenAICompatible:
    provider_name = "openai"

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        limits: dict[str, ModelLimits] | None = None,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._limits = dict(limits or {})
        self._client = AsyncOpenAI(
            base_url=self.base_url,
            api_key=api_key,
            http_client=http_client,
            timeout=timeout,
            max_retries=0,
        )
        self._catalogue_loaded = False
        self._catalogue_lock = asyncio.Lock()
        self._discovered_limits: dict[str, ModelLimits] = {}

    async def limits(self, model: str) -> ModelLimits:
        configured = self._limits.get(model)
        if configured is not None:
            return configured
        if not self._catalogue_loaded:
            async with self._catalogue_lock:
                if not self._catalogue_loaded:
                    self._discovered_limits = await self._discover_limits()
                    self._catalogue_loaded = True
        return self._discovered_limits.get(model, FALLBACK_LIMITS)

    async def _discover_limits(self) -> dict[str, ModelLimits]:
        try:
            catalogue = await self._client.models.list()
            discovered = {}
            for entry in catalogue.data:
                raw = entry.model_dump()
                model = raw.get("id")
                if isinstance(model, str):
                    discovered[model] = _metadata_limits(raw)
            return discovered
        except Exception:
            return {}

    async def aclose(self) -> None:
        await self._client.close()

    def build_payload(self, req: SampleRequest) -> dict[str, Any]:
        messages: list[dict[str, Any]] = []
        if req.system:
            messages.append({"role": "system", "content": "\n\n".join(block.text for block in req.system)})
        messages.extend(_message_payload(message) for message in req.messages)

        payload: dict[str, Any] = {
            "model": req.model,
            "messages": messages,
            "stream_options": {"include_usage": True},
        }
        if req.tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters,
                    },
                }
                for tool in req.tools
            ]
        extra = {key: value for key, value in req.params.items() if key not in RESERVED_KEYS}
        if extra:
            payload["extra_body"] = extra
        return payload

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        reasoning = ""
        saw_chunk = False

        state = ChatCompletionStreamState()
        try:
            chunks = await self._client.chat.completions.create(stream=True, **self.build_payload(req))
            async for chunk in chunks:
                saw_chunk = True
                state.handle_chunk(chunk)
                if not chunk.choices:
                    continue

                delta = chunk.choices[0].delta
                if delta.content:
                    yield TextDelta(text=delta.content)

                fragment = getattr(delta, "reasoning", None) or getattr(delta, "reasoning_content", None)
                if fragment:
                    reasoning += fragment
                    yield ReasoningDelta(text=fragment)

                for raw_call in delta.tool_calls or []:
                    function = raw_call.function
                    yield ToolCallDelta(
                        index=raw_call.index,
                        id=raw_call.id,
                        name=function.name if function else None,
                        args_fragment=(function.arguments if function else None) or "",
                    )

            if not saw_chunk:
                raise ProviderError(f"no SSE data frames from {self.base_url}")
            final = state.get_final_completion()
        except openai.OpenAIError as exc:
            raise ProviderError(
                str(exc),
                status_code=getattr(exc, "status_code", None),
                retryable=True if isinstance(exc, openai.APIConnectionError) else None,
                context_overflow=_context_overflow(exc),
            ) from exc
        except (TypeError, ValueError, AttributeError, KeyError, AssertionError) as exc:
            raise ProviderError(f"malformed stream from {self.base_url}: {exc!r}") from exc

        choice = final.choices[0] if final.choices else None
        message = choice.message if choice else None
        calls = [
            ToolCall(
                id=raw_call.id or f"call_{index}",
                name=raw_call.function.name or "",
                args=raw_call.function.arguments or "",
            )
            for index, raw_call in enumerate(message.tool_calls or [] if message else [])
        ]
        for call in calls:
            yield call

        yield StreamEnd(
            text=(message.content if message else None) or "",
            reasoning=[ReasoningBlock(text=reasoning)] if reasoning else [],
            tool_calls=calls,
            usage=_usage_payload(final.usage.model_dump()) if final.usage else Usage(),
            finish_reason=choice.finish_reason if choice else None,
        )


class OpenAICompatibleEmbedder:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        *,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self._client = AsyncOpenAI(
            base_url=self.base_url,
            api_key=api_key,
            http_client=http_client,
            timeout=timeout,
            max_retries=0,
        )

    async def aclose(self) -> None:
        await self._client.close()

    async def embed(self, texts: list[str]) -> list[list[float]]:
        try:
            response = await self._client.embeddings.create(model=self.model, input=texts)
        except openai.OpenAIError as exc:
            raise ProviderError(str(exc), status_code=getattr(exc, "status_code", None)) from exc
        return [row.embedding for row in sorted(response.data, key=lambda row: row.index)]
