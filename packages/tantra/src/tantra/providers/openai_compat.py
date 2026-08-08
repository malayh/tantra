from __future__ import annotations

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


class OpenAICompatible:
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

    def limits(self, model: str) -> ModelLimits:
        return self._limits.get(model, FALLBACK_LIMITS)

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

                fragment = getattr(delta, "reasoning", None)
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
