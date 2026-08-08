from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

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


def _slot_for(
    raw_call: dict[str, Any],
    slots: list[dict[str, Any]],
    by_key: dict[Any, dict[str, Any]],
) -> dict[str, Any]:
    key = raw_call.get("index")
    if key is None:
        key = raw_call.get("id")
    if key is None:
        if slots:
            return slots[-1]
    elif key in by_key:
        return by_key[key]

    slot: dict[str, Any] = {"index": raw_call.get("index", len(slots)), "id": None, "name": None, "args": ""}
    slots.append(slot)
    if key is not None:
        by_key[key] = slot
    return slot


class OpenAICompatible:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        limits: dict[str, ModelLimits] | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._limits = dict(limits or {})
        self._client = client if client is not None else httpx.AsyncClient(timeout=timeout)

    def limits(self, model: str) -> ModelLimits:
        return self._limits.get(model, FALLBACK_LIMITS)

    async def aclose(self) -> None:
        await self._client.aclose()

    def build_payload(self, req: SampleRequest) -> dict[str, Any]:
        messages: list[dict[str, Any]] = []
        if req.system:
            messages.append({"role": "system", "content": "\n\n".join(block.text for block in req.system)})
        messages.extend(_message_payload(message) for message in req.messages)

        payload: dict[str, Any] = {key: value for key, value in req.params.items() if key not in RESERVED_KEYS}
        payload["model"] = req.model
        payload["messages"] = messages
        payload["stream"] = True
        payload["stream_options"] = {"include_usage": True}
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
        return payload

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "text/event-stream"}
        text = ""
        reasoning = ""
        slots: list[dict[str, Any]] = []
        by_key: dict[Any, dict[str, Any]] = {}
        usage = Usage()
        finish_reason: str | None = None
        saw_data = False

        async with self._client.stream(
            "POST",
            f"{self.base_url}/chat/completions",
            json=self.build_payload(req),
            headers=headers,
        ) as response:
            if response.status_code >= 300:
                body = (await response.aread()).decode(errors="replace")
                raise ProviderError(f"{response.status_code} from {self.base_url}: {body[:500]}")

            async for line in response.aiter_lines():
                data = _sse_data(line)
                if data is None:
                    continue
                if data == "[DONE]":
                    break
                try:
                    chunk = json.loads(data)
                except json.JSONDecodeError as exc:
                    raise ProviderError(f"malformed SSE payload: {data[:200]}") from exc

                saw_data = True
                if chunk.get("error"):
                    raise ProviderError(f"stream error from {self.base_url}: {json.dumps(chunk['error'])[:500]}")

                if chunk.get("usage"):
                    usage = _usage_payload(chunk["usage"])

                choices = chunk.get("choices") or []
                if not choices:
                    continue
                choice = choices[0]
                if choice.get("finish_reason"):
                    finish_reason = choice["finish_reason"]

                delta = choice.get("delta") or {}
                if delta.get("content"):
                    text += delta["content"]
                    yield TextDelta(text=delta["content"])
                if delta.get("reasoning"):
                    reasoning += delta["reasoning"]
                    yield ReasoningDelta(text=delta["reasoning"])

                for raw_call in delta.get("tool_calls") or []:
                    function = raw_call.get("function") or {}
                    fragment = function.get("arguments") or ""
                    slot = _slot_for(raw_call, slots, by_key)
                    if raw_call.get("id"):
                        slot["id"] = raw_call["id"]
                    if function.get("name"):
                        slot["name"] = function["name"]
                    slot["args"] += fragment
                    yield ToolCallDelta(
                        index=slot["index"],
                        id=raw_call.get("id"),
                        name=function.get("name"),
                        args_fragment=fragment,
                    )

        if not saw_data:
            raise ProviderError(f"no SSE data frames from {self.base_url}")

        calls = [
            ToolCall(id=slot["id"] or f"call_{slot['index']}", name=slot["name"] or "", args=slot["args"])
            for slot in sorted(slots, key=lambda slot: slot["index"])
        ]
        for call in calls:
            yield call

        yield StreamEnd(
            text=text,
            reasoning=[ReasoningBlock(text=reasoning)] if reasoning else [],
            tool_calls=calls,
            usage=usage,
            finish_reason=finish_reason,
        )


def _sse_data(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith(":") or not stripped.startswith("data:"):
        return None
    return stripped[len("data:") :].strip()


class OpenAICompatibleEmbedder:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self._client = client if client is not None else httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def embed(self, texts: list[str]) -> list[list[float]]:
        response = await self._client.post(
            f"{self.base_url}/embeddings",
            json={"model": self.model, "input": texts},
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        if response.status_code >= 300:
            raise ProviderError(f"{response.status_code} from {self.base_url}: {response.text[:500]}")
        rows = response.json().get("data") or []
        return [row["embedding"] for row in sorted(rows, key=lambda row: row.get("index", 0))]
