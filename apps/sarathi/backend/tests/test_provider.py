import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from sarathi.provider import ReasoningCompat
from tantra.providers.base import (
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    TextDelta,
    UserMessage,
)


def _chunk(delta: dict[str, Any], finish_reason: str | None = None) -> str:
    payload = {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "test-model",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    return f"data: {json.dumps(payload)}\n\n"


def _body(reasoning_field: str) -> str:
    return "".join(
        [
            _chunk({"role": "assistant", "content": ""}),
            _chunk({reasoning_field: "think "}),
            _chunk({reasoning_field: "hard"}),
            _chunk({"content": "hello "}),
            _chunk({"content": "world"}),
            _chunk({}, finish_reason="stop"),
            "data: [DONE]\n\n",
        ]
    )


def _transport(body: str) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        async def stream() -> AsyncIterator[bytes]:
            yield body.encode()

        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=stream(), request=request)

    return httpx.MockTransport(handler)


async def _collect(reasoning_field: str) -> list[Any]:
    http_client = httpx.AsyncClient(transport=_transport(_body(reasoning_field)))
    provider = ReasoningCompat("http://provider.test/v1", "key", http_client=http_client)
    request = SampleRequest(model="test-model", messages=[UserMessage(content="hi")])
    try:
        return [event async for event in provider.stream(request)]
    finally:
        await provider.aclose()


async def test_reasoning_content_deltas_are_streamed() -> None:
    events = await _collect("reasoning_content")

    assert "".join(event.text for event in events if isinstance(event, ReasoningDelta)) == "think hard"
    assert "".join(event.text for event in events if isinstance(event, TextDelta)) == "hello world"

    ends = [event for event in events if isinstance(event, StreamEnd)]
    assert len(ends) == 1
    assert ends[0].text == "hello world"
    assert [block.text for block in ends[0].reasoning] == ["think hard"]
    assert ends[0].finish_reason == "stop"


async def test_reasoning_field_still_supported() -> None:
    events = await _collect("reasoning")

    assert "".join(event.text for event in events if isinstance(event, ReasoningDelta)) == "think hard"
    ends = [event for event in events if isinstance(event, StreamEnd)]
    assert [block.text for block in ends[0].reasoning] == ["think hard"]
