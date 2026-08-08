from __future__ import annotations

import codecs
import re
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field

from tantra.errors import ProviderError
from tantra.events import Usage
from tantra.providers.base import (
    ModelLimits,
    ProviderEvent,
    ReasoningBlock,
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    TextDelta,
    ToolCall,
    ToolCallDelta,
)

FAKE_LIMITS = ModelLimits(context_window=1_000_000, max_output=64_000)


class Sample(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str = ""
    reasoning: str = ""
    tool_calls: list[ToolCall] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    finish_reason: str | None = None


def _split(text: str) -> list[str]:
    return [part for part in re.split(r"(?<=\s)", text) if part]


class FakeProvider:
    def __init__(self, samples: list[Sample]) -> None:
        self.samples = list(samples)
        self.requests: list[SampleRequest] = []
        self._next = 0

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if self._next >= len(self.samples):
            raise ProviderError(f"FakeProvider: no scripted sample for request {self._next + 1}")
        sample = self.samples[self._next]
        self._next += 1
        self.requests.append(req)

        for fragment in _split(sample.reasoning):
            yield ReasoningDelta(text=fragment)
        for fragment in _split(sample.text):
            yield TextDelta(text=fragment)

        for index, call in enumerate(sample.tool_calls):
            half = len(call.args) // 2
            yield ToolCallDelta(index=index, id=call.id, name=call.name, args_fragment=call.args[:half])
            yield ToolCallDelta(index=index, args_fragment=call.args[half:])
        for call in sample.tool_calls:
            yield call

        yield StreamEnd(
            text=sample.text,
            reasoning=[ReasoningBlock(text=sample.reasoning)] if sample.reasoning else [],
            tool_calls=list(sample.tool_calls),
            usage=sample.usage,
            finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
        )


class Interaction(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: str = "POST"
    url: str = ""
    status: int = 200
    content_type: str = "text/event-stream"
    chunks: list[str] = Field(default_factory=list)


class Cassette(BaseModel):
    model_config = ConfigDict(extra="allow")

    interactions: list[Interaction] = Field(default_factory=list)


def cassette_transport(path: str | Path) -> httpx.MockTransport:
    """Replay a recorded cassette through the real SSE parser. Interactions are consumed in order."""
    cassette = Cassette.model_validate_json(Path(path).read_text())
    remaining = iter(cassette.interactions)

    def handler(request: httpx.Request) -> httpx.Response:
        interaction = next(remaining, None)
        if interaction is None:
            raise ProviderError(f"cassette exhausted: {path}")

        async def body() -> AsyncIterator[bytes]:
            for chunk in interaction.chunks:
                yield chunk.encode()

        return httpx.Response(
            interaction.status,
            headers={"content-type": interaction.content_type},
            content=body(),
            request=request,
        )

    return httpx.MockTransport(handler)


class _CapturingStream(httpx.AsyncByteStream):
    def __init__(self, response: httpx.Response, interaction: Interaction, on_complete: Callable[[], None]) -> None:
        self._response = response
        self._interaction = interaction
        self._on_complete = on_complete
        self._decoder = codecs.getincrementaldecoder("utf-8")()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._response.stream:
            decoded = self._decoder.decode(chunk)
            if decoded:
                self._interaction.chunks.append(decoded)
            yield chunk
        tail = self._decoder.decode(b"", True)
        if tail:
            self._interaction.chunks.append(tail)
        self._on_complete()

    async def aclose(self) -> None:
        await self._response.aclose()


class CassetteRecorder(httpx.AsyncBaseTransport):
    """Dev-time transport that captures raw response bytes to a cassette file. Hits the network."""

    def __init__(self, path: str | Path, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.path = Path(path)
        self.cassette = Cassette()
        self._inner = transport if transport is not None else httpx.AsyncHTTPTransport()

    def save(self) -> None:
        self.path.write_text(self.cassette.model_dump_json(indent=2))

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        request.headers["accept-encoding"] = "identity"
        response = await self._inner.handle_async_request(request)
        interaction = Interaction(
            method=request.method,
            url=str(request.url),
            status=response.status_code,
            content_type=response.headers.get("content-type", "text/event-stream"),
        )
        self.cassette.interactions.append(interaction)

        return httpx.Response(
            response.status_code,
            headers={"content-type": interaction.content_type},
            stream=_CapturingStream(response, interaction, self.save),
            request=request,
        )
