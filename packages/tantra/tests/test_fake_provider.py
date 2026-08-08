from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from tantra.errors import ProviderError
from tantra.events import Usage
from tantra.providers.base import (
    ReasoningBlock,
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    TextDelta,
    ToolCall,
    ToolCallDelta,
    UserMessage,
)
from tantra.providers.fake import FAKE_LIMITS, Cassette, CassetteRecorder, FakeProvider, Sample, cassette_transport
from tantra.providers.openai_compat import OpenAICompatible

CALL = ToolCall(id="c1", name="search_metrics", args='{"q":"rate"}')
REQ = SampleRequest(model="fake/model", messages=[UserMessage(content="hi")])


async def drain(provider: FakeProvider) -> list:
    return [event async for event in provider.stream(REQ)]


async def test_two_scripted_samples_yield_the_exact_event_sequence() -> None:
    provider = FakeProvider(
        [
            Sample(text="Let me check.", tool_calls=[CALL]),
            Sample(text="Done.", reasoning="All good.", usage=Usage(input_tokens=10, output_tokens=3)),
        ]
    )

    assert await drain(provider) == [
        TextDelta(text="Let "),
        TextDelta(text="me "),
        TextDelta(text="check."),
        ToolCallDelta(index=0, id="c1", name="search_metrics", args_fragment='{"q":"'),
        ToolCallDelta(index=0, args_fragment='rate"}'),
        CALL,
        StreamEnd(text="Let me check.", tool_calls=[CALL], finish_reason="tool_calls"),
    ]
    assert await drain(provider) == [
        ReasoningDelta(text="All "),
        ReasoningDelta(text="good."),
        TextDelta(text="Done."),
        StreamEnd(
            text="Done.",
            reasoning=[ReasoningBlock(text="All good.")],
            usage=Usage(input_tokens=10, output_tokens=3),
            finish_reason="stop",
        ),
    ]


async def test_deltas_concatenate_to_the_terminal_sample() -> None:
    provider = FakeProvider([Sample(text="a b c", tool_calls=[CALL])])

    events = await drain(provider)

    assert "".join(e.text for e in events if isinstance(e, TextDelta)) == events[-1].text
    fragments = "".join(e.args_fragment for e in events if isinstance(e, ToolCallDelta))
    assert fragments == CALL.args


async def test_exhausted_script_raises() -> None:
    provider = FakeProvider([Sample(text="only one")])
    await drain(provider)

    with pytest.raises(ProviderError, match="no scripted sample"):
        await drain(provider)


async def test_requests_are_recorded_and_limits_are_generous() -> None:
    provider = FakeProvider([Sample(text="hi")])
    await drain(provider)

    assert provider.requests == [REQ]
    assert provider.limits("anything") == FAKE_LIMITS


async def test_recorder_captures_raw_bytes_and_replays_them(tmp_path: Path) -> None:
    path = tmp_path / "recorded.json"

    async def upstream_body() -> AsyncIterator[bytes]:
        yield b'data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n'
        yield b"data: [DONE]\n\n"

    upstream = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=upstream_body(),
        )
    )

    async with httpx.AsyncClient(transport=CassetteRecorder(path, upstream)) as client:
        async with client.stream("POST", "https://api.test/v1/chat/completions", json={}) as response:
            async for _ in response.aiter_bytes():
                pass

    recorded = Cassette.model_validate_json(path.read_text())
    assert recorded.interactions[0].url == "https://api.test/v1/chat/completions"
    assert "".join(recorded.interactions[0].chunks).endswith("data: [DONE]\n\n")

    transport = cassette_transport(path)
    async with httpx.AsyncClient(transport=transport) as client:
        replayed = await client.post("https://api.test/v1/chat/completions", json={})
    assert replayed.text == "".join(recorded.interactions[0].chunks)


async def test_recorder_survives_a_multibyte_char_split_across_chunks(tmp_path: Path) -> None:
    path = tmp_path / "recorded.json"
    body = 'data: {"choices":[{"index":0,"delta":{"content":"café"}}]}\n\n'.encode()
    cut = body.index("é".encode()) + 1

    async def upstream_body() -> AsyncIterator[bytes]:
        yield body[:cut]
        yield body[cut:] + b"data: [DONE]\n\n"

    upstream = httpx.MockTransport(lambda request: httpx.Response(200, content=upstream_body()))
    async with httpx.AsyncClient(transport=CassetteRecorder(path, upstream)) as client:
        async with client.stream("POST", "https://api.test/v1/chat/completions", json={}) as response:
            async for _ in response.aiter_bytes():
                pass

    assert "".join(Cassette.model_validate_json(path.read_text()).interactions[0].chunks) == (
        body.decode() + "data: [DONE]\n\n"
    )

    api = OpenAICompatible("https://api.test/v1", "sk", client=httpx.AsyncClient(transport=cassette_transport(path)))
    events = [event async for event in api.stream(REQ)]
    assert events[-1].text == "café"
    await api.aclose()


async def test_recorder_closes_upstream_when_the_consumer_stops_early(tmp_path: Path) -> None:
    class TrackedStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"data: one\n\n"
            yield b"data: two\n\n"

        async def aclose(self) -> None:
            self.closed = True

    stream = TrackedStream()
    upstream = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))

    async with httpx.AsyncClient(transport=CassetteRecorder(tmp_path / "r.json", upstream)) as client:
        async with client.stream("POST", "https://api.test/v1/chat/completions", json={}) as response:
            async for _ in response.aiter_bytes():
                break

    assert stream.closed


def test_cassette_exhaustion_raises(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    path.write_text(Cassette().model_dump_json())
    transport = cassette_transport(path)

    with pytest.raises(ProviderError, match="cassette exhausted"):
        httpx.Client(transport=transport).post("https://api.test/v1/chat/completions", json={})
