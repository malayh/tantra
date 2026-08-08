from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import httpx
import pytest

from tantra.errors import ProviderError
from tantra.providers.base import (
    AssistantMessage,
    ModelLimits,
    ProviderEvent,
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    SystemBlock,
    TextDelta,
    ToolCall,
    ToolCallDelta,
    ToolResultMessage,
    ToolSchema,
    UserMessage,
)
from tantra.providers.fake import Cassette, Interaction, cassette_transport
from tantra.providers.openai_compat import FALLBACK_LIMITS, OpenAICompatible, OpenAICompatibleEmbedder

CASSETTE = Path(__file__).parent / "cassettes" / "tool_call_split.json"
REQ = SampleRequest(model="google/gemini-3-pro", messages=[UserMessage(content="request rate?")])


def frame(delta: dict, finish: str | None = None) -> str:
    return f"data: {json.dumps({'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]})}\n\n"


OK_STREAM = (frame({"content": "ok"}) + "data: [DONE]\n\n").encode()


@pytest.fixture
async def provider() -> AsyncIterator[Callable[[httpx.MockTransport], OpenAICompatible]]:
    built: list[OpenAICompatible] = []

    def factory(transport: httpx.MockTransport, **kwargs) -> OpenAICompatible:
        api = OpenAICompatible(
            "https://api.test/v1", "sk-test", client=httpx.AsyncClient(transport=transport), **kwargs
        )
        built.append(api)
        return api

    yield factory
    for api in built:
        await api.aclose()


@pytest.fixture
async def embedder() -> AsyncIterator[Callable[[httpx.MockTransport], OpenAICompatibleEmbedder]]:
    built: list[OpenAICompatibleEmbedder] = []

    def factory(transport: httpx.MockTransport) -> OpenAICompatibleEmbedder:
        api = OpenAICompatibleEmbedder(
            "https://api.test/v1",
            "sk-test",
            "text-embedding-3-small",
            client=httpx.AsyncClient(transport=transport),
        )
        built.append(api)
        return api

    yield factory
    for api in built:
        await api.aclose()


async def collect(stream: AsyncIterator[ProviderEvent]) -> list[ProviderEvent]:
    return [event async for event in stream]


def replay(tmp_path: Path, chunks: list[str], status: int = 200) -> httpx.MockTransport:
    path = tmp_path / "cassette.json"
    path.write_text(Cassette(interactions=[Interaction(status=status, chunks=chunks)]).model_dump_json())
    return cassette_transport(path)


def capturing(seen: list[dict], body: bytes = OK_STREAM) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, content=body)

    return httpx.MockTransport(handler)


def recorded_argument_fragments() -> list[str]:
    chunks = json.loads(CASSETTE.read_text())["interactions"][0]["chunks"]
    fragments = []
    for chunk in chunks:
        data = chunk.removeprefix("data:").strip()
        if data == "[DONE]":
            continue
        for choice in json.loads(data).get("choices") or []:
            for call in choice.get("delta", {}).get("tool_calls") or []:
                fragment = (call.get("function") or {}).get("arguments") or ""
                if fragment:
                    fragments.append(fragment)
    return fragments


async def test_split_tool_call_args_replay_into_one_byte_identical_call(provider) -> None:
    fragments = recorded_argument_fragments()
    assert len(fragments) >= 12

    events = await collect(provider(cassette_transport(CASSETTE)).stream(REQ))

    calls = [event for event in events if isinstance(event, ToolCall)]
    assert len(calls) == 1
    assert calls[0].name == "search_metrics"
    assert calls[0].id == "call_9xQvT2"
    assert calls[0].args.encode() == "".join(fragments).encode()
    assert json.loads(calls[0].args) == {"query": "http_requests_total rate by (job)", "limit": 25}


async def test_tool_call_deltas_carry_the_raw_fragments(provider) -> None:
    events = await collect(provider(cassette_transport(CASSETTE)).stream(REQ))

    deltas = [event for event in events if isinstance(event, ToolCallDelta)]
    assert len(deltas) == len(recorded_argument_fragments()) + 1
    assert {delta.index for delta in deltas} == {0}
    assert deltas[0].id == "call_9xQvT2"
    assert deltas[0].name == "search_metrics"
    assert "".join(delta.args_fragment for delta in deltas) == "".join(recorded_argument_fragments())


async def test_complete_calls_precede_stream_end(provider) -> None:
    events = await collect(provider(cassette_transport(CASSETTE)).stream(REQ))

    assert isinstance(events[-1], StreamEnd)
    assert isinstance(events[-2], ToolCall)
    assert events[-1].tool_calls == [events[-2]]


async def test_text_and_reasoning_stream_live_and_accumulate(provider) -> None:
    events = await collect(provider(cassette_transport(CASSETTE)).stream(REQ))

    assert [event.text for event in events if isinstance(event, TextDelta)] == ["Let me look ", "that up."]
    assert [event.text for event in events if isinstance(event, ReasoningDelta)] == [
        "The user wants request rate. ",
        "Search the metric catalogue first.",
    ]
    end = events[-1]
    assert end.text == "Let me look that up."
    assert [block.text for block in end.reasoning] == [
        "The user wants request rate. Search the metric catalogue first."
    ]


async def test_usage_splits_cached_tokens_out_of_the_input_count(provider) -> None:
    events = await collect(provider(cassette_transport(CASSETTE)).stream(REQ))

    end = events[-1]
    assert end.finish_reason == "tool_calls"
    assert end.usage.cache_read_tokens == 1024
    assert end.usage.input_tokens == 810
    assert end.usage.input_tokens + end.usage.cache_read_tokens == 1834
    assert end.usage.output_tokens == 47


async def test_two_tool_calls_with_interleaved_index_fragments(tmp_path: Path, provider) -> None:
    chunks = [
        frame({"tool_calls": [{"index": 0, "id": "a", "function": {"name": "left", "arguments": '{"x"'}}]}),
        frame({"tool_calls": [{"index": 1, "id": "b", "function": {"name": "right", "arguments": '{"y"'}}]}),
        frame({"tool_calls": [{"index": 1, "function": {"arguments": ":2}"}}]}),
        frame({"tool_calls": [{"index": 0, "function": {"arguments": ":1}"}}]}),
        frame({}, finish="tool_calls"),
        "data: [DONE]\n\n",
    ]

    events = await collect(provider(replay(tmp_path, chunks)).stream(REQ))

    calls = [event for event in events if isinstance(event, ToolCall)]
    assert [(call.id, call.name, call.args) for call in calls] == [
        ("a", "left", '{"x":1}'),
        ("b", "right", '{"y":2}'),
    ]
    assert events[-1].tool_calls == calls


async def test_tool_calls_without_an_index_stay_separate(tmp_path: Path, provider) -> None:
    chunks = [
        frame({"tool_calls": [{"id": "a", "function": {"name": "left", "arguments": '{"x"'}}]}),
        frame({"tool_calls": [{"function": {"arguments": ":1}"}}]}),
        frame({"tool_calls": [{"id": "b", "function": {"name": "right", "arguments": '{"y"'}}]}),
        frame({"tool_calls": [{"function": {"arguments": ":2}"}}]}),
        "data: [DONE]\n\n",
    ]

    events = await collect(provider(replay(tmp_path, chunks)).stream(REQ))

    calls = [event for event in events if isinstance(event, ToolCall)]
    assert [(call.id, call.name, call.args) for call in calls] == [
        ("a", "left", '{"x":1}'),
        ("b", "right", '{"y":2}'),
    ]


async def test_missing_tool_call_id_is_synthesized(tmp_path: Path, provider) -> None:
    chunks = [
        frame({"tool_calls": [{"index": 0, "function": {"name": "left", "arguments": "{}"}}]}),
        frame({"tool_calls": [{"index": 1, "function": {"name": "right", "arguments": "{}"}}]}),
        "data: [DONE]\n\n",
    ]

    events = await collect(provider(replay(tmp_path, chunks)).stream(REQ))

    assert [call.id for call in events[-1].tool_calls] == ["call_0", "call_1"]


async def test_done_sentinel_ends_the_stream(tmp_path: Path, provider) -> None:
    chunks = [frame({"content": "kept"}), "data: [DONE]\n\n", frame({"content": "dropped"})]

    events = await collect(provider(replay(tmp_path, chunks)).stream(REQ))

    assert events[-1].text == "kept"


async def test_comment_and_blank_lines_are_ignored(tmp_path: Path, provider) -> None:
    chunks = [": openrouter processing\n\n", "\n", frame({"content": "ok"}), "data: [DONE]\n\n"]

    events = await collect(provider(replay(tmp_path, chunks)).stream(REQ))

    assert events[-1].text == "ok"


async def test_frame_split_across_network_chunks_is_reassembled(tmp_path: Path, provider) -> None:
    whole = frame({"content": "split me"})
    chunks = [whole[:20], whole[20:], "data: [DONE]\n\n"]

    events = await collect(provider(replay(tmp_path, chunks)).stream(REQ))

    assert [event.text for event in events if isinstance(event, TextDelta)] == ["split me"]


async def test_mid_stream_error_frame_raises(tmp_path: Path, provider) -> None:
    chunks = [
        frame({"content": "starting"}),
        'data: {"error":{"message":"upstream timed out","code":502}}\n\n',
        "data: [DONE]\n\n",
    ]

    with pytest.raises(ProviderError, match="upstream timed out"):
        await collect(provider(replay(tmp_path, chunks)).stream(REQ))


async def test_stream_without_data_frames_raises(tmp_path: Path, provider) -> None:
    with pytest.raises(ProviderError, match="no SSE data frames"):
        await collect(provider(replay(tmp_path, ["data: [DONE]\n\n"])).stream(REQ))


async def test_non_streaming_body_raises(tmp_path: Path, provider) -> None:
    chunks = ['{"id":"x","choices":[{"message":{"content":"hi"}}]}']

    with pytest.raises(ProviderError, match="no SSE data frames"):
        await collect(provider(replay(tmp_path, chunks)).stream(REQ))


async def test_non_2xx_raises_provider_error(tmp_path: Path, provider) -> None:
    transport = replay(tmp_path, ['{"error":{"message":"no credits"}}'], status=402)

    with pytest.raises(ProviderError, match="402"):
        await collect(provider(transport).stream(REQ))


async def test_malformed_sse_payload_raises_provider_error(tmp_path: Path, provider) -> None:
    transport = replay(tmp_path, ["data: {not json\n\n"])

    with pytest.raises(ProviderError, match="malformed SSE"):
        await collect(provider(transport).stream(REQ))


async def test_request_payload_mapping(provider) -> None:
    seen: list[dict] = []
    req = SampleRequest(
        model="anthropic/claude-opus-4",
        system=[SystemBlock(text="you are terse", cache=True), SystemBlock(text="answer in bullets")],
        messages=[
            UserMessage(content="rate?"),
            AssistantMessage(text="checking", tool_calls=[ToolCall(id="c1", name="search", args='{"q":"x"}')]),
            ToolResultMessage(call_id="c1", content="3 hits"),
        ],
        tools=[ToolSchema(name="search", description="find metrics", parameters={"type": "object"})],
        params={"temperature": 0.2, "max_tokens": 512},
    )

    await collect(provider(capturing(seen)).stream(req))

    payload = seen[0]
    assert payload["model"] == "anthropic/claude-opus-4"
    assert payload["stream"] is True
    assert payload["stream_options"] == {"include_usage": True}
    assert payload["temperature"] == 0.2
    assert payload["max_tokens"] == 512
    assert payload["messages"] == [
        {"role": "system", "content": "you are terse\n\nanswer in bullets"},
        {"role": "user", "content": "rate?"},
        {
            "role": "assistant",
            "content": "checking",
            "tool_calls": [{"id": "c1", "type": "function", "function": {"name": "search", "arguments": '{"q":"x"}'}}],
        },
        {"role": "tool", "tool_call_id": "c1", "content": "3 hits"},
    ]
    assert payload["tools"] == [
        {
            "type": "function",
            "function": {"name": "search", "description": "find metrics", "parameters": {"type": "object"}},
        }
    ]


async def test_textless_assistant_message_sends_empty_content(provider) -> None:
    seen: list[dict] = []
    req = SampleRequest(
        model="m",
        messages=[UserMessage(content="hi"), AssistantMessage(), UserMessage(content="still there?")],
    )

    await collect(provider(capturing(seen)).stream(req))

    assert seen[0]["messages"][1] == {"role": "assistant", "content": ""}


async def test_params_cannot_override_reserved_keys(provider) -> None:
    seen: list[dict] = []
    req = SampleRequest(
        model="real/model",
        messages=[UserMessage(content="hi")],
        tools=[ToolSchema(name="search")],
        params={
            "stream": False,
            "model": "hijacked/model",
            "messages": [],
            "stream_options": {},
            "tools": [],
            "temperature": 0.7,
        },
    )

    await collect(provider(capturing(seen)).stream(req))

    payload = seen[0]
    assert payload["stream"] is True
    assert payload["model"] == "real/model"
    assert payload["stream_options"] == {"include_usage": True}
    assert [message["role"] for message in payload["messages"]] == ["user"]
    assert [tool["function"]["name"] for tool in payload["tools"]] == ["search"]
    assert payload["temperature"] == 0.7


async def test_no_system_blocks_means_no_system_message(provider) -> None:
    seen: list[dict] = []

    await collect(provider(capturing(seen)).stream(REQ))

    assert [message["role"] for message in seen[0]["messages"]] == ["user"]


async def test_limits_configured_and_fallback(provider) -> None:
    configured = ModelLimits(context_window=1_000_000, max_output=65_536)
    api = provider(
        httpx.MockTransport(lambda request: httpx.Response(200, content=OK_STREAM)),
        limits={"google/gemini-3-pro": configured},
    )

    assert api.limits("google/gemini-3-pro") == configured
    assert api.limits("who/knows") == FALLBACK_LIMITS
    assert FALLBACK_LIMITS.context_window == 128_000
    assert FALLBACK_LIMITS.max_output == 4_096


async def test_embedder_returns_vectors_in_input_order(embedder) -> None:
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={"data": [{"index": 1, "embedding": [0.3, 0.4]}, {"index": 0, "embedding": [0.1, 0.2]}]},
        )

    api = embedder(httpx.MockTransport(handler))

    assert await api.embed(["a", "b"]) == [[0.1, 0.2], [0.3, 0.4]]
    assert seen[0] == {"model": "text-embedding-3-small", "input": ["a", "b"]}


async def test_embedder_raises_on_error_status(embedder) -> None:
    api = embedder(httpx.MockTransport(lambda request: httpx.Response(429, json={"error": "slow down"})))

    with pytest.raises(ProviderError, match="429"):
        await api.embed(["a"])
