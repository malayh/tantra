# Providers

```python
from tantra import Embedder, FakeProvider, ModelLimits, OpenAICompatible, Provider, Sample, SampleRequest
from tantra.providers import StreamEnd, TextDelta, ToolCall, ToolSchema  # wire types
```

A provider is one model vendor's transport. The model id itself rides on the request, so one provider serves many models.

## `Provider` (protocol)

```python
def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]
def limits(self, model: str) -> ModelLimits
```

`stream` yields live fragments, then each complete `ToolCall`, then exactly one terminal `StreamEnd`. `ToolCall.args` is the raw JSON string exactly as the vendor sent it — the loop parses it and turns invalid JSON into an error result rather than failing the turn.

`limits` returns the context window and max output for a model; unknown models get a conservative estimate. It is called once per turn and drives [compaction](compaction.md).

!!! warning "A complete `ToolCall` is yielded twice"
    Once standalone, and again inside `StreamEnd.tool_calls`. Consume exactly one of the two or you will double-count calls. The loop reads `StreamEnd`; a UI that renders the standalone `ToolCall` should ignore the list, and vice versa.

## `Embedder` (protocol)

```python
async def embed(self, texts: list[str]) -> list[list[float]]
```

One vector per input text, in input order. Separate from `Provider` — a chat vendor need not serve embeddings. Used by [`BuiltinMemory`](memory.md).

## `ModelLimits`

`context_window: int`, `max_output: int`.

## `SampleRequest`

| Field | Type | Default |
|---|---|---|
| `model` | `str` | required |
| `system` | `list[SystemBlock]` | `[]` |
| `messages` | `list[Message]` | `[]` |
| `tools` | `list[ToolSchema]` | `[]` |
| `params` | `dict[str, Any]` | `{}` |

`params` carries vendor-specific extras; `OpenAICompatible` forwards them as `extra_body`, dropping the keys it owns (`model`, `messages`, `stream`, `stream_options`, `tools`).

## Wire types

Importable from `tantra.providers` (or `tantra.providers.base`) — **not** from top-level `tantra`.

| Type | Kind | Shape |
|---|---|---|
| `SystemBlock` | request | `text: str`, `cache: bool = False` |
| `UserMessage` | request | `role="user"`, `content: str` |
| `AssistantMessage` | request | `role="assistant"`, `text: str \| None`, `reasoning: list[ReasoningBlock]`, `tool_calls: list[ToolCall]` |
| `ToolResultMessage` | request | `role="tool"`, `call_id: str`, `content: str`, `is_error: bool = False` |
| `ReasoningBlock` | both | `text: str`, `signature: str \| None` |
| `ToolCall` | both | `type="tool_call"`, `id: str`, `name: str`, `args: str` (raw JSON) |
| `ToolSchema` | request | `name: str`, `description: str`, `parameters: dict` |
| `TextDelta` | stream | `type="text_delta"`, `text: str` |
| `ReasoningDelta` | stream | `type="reasoning_delta"`, `text: str` |
| `ToolCallDelta` | stream | `type="tool_call_delta"`, `index: int`, `id`, `name`, `args_fragment: str` |
| `StreamEnd` | stream | `type="stream_end"`, `text`, `reasoning`, `tool_calls`, `usage: Usage`, `finish_reason: str \| None` |

`Message = UserMessage | AssistantMessage | ToolResultMessage` and `ProviderEvent = TextDelta | ReasoningDelta | ToolCallDelta | ToolCall | StreamEnd`, both discriminated unions.

`ToolResultMessage.is_error` is carried in tantra's own model but **dropped on the OpenAI-compatible wire** — the message content is the whole error contract the model sees.

## `OpenAICompatible`

```python
OpenAICompatible(
    base_url: str,
    api_key: str,
    *,
    limits: dict[str, ModelLimits] | None = None,
    http_client: httpx.AsyncClient | None = None,
    timeout: float = 120.0,
)
```

Streams over any OpenAI-compatible `/chat/completions` endpoint. `limits` maps model id → `ModelLimits`; anything unmapped falls back to 128k context / 4096 output. `http_client` is the test seam (`httpx.MockTransport`, or a cassette). The SDK's own retries are disabled — tantra's [`RetryConfig`](loop.md#retryconfig) owns retry.

Vendor errors become `ProviderError` carrying `status_code`, with `retryable=True` for connection errors. A stream with no data frames, or a malformed one, is also a `ProviderError`. Call `await provider.aclose()` on shutdown.

```python
provider = OpenAICompatible(
    "https://api.openai.com/v1",
    OPENAI_API_KEY,
    limits={"gpt-5": ModelLimits(context_window=400_000, max_output=128_000)},
)
```

## `OpenAICompatibleEmbedder`

```python
OpenAICompatibleEmbedder(
    base_url: str,
    api_key: str,
    model: str,
    *,
    http_client: httpx.AsyncClient | None = None,
    timeout: float = 120.0,
)
```

`embed(texts)` returns vectors re-sorted into input order. Errors become `ProviderError`. Also has `aclose()`.

## `FakeProvider(samples)` and `Sample`

The offline provider: it replays a scripted list, one `Sample` per model call, and raises `ProviderError` when the script runs out.

| `Sample` field | Default | Meaning |
|---|---|---|
| `text` | `""` | Assistant text, streamed as `TextDelta` fragments. |
| `reasoning` | `""` | Streamed as `ReasoningDelta`, then one `ReasoningBlock`. |
| `tool_calls` | `[]` | `ToolCall` objects, streamed as two `ToolCallDelta` halves each. |
| `usage` | zeros | Reported on `SampleCompleted`. |
| `finish_reason` | `None` | Defaults to `tool_calls` when there are calls, else `stop`. |

`limits()` always reports 1M context / 64k output. `provider.requests` records every `SampleRequest` it received — assert against it to check prompt assembly.

```python
provider = FakeProvider(
    [
        Sample(tool_calls=[ToolCall(id="c1", name="search_metrics", args='{"query": "p99"}')]),
        Sample(text="p99 is fine."),
    ]
)
```

## Cassettes

For testing the real parser against recorded traffic.

- **`cassette_transport(path) -> httpx.MockTransport`** replays a saved cassette through the real SSE parser; interactions are consumed in order and running out raises `ProviderError`. Pass it as `OpenAICompatible(..., http_client=httpx.AsyncClient(transport=cassette_transport(path)))`.
- **`CassetteRecorder(path, transport=None)`** is a dev-time `httpx` transport that **hits the network** and captures raw response bytes to `path` as they stream.
- **`Cassette`** is the file model: a list of `Interaction(method, url, status, content_type, chunks)`.

## See also

- [Loop & events flow](loop.md) — retry and the `Emitted` envelope.
- [Guides: providers](../guides/providers.md).
