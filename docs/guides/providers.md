# Providers

A `Provider` is **transport only** — a base URL and a key. The model rides on the request:

```python
class Provider(Protocol):
    def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]: ...
    def limits(self, model: str) -> ModelLimits: ...
```

`SampleRequest.model` is sourced from `Agent.model`, falling back to `Harness(default_model=)`. One harness therefore runs a pro model for the main loop and a cheap one for a sub-agent without a second provider — a per-provider model could not express that.

## `OpenAICompatible`

```python
from tantra import OpenAICompatible

provider = OpenAICompatible("https://openrouter.ai/api/v1", API_KEY)
```

Covers OpenRouter and any OpenAI-compatible endpoint. It is built on the official `openai` SDK — which owns SSE decoding and tool-call accumulation — with **`max_retries=0`**, because the loop owns retry.

- `limits={"model-id": ModelLimits(context_window=..., max_output=...)}` supplies per-model windows; unknown models fall back to a conservative 128 000 / 4 096, which [compaction](compaction.md) then sizes itself against. Populate it if your model is bigger.
- `http_client=` injects an `httpx.AsyncClient` — the seam for `MockTransport` and cassettes.
- OpenRouter-specific payload extras ride `SampleRequest.params`; `model`, `messages`, `stream`, `stream_options` and `tools` are reserved and cannot be overridden.
- `OpenAICompatibleEmbedder(base_url, api_key, model)` implements the separate `Embedder` protocol — OpenRouter serves no embeddings endpoint, so the chat provider could not implement it.

There is no native Anthropic provider yet. The protocol is shaped for one: `SystemBlock.cache` and `ReasoningBlock.signature` are modelled, and the OpenAI-compatible implementation simply drops them on the wire.

## Retry lives in the loop

`Harness(retry=RetryConfig(max_attempts=3, base_delay=0.5, max_delay=8.0))`. A `ProviderError` is retried when it is flagged retryable, or carries status 429 or ≥ 500; anything else fails the turn immediately with `TurnFailed`. **A partial stream is discarded and never persisted** — a failed sample's log tail is exactly `SampleStarted → TurnFailed`.

## Testing offline

`FakeProvider([Sample(...), ...])` replays scripted samples in order, fragmenting text and tool arguments into deltas the way a real stream does. Exhausting the script raises `ProviderError` — deliberately not retryable, so a scripting mistake surfaces once instead of three times.

For real traffic, record it once at dev time against the live endpoint:

```python
from tantra.providers.fake import CassetteRecorder

recorder = CassetteRecorder("cassettes/turn.json")
provider = OpenAICompatible(ENDPOINT, KEY, http_client=httpx.AsyncClient(transport=recorder))
```

then replay it in tests, with no socket and no key:

```python
from tantra.providers.fake import cassette_transport

transport = cassette_transport("cassettes/turn.json")
provider = OpenAICompatible(ENDPOINT, "unused", http_client=httpx.AsyncClient(transport=transport))
```

The cassette holds raw response bytes, so replay exercises the real SSE parser and the real accumulation path.

## Usage accounting

`Usage` fields are **disjoint**, following Anthropic-native semantics: `input_tokens` excludes `cache_read_tokens`. Total context is `input + cache_read + cache_write + output`. Summing `input_tokens` alone on a caching provider undercounts badly — which is why compaction's estimator adds all four.

## Writing your own

Implement `limits()` and `stream()`. Two things to get right:

!!! warning "Wire types live in `tantra.providers`, not the top level"
    `ToolCall`, `StreamEnd`, `TextDelta`, `ReasoningDelta`, `ToolCallDelta` and the message classes are imported from `tantra.providers.base`. `ModelLimits`, `SampleRequest`, `Provider` and `Embedder` are re-exported from `tantra`.

!!! warning "Emit each complete `ToolCall` exactly once"
    A complete `ToolCall` appears both standalone and inside `StreamEnd.tool_calls`. The loop consumes exactly one of the two — yield both (as the shipped providers do) and the loop reads the `StreamEnd`; a consumer that reads both would duplicate every call.

```python
import asyncio
from collections.abc import AsyncIterator

from tantra import Agent, Harness, MemoryStore, ModelLimits, SampleRequest, collect, tool
from tantra.events import TurnCompleted
from tantra.providers.base import ProviderEvent, StreamEnd, TextDelta, ToolCall


@tool
async def look(q: str) -> str:
    """Look something up."""
    return f"found {q}"


class Bot(Agent):
    tools = [look]


class EchoProvider:
    def __init__(self) -> None:
        self.samples = 0

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=32_000, max_output=2_000)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.samples += 1
        if self.samples == 1:
            call = ToolCall(id="c1", name="look", args='{"q": "tantra"}')
            yield call
            yield StreamEnd(tool_calls=[call], finish_reason="tool_calls")
            return
        for word in ("done", " here"):
            yield TextDelta(text=word)
        yield StreamEnd(text="done here", finish_reason="stop")


async def main() -> None:
    harness = Harness(EchoProvider(), MemoryStore(), [Bot], default_model="echo/1")
    sid = (await harness.create_session(Bot)).id

    events = await collect(harness.run(sid, "go"))
    print([e.event.text for e in events if isinstance(e.event, TextDelta)])
    print([e.event.stop_reason for e in events if isinstance(e.event, TurnCompleted)])


asyncio.run(main())
```

```text
['done', ' here']
['completed']
```

`ToolCall.args` is the raw JSON **string** exactly as the vendor sent it — the loop parses it and turns malformed JSON into an `is_error` result rather than crashing. `stress/driver.py` in the repository has a fuller worked example: a policy-driven `SyntheticProvider` with deterministic usage and a tiny context window for exercising compaction.

## Next

- [Providers reference](../reference/providers.md), [Compaction](compaction.md).
