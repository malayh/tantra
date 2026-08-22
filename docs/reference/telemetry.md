# Telemetry

```python
from tantra import NullTracer, Tracer
from tantra.telemetry import Telemetry
```

`Tracer` is the core seam — a protocol, no dependencies. `Telemetry` is the OpenTelemetry implementation behind the `[telemetry]` extra, and is **not** re-exported from `tantra`: importing it is what raises `ImportError` naming `pip install "tantra-harness[telemetry]"` when the extra is missing.

Pass one to the harness: `Harness(..., telemetry=Telemetry.from_env())`. `None` — the default — installs `NullTracer` and nothing is recorded.

## `Telemetry.from_env(...)`

```python
@classmethod
def from_env(cls, *, capture_content: bool = False, max_content_chars: int = 32_768) -> Telemetry | None
```

The one-liner. Builds and installs a complete OTLP pipeline from the standard OpenTelemetry environment:

1. **Returns `None` when neither `OTEL_EXPORTER_OTLP_ENDPOINT` nor `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set** — the only two variables tantra itself reads, and only to decide whether to build anything. `Harness(telemetry=None)` is the untraced default, so the same expression serves a traced and an untraced deployment.
2. `TracerProvider(resource=Resource.create())` — the SDK reads `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`.
3. `BatchSpanProcessor(OTLPSpanExporter())` — the SDK reads `OTEL_EXPORTER_OTLP_ENDPOINT` (appending `/v1/traces`), `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT` and the rest of the OTLP set, including every `_TRACES_` override.
4. `trace.set_tracer_provider(provider)` — **this is the one place tantra writes global OTel state.** It is opt-in.

!!! warning "`from_env()` is for apps that do not already configure OpenTelemetry"
    OTel ignores a second `set_tracer_provider` call, so an app that installed its own provider first keeps it as the global one — but tantra's spans still go to the provider `from_env` built, because that one is passed to `Telemetry` explicitly. The result is two live providers: your instrumentation exporting one way, the turn loop exporting another. If your process already has a provider, skip `from_env()` and pass yours in with `Telemetry(tracer_provider=...)`.

`capture_content` and `max_content_chars` are passed through to the constructor. The SDK reads only `os.environ`, so configuration held anywhere else — a `.env` file, a secrets manager — must be bridged in with `os.environ.setdefault` before the call.

## `Telemetry(...)`

```python
Telemetry(
    tracer_provider: TracerProvider | None = None,
    *,
    capture_content: bool = False,
    max_content_chars: int = 32_768,
)
```

| Parameter | Meaning |
|---|---|
| `tracer_provider` | The SDK provider to emit into. `None` resolves `opentelemetry.trace.get_tracer_provider()` **lazily on the first span**, so an app may configure the SDK after constructing the harness. |
| `capture_content` | Record prompts, completions, tool arguments and tool results. Off by default, per semconv. |
| `max_content_chars` | Per-attribute cap. Longer content is cut with `…[truncated N chars]`. |

The constructor is the path for an app that owns its own SDK setup. On it, tantra installs no exporter, writes no global state and reads no environment variable — see the [guide](../guides/telemetry.md#bringing-your-own-sdk-setup).

The instrumentation scope is `tantra`, versioned with the installed `tantra-harness`.

## `Telemetry.shutdown()`

Shuts down whatever provider the instance holds, flushing whatever the `BatchSpanProcessor` is still buffering. A no-op when the instance was built with no provider (`Telemetry()`), so it is always safe to call at teardown.

A process that exits without it drops the tail of every in-flight trace.

It is the natural pairing for `from_env()`, which built the provider and has no other owner. **It does not check who else is using that provider** — on a `Telemetry(tracer_provider=...)` you supplied, this shuts down *your* provider, taking every other instrumentation exporting through it with it. An app sharing one provider should leave this alone and call the provider's own `shutdown()` once, at its own teardown.

## Span shape

One trace per `run()` / `resume()` segment. `invoke_agent` is the root; `chat`, `execute_tool` and `compact` are its children; a subagent's `invoke_agent` nests under the `execute_tool` span that spawned it.

| Span | Name | Kind |
|---|---|---|
| turn | `invoke_agent {agent}` | `INTERNAL` |
| model call | `chat {model}` | `CLIENT` |
| tool call | `execute_tool {name}` | `INTERNAL` |
| compaction | `compact` | `INTERNAL` |

## Attributes

Every span carries `gen_ai.conversation.id` (the session id) and `tantra.turn_id`.

### `invoke_agent`

| Attribute | Notes |
|---|---|
| `gen_ai.operation.name` | `invoke_agent` |
| `gen_ai.agent.name` | Registered agent name. |
| `tantra.depth` | Session depth; root is 0. |
| `tantra.turn.resumed` | `true` on a segment picked up by `resume()`. |
| `tantra.ask_id` | Present when the segment resumed an ask, and when it suspended on one. |
| `tantra.turn.outcome` | `completed` · `max_steps` · `cancelled` · `output` · `failed` · `suspended` · `aborted` |
| `tantra.stop_reason` | The `TurnCompleted` stop reason. |
| `gen_ai.usage.*` | Turn totals, summed from the child `chat` spans. |
| `tantra.metadata.<key>` | One per scalar value in the session header's metadata. |
| `error.type` | With status `ERROR`, on `failed` and on an `aborted` turn carrying a real exception. |
| `gen_ai.input.messages` | *Content.* The turn's input as one user message. |
| `gen_ai.output.messages` | *Content.* The final assistant text, or the submitted output; `finish_reason` is the stop reason. |

### `chat`

| Attribute | Notes |
|---|---|
| `gen_ai.operation.name` | `chat` |
| `gen_ai.provider.name` | `provider.provider_name` — `openai` for `OpenAICompatible`, `fake` for `FakeProvider`, else `unknown`. |
| `gen_ai.request.model` | Model for this call. Read from the request, never from `harness.default_model`. |
| `server.address` / `server.port` | Parsed from the provider's `base_url` when it has one. |
| `tantra.sample_id` | Absent on a compactor's own call. |
| `tantra.sample.attempts` | Every try, so a retried call reports more than one. |
| `gen_ai.response.finish_reasons` | One-element list. |
| `gen_ai.usage.*` | `input_tokens`, `output_tokens`, `cache_read.input_tokens`, `cache_creation.input_tokens`. |
| `gen_ai.conversation.compacted` | `true` only on the first call after a compaction landed. |
| `error.type` | With status `ERROR`; a `ProviderError` also sets `tantra.provider.status_code`. |
| `gen_ai.system_instructions` | *Content.* One text part per system block. |
| `gen_ai.input.messages` | *Content.* The full assembled prompt in semconv parts — `text`, `tool_call`, `tool_call_response`. |
| `gen_ai.output.messages` | *Content.* Assistant text and tool-call parts, with `finish_reason`. |
| `gen_ai.tool.definitions` | *Content.* The tool schemas sent with the request. |

### `execute_tool`

| Attribute | Notes |
|---|---|
| `gen_ai.operation.name` | `execute_tool` |
| `gen_ai.tool.name` / `gen_ai.tool.call.id` | |
| `gen_ai.tool.type` | Always `function`. |
| `gen_ai.tool.description` | Absent when the model named an unknown tool. |
| `gen_ai.agent.name` | The calling agent. |
| `tantra.tool.outcome` | `completed` · `error` · `suspended` · `aborted` |
| `tantra.tool.replayed` | `true` on a call re-executed after a suspend. |
| `tantra.ask_id` | On a suspended call. |
| `error.type` | With status `ERROR`. The exception's qualname, or `_OTHER` for a denial, a cap or another synthesized error result. |
| `gen_ai.tool.call.arguments` | *Content.* The **effective** arguments — what a `before_tool` hook produced, if it rewrote them. |
| `tantra.tool.original_arguments` | *Content.* Only when a hook rewrote them. |
| `gen_ai.tool.call.result` | *Content.* What the model saw, as raw text — **not** JSON. Set on error results too, and absent on a swept (aborted or suspended) span. |

### `compact`

| Attribute | Notes |
|---|---|
| `gen_ai.operation.name` | `compact` |
| `tantra.compaction.applied` | `false` when the compactor was consulted and did nothing — which is most of the time. |
| `tantra.compaction.strategy` / `.tokens_before` / `.tokens_after` / `.floor_turn_id` | From `CompactionApplied`. |
| `gen_ai.usage.*` | The summarizer's call. Deliberately **excluded** from the turn totals. |
| `error.type` | With status `ERROR`, on a failed compaction. |
| `tantra.compaction.summary` | *Content.* The brief, as raw text. |

Rows marked *Content* appear only with `capture_content=True`. They are JSON strings — except `gen_ai.tool.call.result` and `tantra.compaction.summary`, which are the raw text — truncated at `max_content_chars`.

!!! note "`input_tokens` from `OpenAICompatible` excludes cached tokens"
    The provider subtracts cached tokens from `input_tokens` before reporting. The semconv says `cache_read.input_tokens` *should* be included in `input_tokens`; tantra records what the provider reported rather than silently adjusting it. Add the two if your dashboard needs the semconv reading.

## `Tracer` (protocol)

```python
from tantra import Tracer
```

The core seam, in `tantra.tracing` — importable with no extra installed. Implement it to route spans somewhere that is not OpenTelemetry.

```python
class Tracer(Protocol):
    def start_turn(self, turn, *, resumed, ask_id, parent) -> Any: ...
    def end_turn(self, span, *, outcome, stop_reason, output, final_text, error, ask_id) -> None: ...
    def start_sample(self, parent, req, *, sample_id, provider, compacted) -> Any: ...
    def end_sample(self, span, *, end, error, attempts) -> None: ...
    def start_tool(self, parent, call, *, args, tool, replayed) -> Any: ...
    def end_tool(self, span, *, result, is_error, outcome, error_type, ask_id) -> None: ...
    def start_compaction(self, parent) -> Any: ...
    def end_compaction(self, span, *, applied, error) -> None: ...
```

Three rules the loop relies on:

- **Handles are opaque.** Every `start_*` returns whatever you like; the loop hands it straight back to the matching `end_*` and never looks inside. `None` is a valid handle — that is how `NullTracer` works.
- **Every method must be total.** A tracer call raising would break the turn, so errors belong inside the implementation. `Telemetry` falls back to `str(value)` on anything it cannot serialize.
- **Every `start_*` is matched by an `end_*` in a `finally`**, including on an abandoned stream, a lost lease and a cancelled turn.

`NullTracer` is the no-op implementation and `tantra.tracing.NULL_TRACER` the shared instance the harness installs when `telemetry=None`.

### `TurnContext.tracer`

The tracer is on [`TurnContext`](context.md), so a custom [`Compactor`](compaction.md) can trace its own model call:

```python
from tantra.tracing import current_span

span = ctx.tracer.start_sample(current_span.get(), req, sample_id=None, provider=ctx.provider, compacted=False)
try:
    ...
finally:
    ctx.tracer.end_sample(span, end=end, error=None, attempts=1)
```

`current_span` is the `ContextVar` holding the enclosing handle. The loop sets it around `compactor.compact` and around `ctx.spawn` / `ctx.fan_out`; read it, never reset it by token.

## See also

- [Telemetry guide](../guides/telemetry.md) · [Harness](harness.md) · [Context](context.md)
- [Sharp edges](../sharp-edges.md) — split traces, replayed tools, untraced custom compactors.
