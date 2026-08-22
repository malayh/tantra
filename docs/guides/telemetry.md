# Telemetry

Opt-in OpenTelemetry tracing for the turn loop. Every turn becomes a trace: the agent, each model call with its prompt and completion, each tool call with its arguments and result, and nested subagent turns under the tool that spawned them.

Needs `pip install "tantra-harness[telemetry]"` — `opentelemetry-sdk` and the OTLP HTTP exporter. Without the extra `import tantra` is unchanged and `Harness()` records nothing; `Telemetry` is deliberately **not** re-exported from `tantra`, so importing it is what fails loudly when the extra is missing.

The attributes are plain [OpenTelemetry GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/) — no vendor keys. Anything that speaks OTLP works.

## Turning it on

Export the standard OpenTelemetry variables:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.example.com
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer <token>"
export OTEL_SERVICE_NAME=my-agent
```

and hand the harness one line:

```python
from tantra import Harness
from tantra.telemetry import Telemetry

harness = Harness(provider, store, [Researcher], telemetry=Telemetry.from_env(capture_content=True))
```

`from_env()` builds a `TracerProvider` from `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`, attaches a `BatchSpanProcessor` around an OTLP HTTP exporter configured from `OTEL_EXPORTER_OTLP_ENDPOINT` (it appends `/v1/traces` itself) and `OTEL_EXPORTER_OTLP_HEADERS`, installs it as the global tracer provider, and returns the `Telemetry`. The SDK does all the parsing — tantra reads the endpoint variables only to decide whether to build anything at all.

**With no endpoint set — neither `OTEL_EXPORTER_OTLP_ENDPOINT` nor the signal-specific `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` — it returns `None`**, and `Harness(telemetry=None)` is the untraced default. One expression covers both deployments; there is no flag to thread through your configuration.

Call `telemetry.shutdown()` at teardown. `BatchSpanProcessor` holds spans in memory until it flushes, and a process that exits without shutting down drops the tail of the trace. In FastAPI that is one line in the lifespan.

!!! note "Config that is not in the environment has to be put there"
    An app whose settings come from a `.env` file, a secrets manager or a config server is invisible to the SDK — it only ever reads `os.environ`. Bridge the values across before calling `from_env()`: `os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", settings.endpoint)`. `setdefault`, not assignment, so a real environment variable still wins. That is exactly what [sarathi](https://github.com/malayh/tantra/tree/main/apps/sarathi) does.

## Bringing your own SDK setup

An app that already configures OpenTelemetry — its own sampler, extra span processors, a non-OTLP exporter — should keep doing that and pass the provider in:

```python
harness = Harness(..., telemetry=Telemetry(provider, capture_content=True))
```

`Telemetry()` with no provider at all resolves `trace.get_tracer_provider()` **lazily, on the first span**, so a harness built before the SDK is configured still works, and one built in a process that never configures a provider emits nothing. Tantra never installs an exporter of its own on this path and never touches global OTel state; `from_env()` is the only thing that calls `set_tracer_provider`, and it is opt-in.

### Langfuse

Langfuse's OTLP ingester maps the semconv straight onto its own model — `chat` spans become generations, `execute_tool` becomes tools, `invoke_agent` becomes agents, and `gen_ai.conversation.id` becomes the session. It authenticates with basic auth over the public OTel endpoint, so the same two variables cover it:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=https://cloud.langfuse.com/api/public/otel
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(printf '%s' 'pk-lf-...:sk-lf-...' | base64 | tr -d '\n')"
```

Input and output columns stay empty until you turn content capture on.

### Any other collector

Point `OTEL_EXPORTER_OTLP_ENDPOINT` at your collector and nothing else changes. Grafana Tempo, Jaeger, Honeycomb, Datadog and a plain `otelcol` all ingest these spans; how much of the GenAI convention each one renders is up to them.

## Content capture

Off by default, per the semantic convention — prompts and results are the sensitive part of an agent's trace, and turning them on can mean tens of kilobytes per span.

```python
Telemetry.from_env(capture_content=True, max_content_chars=32_768)
```

`capture_content=True` adds `gen_ai.input.messages`, `gen_ai.output.messages`, `gen_ai.system_instructions`, `gen_ai.tool.definitions`, `gen_ai.tool.call.arguments` and `gen_ai.tool.call.result`, plus the turn's input and final answer on the root span. Metadata, token usage and timings are recorded either way.

Each content attribute is serialized, then cut at `max_content_chars` with a `…[truncated N chars]` suffix. The cut is a plain string slice, so a truncated JSON attribute is no longer parseable JSON — the marker is how you tell. `gen_ai.tool.call.result` is the raw text the model saw, not JSON, so a tool returning a string lands as that string verbatim.

## What each span carries

One trace per `run()` or `resume()` segment.

```
invoke_agent researcher          ← the turn: input, final answer, turn totals, outcome
├── chat gpt-5                   ← one model call, retries included: prompt, completion, usage
├── execute_tool web_fetch       ← arguments, result, error
├── compact                      ← a compactor consultation
│   └── chat gpt-5-mini          ← the summarizer's own call
└── chat gpt-5
```

- **Tool spans are siblings of the `chat` that requested them**, not children of it — that is what the semconv's agent guidance asks for, and it keeps a turn readable as a flat sequence of steps.
- **A subagent's turn nests under the `execute_tool` span that spawned it**, so `ctx.spawn` and `ctx.fan_out` show the delegation in one trace.
- **A retried model call stays one span.** `tantra.sample.attempts` counts the tries.
- **`compact` appears whenever a compactor is configured**, whether or not it compacted anything — `tantra.compaction.applied` says which.

Every span carries `gen_ai.conversation.id` (the session id) and `tantra.turn_id`. Scalar values in the session header's metadata become `tantra.metadata.<key>`, which is how a multi-tenant app gets its user onto the trace.

The full list is in the [telemetry reference](../reference/telemetry.md).

## Suspend and resume

A turn that suspends on `ctx.ask` is **two traces**, not one: the loop stops, the process may die, and the human's thinking time is not compute. The first trace ends with `tantra.turn.outcome="suspended"` and the `tantra.ask_id`; the resumed segment starts a new trace with `tantra.turn.resumed=true` and the same `tantra.ask_id`.

The two share `tantra.turn_id` and `gen_ai.conversation.id`. Group them in a session view, or filter on `tantra.turn_id` for one logical turn. Because a resumed tool is [re-executed from its first line](../sharp-edges.md), one `call_id` can produce several `execute_tool` spans — the later ones carry `tantra.tool.replayed=true`.

## Cost

Under `NullTracer` — the default — instrumentation is a handful of method calls returning `None`. With `Telemetry`, one span per turn, per model call, per tool call and per compaction. A tool-heavy turn is a few dozen spans; use `BatchSpanProcessor`, not `SimpleSpanProcessor`.

## Next

- [Telemetry reference](../reference/telemetry.md) — every attribute, and the `Tracer` protocol.
- [Sharp edges](../sharp-edges.md) — content capture, split traces, replayed tools.
