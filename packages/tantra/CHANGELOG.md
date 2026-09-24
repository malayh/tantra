# Changelog

## 1.1.0

Breaking:

- Human asks and approval-gated tools are root-only. Child `ctx.ask()` calls now return an ordered tool error, static child `ask` permissions fail Runtime construction, and dynamic child escalations fail without emitting `AskRaised`.
- Plain child turn completion no longer implies result delivery. It leaves the actor reusable and queues a status-only lifecycle input for the direct parent; children must call `finish(result)` to close and deliver a final result.

Added:

- Durable `TurnSummary` and `ActorStatus` snapshots, including queued and active turn state, plus `Runtime.status()`, breadth-first `Runtime.tree_status()`, and the direct-child `status` framework tool.
- Deterministic parent notification and activation-time reconciliation for completed, failed, cancelled, and interrupted child turns that do not call `finish()`.
- Optional durable child display names on `spawn`, session headers, actor events, status responses, and Sarathi UI surfaces. Actor type and UUID semantics are unchanged.
- Lazy, cached `OpenAICompatible` model-limit discovery with explicit configuration precedence, OpenRouter-style metadata support, and per-field fallback to a 128k context window and 4,096-token output limit. `Provider.limits()` may now be synchronous or awaitable.
- Complete-request compaction budgeting across prompts, execution-environment blocks, history, tools, and parameters; bounded recent history; chained durable summaries; one-shot recovery from confirmed pre-output context overflow; and explicit irreducible-payload failures.
- A standardized execution-environment system block for capability guidance, keeping skill bodies on demand.
- Sarathi actor-status polling and lazy child-journal subscriptions, including drawer replay, retained cursors, inactive child placement, replay hydration, and stable manual scrolling.
- Sarathi general-purpose named `subagent` actors and a packaged `research` skill with `shallow`, `normal`, and `deep` effort levels.

Changed:

- `PruneThenSummarize` remains opt-in and now defaults to an 80% trigger, a 4,096-token buffer, a 20,000-token recent window, and a 4,096-token summary-output cap. It preserves application and framework system blocks and tool schemas byte-for-byte.
- Parent-child `send` accepts only a direct edge and activates the target with the root tree identity.
- Sarathi works inline by default, delegates only when requested or independently useful, keeps approval-gated memory writes on the root, and exposes only non-interactive capabilities to subagents.

Compatibility:

- Existing 1.0 headers, events, and stores load without migration or historical status and name backfill. Legacy Sarathi `researcher` history remains readable but cannot resume or accept new work.


## 1.0.0

Breaking:

- Replaced the legacy coordinator and merged turn stream with the process-scoped `Runtime`, explicit actor subscriptions, and independent root and child journals.
- Store appends now assign sequences atomically. A root tree has one live Runtime process and the newest local writable connection owns mutations.
- Removed legacy integration helpers and child and cancellation events. `ChildCreated`, `AgentFinished`, and `CancellationRequested` are the durable actor events.
- `Hook.on_event` now receives `LoggedEvent`, including the actor id and durable sequence.

Added:

- Durable command deduplication for input, ask responses, and cancellation.
- Atomic model updates through `Store.patch_header(model=...)`.
- Runtime writer takeover, descendant asks, recursive cancellation, and fresh-process interruption.

Changed:

- Streaming deltas are persisted and replayed per actor.
- Package and documentation now define the 1.0 single-process execution contract.

## 0.4.0

Added:

- The legacy coordinator accepted a `Tracer` and emitted one trace per execution segment: an `invoke_agent` root, a `chat` span per model call (retries included), an `execute_tool` span per tool call, a `compact` span per compactor consultation, and nested `invoke_agent` spans for subagents under the tool that spawned them. Attributes are OpenTelemetry GenAI semantic conventions only — no vendor keys — so any OTLP backend ingests them.
- The `[telemetry]` extra (`opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http`) and `tantra.telemetry.Telemetry`, the OpenTelemetry implementation. `Telemetry(tracer_provider=None, *, capture_content=False, max_content_chars=32_768)`; content capture is off by default and content attributes are truncated with a `…[truncated N chars]` marker. On this path tantra configures no exporter, sets no global tracer provider and reads no `OTEL_*` environment variable. Not re-exported from `tantra` — import it from `tantra.telemetry`.
- `Telemetry.from_env(*, capture_content=False, max_content_chars=32_768)`, the one-line setup: it builds a `TracerProvider` + `BatchSpanProcessor` + OTLP HTTP exporter entirely from the standard OpenTelemetry environment, installs it as the global tracer provider, and returns `None` when neither `OTEL_EXPORTER_OTLP_ENDPOINT` nor `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. `Telemetry.shutdown()` flushes and closes the provider the instance holds.
- `Tracer` and `NullTracer` in `tantra.tracing`, exported from `tantra`. The core seam takes no new dependency; the base install is unchanged.
- `TurnContext.tracer`, so a custom `Compactor` can trace its own model call.
- An optional `provider_name` class attribute on providers, read via `getattr` and reported as `gen_ai.provider.name` — `openai` on `OpenAICompatible`, `fake` on `FakeProvider`, `unknown` for a provider that does not set it.

Changed:

- `PruneThenSummarize`'s summarizer call is traced as a `chat` child of the `compact` span, and its usage is now captured rather than discarded.

## 0.3.0

Added:

- `web_fetch(proxy=...)` takes one proxy URL (`http`, `https`, `socks4`, `socks4a`, `socks5`, `socks5h`; credentials go in the URL) and applies it to every redirect hop and every retry. An invalid value raises `ValueError` at construction, naming the scheme it received and never the URL. Proxy failures retry inside the existing 3-attempt budget and then raise a proxy-specific message; there is no fallback to a direct connection.

Changed:

- The `[web]` extra now pulls `tenacity>=9`.
- `web_fetch`'s retry loop is restructured on tenacity, with unchanged behaviour.

## 0.2.0

Breaking:

- The `Store` protocol gains `memory_put` / `memory_get` / `memory_all` / `memory_search` and `patch_header`. A store written against 0.1.0 must add them.
- `ToolCallStarted` now precedes every `ToolCallCompleted`, including the error and denial paths. Logs written before 0.2.0 replay without that pairing.
- `Memory.delete(mid, *, scope=None)` returns `bool` instead of raising; `Memory.supersede(old_id, new, *, scope=None)` takes a scope.

Fixed:

- Cancellation no longer livelocks a sequence race while a session is busy.
- A cancel absorbed on a turn's final sample or on the submit-output path now ends the turn cancelled instead of being dropped.
- Recovery re-emits a pending ask without mistaking it for a new log entry.
- Memory metadata matching fails closed: a key the row lacks never matches, and `None` matches only a stored `None`.
- `delete` is idempotent and scope-checked — an unknown id and a row outside the scope both return `False`.
- The provider reads `reasoning_content` as well as `reasoning`, so reasoning from either dialect streams.

Added:

- Recursive cancellation flags every descendant session deepest-first.
- `Store.patch_header(sid, *, title=..., status=..., pending_ask=..., usage=..., metadata=...)` for lost-update-free header edits; `metadata` merges shallowly. The turn loop no longer rewrites the whole header mid-turn.
- `memory_all(metadata=..., include_dead=False)`, with deleted and superseded rows excluded by default.
- `memory_tools(scope=...)` builds tenant-scoped `memory_write` / `memory_recall` tools.

## 0.1.0

Initial release.
