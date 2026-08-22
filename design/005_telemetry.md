# OpenTelemetry tracing (`tantra-harness[telemetry]`) — Spec

## Goal
- `pip install "tantra-harness[telemetry]"` + `Harness(..., telemetry=Telemetry())` emits OTel GenAI-semconv spans for every turn: `invoke_agent` → `chat` (full prompt, completion, usage) + `execute_tool` (args, result) + nested subagent `invoke_agent` + `compact`.
- Pure semconv, no vendor attributes. Langfuse's OTLP ingester maps `gen_ai.operation.name` chat→generation / execute_tool→tool / invoke_agent→agent, `gen_ai.input|output.messages`→input/output, `gen_ai.tool.call.arguments|result`, `gen_ai.usage.*`, `gen_ai.conversation.id`→session. Any OTLP backend works.
- Core stays dependency-free: a tiny `Tracer` seam in core, the OTel implementation behind the extra.

## Scope
- **In:** core seam + loop instrumentation; `tantra.telemetry.Telemetry` (OTel impl); extra `[telemetry]`; sarathi env-gated wiring; live smoke script; docs; 0.4.0 release.
- **Out:** metrics (`gen_ai.client.token.usage` etc.); configuring exporters/SDK inside tantra; agni wiring; embedder/memory spans; per-retry-attempt spans; span Links between run/resume segments; `user.id`/Langfuse-specific attributes; trace ids carried on events.

## Decisions
- **Dependency placement:** core gets zero new deps. `tantra/tracing.py` (core) defines `Tracer` protocol + `NullTracer`; `tantra/telemetry.py` (extra) implements it with `opentelemetry-sdk`. User overruled my otel-api-in-core recommendation; reason: keep the base install untouched.
- **Switch:** `Harness(telemetry: Tracer | None = None)`. `Telemetry(tracer_provider=None, capture_content=False, max_content_chars=32_768)`. `tracer_provider=None` → resolve `opentelemetry.trace.get_tracer_provider()` lazily on first span, so apps may configure the SDK after constructing the harness. Tantra never installs exporters or touches global OTel state.
- **Content capture off by default** (semconv default). `capture_content=True` turns on `gen_ai.input.messages`, `gen_ai.output.messages`, `gen_ai.system_instructions`, `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`, root input/output. Metadata/usage/timing always on.
- **Trace shape: one trace per `run()`/`resume()` segment, attributes only, no Links.** Root span `invoke_agent {agent}`. Every span carries `gen_ai.conversation.id = session_id` and `tantra.turn_id`. A resumed segment's root adds `tantra.turn.resumed=true` + `tantra.ask_id`. Why: idiomatic, works on every backend, cross-process resume needs no persisted span ids, span durations are real compute time (the human wait is the gap between traces). Langfuse groups by session; filter `tantra.turn_id` for one logical turn.
- **Tool spans are siblings of `chat` under `invoke_agent`** (semconv agent-spans guidance), not children of the chat that requested them. Subagent turns nest under the spawning `execute_tool` span.
- **Instrumentation lives in the loop, not in hooks.** Hooks never see `SampleRequest`/`StreamEnd`, `before_sample` has no after, `after_tool` skips six branches, `before_turn` isn't called on resume. In-loop spans are opened/closed explicitly (`start_*`/`end_*`), never as current-span context managers across a `yield` (async generators don't own their contextvars context → mis-parenting under interleaved consumers like sarathi's ws pump).
- **Root I/O when capture is on:** `gen_ai.input.messages` = `[{"role":"user","parts":[{"type":"text","content": turn.input}]}]`; `gen_ai.output.messages` = final assistant text (or `submit_output` JSON) with `finish_reason = stop_reason`. Full prompts live on `chat` spans.
- **Tool result value** = what the model sees: `_as_content(result)` (`context.py:53-54`). Hook-rewritten args: `gen_ai.tool.call.arguments` = *effective* args; when they differ from the model's, `tantra.tool.original_arguments` = original.
- **Truncation:** every content attribute serialized to a JSON string then cut at `max_content_chars` with suffix `…[truncated N chars]`. Whole-string cut (may break JSON validity); structure-preserving truncation rejected as more code for little gain.
- **Retries:** one `chat` span covers all attempts (semconv); `tantra.sample.attempts` = count.
- **Compaction:** `compact` span (custom `gen_ai.operation.name="compact"`) with the summarizer's provider call as a `chat` child; the following `chat` gets `gen_ai.conversation.compacted=true`.
- **Ask/permission:** no span (nothing is alive while waiting). Suspension is recorded as attributes on the span that was open (`tantra.turn.outcome=suspended`, `tantra.ask_id`; on an open tool span `tantra.tool.outcome=suspended`).
- **Provider name:** `gen_ai.provider.name = getattr(provider, "provider_name", "unknown")`; `OpenAICompatible.provider_name = "openai"`, `FakeProvider.provider_name = "fake"`. `server.address`/`server.port` parsed from `getattr(provider, "base_url", None)` when present.
- **Version:** 0.4.0 (new `Harness` kwarg, new `TurnContext` field, new extra).

## Seam (core) — `packages/tantra/src/tantra/tracing.py`

```python
class Tracer(Protocol):
    def start_turn(self, turn: TurnContext, *, resumed: bool, ask_id: str | None, parent: Any) -> Any: ...
    def end_turn(self, span: Any, *, outcome: str, stop_reason: str | None, output: Any, final_text: str | None,
                 error: BaseException | str | None, ask_id: str | None) -> None: ...
    def start_sample(self, parent: Any, req: SampleRequest, *, sample_id: str | None, provider: Provider,
                     compacted: bool) -> Any: ...
    def end_sample(self, span: Any, *, end: StreamEnd | None, error: BaseException | None, attempts: int) -> None: ...
    def start_tool(self, parent: Any, call: ToolCallRequested, *, args: dict[str, Any], tool: Tool | None,
                   replayed: bool) -> Any: ...
    def end_tool(self, span: Any, *, result: Any, is_error: bool, outcome: str, error_type: str | None,
                 ask_id: str | None) -> None: ...
    def start_compaction(self, parent: Any) -> Any: ...
    def end_compaction(self, span: Any, *, applied: CompactionApplied | None, error: BaseException | None) -> None: ...

class NullTracer: ...            # every method no-op, start_* return None
NULL_TRACER = NullTracer()
current_span: ContextVar[Any] = ContextVar("tantra_current_span", default=None)
```
- Spans are opaque handles; the loop never inspects them. `None` handles are legal everywhere (NullTracer).
- `outcome` values — turn: `completed | max_steps | cancelled | output | failed | suspended | aborted`; tool: `completed | error | suspended | aborted`.
- `TurnContext.tracer: Tracer = NULL_TRACER` (new field, `context.py:38-50`) — set by `TurnLoop.__init__` next to `turn.provider` (`loop.py:227-230`). Compactors reach the tracer through it.
- `current_span` carries the parent handle into (a) child sessions spawned in-process and (b) the compactor's provider call. Set/restore with `set(prev)`, never `reset(token)` (a `finally` may run in a different context during generator finalization → `ValueError`).
- `tracing.py` imports `TurnContext`, `SampleRequest`, etc. under `TYPE_CHECKING` only.
- Exported from `tantra/__init__.py`: `Tracer`, `NullTracer` (alphabetical `__all__`).

## Loop instrumentation points

| Span | Start | End | Notes |
|---|---|---|---|
| turn | `harness.run` after `TurnContext` built, before `TurnStarted` append (`harness.py:319-328`); `harness.resume` before `patch_header(status="running")` (`harness.py:403`) | `finally` next to `_settle` (`harness.py:351-352`, `:428-429`) | `parent=current_span.get()`. `resumed=True` in `resume`. Outcome from `loop.terminal` (new `TurnLoop` attr set in `_failed`/`_terminal`), `loop.suspended`, `loop.lease_lost`, else `aborted` with `sys.exc_info()[1]`. `loop is None` → `aborted`. `final_text` via `_final_text(loop.history tail)` (`harness.py:138-147`). |
| chat | inside `_sample` (`loop.py:416`) before the attempt loop | `finally` in `_sample` (after `StreamEnd` obtained or on raise/GeneratorExit) | `_sample(req, *, sample_id, compacted)` — two new kwargs from `_drive` (`loop.py:773`, `compacted=bool(compacted)`). `attempts` counted in the loop. `parent=self.turn_span`. |
| execute_tool | `_batch` immediately before `_execute` (`loop.py:680`), `args=effective.args`, `replayed = call.call_id in self.state.started`; or inside `_completed` (`loop.py:271`) when no open span for `call_id` (zero-duration span for denied/unknown/capped/synthesized/submit_output results) | `_completed` (`loop.py:271`) pops `self.tool_spans[call_id]` and ends it; `TurnLoop.run` (`loop.py:812`) `finally` ends any still-open tool spans with `suspended` (if `self.suspended`) else `aborted` | `error_type` = `type(exc).__qualname__` from `_execute` (`loop.py:513-514`, pass through a new `_completed(..., error_type=)` kwarg); other error results → `_OTHER`. `tool=self.tools.get(call.name)` for `gen_ai.tool.description`. Original args = `call.args` when `effective.args != call.args`. |
| compact | `_drive` before `await self.compactor.compact(self.turn)` (`loop.py:750`) | right after (success/`ProviderError`) | `current_span` set to the compact span around the `await` (no `yield` in between → no leak). `PruneThenSummarize._brief` (`compaction.py:193-204`) wraps its `ctx.provider.stream` in `ctx.tracer.start_sample(current_span.get(), req, sample_id=None, provider=ctx.provider, compacted=False)` / `end_sample`, and captures `StreamEnd` (usage no longer discarded). |
| child turn | child `harness.run/resume` via `_ChildRunner.drive` (`harness.py:516-526`) | as turn | `_spawn` (`loop.py:314`) sets `current_span` to the tool span (`self.tool_spans[call_id]`) before `self.spawner.drive(...)`, restores in `finally`. `_merge` (`loop.py:338`) sets it before `workers = [asyncio.ensure_future(child(*entry)) ...]` (`loop.py:367`) and restores immediately after — tasks copy the context at creation. |

- `TurnLoop.__init__` gains `tracer: Tracer = NULL_TRACER`, `turn_span: Any = None`; `Harness._build_loop` (`harness.py:252-283`) forwards `tracer=self.tracer, turn_span=span`.
- Permission-ask suspension before `_execute` (`loop.py:659-676`) opens no tool span; only the turn span records `suspended` + `ask_id`. On resume the tool executes and gets its span then.
- Tracer calls must never raise into the loop. `Telemetry` methods are written to be total (no exceptions on bad input; serialization uses `default=str`); OTel SDK export errors are logged by the SDK, never raised.

## OTel implementation — `packages/tantra/src/tantra/telemetry.py`

- Top-of-module guard (hard, like `extratools/doc.py:9-13`): `except ImportError: raise ImportError("opentelemetry not installed: install tantra-harness[telemetry]")`. Not imported from `tantra/__init__.py`.
- `class Telemetry:` implements `Tracer`. Handle = `_Handle(span, ctx, usage: Usage)`; `ctx = trace.set_span_in_context(span)`. Children: `tracer.start_span(name, context=parent.ctx, kind=..., attributes=...)`; roots: `context=None` (ambient OTel context → child of an app's HTTP span if one is active, else a new trace). Never `use_span`/`attach`.
- Tracer: `(provider or trace.get_tracer_provider()).get_tracer("tantra", __version__)`, resolved on first use.
- End: set attributes, `span.set_status(ERROR, msg)` + `span.record_exception(exc)` when applicable, `span.end()`.
- Turn usage = sum of child `end_sample` usages (accumulate on the parent handle); set on the turn span at end.

### Attributes

| Span (name · kind) | Always | With `capture_content` |
|---|---|---|
| `invoke_agent {agent}` · INTERNAL | `gen_ai.operation.name=invoke_agent`, `gen_ai.agent.name`, `gen_ai.conversation.id=session_id`, `tantra.turn_id`, `tantra.depth`, `tantra.turn.resumed` (bool), `tantra.ask_id` (when resumed or suspended), `tantra.turn.outcome`, `tantra.stop_reason`, `gen_ai.usage.input_tokens|output_tokens|cache_read.input_tokens|cache_creation.input_tokens` (turn totals), `tantra.metadata.<key>` for scalar `turn.metadata` values, `error.type` + status ERROR on `failed` and on `aborted` with a non-`GeneratorExit` exception | `gen_ai.input.messages` (user input), `gen_ai.output.messages` (final text / output, `finish_reason=stop_reason`) |
| `chat {model}` · CLIENT | `gen_ai.operation.name=chat`, `gen_ai.provider.name`, `gen_ai.request.model`, `server.address`/`server.port` (if known), `gen_ai.conversation.id`, `tantra.turn_id`, `tantra.sample_id`, `tantra.sample.attempts`, `gen_ai.response.finish_reasons=[finish_reason]`, `gen_ai.usage.input_tokens|output_tokens|cache_read.input_tokens|cache_creation.input_tokens`, `gen_ai.conversation.compacted=true` (only when true), `error.type` + ERROR on failure (`ProviderError` → `tantra.provider.status_code` when set) | `gen_ai.system_instructions` (one text part per `req.system` block), `gen_ai.input.messages` (semconv roles/parts: user→text; assistant→text + `tool_call{id,name,arguments}`; tool result→`tool_call_response{id,response}`), `gen_ai.output.messages` (assistant text + tool_call parts, `finish_reason`), `gen_ai.tool.definitions` (`req.tools` schemas) |
| `execute_tool {name}` · INTERNAL | `gen_ai.operation.name=execute_tool`, `gen_ai.tool.name`, `gen_ai.tool.call.id`, `gen_ai.tool.type=function`, `gen_ai.tool.description` (if tool known), `gen_ai.agent.name`, `gen_ai.conversation.id`, `tantra.turn_id`, `tantra.tool.outcome`, `tantra.tool.replayed` (bool), `tantra.ask_id` (suspended), `error.type` + ERROR when `is_error` | `gen_ai.tool.call.arguments` (effective), `tantra.tool.original_arguments` (only when rewritten), `gen_ai.tool.call.result` (`_as_content(result)`, also on error) |
| `compact` · INTERNAL | `gen_ai.operation.name=compact`, `gen_ai.conversation.id`, `tantra.turn_id`, `tantra.compaction.applied` (bool), and from `CompactionApplied`: `tantra.compaction.strategy`, `.tokens_before`, `.tokens_after`, `.floor_turn_id`; `error.type` + ERROR on `ProviderError` | `tantra.compaction.summary` |
| child `invoke_agent {child}` | as turn; parent = spawning `execute_tool` span | as turn |

- Content attributes are JSON strings (`json.dumps(..., default=str)`) truncated at `max_content_chars`. Reasoning text is not included in messages (Open Decisions).
- Token mapping: `Usage.input_tokens`→`gen_ai.usage.input_tokens`, `output_tokens`→`gen_ai.usage.output_tokens`, `cache_read_tokens`→`gen_ai.usage.cache_read.input_tokens`, `cache_write_tokens`→`gen_ai.usage.cache_creation.input_tokens`. Note `OpenAICompatible._usage_payload` (`openai_compat.py:47-54`) already subtracts cached tokens from `input_tokens`, which deviates from semconv ("cache_read SHOULD be included in input_tokens") — record as-is, document in sharp edges.

## Sarathi wiring
- `config.py`: `OTEL_EXPORTER_OTLP_ENDPOINT: str = ""`, `OTEL_EXPORTER_OTLP_HEADERS: str = ""`, `OTEL_SERVICE_NAME: str = "sarathi"`, `TELEMETRY_CAPTURE_CONTENT: bool = False`. Read through `Settings` (not `os.environ`) because `.env` values never reach the OTel SDK's own env parsing.
- New `sarathi/telemetry.py`: `@cache get_telemetry() -> Telemetry | None` — `None` when endpoint empty; else `TracerProvider(resource=Resource.create({"service.name": OTEL_SERVICE_NAME}))` + `BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces", headers=parse("k=v,k2=v2" via partition("="))))`, `trace.set_tracer_provider(provider)`, return `Telemetry(tracer_provider=provider, capture_content=settings.TELEMETRY_CAPTURE_CONTENT)`. `shutdown_telemetry()` → `provider.shutdown()`.
- `main.py` lifespan: call `get_telemetry()` at startup, `shutdown_telemetry()` at teardown (flushes the batch processor).
- `agent.py make_harness`: `telemetry=get_telemetry()`.
- `apps/sarathi/backend/pyproject.toml`: `tantra-harness[postgres,web,doc,telemetry]>=0.4`. `.env.example`: four commented-out optional vars with a Langfuse example (`OTEL_EXPORTER_OTLP_ENDPOINT=https://cloud.langfuse.com/api/public/otel`, `OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic <base64 pk:sk>`).

## Sharp edges (document in `docs/sharp-edges.md`)
- Content capture is off by default; Langfuse shows empty input/output until `capture_content=True`.
- A suspended turn is 2+ traces; stitch via session view or `tantra.turn_id`.
- Tools replay on resume → one `call_id` can have several `execute_tool` spans (`tantra.tool.replayed=true` on the later ones).
- `current_span` stays set in the consumer task's context between child events during `ctx.spawn` and `ctx.fan_out`; only `start_turn` reads it, so starting an unrelated `harness.run` in the same task while iterating a spawning turn would mis-parent that turn.
- Custom `Compactor`s are untraced unless they use `ctx.tracer.start_sample/end_sample`; called with a non-handle parent (outside the loop's `current_span` window) the chat span lacks `gen_ai.conversation.id`/`tantra.turn_id`.
- `gen_ai.usage.input_tokens` from `OpenAICompatible` excludes cached tokens (see token mapping note).
- `harness.default_model` may be mutated at runtime (sarathi does); spans read the model from the loop/request, never the harness.

## Considered & rejected
- **`opentelemetry-api` as a core dependency** — idiomatic, zero-cost no-op without SDK; rejected by user to keep the base install dependency-free.
- **Hooks-only telemetry** — misses prompts/completions, retries, compactor LLM call, six tool-error branches, resume; competes with user gates in the ordered hooks list.
- **`trace_id = turn_id` (one trace per logical turn)** — needs a phantom remote parent; backend tolerance unverified; trace durations absorb human wait. Parked (Open Decisions) as a possible later opt-in; the span model doesn't change.
- **Persisting span ids on `TurnStarted` for resume Links** — extra durable state; Langfuse ignores Links; `tantra.turn_id` suffices.
- **Per-attempt retry child spans** — noise on every LLM call for the rare retry; `tantra.sample.attempts` covers it.
- **Tantra configuring exporters from `OTEL_*` env** — tantra would own global OTel state and conflict with apps that already configure it; sarathi does it at app level instead.
- **Structure-preserving truncation** — more code; whole-string cut is fine for 32 KiB.
- **Events (`gen_ai.client.inference.operation.details`) for content** — Langfuse only recently added support; span attributes are universally read.

## Implementation phases

```
P1 seam + loop instrumentation ── P2 OTel Telemetry + extra ── P3 sarathi + smoke + docs + 0.4.0
```
Strictly sequential: P2 implements the P1 protocol; P3 wires P2.

### Conventions (all phases)
- uv workspace; Python 3.13; ruff line-length 120; `just lint`, `just test`, `just sync` from the repo root; sarathi's own `just lint`/`just test` from `apps/sarathi/backend/`.
- No comments. Docstrings only on tools (model-facing) and public protocols (`Tracer`).
- New extra deps go in `packages/tantra/pyproject.toml` `[telemetry]` **and** the root `pyproject.toml` dev group, then `uv lock`/`just sync`.
- Tests: `asyncio_mode=auto` (plain `async def`), `FakeProvider` (`providers/fake.py`) + `MemoryStore`, no network. OTel tests use `TracerProvider` + `SimpleSpanProcessor(InMemorySpanExporter())` passed explicitly (never the global provider).
- Run `just lint` + `just test` before marking a phase done.
- **Contract freeze:** the `Tracer` protocol signature in the Seam section and the `outcome` vocabularies, from P1 on; the attribute table from P2 on. Changing them means updating this spec first, then telling dependent phases.

### Keeping this spec current
- Update the status marker on the heading and tick the checklist as you go.
- When the build deviates from the plan, **strike the original line and say why it changed** — `~~original~~ **Cut in P2.** <reason>`. Never silently rewrite; the reason a plan changed is worth more than the plan.
- After a phase lands, add only detail that would surprise the next reader — a constant whose value is load-bearing, a behavior that isn't what the name suggests, an ordering that matters. Skip anything the code already says plainly.
- Problems found but not fixed go to Open Decisions or a Follow-up note, with enough detail to act on later. Don't fix them inline and don't leave them unrecorded.

### Phase 1 — core seam + loop instrumentation · deps: none · **done 2026-08-22**
- `tantra/tracing.py`: `Tracer`, `NullTracer`, `NULL_TRACER`, `current_span`. Export `Tracer`, `NullTracer` from `tantra/__init__.py`.
- `TurnContext.tracer` field; `Harness(telemetry=)` kwarg stored as `self.tracer` (`NULL_TRACER` when None); `_build_loop` forwards `tracer` + `turn_span`; `TurnLoop.__init__` stores them, sets `turn.tracer`, adds `self.tool_spans: dict[str, Any]`, `self.terminal: TurnCompleted | TurnFailed | None`.
- Turn span in `run`/`resume` per the instrumentation table; chat span in `_sample` (new `sample_id`, `compacted` kwargs); tool spans in `_batch`/`_completed`/`run()` finally; compact span in `_drive`; `current_span` handling in `_spawn`, `_merge`, around `compactor.compact`; `_brief` wraps its provider call via `ctx.tracer`.
- `OpenAICompatible.provider_name = "openai"`, `FakeProvider.provider_name = "fake"`.
- Tests `packages/tantra/tests/test_tracing.py` with a test-local `RecordingTracer` (appends `(method, kwargs)` and returns incrementing handles):
  - text-only turn → `start_turn`, `start_sample(compacted=False)`, `end_sample(attempts=1, end=StreamEnd)`, `end_turn(outcome="completed")`, in that order; `parent` of sample == turn handle.
  - tool turn → `start_tool(replayed=False, args=…)` parent == turn handle; `end_tool(outcome="completed")` before the next `start_sample`.
  - tool raising → `end_tool(is_error=True, error_type="ValueError", outcome="error")`.
  - hook-denied tool → `start_tool`+`end_tool` pair with `is_error=True`, `error_type="_OTHER"`; hook-rewritten args → `start_tool(args=effective)`.
  - retryable `ProviderError` once → single `start_sample`/`end_sample(attempts=2)`; non-retryable → `end_sample(error=ProviderError)`, `end_turn(outcome="failed")`.
  - `ctx.ask` suspend → `end_tool(outcome="suspended", ask_id=…)`, `end_turn(outcome="suspended", ask_id=…)`; `resume` → `start_turn(resumed=True, ask_id=…)`, `start_tool(replayed=True)`, `end_turn(outcome="completed")`. Permission-ask suspend → no `start_tool`, turn `suspended`.
  - `ctx.spawn` → child `start_turn(parent == tool handle)`; `ctx.fan_out` of 2 → two child `start_turn`s, both with the tool handle as parent; `current_span.get()` is `None` after the turn ends.
  - compaction (`PruneThenSummarize` forced via small limits) → `start_compaction` → `start_sample(sample_id=None)` with parent == compact handle → `end_compaction(applied=CompactionApplied)` → next `start_sample(compacted=True)`.
  - consumer calls `aclose()` on the stream mid-tool → `end_tool(outcome="aborted")`, `end_turn(outcome="aborted")`; lease lost → `end_turn(outcome="aborted", error=TantraError)`.
  - `Harness()` without `telemetry` → `turn.tracer is NULL_TRACER`; full existing suite green.
- **Verify:** `just test` green (existing suites unchanged); `test_tracing.py` asserts the call sequences above; `grep -rn opentelemetry packages/tantra/src` → no hits.
- Checklist:
  - [x] `tracing.py` + exports
  - [x] `TurnContext.tracer`, `Harness(telemetry=)`, `_build_loop`, `TurnLoop` fields
  - [x] turn / chat / tool / compact spans + `current_span` propagation
  - [x] `_brief` instrumented, usage captured
  - [x] `provider_name` attrs
  - [x] `test_tracing.py` (24 tests)
  - [x] `just lint` + `just test` (665 passed; 641 pre-existing unchanged)

#### P1 landed notes / deviations
- ~~`resume` starts the turn span before `patch_header(status="running")`~~ **Moved.** The `TurnContext` it needs is built after that call; span starts right after construction.
- ~~zero-duration spans only via `_completed`~~ **Extended.** `_synthesize` and `_parts` (invalid-JSON args) bypass `_completed` via `_append`; both emit the stub span pair directly, so invalid-JSON results are traced too.
- ~~`aborted` error from `sys.exc_info()[1]`~~ **Replaced everywhere.** In a generator's `finally`, ambient `sys.exc_info()` chains to the *caller's* exception state — a successful sample driven from inside a caller's `except ValueError:` recorded `error=ValueError`. All error reporting now binds explicitly (`except BaseException as exc: error = exc; raise`); regression test included.
- `TurnLoop.run` had no `finally`; one was added around the existing `aclosing` block to end leftover tool spans.
- `end_compaction` runs in a `finally` (a non-`ProviderError` compactor exception previously leaked the span); non-`ProviderError` failures are recorded on the span, then propagate.
- `replayed` is computed *before* the `ToolCallStarted` append — the append inserts into `state.started`, so computing after would always give `True`.
- In `run`/`resume` `finally`: `_settle` runs before `end_turn`, so a raising user tracer can't strand the lease.
- `loop.terminal` is assigned only after the terminal event's append succeeds (a `SeqConflict` no longer reports `completed` for an unpersisted turn).
- Deny-branch stub spans use `effective` (hook-rewritten) args, matching what the approval body showed the human.
- A configured compactor produces a `start_compaction`/`end_compaction(applied=None)` pair on **every** sample iteration, not only when compaction applies — P2 span volume note.
- `current_span` for `fan_out` is set in `_fan_out` around the whole merge, so the consumer task sees it set on every forwarded child event (see Sharp edges).

### Phase 2 — `tantra.telemetry.Telemetry` + extra · deps: P1 · **done 2026-08-22**
- `packages/tantra/pyproject.toml`: `telemetry = ["opentelemetry-sdk>=1.30", "opentelemetry-exporter-otlp-proto-http>=1.30"]`; root dev group mirrors both; `uv lock`.
- `tantra/telemetry.py` per the OTel implementation section: guard, `Telemetry`, `_Handle`, message/tool-definition builders, truncation, usage accumulation, `server.*` parsing.
- Tests `packages/tantra/tests/test_telemetry.py` (InMemorySpanExporter):
  - text turn → 2 spans; root name `invoke_agent <agent>`, no parent, attrs `gen_ai.operation.name`, `gen_ai.agent.name`, `gen_ai.conversation.id`, `tantra.turn_id`, usage; chat name `chat <model>`, kind CLIENT, parent == root, `gen_ai.provider.name="fake"`, `gen_ai.request.model`, `gen_ai.response.finish_reasons`, `gen_ai.usage.*`; **no** `gen_ai.input.messages`/`gen_ai.system_instructions` when capture off.
  - `capture_content=True` → `json.loads(chat["gen_ai.input.messages"])` is the semconv list (user text; after a tool round: assistant `tool_call` part + tool `tool_call_response` part); `gen_ai.system_instructions` has the agent prompt; `gen_ai.output.messages[0].finish_reason`; root input/output set.
  - tool turn → `execute_tool <name>` parent == root (not the chat), `gen_ai.tool.call.id`, `gen_ai.tool.type=function`; with capture: `gen_ai.tool.call.arguments`/`result` JSON; tool raising → status ERROR, `error.type="ValueError"`; hook rewrite → `tantra.tool.original_arguments` present and differs.
  - retry → one chat span, `tantra.sample.attempts=2`; failure → chat ERROR + `error.type="ProviderError"`, root `tantra.turn.outcome="failed"` ERROR.
  - ask/resume → two root spans with different trace ids, same `tantra.turn_id`; first `tantra.turn.outcome="suspended"` + `tantra.ask_id`; second `tantra.turn.resumed=true`, contains the `execute_tool` span.
  - spawn → child `invoke_agent` span parent == the parent's `execute_tool` span, same trace id; fan_out → both children under the same tool span.
  - compaction → `compact` span parent == root with `tantra.compaction.applied=true`, a `chat` child, next `chat` has `gen_ai.conversation.compacted=true`.
  - 50 KiB tool result with capture → `len(attr) <= 32_768 + len(marker)`, endswith `…[truncated N chars]`.
  - stream `aclose()` mid-tool → every span in the exporter has `end_time`; root `tantra.turn.outcome="aborted"`.
  - `tracer_provider=None` + `trace.set_tracer_provider(p)` after `Harness()` construction → spans reach `p`.
  - `test_extratools_imports.py` gains a case: `opentelemetry` missing → `import tantra.telemetry` raises `ImportError` matching `tantra-harness\[telemetry\]`.
- **Verify:** `just test` green; `uv run python -c "import tantra; import tantra.telemetry"` works in the dev env; a fresh `uv venv` with `tantra-harness` only → `import tantra` ok, `import tantra.telemetry` → the guarded ImportError.
- Checklist:
  - [x] extra + dev deps + lock (resolved opentelemetry 1.44.0)
  - [x] `telemetry.py` (305 lines, OTel API imports only)
  - [x] `test_telemetry.py` (21 tests) + import-guard case
  - [x] `just lint` + `just test` (687 passed; 665 pre-existing unchanged)

#### P2 landed notes / deviations
- ~~`get_tracer("tantra", __version__)`~~ **No `__version__` exists.** Uses `importlib.metadata.version("tantra-harness")` at module top.
- ~~compact span carries no usage~~ **Added.** `end_compaction` sets `gen_ai.usage.*` from the summarizer chat's accumulated usage (still excluded from turn totals) — otherwise the compactor's tokens appeared on no parent aggregate.
- Serialization is total: `json.dumps(..., default=str)` still raises on non-scalar dict keys and circular refs; all content paths fall back to `str(value)` via `_dumps`/`_text` helpers.
- `_server` wraps the whole `urlsplit` in `try/except ValueError` (a malformed `base_url` like `http://[::1]` truncated must not kill the turn — `start_sample` runs outside `_sample`'s try); port from `SplitResult.port`, default 443/80 by scheme.
- Tool-span ERROR status description is `error_type` — `end_tool` never receives the exception object, only its qualname.
- `end_turn` output capture: `final_text or _dumps(output)` — `_final_text` returns `""` (never `None`) on `submit_output` turns. When a sample yields both text and `submit_output`, the text wins.
- Swept (aborted/suspended) tool spans set no `gen_ai.tool.call.result` (`result is None`).
- `tantra.provider.status_code` only for `isinstance(error, ProviderError)`.
- `gen_ai.tool.call.result` and `tantra.compaction.summary` are `_as_content` raw text per the table — a str result is NOT JSON-encoded; docs must not claim these parse as JSON.
- Identity attrs (`gen_ai.conversation.id`, `tantra.turn_id`) flow from the parent `_Handle`; `start_sample` with a non-handle parent (custom compactor outside the loop's `current_span` window) yields a chat span without them — see Sharp edges.
- Follow-up (core, pre-existing, not P2): a tool result with non-str dict keys makes `assemble_messages`/`_as_content` (`context.py:56`) raise `TypeError` on the next loop iteration, telemetry or not.
- Unfixed nits: `end_sample` double-call would double-count parent usage (loop never does); `_VERSION` lookup precedes the guard so an uninstalled source tree raises `PackageNotFoundError` instead of the friendly message.

### Phase 3 — sarathi wiring, smoke, docs, 0.4.0 · deps: P2 · —
- Sarathi per the wiring section (`config.py`, `telemetry.py`, `main.py` lifespan, `agent.py`, `pyproject.toml`, `.env.example`). Sarathi tests: `get_telemetry()` is `None` with empty endpoint; with endpoint set and a monkeypatched exporter class, `make_harness().tracer` is a `Telemetry`.
- `stress/live_telemetry.py` (manual, not collected): `OTEL_EXPORTER_OTLP_ENDPOINT=… OTEL_EXPORTER_OTLP_HEADERS=… uv run python stress/live_telemetry.py` — `MemoryStore` + `FakeProvider` scripted for a tool call + a subagent spawn + final answer, `Telemetry(capture_content=True)` on an explicit SDK provider with OTLP batch exporter, runs one turn, `force_flush()`, prints the trace id and exits non-zero on export failure. Human checks Langfuse: trace with agent → generation (input/output visible) → tool (args/result) → nested agent.
- Docs: `docs/guides/telemetry.md` (install, `Telemetry(...)`, Langfuse recipe with OTLP endpoint + basic-auth header, generic collector recipe, what each span carries, content opt-in + truncation, suspend/resume shape); `docs/reference/telemetry.md` (`Telemetry` kwargs, `Tracer` protocol, attribute table); extras row in `docs/getting-started/install.md` + root `README.md`; `docs/reference/harness.md` `telemetry` kwarg; `docs/reference/context.md` `tracer` field; `docs/sharp-edges.md` entries from the Sharp edges section; `mkdocs.yml` nav (Guides + Reference).
- `packages/tantra/CHANGELOG.md` `## 0.4.0` (Added: telemetry extra, `Harness(telemetry=)`, `Tracer` seam, `TurnContext.tracer`; Changed: `PruneThenSummarize` summarizer call now traced); `packages/tantra/pyproject.toml` version `0.4.0`; `uv lock`.
- **Verify:** `apps/sarathi/backend` `just lint` + `just test` green; `mkdocs build --strict` green; `stress/live_telemetry.py` run once against Langfuse (or an OTel collector) and the four observation types render with content — record the result in this spec's Landed notes.
- Checklist:
  - [ ] sarathi config/telemetry/lifespan/harness + tests
  - [ ] `stress/live_telemetry.py` + live run recorded
  - [ ] docs + nav + sharp edges
  - [ ] CHANGELOG + version + lock
  - [ ] root `just lint` + `just test`

## Open Decisions
- **`trace_id = turn_id` opt-in** — would make a suspended turn one trace. Needs verification that Langfuse/Tempo tolerate a never-emitted parent span. Add as `Telemetry(trace_per_turn=True)` later if wanted.
- **Reasoning text in `gen_ai.*.messages`** — semconv parts shown are `text`/`tool_call`/`tool_call_response`; no confirmed `reasoning` part in the JSON schema. Omitted for now; revisit against `gen-ai-output-messages.json`.
- **`user.id` / Langfuse user column** — sarathi stores the user in session metadata; spans expose it as `tantra.metadata.user`. Mapping to `user.id` is an app concern (a `SpanProcessor` in sarathi) if wanted.
- **Time to first chunk (`gen_ai.response.time_to_first_chunk`)** — deltas are yielded from `_sample`; cheap to add later inside `_sample`.

## Risks
- **Content volume** — capture on + doc-heavy tools = tens of KB per span. Mitigation: off by default, 32 KiB cap, `BatchSpanProcessor` in the recipe.
- **Span leak on abnormal exits** — every `start_*` must reach an `end_*` in a `finally` (`harness.run/resume`, `_sample`, `TurnLoop.run`). Mitigation: P1/P2 `aclose()` and lease-loss tests; `InMemorySpanExporter` only receives ended spans, so an unended span is a failing assertion.
- **Mis-parenting via `current_span`** — documented leak window; only `start_turn` reads it.
- **Semconv is `Development` status** — attribute names may change. Mitigation: names centralized in `telemetry.py`; Langfuse mapping verified against its current ingester.
- **Loop churn** — instrumentation touches `_drive`, `_sample`, `_batch`, `_completed`, `_spawn`, `_merge`, `run`, `resume`. Mitigation: P1 adds no behavior under `NullTracer`; existing suite must stay byte-for-byte green.

## Success criteria
- `Harness(telemetry=Telemetry(capture_content=True))` with an OTLP exporter pointed at Langfuse renders, for one turn: an agent observation with input/output, a generation per LLM call with prompt, completion, model, tokens and finish reason, a tool observation per call with arguments and result, nested agent observations for subagents, and a `compact` span when compaction ran — all grouped under the session id.
- Without `[telemetry]` installed nothing changes: `import tantra` works, `Harness()` defaults to `NullTracer`, the existing test suite is unchanged.
- A suspended-then-resumed turn produces two traces sharing `tantra.turn_id` and `gen_ai.conversation.id`, each with coherent parenting, no unended spans.
