# 010 Adaptive Context Compaction

Status: Phases 0 and 1 complete; Phase 2 not started.

## Goal

Provide adaptive, durable compaction comparable to Pi and OpenCode while remaining opt-in. Configured runtimes compact before a request reaches the smaller of 80% of the model context window and the hard context ceiling after output and buffer reservations. Application prompts, execution-environment blocks, and tool schemas remain byte-identical; only journal-derived history may be pruned or summarized.

## Scope

This design covers model-limit discovery, complete-request budgeting, token-bounded durable history compaction, one-shot recovery from explicit context overflow, and documentation/release integration.

Compaction remains opt-in. The journal remains append-only. No storage migration, worker change, dependency, or Sarathi UI change is required.

## Public contracts

### Provider limits

`Provider.limits(model)` may return `ModelLimits` synchronously or as an awaitable. The turn engine resolves either form.

Limit precedence is:

1. Explicitly configured `ModelLimits`.
2. Provider catalogue discovery.
3. `ModelLimits(context_window=128_000, max_output=4_096)`.

`OpenAICompatible` queries the model catalogue lazily, at most once per provider instance, only when a requested model lacks explicit limits. It caches both successful discovery and failure. Recognized optional fields are top-level `context_length` and `max_completion_tokens`, or those fields nested under `top_provider`. Missing, malformed, or non-positive values fall back independently. Standard OpenAI catalogue responses, absent models, and catalogue request failures use fallback limits without blocking a turn.

In Phase 1, `ProviderError` gains backward-compatible context-overflow classification.

### Compaction configuration

`CompactionConfig` retains its existing fields and positional order. Its default buffer becomes 4,096 tokens. It adds:

- `trigger_at=0.80`
- `recent_tokens=20_000`
- `summary_max_output=4_096`

Ratios and token counts are validated. `tail_turns=2` remains a preferred whole-turn minimum for Phase 1. `summarize_at=0.95` is a target relative to the compaction trigger, not the total context window.

### Turn context and compactor compatibility

`Compactor.compact(ctx)` remains unchanged so existing custom compactors continue to work. `TurnContext` gains only the prepared `SampleRequest` needed by the built-in compactor.

## Request budgeting

Before every sample, the engine resolves the agent prompt and constructs the complete `SampleRequest` before consulting the compactor. Estimation includes all system blocks, the execution environment, journal-derived messages, tool schemas, and request parameters. Prior provider-reported usage is an additional lower bound where applicable.

The trigger is:

`min(floor(context_window * trigger_at), context_window - max_output - buffer)`

A request at the trigger compacts; a request below it does not. If compaction emits events, the engine rebuilds the request from the updated append-only journal before sampling. The already-resolved agent prompt, execution-environment blocks, and tool schemas are unchanged.

## Durable compaction behavior

Phase 1 replaces the unbounded two-turn tail with a recent history window capped at 20,000 estimated tokens. It prefers complete turns and valid tool-call/result pairs, but shrinks the retained tail for small model windows. The current user input remains exact.

Regenerable tool output is pruned before summarization. Skill output is preserved. A summary folds in the prior summary and covers the evicted prefix. Summary generation is capped at 4,096 output tokens. `CompactionApplied` records the durable summary and floor in the append-only journal.

After compaction, the engine rebuilds and verifies the full request. Fixed payload or current input that still cannot fit fails explicitly.

## Overflow recovery

An explicit provider context-overflow error forces one additional compaction and retries the same logical sample once, only if no stream delta was emitted. Recovery does not consume an extra agent step and does not repeat `before_sample`. A partial-stream overflow, second overflow, unclassified error, or irreclaimable request fails without duplicate output.

## Phases

### Phase 0 — Model limits and complete request budgeting

Dependencies: none.

Deliverables:

- Sync-or-awaitable provider limits resolution.
- Lazy, cached OpenAI-compatible catalogue discovery with explicit-config precedence and conservative fallback.
- New compatible `CompactionConfig` fields, defaults, and validation.
- Full-request estimation with prior usage as a lower bound.
- Complete request preparation before compaction and rebuild after compaction events.
- Exact preservation of agent prompt, execution environment, and tool schemas.
- No Phase 1 tail rewrite or overflow recovery.

Verify:

- Explicit limits make no catalogue request.
- OpenRouter-like metadata is discovered.
- Standard metadata, missing models, malformed fields, and catalogue failure use per-field fallbacks.
- Catalogue discovery is attempted once per provider instance.
- Sync and async custom provider limits both work.
- Estimation counts system blocks, execution environment, messages, tool schemas, and parameters.
- Below 80% does not compact; 80% does; the hard ceiling may trigger first.
- Requests are rebuilt after compaction events.
- Prompt, environment, and tool schemas remain byte-identical.
- Existing provider, turn-engine, compaction, and custom-compactor tests pass.
- Ruff, full Tantra tests, project lint/test commands when available, and `git diff --check` pass.

Checklist:

- [x] Provider limit precedence and async compatibility.
- [x] Cached OpenAI-compatible discovery and fallback.
- [x] Compatible config fields, defaults, and validation.
- [x] Complete request estimator and prior-usage lower bound.
- [x] Pre-compaction request preparation and post-event rebuild.
- [x] Focused and full verification.
- [x] Phase 0 status updated after verification.

### Phase 1 — Token-bounded durable compaction ✅ DONE

Dependencies: Phase 0.

Deliverables:

- Replace the unbounded two-turn tail with a 20,000-token capped recent window.
- Preserve whole turns and valid tool-call/result pairs where possible.
- Fold previous summaries into new summaries.
- Bound summary output at 4,096 tokens.
- Verify fit after compaction.
- Add one-shot explicit-overflow compaction and retry.

Verify:

- Prompt and execution environment remain unchanged.
- A huge recent turn cannot defeat compaction.
- Tool-call/result pairs stay valid and ordered.
- Repeated compaction folds the prior summary.
- Overflow retry consumes no extra agent step and does not repeat `before_sample`.
- Partial-stream and second overflow fail without duplicate output.
- Oversized fixed payload or current input fails clearly.
- Actor sessions compact independently.

Checklist:

- [x] Token-bounded recent history.
- [x] Summary output limit and summary folding.
- [x] Post-compaction fit check.
- [x] Overflow classification.
- [x] One-shot recovery retry.
- [x] Focused and full tests.

### Phase 2 — Documentation and release integration

Dependencies: Phases 0 and 1.

Deliverables:

- Document invocation, discovery, fallback, configuration, durable summaries, and system preservation.
- Document lossy summarization, opt-in behavior, and fixed-payload caveats.
- Compare behavior with Pi and OpenCode.
- Include the feature in the pending Tantra 1.1 release without a separate tag.

Verify:

- Ruff, Tantra package tests and stress tests.
- Sarathi backend tests for discovered, configured, and fallback limits.
- Strict documentation, package, lock, and diff checks.
- Long-context Sarathi verification.

Checklist:

- [ ] User and reference documentation.
- [ ] Migration and compatibility notes.
- [ ] Full repository verification.
- [ ] Sarathi verification.
- [ ] Tantra 1.1 integration.

## Rejected approaches

- Always-on compaction.
- A hardcoded model registry.
- Catalogue-only model limits.
- Fixed-turn retention.
- Journal deletion or rewriting.
- Multiple overflow retries.
- Tokenizer dependencies.

## Risks and mitigations

- Estimation is approximate. Conservative thresholds, full-request coverage, and provider usage lower bounds reduce undercounting.
- Provider metadata is optional and non-standard. Discovery recognizes only explicit extension fields and falls back independently.
- Summaries are lossy. Compaction is opt-in, preserves recent exact history, and remains durable and inspectable.
- Sarathi may configure explicit limits. Explicit configuration always wins over discovery.
- Compatibility regressions are possible. Existing config fields retain positional order, custom compactors retain their method contract, and sync providers remain valid.

## Unresolved issues

None for the approved design.

## Keeping this spec current

Implement one phase at a time. After that phase's verify criteria pass, update only its status and checklist. Record deviations with reasons. Keep later phases unchecked until implemented. Preserve unrelated work and add no code comments or dependencies.
