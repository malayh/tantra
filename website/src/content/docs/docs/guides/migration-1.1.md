# Migrate to 1.1

Tantra 1.1 keeps the 1.0 journals and store schemas. Existing JSON, filesystem, SQLite, and PostgreSQL data loads without a migration or historical backfill.

## Child actors

Child human asks are no longer supported. Move approval-gated work to the root. When a child needs input or authority, have it `send()` a message to its direct parent. Runtime construction now rejects static child permissions whose effective verdict is `ask`, and dynamic child escalation fails without emitting `AskRaised`.

Plain child completion is not a result. It leaves the child reusable and sends the direct parent a status-only lifecycle input. A completed assignment must call `finish(result)` exactly once to close the child and deliver its result. Parents can use the injected `status(agent_id)` tool for direct children; applications can use `Runtime.status()` and `Runtime.tree_status()` without reading journals.

Applications should poll actor status and subscribe to child journals only when detailed output is visible or otherwise needed. Keep a separate cursor for every subscribed actor. `ActorStatus.active` is process-local, not distributed ownership.

`spawn()` accepts an optional durable display `name`. The registered `agent` remains the reconstruction type. Existing records have no name and display their actor type; no backfill is required.

## Sarathi subagents and skills

Sarathi now registers `sarathi` and one general-purpose `subagent`. Legacy `researcher` journals and headers remain readable, but those actors cannot resume or accept new work because the type is no longer registered. Start new delegated work with `subagent`.

Research specialization moved into the packaged `research` skill. Sarathi works inline by default and delegates only when explicitly requested or independently useful. Put `shallow`, `normal`, or `deep` in the task when a particular research level matters; omission means `normal`. Subagents have non-interactive read/search capabilities and cannot use approval-gated memory writes or ask a human.

## Compaction and providers

Compaction remains opt-in. Install `PruneThenSummarize` on the Runtime to enable complete-request budgeting, token-bounded recent history, durable summaries, and one-shot confirmed-overflow recovery. Summarization is lossy, while prompts, execution-environment blocks, tool schemas, and current input remain exact.

`Provider.limits(model)` may now be synchronous or awaitable. `OpenAICompatible` uses explicit per-model limits first, then lazy catalogue metadata, then `128_000` context and `4_096` output tokens. Configure explicit limits for endpoints whose catalogue omits or misreports them.
