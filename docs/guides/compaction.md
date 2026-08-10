# Compaction

Opt-in. Without a compactor a long session eventually exceeds the model's window and the provider returns a 400.

```python
from tantra import CompactionConfig, Harness, PruneThenSummarize

harness = Harness(
    provider,
    store,
    [Coder],
    default_model="openai/gpt-5",
    compactor=PruneThenSummarize(CompactionConfig(), model="openai/gpt-5-mini"),
)
```

The compactor is consulted before **every** sample. It returns events, which the loop appends and emits like any others — so a compaction survives a resume in another process.

## Two stages

**Stage 1 — prune (free).** Bulky tool results in the older part of the window are replaced with a stub: `[pruned: read_file output, 61044 chars omitted]`. Newest-first, stopping as soon as the estimate is under target. It persists as **re-emitted `ToolCallCompleted` events** with the same `call_id`; assembly is last-wins per `call_id`, so the result message is updated in place and the message list never shrinks. `skill` output is never pruned — dropping it would silently remove a capability the model believes it has.

**Stage 2 — summarize (one model call).** The pruned prefix is replaced by a structured brief — Goal / Constraints / Progress / Key Decisions / Next Steps / Critical Context — and one `CompactionApplied` is emitted naming the oldest kept turn in `floor_turn_id`. Cuts land only on turn boundaries.

A prune-only outcome emits no `CompactionApplied`.

## The constants

```python
CompactionConfig(
    buffer=20_000,
    prune_pool_min=40_000,
    prune_gain_min=20_000,
    tail_turns=2,
    summarize_at=0.95,
)
```

| Field | Default | Meaning |
|---|---|---|
| `buffer` | 20 000 | Subtracted from the window; absorbs estimator error. |
| `prune_pool_min` | 40 000 | Skip stage 1 unless prunable tool output totals at least this. |
| `prune_gain_min` | 20 000 | Skip stage 1 unless it would reclaim at least this. |
| `tail_turns` | 2 | Never touch the last N turns. |
| `summarize_at` | 0.95 | Target after pruning, as a fraction of usable. |

`usable = context_window - max_output - buffer`. The two `prune_*` floors exist so a compaction that would churn the log for a rounding error simply does not happen.

!!! note "`tail_turns` counts the in-flight turn"
    The default of 2 protects the current turn plus one completed turn. A session with fewer turns than `tail_turns + 1` — one monster turn — is un-compactable by design: it prunes, or it does nothing.

## Nothing is rewritten

The log is append-only and compaction respects that. `CompactionApplied` is an **assembly floor**: context assembly takes the latest one's summary plus every event at or after `floor_turn_id`. `replay()` still returns the full history, and a UI built on it shows everything that ever happened.

Two invariants the implementation holds and a custom compactor must too:

- **Never orphan a `tool_call` / result pair.** An assistant message whose tool call has no result is a 400 on every OpenAI-compatible provider. Stage 1 replaces content and never removes a message; stage 2 cuts only at turn boundaries.
- **Never prune `skill` output.**

## Token counts are estimates

There is no local tokenizer. The estimate is the provider's reported usage on the last sample (all four `Usage` fields, since `input_tokens` alone excludes cache hits) plus a `len(text) // 4` pass over everything added since, floored by a whole-window `chars // 4` count so that a log with no usage at all still compacts. `buffer` exists to absorb the error. `tokens_before` and `tokens_after` on `CompactionApplied` are measured on different scales — usage-scale and chars-scale — so tune with care.

!!! warning "A failed summarize fails the turn"
    The summarize call is not retried, its usage never reaches the session header, and a `ProviderError` — including an empty brief, which raises rather than deleting the prefix — appends `TurnFailed`. Recovery is a fresh `run()`, **not** `resume()`: the turn is terminated, not incomplete.

## Customising

`PruneThenSummarize(config, model=None, *, instruction=SUMMARIZE_INSTRUCTION)` — `model` picks a cheaper model for the brief (unset uses the turn's model), `instruction` replaces the summarization prompt. A domain-specific brief is usually worth writing; the default is written for a coding agent.

For anything else, implement the protocol:

```python
class Compactor(Protocol):
    async def compact(self, ctx: TurnContext) -> list[SessionEvent]: ...
```

`ctx.history` **aliases the loop's live log** — read it, never mutate it. `ctx.limits`, `ctx.model` and `ctx.provider` are populated by the loop. Returning `[]` leaves the turn alone.

## Next

- [Compaction reference](../reference/compaction.md), [Providers](providers.md), [Sharp edges](../sharp-edges.md).
