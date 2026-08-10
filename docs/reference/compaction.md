# Compaction

```python
from tantra import CompactionConfig, Compactor, PruneThenSummarize
from tantra.compaction import DEFAULT_COMPACTION, estimate_tokens
```

Compaction keeps an assembled turn inside the model's context window. It is off unless you pass one: `Harness(compactor=PruneThenSummarize())`.

## `Compactor` (protocol)

```python
async def compact(self, ctx: TurnContext) -> list[SessionEvent]
```

Consulted **before every sample**. Return events that shrink the assembled context, or `[]` to leave the turn alone. `ctx.history` aliases the live log and `ctx.limits` describes the model in use; `ctx.provider` and `ctx.model` are there for a compactor that needs to sample.

Returned events are appended to the log and emitted like any other, so a compaction survives a resume in another process. **Nothing is ever rewritten** — assembly derives the compacted view from those events, and `replay` still returns the full history.

A `ProviderError` raised out of `compact` fails the turn with `TurnFailed`. Other exceptions propagate.

## `CompactionConfig`

Frozen dataclass.

| Field | Default | Meaning |
|---|---|---|
| `buffer` | `20_000` | Tokens held back from the window to absorb estimation error. |
| `prune_pool_min` | `40_000` | Minimum estimated tokens sitting in prunable tool results before stage 1 bothers. |
| `prune_gain_min` | `20_000` | Minimum estimated tokens stage 1 must save, or it does nothing. |
| `tail_turns` | `2` | Recent turns never pruned and never summarized away. |
| `summarize_at` | `0.95` | Stage 2 targets this fraction of usable. |

**`usable(limits: ModelLimits) -> int`** returns `context_window - max_output - buffer`.

`DEFAULT_COMPACTION = CompactionConfig()`.

## `PruneThenSummarize`

```python
PruneThenSummarize(
    config: CompactionConfig = DEFAULT_COMPACTION,
    model: str | None = None,
    *,
    instruction: str = SUMMARIZE_INSTRUCTION,
)
```

Note `config` and `model` are positional-or-keyword; only `instruction` is keyword-only. `model=None` summarizes with the turn's own model; set it to route summaries to a cheaper one. `instruction` replaces the built-in brief prompt (`tantra.compaction.SUMMARIZE_INSTRUCTION`).

Two stages, run only when the estimate exceeds `usable`:

1. **Prune (free).** Replace bulky tool-result *content* with `[pruned: <tool> output, N chars omitted]` stubs, newest-first, until the target is met. Only results outside the `tail_turns` window and at least `MIN_RESULT_CHARS` (256) long are candidates, and only the latest result for a given `call_id`. `skill` output is never pruned — that would silently drop a capability the model believes it has. The whole stage is skipped unless the pool clears `prune_pool_min` and the saving clears `prune_gain_min`. Emits replacement `ToolCallCompleted` events.
2. **Summarize (one model call).** If pruning was not enough, summarize the prefix into a structured brief — Goal / Constraints / Progress / Key Decisions / Next Steps / Critical Context — and emit one `CompactionApplied` with `floor_turn_id` at the start of the tail. Assembly then reads: the summary as a user message, plus everything from the floor onward.

An empty summary from the provider raises `ProviderError` and the prefix is left intact.

!!! warning "Both stages cut only at safe boundaries"
    Stage 1 never removes a message, and stage 2 cuts only at a `TurnStarted`. A custom compactor must hold the same line: dropping an assistant message that requested a tool without its result is a 400 on every OpenAI-compatible provider. Large tool outputs — including `web_fetch` pages — get stubbed with no exemption mechanism.

## Writing your own

A compactor needs to read the current window and predict what assembly will do with its return value. Three helpers in `tantra.context` do that — see [Context](context.md#assembly-helpers):

```python
from tantra.context import assemble_messages, compaction_window

summary, window = compaction_window(ctx.history)
messages = assemble_messages(summary, window)
```

`compaction_window` gives the compacted view (latest summary + everything after its floor), `assemble_messages` turns it into provider messages, and `build_messages` is the two combined. `PruneThenSummarize` uses all three.

## `estimate_tokens(events, summary="") -> int`

The number the config is compared against. It is an **estimate**, not a count: the provider's reported usage on the most recent `SampleCompleted` plus `len(text) // 4` for content added since, floored by a `len(json) // 4` estimate of the whole assembled window. A `CompactionApplied` resets the base. There is no local tokenizer — `buffer` exists to absorb the error.

## See also

- [Events](events.md) — `CompactionApplied` fields.
- [Context](context.md) — what a compactor receives.
- [Guides: compaction](../guides/compaction.md).
