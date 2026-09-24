# Compaction

`Compactor` defines the async compaction seam. `PruneThenSummarize` is the built-in implementation; configure it with `CompactionConfig` and pass it as `Runtime(compactor=...)`.

`Compactor.compact(ctx) -> list[SessionEvent]` remains the custom-compactor contract. `TurnContext.sample_request` contains the complete prepared request. Returned events are appended before the request is rebuilt.

`CompactionConfig` fields and defaults:

| Field | Default | Meaning |
|---|---:|---|
| `buffer` | `4_096` | Safety reserve below the hard context ceiling |
| `prune_pool_min` | `40_000` | Minimum candidate tool-output pool before pruning |
| `prune_gain_min` | `20_000` | Minimum estimated gain before pruning |
| `tail_turns` | `2` | Preferred whole-turn minimum, subject to the token cap |
| `summarize_at` | `0.95` | Post-compaction target relative to the trigger |
| `trigger_at` | `0.80` | Context-window ratio that triggers compaction |
| `recent_tokens` | `20_000` | Maximum estimated recent-history window |
| `summary_max_output` | `4_096` | Maximum summary-generation output |

The effective trigger is `min(floor(context_window * trigger_at), context_window - max_output - buffer)`. Estimation covers the full `SampleRequest`; prior provider usage is an additional lower bound.

`PruneThenSummarize` preserves valid tool pairs, never prunes skill output, folds the prior summary into the next summary, and writes `CompactionApplied` to the current actor journal. The event selects a summary and retained floor during future assembly without rewriting history. Prompt blocks, execution-environment blocks, and tool schemas are not journal-derived and are not changed.

The current input must fit with the fixed request payload. Post-compaction requests at or above the trigger fail. An explicit pre-output context overflow can force one additional compaction and retry; no other provider error or partial stream is retried through this path.
