# Compaction

Compaction keeps provider requests within the model context window. It is opt-in: a Runtime without a compactor never prunes or summarizes history. Each actor is budgeted and compacted independently.

```python
from tantra import CompactionConfig, PruneThenSummarize, Runtime

config = CompactionConfig(
    trigger_at=0.80,
    buffer=4_096,
    recent_tokens=20_000,
    summary_max_output=4_096,
)
runtime = Runtime(
    provider,
    store,
    [Researcher],
    compactor=PruneThenSummarize(config),
)
```

## Budget and model limits

The turn engine builds the complete `SampleRequest` before consulting the compactor. The estimate includes the application prompt, execution-environment block, journal-derived messages, tool schemas, and request parameters. Provider-reported usage is a lower bound when available.

Compaction starts at the smaller of:

- `floor(context_window * trigger_at)`
- `context_window - max_output - buffer`

A request exactly at the threshold compacts. `OpenAICompatible` resolves limits in this order: explicit per-model configuration, one lazy model-catalogue request, then `128_000` context tokens and `4_096` output tokens. Configure limits explicitly when an endpoint does not expose trustworthy catalogue metadata.

## What is preserved

`PruneThenSummarize` first replaces large, regenerable tool results with durable stubs. Skill output is not pruned. It then summarizes the evicted prefix while retaining a recent window capped by `recent_tokens`, preferring whole turns and valid tool-call/result pairs. A later summary includes the previous summary, so repeated compaction carries critical context forward.

The journal stays append-only. `CompactionApplied` records the summary, estimated sizes, and retained floor in the actor's journal; old events are not deleted or rewritten. Request assembly selects the latest summary plus the retained suffix. The application prompt, execution-environment block, and tool schemas remain byte-identical across the rebuild.

Summaries are lossy. Keep critical state in durable application data or files rather than relying only on conversation history. The current user input is never summarized. A single huge input, prompt, execution environment, or tool schema may be too large to fit even after compaction; Tantra then raises a clear provider error instead of changing that fixed payload.

## Overflow recovery

When the provider explicitly classifies an error as a context overflow before emitting any stream delta, Tantra forces one compaction and retries the same logical sample once. The retry does not consume another agent step or repeat `before_sample`. A partial-stream overflow, a second overflow, an unclassified error, or an irreducible request fails without duplicating output.

A child compacts its own context; its history is never merged into the parent. The model used for summaries is the actor's resolved model unless configured otherwise.

## Pi and OpenCode

Like Pi and OpenCode, Tantra retains recent context, folds older work into a durable summary, and can recover once from a confirmed overflow. Tantra's defaults also use a 20,000-token recent window, similar to Pi. Unlike those coding-agent applications, Tantra is a library and leaves compaction disabled until the application installs a compactor. It budgets the complete provider request, preserves framework and application system blocks exactly, records compaction in each actor's append-only journal, and never merges parent and child histories.
