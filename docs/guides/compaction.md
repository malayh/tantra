# Compaction

Compaction keeps provider requests within the model context window. It is opt-in and applies independently to each actor's history.

```python
from tantra import CompactionConfig, PruneThenSummarize, Runtime

runtime = Runtime(
    provider,
    store,
    [Researcher],
    compactor=PruneThenSummarize(CompactionConfig()),
)
```

`PruneThenSummarize` preserves complete tool-call/result pairs, recent turns, active input, and memory of prior summaries. `CompactionApplied` records the strategy, estimated sizes, summary, and floor turn ID in that actor's journal.

A child compacts its own context; its history is never merged into the parent. The model used for summaries is the actor's resolved model unless configured otherwise.
