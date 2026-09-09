# Compaction

`Compactor` defines the async compaction seam. `PruneThenSummarize` is the built-in implementation; configure it with `CompactionConfig` and pass it as `Runtime(compactor=...)`.

Compaction preserves valid tool pairs and writes `CompactionApplied` to the current actor journal. Each actor compacts independently.
