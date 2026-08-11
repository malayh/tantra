# Changelog

## 0.2.0

Breaking:

- `Store.append` takes `expect_seq: int | None`; `None` skips the check and appends onto whatever the current last seq is.
- The `Store` protocol gains `memory_put` / `memory_get` / `memory_all` / `memory_search` and `patch_header`. A store written against 0.1.0 must add them.
- `ToolCallStarted` now precedes every `ToolCallCompleted`, including the error and denial paths. Logs written before 0.2.0 replay without that pairing.
- `Memory.delete(mid, *, scope=None)` returns `bool` instead of raising; `Memory.supersede(old_id, new, *, scope=None)` takes a scope.

Fixed:

- `Harness.cancel` appends blind, so a busy session can no longer livelock the seq race and refuse to be cancelled.
- A cancel absorbed on a turn's final sample or on the submit-output path now ends the turn cancelled instead of being dropped.
- Bare `resume()` re-emits the pending ask with `seq=None`, so a re-presented question is not mistaken for a new log entry.
- Memory metadata matching fails closed: a key the row lacks never matches, and `None` matches only a stored `None`.
- `delete` is idempotent and scope-checked — an unknown id and a row outside the scope both return `False`.
- The provider reads `reasoning_content` as well as `reasoning`, so reasoning from either dialect streams.

Added:

- `Harness.cancel(sid, *, recursive=False)` flags every descendant session deepest-first.
- `Store.patch_header(sid, *, title=..., status=..., pending_ask=..., usage=..., metadata=...)` for lost-update-free header edits; `metadata` merges shallowly. The turn loop no longer rewrites the whole header mid-turn.
- `memory_all(metadata=..., include_dead=False)`, with deleted and superseded rows excluded by default.
- `memory_tools(scope=...)` builds tenant-scoped `memory_write` / `memory_recall` tools.

## 0.1.0

Initial release.
