# Library issues observed while building apps

Warts and suspected bugs in tantra core, hit while building `apps/sarathi`. Candidates for fixes, not commitments — triage later. Each entry: what, where, impact, current workaround. Entries carrying a **Fixed in 0.2.0** line are closed and kept for the record.

## Metadata filter treats `None` as a wildcard for unscoped rows

- `_Filter.passes` does `row.metadata.get(key) != value` — a filter of `{"user": None}` matches every row missing the `user` key entirely (`memory.py:137`).
- Fail-open default in a scoping primitive: a caller whose scope value resolves to `None` silently reads unscoped rows instead of nothing.
- Not reachable in sarathi (child sessions inherit parent metadata, so `user` is always set). No workaround needed yet.
- **Fixed in 0.2.0:** `matches_metadata` (`stores/base.py`) fails closed — a key the row lacks never matches, and `None` matches only a stored `None`. Every store's `memory_all` and `BuiltinMemory` go through it.

## No filtered memory listing; `memory_*` not on the Store protocol

- `store.memory_all()` takes no arguments and returns deleted/superseded rows too; the keyword pass of every `recall` loads the whole table (`memory.py:203-211`). `memory_put/get/all/search` are absent from the `Store` protocol (`stores/base.py`) — `BuiltinMemory` duck-checks them, so app code calling them is using undeclared API.
- Multi-tenant apps re-implement the same three-clause filter (user + `deleted` + `superseded_by`) and pay O(all rows) per list/recall.
- Workaround: app-side filtering in `apps/sarathi/backend/src/sarathi/api/memory.py`.
- **Fixed in 0.2.0:** the four `memory_*` methods are protocol members, and `memory_all(metadata=..., include_dead=False)` filters in the store — pushed into SQL on Postgres.

## Shipped memory tools are cross-tenant

- `memory_write`/`memory_recall` in `memory.py:269-330` always write `metadata={}` and have no metadata parameter on recall.
- Any multi-user app must fork them; using them as-is leaks memories across tenants.
- Workaround: sarathi defines its own tools stamping `metadata={"user": ...}` from `ctx.deps` (documented in `docs/guides/memory.md`, so arguably by design — but there is no scoped variant to reach for).
- **Fixed in 0.2.0:** `memory_tools(scope=...)` builds a scoped pair; the callable runs per invocation and its result is both the written metadata and the recall filter. The module-level tools are `memory_tools()`.

## `BuiltinMemory.delete` has no ownership concept and is not idempotent

- `delete(mid)` soft-deletes any row and raises `TantraError` on an unknown id (`memory.py:244-249`); same for `supersede`.
- A REST delete needs a `memory_get` + ownership + liveness pre-check purely to produce a 404.
- Workaround: pre-check in `api/memory.py`.
- **Fixed in 0.2.0:** `delete(mid, *, scope=None) -> bool` never raises and is idempotent; `supersede(old_id, new, *, scope=None)` refuses an out-of-scope row as unknown.

## Bare `resume()` re-emits the pending `AskRaised` with its original seq

- `harness.py:385-389`: with an ask pending, `resume(sid)` yields the stored `AskRaised` again, original `seq`, then returns.
- Combined with replay, a reconnecting client receives two frames sharing `(session_id, seq)` — anyone treating seq as monotonic or as a dedupe key breaks. Undocumented wire behavior.
- Workaround: sarathi's reducer upserts asks by `ask_id`.
- **Fixed in 0.2.0:** the re-emitted frame carries `seq=None`, the same marker live deltas use for "not a new log entry".

## Replayed child-session frames carry `depth: 0`

- Each session replays as its own root, so `Emitted.depth` from replay disagrees with the live value for subagent frames.
- Grouping nested UI by depth works live and breaks after reload.
- Workaround: group by `session_id` only (sarathi reducer).
- **Stale as of 0.2.0:** not reproducible. `replay` reads the child's own header, and `_ChildRunner.create` persists `depth` on it (`harness.py`), so child frames replay at their live depth. Regression-tested in `packages/tantra/tests/test_subagents.py`.

## `ToolCallStarted` is skipped on every error path

- Unknown tool, invalid JSON args, and permission-deny emit `ToolCallRequested` → `ToolCallCompleted` with no `Started` between (`loop.py:642-670`).
- A UI spinner keyed on Started→Completed hangs forever on those paths.
- Workaround: key on Requested→Completed.
- **Fixed in 0.2.0:** every `ToolCallCompleted` is preceded by a `ToolCallStarted`, error and deny paths included. Logs written before 0.2.0 replay without the pairing.

## `put_header` is last-writer-wins; concurrent header edits during a turn are silently lost

- `Harness.run`/`resume` read the header once and hand the same object to the loop; `TurnLoop` re-puts it after every sample (`loop.py:777`) and `_settle` writes it again at turn end (`harness.py:296-301`). `put_header` protects only `last_seq`/`lease` — no CAS/`expect` token, unlike `append(expect_seq=...)`.
- Any external read-modify-write while a turn runs — a model change via PATCH, a title write, any metadata edit — is reverted by the loop's next header write, with no way for the caller to detect the loss.
- Repro: `MemoryStore` + `FakeProvider`, patch `metadata["model"]` on `TurnStarted` → header after the turn shows the old value.
- Workaround: sarathi disables the model picker while a turn is running and writes titles only after the turn generator is fully drained (post-`_settle`), re-reading the header immediately before the write. A library fix would be an `expect` token on `put_header` or a narrow `patch_metadata` op.
- **Fixed in 0.2.0:** `Store.patch_header` applies one atomic field-level edit under each store's exclusion primitive, with a shallow `metadata` merge. The turn loop patches `usage` only, and `run`/`resume`/`_settle` patch `status`/`pending_ask`, so no library write rewrites the whole header any more. `put_header` is unchanged and still last-writer-wins.

## `harness.cancel` is single-session; no way to cancel a session tree

- `cancel(sid)` appends `CancelRequested` to that one log (`harness.py:441-459`); a running child loop only checks its own log, and the parent is blocked inside the subagent tool call — so cancelling the root does nothing until the child's whole turn finishes.
- Stop/cancel UX is broken for any app using subagents unless the app re-implements tree discovery.
- Workaround: sarathi walks `ChildSessionSpawned` events recursively and cancels descendants deepest-first, then the root (`api/ws.py`). A `cancel(sid, recursive=True)` on the harness would remove the app-side walk.
- **Fixed in 0.2.0:** `cancel(sid, *, recursive=False)`; with `recursive=True` the harness walks `ChildSessionSpawned` and flags every descendant deepest-first before the target.

## `cancel` cannot win its seq race against an actively-appending session

- `cancel(sid)` reads the whole log, then `append(expect_seq=last_seq)`, retrying `CANCEL_ATTEMPTS = 5` times with no backoff (`harness.py:441-459`). A running session appends constantly (tool results, sample parts), so every retry re-reads and loses the race — after 5 losses `SeqConflict` propagates and no `CancelRequested` is ever written.
- Cancelling the exact sessions you most want to cancel — busy ones, e.g. a researcher child mid-fetch-loop — fails essentially always; only idle logs (a parent blocked inside the spawn tool call) accept the cancel. Live E2E proof: two stop presses during running children, both children finished with `stop_reason="completed"` and zero cancel events in their logs (one ran to `max_steps`, 318 events), while the root accepted its cancel on the first try.
- Workaround: none app-side — sarathi's tree walk calls `cancel` per descendant and can only swallow the `SeqConflict` (`api/ws.py`). Fix: append `CancelRequested` without `expect_seq` (it is a flag; it needs no seq consistency), or retry with backoff/jitter.
- **Fixed in 0.2.0:** `Store.append` accepts `expect_seq=None` and `cancel` uses it, so the flag lands in one attempt regardless of how busy the log is.

## Cancel is only observed at store boundaries

- The loop checks `state.cancelled` before each sample (`loop.py:726`) and before each tool call (`loop.py:602`); an in-flight sample stream or tool call runs to completion first.
- A stop press during a long generation or a slow tool (e.g. web_fetch) appears ignored for the remainder of that step; nothing cooperatively interrupts the provider stream or tool coroutine.
- No workaround at the app level; latency is bounded by the current step. A library fix would poll for cancel during streaming or cancel the sample/tool task.

## Cancel landing on a turn's final sample is silently dropped

- After a sample, `_append(self._parts(...))` absorbs a concurrent `CancelRequested` (setting `state.cancelled`) to resolve its `SeqConflict`, but the no-tool-calls branch goes straight to `_terminal("completed", None)` without re-checking the flag (`loop.py:773-782`).
- A cancel that arrives mid-sample when that sample turns out to be the last one yields `stop_reason="completed"` — the cancel is recorded in the log yet has no effect.
- Repro: gate FakeProvider mid-sample on a text-only sample, append cancel, release. Workaround: none app-side; sarathi's test scripts the cancelled sample with a trailing tool call so the loop reaches `_after_batch`, which does check. Fix is a one-line `state.cancelled` guard before that `_terminal("completed")`.
- **Fixed in 0.2.0:** the final-sample branch and the submit-output path both re-check `state.cancelled` and end the turn with `stop_reason="cancelled"`.

## Provider reads only the `reasoning` delta field

- `openai_compat.py:129` reads `delta.reasoning`; endpoints emitting `reasoning_content` (DeepSeek-style) stream no thoughts.
- Headline thoughts UI is silently empty depending on endpoint.
- Workaround: `sarathi.provider.ReasoningCompat` subclass reads both.
- **Fixed in 0.2.0:** `OpenAICompatible` reads `reasoning` or `reasoning_content`; the sarathi subclass is deleted.

## Session `list(metadata=...)` keeps the `None`-wildcard fail-open

- `select_headers` filters with `h.metadata.get(k) == v` (`stores/base.py`), so `{"user": None}` matches every session whose metadata lacks `user`. Postgres pushes the same filter into `metadata @> ...` jsonb containment, which reads a list or dict value as a recursive subset rather than an equality test.
- The memory-side version of this bug was fixed in 0.2.0 (`matches_metadata`); this is the same fail-open one layer up, on the primitive apps use for tenant scoping. A scope value that resolves to `None` lists other tenants' sessions.
- Workaround: keep scope values scalar and always set, so the `None` branch is never taken.

## `memory_search` is unscoped; a tenant can silently lose vector recall

- `BuiltinMemory._vector_pass` calls `store.memory_search(vector, k)` (`memory.py`), which ranks the top-k globally in SQL; the scope filter is applied afterwards in Python by `_Filter.passes`.
- A tenant whose rows do not make the global top-k gets an empty vector pass and degrades to keyword-only recall, with `hit.mode` the only clue. The larger the shared table, the worse it gets.
- Workaround: none app-side short of a per-tenant store. Fix: a `metadata` parameter on `memory_search` so the scope reaches the SQL.
