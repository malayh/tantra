# Library issues observed while building apps

Warts and suspected bugs in tantra core, hit while building `apps/sarathi`. Candidates for fixes, not commitments — triage later. Each entry: what, where, impact, current workaround.

## Metadata filter treats `None` as a wildcard for unscoped rows

- `_Filter.passes` does `row.metadata.get(key) != value` — a filter of `{"user": None}` matches every row missing the `user` key entirely (`memory.py:137`).
- Fail-open default in a scoping primitive: a caller whose scope value resolves to `None` silently reads unscoped rows instead of nothing.
- Not reachable in sarathi (child sessions inherit parent metadata, so `user` is always set). No workaround needed yet.

## No filtered memory listing; `memory_*` not on the Store protocol

- `store.memory_all()` takes no arguments and returns deleted/superseded rows too; the keyword pass of every `recall` loads the whole table (`memory.py:203-211`). `memory_put/get/all/search` are absent from the `Store` protocol (`stores/base.py`) — `BuiltinMemory` duck-checks them, so app code calling them is using undeclared API.
- Multi-tenant apps re-implement the same three-clause filter (user + `deleted` + `superseded_by`) and pay O(all rows) per list/recall.
- Workaround: app-side filtering in `apps/sarathi/backend/src/sarathi/api/memory.py`.

## Shipped memory tools are cross-tenant

- `memory_write`/`memory_recall` in `memory.py:269-330` always write `metadata={}` and have no metadata parameter on recall.
- Any multi-user app must fork them; using them as-is leaks memories across tenants.
- Workaround: sarathi defines its own tools stamping `metadata={"user": ...}` from `ctx.deps` (documented in `docs/guides/memory.md`, so arguably by design — but there is no scoped variant to reach for).

## `BuiltinMemory.delete` has no ownership concept and is not idempotent

- `delete(mid)` soft-deletes any row and raises `TantraError` on an unknown id (`memory.py:244-249`); same for `supersede`.
- A REST delete needs a `memory_get` + ownership + liveness pre-check purely to produce a 404.
- Workaround: pre-check in `api/memory.py`.

## Bare `resume()` re-emits the pending `AskRaised` with its original seq

- `harness.py:385-389`: with an ask pending, `resume(sid)` yields the stored `AskRaised` again, original `seq`, then returns.
- Combined with replay, a reconnecting client receives two frames sharing `(session_id, seq)` — anyone treating seq as monotonic or as a dedupe key breaks. Undocumented wire behavior.
- Workaround: sarathi's reducer upserts asks by `ask_id`.

## Replayed child-session frames carry `depth: 0`

- Each session replays as its own root, so `Emitted.depth` from replay disagrees with the live value for subagent frames.
- Grouping nested UI by depth works live and breaks after reload.
- Workaround: group by `session_id` only (sarathi reducer).

## `ToolCallStarted` is skipped on every error path

- Unknown tool, invalid JSON args, and permission-deny emit `ToolCallRequested` → `ToolCallCompleted` with no `Started` between (`loop.py:642-670`).
- A UI spinner keyed on Started→Completed hangs forever on those paths.
- Workaround: key on Requested→Completed.

## `put_header` is last-writer-wins; concurrent header edits during a turn are silently lost

- `Harness.run`/`resume` read the header once and hand the same object to the loop; `TurnLoop` re-puts it after every sample (`loop.py:777`) and `_settle` writes it again at turn end (`harness.py:296-301`). `put_header` protects only `last_seq`/`lease` — no CAS/`expect` token, unlike `append(expect_seq=...)`.
- Any external read-modify-write while a turn runs — a model change via PATCH, a title write, any metadata edit — is reverted by the loop's next header write, with no way for the caller to detect the loss.
- Repro: `MemoryStore` + `FakeProvider`, patch `metadata["model"]` on `TurnStarted` → header after the turn shows the old value.
- Workaround: sarathi disables the model picker while a turn is running and writes titles only after the turn generator is fully drained (post-`_settle`), re-reading the header immediately before the write. A library fix would be an `expect` token on `put_header` or a narrow `patch_metadata` op.

## `harness.cancel` is single-session; no way to cancel a session tree

- `cancel(sid)` appends `CancelRequested` to that one log (`harness.py:441-459`); a running child loop only checks its own log, and the parent is blocked inside the subagent tool call — so cancelling the root does nothing until the child's whole turn finishes.
- Stop/cancel UX is broken for any app using subagents unless the app re-implements tree discovery.
- Workaround: sarathi walks `ChildSessionSpawned` events recursively and cancels descendants deepest-first, then the root (`api/ws.py`). A `cancel(sid, recursive=True)` on the harness would remove the app-side walk.

## `cancel` cannot win its seq race against an actively-appending session

- `cancel(sid)` reads the whole log, then `append(expect_seq=last_seq)`, retrying `CANCEL_ATTEMPTS = 5` times with no backoff (`harness.py:441-459`). A running session appends constantly (tool results, sample parts), so every retry re-reads and loses the race — after 5 losses `SeqConflict` propagates and no `CancelRequested` is ever written.
- Cancelling the exact sessions you most want to cancel — busy ones, e.g. a researcher child mid-fetch-loop — fails essentially always; only idle logs (a parent blocked inside the spawn tool call) accept the cancel. Live E2E proof: two stop presses during running children, both children finished with `stop_reason="completed"` and zero cancel events in their logs (one ran to `max_steps`, 318 events), while the root accepted its cancel on the first try.
- Workaround: none app-side — sarathi's tree walk calls `cancel` per descendant and can only swallow the `SeqConflict` (`api/ws.py`). Fix: append `CancelRequested` without `expect_seq` (it is a flag; it needs no seq consistency), or retry with backoff/jitter.

## Cancel is only observed at store boundaries

- The loop checks `state.cancelled` before each sample (`loop.py:726`) and before each tool call (`loop.py:602`); an in-flight sample stream or tool call runs to completion first.
- A stop press during a long generation or a slow tool (e.g. web_fetch) appears ignored for the remainder of that step; nothing cooperatively interrupts the provider stream or tool coroutine.
- No workaround at the app level; latency is bounded by the current step. A library fix would poll for cancel during streaming or cancel the sample/tool task.

## Cancel landing on a turn's final sample is silently dropped

- After a sample, `_append(self._parts(...))` absorbs a concurrent `CancelRequested` (setting `state.cancelled`) to resolve its `SeqConflict`, but the no-tool-calls branch goes straight to `_terminal("completed", None)` without re-checking the flag (`loop.py:773-782`).
- A cancel that arrives mid-sample when that sample turns out to be the last one yields `stop_reason="completed"` — the cancel is recorded in the log yet has no effect.
- Repro: gate FakeProvider mid-sample on a text-only sample, append cancel, release. Workaround: none app-side; sarathi's test scripts the cancelled sample with a trailing tool call so the loop reaches `_after_batch`, which does check. Fix is a one-line `state.cancelled` guard before that `_terminal("completed")`.

## Provider reads only the `reasoning` delta field

- `openai_compat.py:129` reads `delta.reasoning`; endpoints emitting `reasoning_content` (DeepSeek-style) stream no thoughts.
- Headline thoughts UI is silently empty depending on endpoint.
- Workaround: `sarathi.provider.ReasoningCompat` subclass reads both.
