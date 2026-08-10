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

## Provider reads only the `reasoning` delta field

- `openai_compat.py:129` reads `delta.reasoning`; endpoints emitting `reasoning_content` (DeepSeek-style) stream no thoughts.
- Headline thoughts UI is silently empty depending on endpoint.
- Workaround: `sarathi.provider.ReasoningCompat` subclass reads both.
