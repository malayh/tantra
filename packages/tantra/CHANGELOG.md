# Changelog

## 3.1.0

Added:

- `Runtime.deliver(agent_id, input, *, command_id, source)` wakes any agent of a tree that isn't finished (root, child or grandchild) from code outside a turn. It queues `[event <source>] <input>` with the new `InputQueued.origin="event"`, no `sender_id` and no `submitted_by`, on the local and the coordinator path. It needs no `Connection` or writer token, and the tree's writer never changes. `source` must match `^[a-z][a-z0-9_]{0,31}$`, otherwise `ValueError`. An unknown id raises `SessionNotFound`, and a finished target raises `TantraError`. A retry with the same `command_id`, target and content returns `duplicate=True` and wakes the target again; other content raises `InvalidCommandReuse`. With a coordinator and no owner for the tree, the caller becomes the owner and runs the woken turn, so its Runtime must register the same agents, provider, hooks and `deps_factory`. See the [Runtime reference](https://malayh.github.io/tantra/docs/reference/runtime/#deliver).
- `DeliverPayload`, exported from `tantra`, and the `deliver` coordinator operation, which carries no writer token.
- `Hook.on_cancel(root_id, agent_ids)`, awaited after a tree cancel or `cancel_agent` is saved, with every agent it covers, idle and finished ones included. A tree cancel calls it on the Runtime that called `Connection.cancel`, with every agent in the tree; `cancel_agent` calls it on the owner, with the child and its descendants. Root-only cancel doesn't call it. A retried cancel calls it again. Its error makes `cancel` raise and `cancel_agent` return a tool error, and the cancel stays saved. The owner's tree cancel reply carries `agent_ids`.
- Over ACP, an event input is an incoming `session_message` with no sender, the agent as recipient, the text without the prefix, and `_meta.tantra.source`, on every client. It never enters `_tantra_queue_update`. `tantra-acp-client` 2.2.0 adds `MessageEntry.source`. The profile version stays 1.

Changed:

- Root-only cancel (`descendants=False`) keeps queued event inputs, as it keeps inputs from agents. It still cancels user inputs, and inputs with no `origin` and no `sender_id` from older journals.

Compatibility and upgrade:

- Run every Runtime on 3.1.0 before any caller uses `deliver`. A 3.0.0 owner's command dispatcher dies on a `deliver` request, and that Runtime handles no more commands for any tree it owns until it restarts. 3.0.0 also can't read a journal that holds an event input.
- No downgrade to 3.0.0 once an event input is journaled.
- During a rolling deploy, a 3.0.0 owner's cancel reply has no `agent_ids`, and `on_cancel` isn't called.
- Journals written by 3.0 load unchanged and need no migration.

## 3.0.0

Removed:

- The `finish` framework tool, with `FinishResult` and `TurnEngine(terminal_tool=...)`. Children no longer close themselves, and new sessions never write `AgentFinished` or `TurnCompleted(stop_reason="finished")`.

Changed:

- Every completed child turn queues `[agent <child-uuid> replied] <text>` for the direct parent, where the text is the turn's final assistant message, or the canonical JSON of `submit_output` for a child with an `output_schema`. Text over 32 KiB of UTF-8 is cut at a character boundary and ends with `[reply truncated at 32 KiB]`; an empty message becomes `(no text reply)`. The input keeps `origin="turn_ended"` and the command ID `uuid5(child_id, turn_id)`. `max_steps`, failed, cancelled and interrupted turns keep the status-only `[agent <child-uuid> turn ended] <stable-json>` notice, and a cancel by the parent still queues nothing.
- The child guidance and the `spawn`, `status` and `cancel_agent` descriptions describe replies.

Added:

- Over ACP, a reply is an incoming `session_message` from the child with `_meta.tantra.reply: true`. `tantra-acp-client` 2.1.0 adds `MessageEntry.reply`. The profile version stays 1.

Compatibility and upgrade:

- Journals written by 2.x load unchanged and need no migration. A child finished under 2.x stays closed and its finished input projects as before. A turn that 2.x already notified under the same command ID is not delivered again.
- `finish` remains a reserved tool name for child agents.
- Parents now receive child text. Check views or logs that show parent inputs to people who must not see child output.
- Stop 2.x workers before starting 3.0 workers. See the [3.0 upgrade guide](https://malayh.github.io/tantra/docs/guides/migration-3.0/).

## 2.2.0

Added:

- `Agent.model_params` sets provider request parameters for each model call, for example `{"reasoning": {"effort": "high"}}` on OpenRouter. It is a mapping, or a callable taking `TurnContext` that returns one, sync or async, resolved before each sample like `prompt`. Each agent uses its own value, and the overflow retry keeps it. `OpenAICompatible` sends it as extra body fields. Compaction summaries keep their own parameters.

## 2.1.0

Added:

- `Connection.cancel(..., descendants=False)` cancels only the root's active turn and its queued user inputs. Descendants keep running, and queued agent inputs stay queued and run next, so a finished child's result still wakes the root. `CancelPayload` and `CancellationRequested` carry `descendants`, and a retry with a different value raises `InvalidCommandReuse`. The root's history says that descendant agents keep running.
- `AcpServer(..., cancel_descendants=True)`. `False` makes `session/cancel` root-only.
- `cancel_agent(agent_id)`, a framework tool for agents with `subagents`. It cancels the active and queued work of one direct child and its descendants, with `TurnCancelled.reason` `cancelled_by_parent`, and returns `{"agent_id", "cancelled"}`. No turn-ended input wakes a parent, during the call or on recovery. The child stays reusable through `send`. Over ACP it projects as an ordinary tool call, withheld when `view` hides the child.

Compatibility and upgrade:

- `cancel_agent` is a reserved tool name for agents with `subagents`.
- Drain older coordinated workers before relying on root-only cancel: an older owner ignores `descendants` and cancels the whole tree.

## 2.0.1

Fixed:

- Tool and `submit_output` schemas no longer use `$ref`/`$defs` except for recursive models. A shared branch `type` is set on `anyOf`/`oneOf` (past a `null` branch only when the field has a default), and `discriminator` is dropped. Some OpenAI-compatible hosts returned object arguments as JSON strings.
- A recursive root `output_schema` now declares `type: object`.

## 2.0.0

Added:

- `Runtime.watch_tree(root_id)` yields `TreeChange` snapshots (the root's title and model plus every actor status) on each tree change, from local signals and coordinator notices, without reading journals. Reads are shared per root and at least 250 ms apart.
- `Runtime.patch_header(agent_id, title=..., model=...)` changes a title or model from any Runtime and signals tree watchers. Route application title and model changes through it.
- `InputQueued.sender_id` and `InputQueued.origin` record who queued an input. The text is unchanged.
- `tantra.acp` serves a Runtime over ACP v2 (`protocolVersion: 2`, reference schema `2.0.0-alpha.8`) and the `tantra` profile v1: journal cursors, client command IDs, one fenced writer per root, lazy child transcripts, and a shared input queue. It classifies agent inputs by `origin`, falling back to text prefixes only for older journals, and its child mirror and title/model updates follow `watch_tree`, so they reach other connections without a coordinator. `AcpServer(runtime, *, agent, scope, policy, encode_prompt, decode_input, view, models, name)` offers `connect(transport, auth)`, `websocket(*, bind, allowed_origins, allow_missing_origin=False)` (an ASGI `AcpWebSocket`) and `serve_stdio(auth)`. `AcpAuth` is the connection identity; `AcpAccess` is the policy input. Limits: 8 open sessions per connection, 1 MiB inbound frames, 10 s per WebSocket send. No dependency beyond pydantic. See the [ACP server reference](https://malayh.github.io/tantra/docs/reference/acp/).
- `submitted_by` on `InputQueued`, `AskAnswered` and `CancellationRequested`, a string of 1–256 characters. `Connection.send`, `prompt`, `answer` and `cancel` accept it, and turns expose it as `TurnContext.submitted_by` and tool `Context.submitted_by`.
- `Runtime.lookup_ask(root_id, ask_id)` returns a `LocatedAsk` (exported from `tantra`) for the original `AskRaised` anywhere in a root tree. `PostgresStore.lookup_ask` serves it from an index; other stores fall back to reading history.

Changed:

- `CoordinatedStore` publishes a `header` change when an append changes an actor's status, current turn or last turn.
- Tool approval asks carry the call's effective arguments in `extra["arguments"]`.
- `bash` runs commands with stdin set to `/dev/null`.

Fixed:

- Tool call IDs are unique within a session. A provider ID already used in the turn or the loaded history, including the `OpenAICompatible` fallback `call_{index}`, is rewritten to a deterministic `call_<24 hex>` before it is journaled. Repeats used to alias spawn, send and finish commands and drop tool results from the assembled context. IDs without a collision are unchanged.
- `watch_tree` reflects lease expiry within about one coordinator `catch_up_interval`, and yields no snapshot once Runtime shutdown starts.

Performance:

- Coordinated PostgreSQL transactions run fewer statements: publishing a change is one statement, the root lock check and fence authorization are fused, and lease expiry is checked in the locking read.
- On PostgreSQL, Runtime reads an actor tree in one recursive query (`watch_tree` snapshots, `tree_status`, recovery, close and request handling), and `watch_tree` and `tree_status` read actor activity in one query. Observations carry a tree change marker, so journal-only changes no longer page `coordinator_changes`. Coordinated journal reads check root membership without loading the header.

Compatibility and upgrade:

- See the [2.0 migration guide](https://malayh.github.io/tantra/docs/guides/migration-2.0/) for the upgrade steps.
- A retry compared with an event written before `origin` existed ignores `sender_id` and `origin`, so recovery and command retries over older journals stay duplicates. Drain older coordinated workers before rolling out: an older owner rejects retries of inputs that carry the new fields.
- `tantra.testing.store_conformance` now requires a store to detect duplicate inputs by comparing events with `==`, so that a legacy `InputQueued` equals its structured retry. Custom stores that compare serialized events fail it.
- Every worker must run this release before migration 12 is applied: an older worker on the migrated schema writes no `journal_index` row for `AskRaised`, and `PostgresStore.lookup_ask` has no fallback, so `tantra.acp` never issues those asks. Stop all PostgreSQL writers, run `PostgresStore.setup()`, and restart only upgraded workers. Migration 12 adds `journal_index.ask_id` and the partial index `journal_ask_idx`, then backfills ask entries by decoding every stored event in pages of 1,000. It runs in the setup transaction and holds an exclusive lock on `journal_index` until it commits, so its time grows with total journal size and every append and indexed read waits for it. Allow a maintenance window. Journal bodies and sequences are unchanged.
- Migration 13 only adds the partial index `coordinator_changes_tree_idx`. It is built in the setup transaction, so change inserts wait for it briefly. Older workers ignore it. As with earlier migrations, run `PostgresStore.setup()` with writers stopped; mixed-version writers are unsupported during migration.
- `pg_notify` change payloads are now built by PostgreSQL: the same keys, with different whitespace.

## 1.4.0

Added:

- `just bench cpu` with deterministic streaming, reader fan-out, long-history and compaction workloads; per-worker Python CPU, PostgreSQL CPU/SQL/WAL, delivery timing, independent commit/replay evidence, and JSON/HTML comparisons.

Changed:

- Built-in PostgreSQL provider deltas commit in bounded batches: 25 ms, 32 events, or 64 KiB of serialized payload, whichever comes first. Each event retains its original body, sequence and replay cursor, and reaches readers only after commit. Tool progress and lifecycle events remain immediate.
- Journal-only observation hints coalesce for 25 ms; control changes remain immediate. Result waits use indexed terminal evidence instead of checking after every delta, with authoritative periodic catch-up for missed notifications.
- PostgreSQL combines transaction settings and ownership checks, updates delta-only headers without Python hydration, and uses a generated root key for indexed session lookup.
- Runtime streams share bounded recent committed pages with independent cursors and event copies. Internal observation reads only interested actors; public root-wide observers retain their existing behavior.
- The built-in compactor reuses unchanged request projections within one invocation. Full history remains the default; direct and custom compactors retain their existing path.

Fixed:

- Closing public event generators immediately releases underlying streams, watchers and cache interest. Deletion and shutdown discard cached evidence and prevent older in-flight reads from repopulating it.
- Streaming flush-boundary handling preserves the committed prefix during cancellation, ownership loss, database failure and uncertain commit acknowledgments, without blindly retrying a possibly committed batch.

Compatibility and upgrade:

- Stop all PostgreSQL writers before running `PostgresStore.setup()` with 1.4. Migration 10 adds a terminal-event index; migration 11 adds stored `sessions.root_key` and replaces the root expression index. Migration 11 rewrites session rows, so allow a maintenance window; journal bodies and sequences remain unchanged. Mixed-version writers are unsupported during migration.
- Public Runtime APIs and required Store/Coordinator interfaces remain unchanged. SQLite, memory, custom stores and custom coordination paths retain immediate delta commits. A custom `Hook.on_event` implementation disables batching; inheriting the base no-op remains eligible.
- Received but uncommitted buffered deltas can be lost on process death, cancellation, deletion, shutdown or ownership loss. The journal retains the committed prefix, and published events remain durable. Batching and observation can each add 25 ms before database and scheduler delay.
- Nine primary repository PostgreSQL streaming benchmarks, with five warmed trials each, reduced median Python CPU by **72.8–93.3%** and PostgreSQL CPU by **58.0–94.1%** against the pre-optimization baseline. These measure runtime overhead with deterministic providers. Incremental P4 results are mixed: eight-reader delivery P95 increased **42.19 → 73.77 ms**, and one 1,000-observer campaign increased active Python CPU **9.6%** versus P3. Full-history/replay costs and individual regressions remain documented follow-ups.
- The final package/bench/durable PostgreSQL stress suite passed **1,090 tests with zero skips**, alongside lint, an independent Ponytail review, and both scale workloads. See the [1.4 migration guide](https://malayh.github.io/tantra/docs/guides/migration-1.4/).

## 1.3.0

Added:

- `Runtime.delete(root_id, allow_active=False)` for atomic root-tree deletion on PostgreSQL, SQLite, and memory stores, with active-work protection and permanent ID-only markers. Coordinated deletion revokes ownership and ends readers with `SessionNotFound`.
- `Runtime.cleanup` with root-ID, scalar metadata, and tree-age selectors; read-only dry runs; deletion-safe cursors; per-tree outcomes; and transactional revision checks. Exported `CleanupSelector`, `CleanupResult`, `CleanupReport`, and `SessionBusy` support application-managed retention.
- PostgreSQL command and lifecycle indexes, transactional operational checkpoints, and targeted completed-turn reads. Command retries and recovery no longer require full-journal scans when projections are current.
- Optional `Runtime(history_mode="compacted")` loading of the latest summary and retained event window. Full history remains the default for hooks, callable prompts, and custom compactors.
- A repository Runtime bench with isolated durable Compose PostgreSQL, two workers, synthetic scale/fault campaigns, fresh OpenRouter behavioral tests, exact recorded replay, persistent token budgeting, and JSON/HTML reports.

Changed:

- PostgreSQL uses the official Psycopg pool with up to four data connections per worker, separate control/LISTEN connections, and bounded concurrent dispatch across unrelated roots. The `[postgres]` extra now includes `psycopg-pool`.
- Observers share batched catch-up queries and receive routed notification hints. Unused watchers are released, and expired transport records drain in bounded batches without removing lifetime command identity.
- PostgreSQL tracks native actor update timestamps for tree-age selection without reading historical event bodies or writing the root on each child event. Header replacements stamp their actual edit time.

Fixed:

- Recovery and result-wait races around queued work, child notifications, shutdown, writer replacement, and stale ownership observations.
- Broken control connections reconnect before lease transactions; database cleanup failures no longer retain local drain tasks. Lease transactions are not automatically replayed.

Compatibility and upgrade:

- Stop all PostgreSQL writers before running `PostgresStore.setup()` with 1.3. It applies migrations 5–9, backfills journal projections and actor timestamps, and adds deletion markers/write guards; mixed-version writers are unsupported. Original event bodies, sequences, and replay cursors remain unchanged.
- Required Store/Coordinator interfaces remain unchanged; custom implementations retain full-read fallbacks for optimized reads. SQLite setup adds permanent deletion markers idempotently. Filesystem and unsupported custom stores/coordinators reject deletion and cleanup before mutation. Compacted mode changes turn context only; retained sessions keep complete public replay.
- See the [1.3 migration guide](https://malayh.github.io/tantra/docs/guides/migration-1.3/). Serial latency regressions and full-history decoding costs remain documented follow-up work.

## 1.2.0

Added:

- An optional `Coordinator` contract and `PostgresCoordinator` implementation for one leased execution owner across Runtime processes, with database-time generations and transaction-bound fencing for execution writes.
- Distributed writer claims, command forwarding, journal wake-ups with bounded catch-up reads, and whole-tree takeover recovery for accepted work, started turns, asks, cancellation, and child lifecycle state.
- A two-backend Sarathi deployment behind Nginx with shared PostgreSQL and uploads, cross-backend writable reconnects, read-only viewers, and an isolated deterministic failure-test stack.

Compatibility and limits:

- `Runtime(..., coordinator=None)` keeps the 1.1 single-process behavior. Existing PostgreSQL stores upgrade additively through `PostgresStore.setup()`; coordinated Runtime requires the same `PostgresStore` object in `PostgresCoordinator` and an awaited `Runtime.start()`.
- Takeover waits for lease expiry, interrupts abandoned started turns instead of replaying model or tool work, and cannot undo external side effects. PostgreSQL remains the availability and ordering authority; there is no automatic startup scan, persistent browser outbox, or cross-process child worker pool.

## 1.1.0

Breaking:

- Human asks and approval-gated tools are root-only. Child `ctx.ask()` calls now return an ordered tool error, static child `ask` permissions fail Runtime construction, and dynamic child escalations fail without emitting `AskRaised`.
- Plain child turn completion no longer implies result delivery. It leaves the actor reusable and queues a status-only lifecycle input for the direct parent; children must call `finish(result)` to close and deliver a final result.

Added:

- Durable `TurnSummary` and `ActorStatus` snapshots, including queued and active turn state, plus `Runtime.status()`, breadth-first `Runtime.tree_status()`, and the direct-child `status` framework tool.
- Deterministic parent notification and activation-time reconciliation for completed, failed, cancelled, and interrupted child turns that do not call `finish()`.
- Optional durable child display names on `spawn`, session headers, actor events, status responses, and Sarathi UI surfaces. Actor type and UUID semantics are unchanged.
- Lazy, cached `OpenAICompatible` model-limit discovery with explicit configuration precedence, OpenRouter-style metadata support, and per-field fallback to a 128k context window and 4,096-token output limit. `Provider.limits()` may now be synchronous or awaitable.
- Complete-request compaction budgeting across prompts, execution-environment blocks, history, tools, and parameters; bounded recent history; chained durable summaries; one-shot recovery from confirmed pre-output context overflow; and explicit irreducible-payload failures.
- A standardized execution-environment system block for capability guidance, keeping skill bodies on demand.
- Sarathi actor-status polling and lazy child-journal subscriptions, including drawer replay, retained cursors, inactive child placement, replay hydration, and stable manual scrolling.
- Sarathi general-purpose named `subagent` actors and a packaged `research` skill with `shallow`, `normal`, and `deep` effort levels.

Changed:

- `PruneThenSummarize` remains opt-in and now defaults to an 80% trigger, a 4,096-token buffer, a 20,000-token recent window, and a 4,096-token summary-output cap. It preserves application and framework system blocks and tool schemas byte-for-byte.
- Parent-child `send` accepts only a direct edge and activates the target with the root tree identity.
- Sarathi works inline by default, delegates only when requested or independently useful, keeps approval-gated memory writes on the root, and exposes only non-interactive capabilities to subagents.

Compatibility:

- Existing 1.0 headers, events, and stores load without migration or historical status and name backfill. Legacy Sarathi `researcher` history remains readable but cannot resume or accept new work.


## 1.0.0

Breaking:

- Replaced the legacy coordinator and merged turn stream with the process-scoped `Runtime`, explicit actor subscriptions, and independent root and child journals.
- Store appends now assign sequences atomically. A root tree has one live Runtime process and the newest local writable connection owns mutations.
- Removed legacy integration helpers and child and cancellation events. `ChildCreated`, `AgentFinished`, and `CancellationRequested` are the durable actor events.
- `Hook.on_event` now receives `LoggedEvent`, including the actor id and durable sequence.

Added:

- Durable command deduplication for input, ask responses, and cancellation.
- Atomic model updates through `Store.patch_header(model=...)`.
- Runtime writer takeover, descendant asks, recursive cancellation, and fresh-process interruption.

Changed:

- Streaming deltas are persisted and replayed per actor.
- Package and documentation now define the 1.0 single-process execution contract.

## 0.4.0

Added:

- The legacy coordinator accepted a `Tracer` and emitted one trace per execution segment: an `invoke_agent` root, a `chat` span per model call (retries included), an `execute_tool` span per tool call, a `compact` span per compactor consultation, and nested `invoke_agent` spans for subagents under the tool that spawned them. Attributes are OpenTelemetry GenAI semantic conventions only — no vendor keys — so any OTLP backend ingests them.
- The `[telemetry]` extra (`opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http`) and `tantra.telemetry.Telemetry`, the OpenTelemetry implementation. `Telemetry(tracer_provider=None, *, capture_content=False, max_content_chars=32_768)`; content capture is off by default and content attributes are truncated with a `…[truncated N chars]` marker. On this path tantra configures no exporter, sets no global tracer provider and reads no `OTEL_*` environment variable. Not re-exported from `tantra` — import it from `tantra.telemetry`.
- `Telemetry.from_env(*, capture_content=False, max_content_chars=32_768)`, the one-line setup: it builds a `TracerProvider` + `BatchSpanProcessor` + OTLP HTTP exporter entirely from the standard OpenTelemetry environment, installs it as the global tracer provider, and returns `None` when neither `OTEL_EXPORTER_OTLP_ENDPOINT` nor `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. `Telemetry.shutdown()` flushes and closes the provider the instance holds.
- `Tracer` and `NullTracer` in `tantra.tracing`, exported from `tantra`. The core seam takes no new dependency; the base install is unchanged.
- `TurnContext.tracer`, so a custom `Compactor` can trace its own model call.
- An optional `provider_name` class attribute on providers, read via `getattr` and reported as `gen_ai.provider.name` — `openai` on `OpenAICompatible`, `fake` on `FakeProvider`, `unknown` for a provider that does not set it.

Changed:

- `PruneThenSummarize`'s summarizer call is traced as a `chat` child of the `compact` span, and its usage is now captured rather than discarded.

## 0.3.0

Added:

- `web_fetch(proxy=...)` takes one proxy URL (`http`, `https`, `socks4`, `socks4a`, `socks5`, `socks5h`; credentials go in the URL) and applies it to every redirect hop and every retry. An invalid value raises `ValueError` at construction, naming the scheme it received and never the URL. Proxy failures retry inside the existing 3-attempt budget and then raise a proxy-specific message; there is no fallback to a direct connection.

Changed:

- The `[web]` extra now pulls `tenacity>=9`.
- `web_fetch`'s retry loop is restructured on tenacity, with unchanged behaviour.

## 0.2.0

Breaking:

- The `Store` protocol gains `memory_put` / `memory_get` / `memory_all` / `memory_search` and `patch_header`. A store written against 0.1.0 must add them.
- `ToolCallStarted` now precedes every `ToolCallCompleted`, including the error and denial paths. Logs written before 0.2.0 replay without that pairing.
- `Memory.delete(mid, *, scope=None)` returns `bool` instead of raising; `Memory.supersede(old_id, new, *, scope=None)` takes a scope.

Fixed:

- Cancellation no longer livelocks a sequence race while a session is busy.
- A cancel absorbed on a turn's final sample or on the submit-output path now ends the turn cancelled instead of being dropped.
- Recovery re-emits a pending ask without mistaking it for a new log entry.
- Memory metadata matching fails closed: a key the row lacks never matches, and `None` matches only a stored `None`.
- `delete` is idempotent and scope-checked — an unknown id and a row outside the scope both return `False`.
- The provider reads `reasoning_content` as well as `reasoning`, so reasoning from either dialect streams.

Added:

- Recursive cancellation flags every descendant session deepest-first.
- `Store.patch_header(sid, *, title=..., status=..., pending_ask=..., usage=..., metadata=...)` for lost-update-free header edits; `metadata` merges shallowly. The turn loop no longer rewrites the whole header mid-turn.
- `memory_all(metadata=..., include_dead=False)`, with deleted and superseded rows excluded by default.
- `memory_tools(scope=...)` builds tenant-scoped `memory_write` / `memory_recall` tools.

## 0.1.0

Initial release.
