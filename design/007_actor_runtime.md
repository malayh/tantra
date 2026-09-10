# Actor Runtime — Spec

> Supersedes the unshipped execution design in `006_async_subagents.md`. The
> earlier document remains as design history; this specification is the sole
> implementation contract for Tantra 1.0.0.

## Goal

- Replace `Harness` with a small process-wide `Runtime` built from independent
  agent-session actors.
- Give every agent a FIFO inbox, one active turn, an independent durable event
  log, and an integer replay cursor.
- Run subagents and tool calls concurrently without leases, supervisors, merged
  tree streams, or resumable in-flight work.
- Preserve Sarathi's visible interactive experience while deleting its generic
  execution coordination.

## Scope

- **In:** the Runtime API, actor activation, FIFO input, durable command
  acceptance, exact event replay, parallel tools, recursive actor subagents,
  typed asks, tree cancellation, writer takeover, Sarathi migration, Agni
  removal, and the Tantra 1.0.0 release.
- **Preserve:** `Agent`, `Tool`, `@tool`, providers, output schemas,
  permissions, hooks, memory, compaction, skills, and telemetry where they
  compose with the new loop.
- **Out:** cross-process execution ownership, leases, fencing, automatic crash
  recovery, startup scans, merged descendant streams, schedulers, job tables,
  retry policy, event buses, old-log migration, and compatibility wrappers.
- Existing Sarathi conversations are not migrated. The experimental branch
  uses fresh storage; repository history remains the way to run old sessions.
- Remove `apps/agni`. Do not preserve `Harness` only for that application.

## Decisions

- **Actor unit:** every root or subagent is an independent session actor with
  its own UUID, journal, cursor, context, FIFO inbox, and active asyncio task.
- **Runtime unit:** one `Runtime` owns providers, stores, agent resolution,
  active actors, local subscriber notifications, and writer generations for a
  process.
- **Turn unit:** one queued input produces one complete model/tool turn. Inputs
  arriving during a turn never interrupt or alter it.
- **Idle actors:** an actor has no permanent worker coroutine. Appending input
  starts one drain task, which exits when the durable inbox is empty.
- **Isolation:** subagents use asyncio tasks, not operating-system processes.
  Python agents, providers, tools, callbacks, and dependencies therefore need
  no serialization or IPC protocol.
- **Logs:** each agent has an independent append-only log and monotonically
  increasing integer sequence. No root-tree log or composite cursor exists.
- **Durability:** every event, including streamed text, reasoning, and tool-call
  deltas, is appended before publication.
- **Readers:** subscriptions read the durable journal after their cursor and
  wait on a process-local condition when caught up. They have no bounded live
  queues and cannot slow execution.
- **Writer:** one human writer owns a whole root tree. A newer writable
  connection atomically replaces the previous writer.
- **Human messages:** ordinary human input targets only the root. A typed answer
  may target an ask raised by any descendant in that root tree.
- **Actor messages:** cross-agent delivery is explicit through `send` or
  `finish`; assistant output is never forwarded automatically.
- **Subagent completion:** `finish(result)` permanently closes a child and
  delivers the result to its parent. It fails while the child has unfinished
  descendants.
- **Depth:** `Runtime.max_depth` defaults to 3. Breadth and process-wide actor
  concurrency are not capped in 1.0.
- **Tools:** async tools run on the event loop; sync tools run through
  `asyncio.to_thread`. Tool calls from one assistant response start in
  parallel.
- **Tool failure:** one failed parallel call becomes an error result for that
  call. Already-started sibling calls finish and all results are returned to
  the model in original call order.
- **Asks:** a typed ask suspends only its current live turn using an in-memory
  future. It is not reconstructable after process loss.
- **Crash contract:** connecting and subscribing never restart work. A later
  human send activates the root, marks an old active turn interrupted, and
  drains previously queued but unstarted inputs in FIFO order.
- **Jobs:** scripts and external workers use `connect(..., writable=True)` and
  `prompt()`. Tantra owns no scheduling, claiming, timeout, or retry layer.
- **Compatibility:** the final 1.0 package exports no `Harness.run()` or
  `Harness.resume()` compatibility layer and does not read old event logs.
- **Release:** this clean public and persistence break ships as Tantra 1.0.0.

Pi validates the useful separation: its low-level agent loop performs the
model/tool cycle while its stateful agent/session layer owns history, queues,
events, cancellation, and persistence. Pi's example subagent extension launches
isolated CLI processes and can run them concurrently, but uses `--no-session`
and is not a durable tree runtime. Tantra adopts the separation and concurrency,
not the subprocess boundary:

- <https://github.com/earendil-works/pi/blob/main/packages/agent/src/agent-loop.ts>
- <https://github.com/earendil-works/pi/blob/main/packages/agent/src/agent.ts>
- <https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/sdk.md>
- <https://github.com/earendil-works/pi/blob/main/packages/coding-agent/examples/extensions/subagent/index.ts>

## Public API

```python
runtime = Runtime(
    provider=provider,
    store=store,
    agents=[agent],
    default_model=model,
    max_depth=3,
)

root_id = await runtime.create(
    agent,
    session_id=session_id,
    model=session_model,
    metadata=metadata,
)

async with runtime.connect(
    root_id,
    after=cursor,
    writable=True,
) as connection:
    receipt = await connection.send(input, command_id=command_id)
    result = await connection.prompt(input, command_id=command_id)

    await connection.answer(
        ask_id,
        response,
        command_id=command_id,
    )
    await connection.cancel(command_id=command_id)

    async for item in connection:
        consume(item.event)
        cursor = item.seq

async for item in runtime.events(child_id, after=child_cursor):
    consume(item.event)
    child_cursor = item.seq

await runtime.aclose()
```

### Runtime

- ~~`Runtime(provider, store, *, default_model=None, max_depth=3)` is the
  process-scoped constructor.~~
- `Runtime(provider, store, agents, *, default_model=None, max_depth=3,
  deps_factory=None, retry=DEFAULT_RETRY, hooks=(), default_permission="allow",
  skills=None, memory=None, compactor=None, telemetry=None)` is process-scoped.
  The explicit registry resolves durable agent names after process restart;
  the remaining options preserve the existing engine integration seams.
- `create(agent, *, session_id=None, model=None, metadata=None) -> UUID`
  creates an idle root actor and durably records its header. A caller may supply
  the UUID for an application-owned URL or database row.
- `connect(root_id, *, after=0, writable=False) -> Connection` validates that
  the target is a root. The connection iterates only that root log.
- `events(agent_id, *, after=0) -> AsyncIterator[LoggedEvent]` replays and tails
  any root or child log without activating it.
- `aclose()` prevents new work, interrupts known active turns, preserves queued
  inputs, releases writer connections, and closes only Runtime-owned state. The
  application still owns provider, store, memory, embedding, and telemetry
  resource cleanup unless their existing contracts say otherwise.

### Connection

- A read-only connection is an event iterator and cannot mutate the tree.
- `writable=True` is valid only for a root. Claiming it increments an in-memory
  tree writer generation and signals the prior writer with `WriterReplaced`.
- Every mutation verifies the connection generation while accepting the
  command. A stale socket cannot race a replacement and enqueue input.
- Releasing or losing a writer connection does not stop accepted execution.
- `send(input, *, command_id) -> CommandReceipt` durably queues root input and
  returns after acceptance.
- `prompt(input, *, command_id) -> TurnResult` performs the same acceptance and
  waits for that input's terminal turn. Cancelling the local wait does not
  cancel the accepted turn.
- `answer(ask_id, response, *, command_id) -> CommandReceipt` resolves a live
  ask in the tree. It does not answer asks through ordinary messages.
- `cancel(*, command_id) -> CommandReceipt` cancels active and queued work in
  the tree. The root remains usable by later commands.

### Exported types

- `LoggedEvent(agent_id: UUID, seq: int, event: Event)`; `seq` starts at 1 and
  cursor 0 means replay from the beginning.
- `CommandReceipt(command_id: UUID, duplicate: bool)`.
- `TurnResult(agent_id, command_id, outcome, stop_reason, text, output, usage,
  error)`.
- `TurnResult.outcome` is `completed`, `failed`, `cancelled`, or `interrupted`.
- `WriterReplaced`, `WriterRequired`, `InvalidCommandReuse`, `SessionNotFound`,
  `AskExpired`, and `MaxDepthExceeded` are infrastructure/API exceptions.
- Agent failures represented by terminal log events return as `TurnResult`.
  Invalid calls, store failures, malformed command reuse, and programming
  errors raise.

### Model selection

- Model precedence is explicit `Agent.model`, then the root session model, then
  `Runtime.default_model`.
- Children inherit the root session model unless their declared `Agent.model`
  overrides it.
- Model selection is stored in the session header so later turns are stable.

## Actor State and Input Flow

Each session header contains:

- `id`, `root_id`, `parent_id`, `agent`, and `depth`.
- `model`, metadata, creation time, and permanent child-finished state.

Runtime keeps only live process state:

- `active: dict[session_id, asyncio.Task]` for current inbox drainers.
- `asks: dict[ask_id, Future]` for live typed asks.
- `conditions: dict[session_id, Condition]` for local subscriber wakeups.
- `writers: dict[root_id, WriterGeneration]` for current human ownership.

Input flow:

1. Validate writer authority or direct parent-child authority.
2. Atomically append `InputQueued` using its command ID.
3. Return the existing receipt for identical duplicate input; raise
   `InvalidCommandReuse` for different content under the same ID.
4. Start the session drain task if one is not already active.
5. The drainer selects the oldest unhandled input, appends `TurnStarted`, and
   invokes the turn engine.
6. Append exactly one terminal turn event tied to that input.
7. Continue with the next queued input or remove the drainer from `active`.

One session never runs two turns concurrently. Different sessions do.

If an actor is activated with a prior `TurnStarted` lacking a terminal event,
append `TurnInterrupted(reason="process_stopped")`; never repeat that input's
model or tool work. Unstarted inputs remain eligible and retain FIFO order.

## Events and Store Contract

Keep the useful provider and tool lifecycle events, but replace execution-owner
state with actor events. The minimum durable vocabulary is:

- `SessionCreated`, `InputQueued`, and `TurnStarted`.
- `SampleStarted`, text/reasoning/tool-call deltas, and `SampleCompleted`.
- `ToolCallStarted`, `ToolProgress`, and `ToolCallCompleted`.
- `AskRaised` and `AskAnswered`.
- `ChildCreated` and `AgentFinished`.
- `CancellationRequested`.
- `TurnCompleted`, `TurnFailed`, `TurnCancelled`, and `TurnInterrupted`.

Every event is wrapped as `LoggedEvent` only after the store assigns its
per-session sequence. Remove `seq=None`, live-only durable event variants, and
recursive `Emitted` forwarding.

The new `Store` protocol supplies only the primitives needed by the actor model:

- Create and load a session header.
- Atomically append one event and return its assigned sequence.
- Atomically append or deduplicate an input by command ID.
- Read a bounded page after a sequence.
- Load the branch needed for model context.
- Identify unhandled inputs and an incomplete active turn for one session.
- Validate the parent/root relationship for an already known session.

Implement the same semantics in `MemoryStore`, `FileSystemStore`, `SQLiteStore`,
and `PostgresStore`. No migration path from old records is required.

Subscriptions use a read-check-wait loop:

1. Read every available page after the caller's cursor.
2. Capture the local condition generation.
3. Re-read after the cursor to close the append-before-wait race.
4. Wait only if no event appeared and the condition generation is unchanged.

This creates a gap-free replay-to-live handoff without subscriber buffers,
fan-out queues, lag eviction, or cursor vectors. Cross-process wakeups are not
provided. Event retention and pruning are excluded from 1.0.

## Tool Execution

- Validate and authorize all tool calls before starting their executions.
- Start the accepted calls together with structured asyncio concurrency.
- Emit each call's start, progress, and completion events in real execution
  order.
- Assemble provider-visible tool results in the assistant's original call
  order.
- Convert a call exception into that call's tool error result. Do not cancel
  siblings merely because one call fails.
- Run synchronous callables with `asyncio.to_thread`; async callables remain on
  the event loop.
- Tree or turn cancellation cancels async awaits and abandons offloaded sync
  results. A thread or external side effect may still finish, so tools remain
  responsible for idempotent effects.

## Subagent Actors

Runtime reserves three model-visible framework tool names. An `Agent`
declaration that collides with one it receives fails at construction.

```python
spawn(agent_name, input) -> child_id
send(agent_id, input) -> receipt
finish(result) -> never
```

### spawn

- Available when `Agent.subagents` is non-empty.
- Resolves `agent_name` only from the parent's declared subagents.
- Rejects `parent.depth + 1 > Runtime.max_depth` before creating a session.
- Creates a child header containing the parent/root relationship, appends
  `ChildCreated` to the parent, queues the child's initial input, starts the
  child actor, and returns the child UUID immediately.
- Multiple spawn tool calls in one model response execute in parallel.

### send

- May target only the caller's direct parent or direct children.
- Appends directly to the recipient log using a deterministic internal command
  ID derived from the sending session, turn, and tool call.
- The recipient journal is authoritative. A retry returns a duplicate receipt
  instead of delivering twice.
- Activates an idle recipient immediately while the Runtime remains alive.
- Does not suspend the sender after durable acceptance.

### finish

- Available only to child actors.
- Validates the result against the child's output schema when one exists.
- Returns a tool error listing active child IDs if any direct or transitive
  descendant remains unfinished.
- Delivers one deterministic terminal-result input to the parent, records
  `AgentFinished` in the child log, and permanently rejects later child input.
- Terminates the current child turn without another provider call.

Ordinary assistant text remains in its own log. A child that wants to update its
parent calls `send`; a child that is done calls `finish`. There is no automatic
reply forwarding, mailbox API, `task_wait`, task-status polling, or parent
coroutine held open for child completion.

Deep trees require no second coordinator: a child is an ordinary actor with its
own declared subagents and may call `spawn` under the same depth rule.

## Asks, Cancellation, and Shutdown

### Typed asks

- `ctx.ask` appends `AskRaised` and awaits one Runtime-owned future.
- Only the current writable root connection may answer it.
- The writer may answer a descendant ask even though ordinary human messages
  target only the root.
- Ordinary inputs queued during the ask remain behind its current turn.
- A graceful shutdown appends `TurnInterrupted` and expires the ask.
- After an ungraceful process loss, the next activation identifies the
  incomplete turn as interrupted. `answer` raises `AskExpired`; a new root
  message is required.

### Cancellation

- `cancel` records the root command before acting.
- It cancels every live turn in the in-memory root tree and marks its currently
  queued inputs cancelled.
- Cancellation never scans the store for inactive historical descendants.
- Results arriving from abandoned async work or sync threads are ignored.
- A later root input starts normal execution again; child actors permanently
  closed by `finish` stay closed.

### Runtime shutdown

- Stop accepting new commands.
- Cancel known active actor tasks.
- Append `TurnInterrupted(reason="runtime_closed")` where storage remains
  available.
- Preserve unstarted inputs for activation by a later user send.
- Do not wait indefinitely for providers, tools, or children.

## Deployment Contract

- One root tree is active in exactly one Runtime process.
- Writer ownership and subscriber notification are process-local.
- A shared store does not make concurrent Runtime processes safe for the same
  live tree.
- Interactive deployments use one process or sticky routing by root ID.
- No lease, heartbeat, distributed lock, event bus, or fencing token is hidden
  behind the API.
- Process death stops all local work. Already-persisted events and inputs remain;
  no logical work restarts until a new root send.

## Sarathi Migration

Sarathi is the final implementation phase. Preserve authentication, attachment
validation, session titles, model selection, HTTP APIs unrelated to execution,
and the visible transcript UI.

Create one provider, store, `Harness`-independent agent graph, and `Runtime` in
FastAPI lifespan. Remove from `apps/sarathi/backend/src/sarathi/api/ws.py`:

- Per-socket Harness construction and mutation.
- `ConnectionHub` and server-side subscriber queues.
- Pump ownership and `run()` versus `resume()` routing.
- Recursive incomplete-turn inspection and child recovery.
- Server-side event deduplication and recursive replay merging.
- Application-owned command idempotency.

The WebSocket protocol intentionally changes while the visible UI stays
similar:

- A socket explicitly subscribes or unsubscribes from a session ID with that
  session's integer cursor.
- Server event frames carry `agent_id`, `seq`, and the durable event.
- One socket may hold read subscriptions for the root and any discovered
  children.
- A writable socket claims the root tree. A newer authenticated writable socket
  closes the old one with a retryable writer-replaced reason.
- `ChildCreated` supplies the child ID. The UI subscribes when it needs the
  child's expandable live activity.
- A browser refresh replays the root, discovers children, and replays each child
  independently from its saved cursor or from 0 when local cursor state is
  absent.
- Disconnecting every socket removes only subscriptions. Accepted execution
  continues.
- A slow socket stalls only its own store reads and sends; it cannot block an
  actor or another socket.

Sarathi runs with one application process in the supported deployment. Sticky
routing is allowed if a later deployment provides it, but cross-process live
viewing remains outside Tantra 1.0.

## Considered and Rejected

- **TaskSupervisor and durable mailboxes** — one journaled FIFO inbox and one
  drain task per active actor cover the required coordination.
- **Literal OS processes** — Pi uses them to wrap a CLI; Tantra would need to
  serialize arbitrary Python agents, providers, tools, callbacks, and deps.
- **Blocking subagent tool calls** — holding the parent open prevents the actor
  from becoming idle while children work.
- **Automatic reply forwarding** — creates unbounded ping-pong or needs
  request/reply correlation state.
- **One root-tree event log** — couples independent actors and obscures their
  histories.
- **Composite tree subscriptions** — recreate vector cursors, recursive replay,
  deduplication, and replay-to-live fan-out.
- **Connection-owned execution** — disconnects must not cancel accepted work.
- **Steering injection** — inputs are FIFO turns and never alter in-flight model
  or tool work.
- **Leases, heartbeats, and fencing** — unnecessary under the explicit
  single-process and no-recovery contract.
- **Automatic crash recovery** — repeating model calls or tools can duplicate
  external side effects.
- **Mode-specific runtimes** — `send`, `prompt`, and `events` cover interactive,
  attached, and externally scheduled callers.
- **Run-to-completion wrapper** — it would duplicate `Connection.prompt`.
- **Cross-process event bus** — no current deployment requires it.
- **Harness compatibility shim** — retains the obsolete execution contract.
- **Agni migration** — the application is removed instead.
- **Old Sarathi log migration** — the 1.0 branch starts from fresh storage.
- **Client-transparent Sarathi migration** — explicit child subscriptions are
  simpler than hiding recursive aggregation in the backend.

## Implementation Phases

Dependency graph: `P0 -> P1 -> P2 -> P3 -> P4`. These phases intentionally do
not run in parallel because they share the store, event, execution, export, and
application contracts.

### Conventions (all phases)

- Work directly on `codex/actor-runtime` in `/home/malay/Code/tantra`; do not
  create implementation worktrees.
- Write no code comments.
- Reuse existing Agent, provider, tool, permission, memory, compaction, skill,
  and telemetry components before adding abstractions.
- Use standard-library asyncio primitives. Add no queue, lock, or actor
  dependency.
- Keep the old Harness only while an un-migrated in-repo caller still needs it;
  remove it in P4 before release.
- Run `just lint` and `just test` before marking every phase done, plus the
  phase-specific verification.
- **Contract freeze:** after P1, session identity, command identity, FIFO input,
  writer takeover, cursor representation, and the public Runtime/Connection API
  change only after updating this spec and notifying dependent phases.

### Keeping this spec current

- Update the status marker on the heading and tick the checklist as work
  proceeds.
- When implementation deviates, strike the original line and state why it
  changed. Never silently rewrite a completed decision.
- After a phase lands, add only details that would surprise the next reader.
- Record deferred problems in Open Decisions or a Follow-up note; do not expand
  phase scope silently.

### Phase 0 — Journal and turn engine · deps: none · blocks all · DONE

- Add the new session journal and input primitives to
  `stores/base.py`, then implement them for memory, filesystem, SQLite, and
  Postgres.
- Simplify `events.py` around per-session sequenced durable events.
- Extract a single-turn engine in `loop.py` using existing Agent, provider,
  context, permission, memory, compaction, hook, skill, and telemetry seams.
- Run sync tools in threads and execute one assistant tool batch concurrently.
- Leave the old Harness working temporarily through its existing store path so
  the repository remains coherent.
- **Verify:** persisted deltas replay byte-for-byte; per-session sequences are
  gap-free; four store contract suites pass; two slow tools overlap; parallel
  results reach the model in call order; a blocked sync tool does not starve an
  unrelated turn.
- ~~**Verification pending:** PostgreSQL tests require Docker, which is
  unavailable in the implementation environment.~~
- **Accepted verification exception:** PostgreSQL remained unavailable; the
  user accepted the environment limitation after Memory, filesystem, SQLite,
  focused engine, lint, and full repository checks passed.
- Checklist:
  - [x] Minimal journal protocol
  - [x] Four store implementations
  - [x] Durable event vocabulary
  - [x] Single-turn engine
  - [x] Parallel sync/async tool execution

### Phase 1 — Root actor Runtime · deps: P0 · DONE

- Add `runtime.py` with process-wide Runtime state, root creation, on-demand
  inbox drainers, read-only event subscriptions, and writable connections.
- Implement `send`, `prompt`, `answer`, `cancel`, `aclose`, exported result
  types, and durable command deduplication.
- Implement last-writer-wins generations and stale-send rejection.
- Implement typed ask suspension and per-session interruption discovery.
- **Verify:** root inputs execute once in FIFO order; duplicate commands return
  the prior receipt; conflicting reuse fails; multiple readers replay exact
  sequences; a newer writer invalidates an in-flight old socket; zero-reader
  execution finishes; merely subscribing never activates work; a post-crash
  send marks only the started turn interrupted and drains old unstarted input.
- **Implementation note:** cancellation and shutdown durably terminate and
  generation-fence a turn before cancelling its task. A cancellation-resistant
  coroutine is detached and cannot append events or overwrite replacement state.
- Checklist:
  - [x] Runtime actor registry
  - [x] Connection and event APIs
  - [x] Writer generations
  - [x] Command receipts and TurnResult
  - [x] Ask, cancellation, and shutdown behavior

### Phase 2 — Recursive actor subagents · deps: P1 · DONE

- Reserve and inject `spawn`, `send`, and `finish` according to each actor's
  relationships.
- Create independent child headers, journals, contexts, drainers, and cursors.
- Add direct-edge authorization, deterministic internal command IDs, depth
  enforcement, permanent finish state, and tree cancellation.
- **Verify:** a parent returns from spawn immediately and may become idle while
  children run; siblings overlap; a child message wakes an idle parent;
  parent-child round trips require explicit sends; grandchildren run at depth
  2; depth 4 fails before session creation; root and child cursors replay
  independently; retried delivery occurs once; finish rejects unfinished
  descendants and permanently closes a completed child.
- **Implementation note:** `CancellationRequested` persists its actor-to-turn
  target set. A duplicate command resumes only unfinished cancellation from
  that set, while task-to-turn generation tracking fences post-commit local
  work without cancelling later turns.
- Checklist:
  - [x] Framework actor tools
  - [x] Independent child sessions
  - [x] Direct parent-child messaging
  - [x] Deep-tree and finish rules
  - [x] Tree cancellation

### Phase 3 — 1.0 preparation and Agni removal · deps: P2 · DONE

- Remove `apps/agni` and its workspace, documentation, and test references.
- Convert package examples and non-Sarathi internal callers to Runtime.
- Draft Runtime, actor, subagent, durability, tool, and migration documentation.
- Mark Harness as internal transitional code used only by Sarathi until P4; do
  not publish 1.0 yet.
- ~~**Verify:** all non-Sarathi examples use Runtime; the workspace contains no
  Agni references; library lint, unit, store, stress, and strict documentation
  checks pass while Sarathi remains functional on the transitional Harness.~~
- **Verified with approved boundary:** all public examples and non-Sarathi
  callers use Runtime; active Agni references are gone outside preserved design
  history and untouched Sarathi; library, store, stress, lint, lock, and strict
  documentation checks pass. Sarathi integration and checks remain deferred to
  P4 by user direction.
- **Implementation note:** Harness remains exported and covered by its legacy
  package tests solely as the transitional Sarathi dependency until P4; the
  public documentation presents Runtime only.
- Checklist:
  - [x] Agni deletion
  - [x] Non-Sarathi caller migration
  - [x] 1.0 documentation draft
  - [x] Transitional Harness isolation

### Phase 4 — Sarathi migration and Tantra 1.0.0 · deps: P3 · final · DONE

- Create one Runtime in FastAPI lifespan and move all execution ownership to it.
- Replace the WebSocket coordinator with writer connections and explicit
  root/child subscriptions.
- Update server frames, generated UI types, chat state, and transcript handling
  for per-agent integer cursors without changing the visible experience.
- Remove Harness, old collection adapters, obsolete events, old tests, and all
  remaining `run`/`resume` documentation and exports.
- Set the package version to 1.0.0; update changelog, locks, reference docs, and
  release metadata.
- **Verify:** two tabs demonstrate deterministic latest-writer takeover;
  multiple readers receive independent exact replay; mid-run disconnect does
  not stop execution; refresh discovers and replays children; child and
  grandchild activity remains expandable; root messages and descendant asks
  route correctly; cancellation stops the live tree; a slow socket does not
  delay actors or peers; fresh-process work remains stopped until a new send.
  Run `just lint`, `just test`, `just stress`, Sarathi backend tests, UI lint,
  UI tests, UI build, end-to-end tests, and strict documentation build.
- Checklist:
  - [x] FastAPI Runtime lifespan
  - [x] Explicit WebSocket subscriptions
  - [x] UI per-agent cursor state
  - [x] Harness and legacy execution removal
  - [x] Full repository verification
  - [x] Tantra 1.0.0 metadata

- Verification status (2026-09-10): complete. All 486 Tantra package tests and
  all 84 stress tests pass against the real Compose PostgreSQL service; all 68
  Sarathi backend tests and all 6 UI reducer tests pass. Ruff lint and format,
  UI lint and production build, strict documentation build, lock check,
  package build, diff check, and legacy-symbol audits pass. Credentialed Brave
  verification passes basic chat, web search, memory approval and recall,
  denial without re-requesting permission, model switching, replay without
  duplicates, deterministic writer takeover, recursive child replay and
  finish, compact running-child status with spinner, tree cancellation, and
  interruption followed by an explicit retry. Two live-runbook exceptions are
  recorded: account-isolation signup was not rerun, and the attached Brave
  control cannot transfer the local PDF fixture into a file input; all backend
  attachment tests pass.

## Open Decisions

- None.

## Risks

- **Delta storage growth:** exact delta replay increases write volume and log
  size. Accepted for 1.0; retention waits for a measured need.
- **Single-process routing:** two Runtime processes can both act on one stored
  tree. Mitigation: one Sarathi process or sticky root routing, documented as a
  deployment requirement.
- **Unbounded breadth:** depth 3 still permits many siblings. Accepted until a
  real workload establishes a useful cap.
- **Sync-tool cancellation:** an abandoned thread can finish and repeat an
  external effect after retry. Mitigation: document idempotent tool effects and
  discard late results.
- **Independent-log delivery:** a process can die between the authoritative
  recipient append and the sender's completion event. The recipient message is
  retained exactly once; the sender turn is later marked interrupted. No
  cross-log transaction or reconciliation worker is added.
- **Expired asks:** typed asks cannot resume after process loss. The UI must show
  the interruption and require a new root message.
- **Clean break:** old clients, old logs, and old Harness callers do not work on
  1.0. The major version and fresh Sarathi storage make this explicit.

## Success Criteria

- A root processes durable FIFO inputs without an event consumer owning its
  execution.
- Tool batches and independent agents run concurrently without blocking the
  event loop.
- Every subagent and grandchild has its own ID, context, journal, and scalar
  cursor.
- Parent-child messages wake idle actors without steering, mailboxes, or
  suspended parent waits.
- Readers reconnect after integer cursors with no durable gaps or duplicates.
- The newest authenticated writer is the only human writer for a tree.
- Process loss performs no new work until a human sends a new root message.
- Sarathi retains its visible interactive behavior with substantially less
  backend execution code.
- Tantra 1.0.0 contains Runtime and no public or private Harness compatibility
  layer.
