# 008 — Child Lifecycle and Lazy Observation

## Goal

Let applications and parent agents inspect child status without consuming the child journal. Ensure every child turn termination becomes visible to its parent while preserving explicit `finish()` and reusable actor semantics.

## Scope

In:

- Durable lightweight actor status.
- Runtime status and tree-status APIs.
- Model-callable direct-child `status()` tool.
- Parent notification when a child turn ends without `finish()`.
- Root-only human asks.
- Sarathi polling and on-demand child streaming.
- Tantra 1.1.0 preparation.

Out:

- Redis or multi-process coordination.
- Generated progress summaries or mid-turn steering.
- Automatic child output forwarding or auto-finish.
- Aggregate tree journals.
- Background reconciliation workers.
- Cross-process `active` detection.

## Decisions

- Status is a persisted snapshot, never derived by replaying the journal.
- Children remain reusable until explicitly calling `finish()`.
- Every non-finish child terminal wakes its direct parent with status only.
- Parent agents may inspect direct children only.
- Applications may inspect an entire root tree.
- Child agents cannot interact with humans directly; they message their parent.
- Sarathi streams a child journal only while that child is expanded.
- Existing storage loads without migration or historical backfill.
- Public IDs remain `UUID`; Sarathi wire IDs remain lowercase UUID hex strings.

## Public API

```python
status = await runtime.status(agent_id)
tree = await runtime.tree_status(root_id)
```

Export:

```python
@dataclass(frozen=True)
class TurnSummary:
    turn_id: UUID
    outcome: Literal["completed", "failed", "cancelled", "interrupted"]
    stop_reason: str | None
    error: str | None

@dataclass(frozen=True)
class ActorStatus:
    agent_id: UUID
    root_id: UUID
    parent_id: UUID | None
    agent: str
    state: Literal[
        "queued",
        "running",
        "awaiting_input",
        "idle",
        "finished",
        "failed",
        "cancelled",
        "interrupted",
    ]
    active: bool
    current_turn_id: UUID | None
    last_turn: TurnSummary | None
    last_seq: int
    updated_at: datetime
```

`active` means a live task exists in this Runtime process. It is not a cross-process ownership claim.

`tree_status()` requires a root actor ID, returns the root and descendants breadth-first ordered by creation time then ID, reads headers only, and does not activate actors or subscribe to journals.

Agents declaring subagents receive `status(agent_id) -> ActorStatus payload`. The tool accepts direct-child IDs only, returns no journal content or generated summary, does not activate the child, and is reserved against user-tool collisions.

## Durable status snapshot

Extend `SessionHeader` with `current_turn_id: str | None`, `last_turn: TurnSummary | None`, and support for `status="queued"`.

Use one shared header reducer from every store's existing `enqueue()` and `append()` critical section:

- `InputQueued`: `queued` when no turn is executing.
- `TurnStarted`: `running`, set `current_turn_id`.
- Root `AskRaised`: `awaiting_input`.
- `AskAnswered`: `running`.
- `TurnCompleted`: clear current turn, record outcome and stop reason, become `idle`.
- `TurnFailed`: record failure and become `failed`.
- `TurnCancelled`: record cancellation and become `cancelled`.
- `TurnInterrupted`: record interruption and become `interrupted`.
- `AgentFinished`: set the existing finished mirror; public state becomes `finished`.

A later input changes a failed, cancelled, or interrupted reusable actor back to `queued`. Existing headers use defaults; do not replay or backfill historical journals.

## Child lifecycle notification

After every child terminal except successful `finish()`, queue `[agent <child-uuid> turn ended] <stable-json>`.

The JSON contains child and turn IDs, outcome, stop reason, and error when present. It contains no assistant text, tool output, or structured result.

- Use UUID5 derived from child ID and turn ID as the command ID.
- Deduplicate through `Store.enqueue()`.
- Wake the direct parent after durable acceptance.
- Preserve FIFO ordering with other parent inputs.
- Successful `finish()` keeps its existing result delivery without a second notification.
- Root terminals produce no parent notification.
- Deep trees notify one direct edge at a time.

Before a parent or child drainer processes new work, reconcile its latest child terminal against the deterministic parent command ID. This repairs a process stop between recording the child terminal and enqueueing the parent notification without a startup scan or worker.

Cross-journal atomicity remains absent. If persistent storage prevents delivery, child status remains authoritative and Runtime surfaces the storage error.

## Human interaction boundary

Only root agents may raise human asks.

- Child `ctx.ask()` becomes an ordered tool error directing the child to `send()` its parent.
- Runtime construction rejects statically configured child permissions whose effective policy is `"ask"`.
- A dynamic child permission resolving to `"ask"` fails at execution without emitting `AskRaised`.
- Remove Sarathi descendant-ask routing.
- Root asks and permission approvals remain unchanged.

## Sarathi behavior

Add `GET /api/sessions/{root_id}/actors`. The authenticated response contains the root and all descendant `ActorStatus` records and performs no activation.

- Poll every two seconds while the chat page is mounted.
- Keep only the root journal subscribed by default.
- Do not subscribe automatically on `ChildCreated`.
- Render subagent cards collapsed initially.
- Expanding subscribes from that actor's retained cursor.
- Collapsing unsubscribes that actor and its expanded descendants without cancelling execution or deleting reduced UI state.
- Re-expansion resumes from the saved cursor.
- Refresh reconstructs direct children from root replay and obtains the full descendant inventory from tree status.
- Opening a nested descendant loads and expands its ancestor journals first.
- Show every unfinished descendant in the compact footer: queued/running with a spinner, idle as `Idle — awaiting parent`, and terminal attention states by name. Finished actors leave the footer.
- Drive subagent-card status from `ActorStatus`, never only `item.final`.
- Hide synthetic lifecycle inputs while rendering the parent's response.
- Disable the composer while any tree actor is actively queued or running.

## Considered and rejected

- **Subscribe to every child:** collapsed children still transfer every delta.
- **Aggregate lifecycle journal:** recreates merged-tree ordering and cross-journal coordination.
- **Reduce journals during polling:** the observed child already contained 9,026 records.
- **Auto-finish plain child output:** prevents reusable multi-turn child actors.
- **Fail missing `finish()`:** an idle child can validly await follow-up work.
- **Progress summary or steering:** users inspect the journal on expansion.
- **Child-to-human asks:** communication crosses parent-child actor edges.
- **Redis coordination:** belongs to a separate specification.

## Implementation phases

### Phase 0 — Durable actor status · deps: none · ✅ DONE

Deliverables:

- Add `TurnSummary`, `ActorStatus`, header fields, and the shared header reducer.
- Apply status changes atomically in Memory, filesystem, SQLite, and PostgreSQL stores.
- Add `Runtime.status()`, `Runtime.tree_status()`, and the framework `status()` tool.
- Enforce root-only human asks.

Verify:

- Every store produces identical snapshots for queued, running, completed, exhausted, failed, cancelled, interrupted, asking, answered, and finished sequences.
- Duplicate enqueue does not corrupt status.
- Status reads do not activate actors or read event pages.
- Direct-child authorization and reserved-tool collisions fail deterministically.
- Root asks work; child asks emit no `AskRaised`.

Checklist:

- [x] Public types and exports
- [x] Atomic header reducer in all stores
- [x] Runtime polling APIs
- [x] Framework status tool
- [x] Root-only ask enforcement
- [x] Store and Runtime tests

Verification: Ruff check and format passed; 474 package tests passed with 16 Docker-backed PostgreSQL skips; the shared store contract also passed against the live PostgreSQL service (5 tests); `git diff --check` passed.

### Phase 1 — Parent lifecycle delivery · deps: P0 · —

Deliverables:

- Enqueue deterministic lifecycle inputs for every non-finish child terminal.
- Wake direct parents after acceptance.
- Reconcile the latest missing notice before related parent or child activation.
- Update agent-facing instructions for lifecycle messages.

Verify:

- Normal completion, `max_steps`, failure, cancellation, and interruption wake the parent once.
- No child output is forwarded.
- `finish()` delivers only its existing result message.
- Repeated recovery does not duplicate parent inputs.
- Simulated failure before and after parent enqueue converges correctly.
- Children remain reusable after every non-finish outcome.
- Grandchildren notify their direct parent, not the root.

Checklist:

- [ ] Stable lifecycle payload and UUID5 command IDs
- [ ] Parent wake-up
- [ ] Activation-time reconciliation
- [ ] Deep-tree behavior
- [ ] Failure-window tests

### Phase 2 — Sarathi lazy child observation · deps: P0, P1 · —

Deliverables:

- Add the authorized tree-status endpoint and regenerate API models.
- Replace automatic child subscriptions with two-second status polling.
- Subscribe and unsubscribe child journals on expansion and collapse.
- Drive cards, footer, spinner, and composer state from actor status.
- Remove descendant ask handling.

Verify:

- A collapsed running child transfers status but no journal events.
- Expansion replays from the correct cursor and then tails live.
- Collapse stops journal delivery without stopping execution.
- A `max_steps` or plain completion shows `Idle — awaiting parent`, and the parent receives its lifecycle input.
- Refresh discovers unfinished descendants without streaming them.
- Nested descendants load through their ancestor path.
- Multiple viewers inspect independently without affecting execution.
- Finished children disappear from the running footer.

Checklist:

- [ ] Backend endpoint and authorization
- [ ] Generated client types
- [ ] Polling state
- [ ] Lazy subscriptions
- [ ] Compact status UI
- [ ] Reducer and backend tests
- [ ] Brave verification

### Phase 3 — Documentation and Tantra 1.1.0 · deps: P0, P1, P2 · —

Deliverables:

- Document application polling, parent status checks, explicit finish, lifecycle messages, lazy event inspection, and root-only asks.
- Add migration notes for child agents previously relying on descendant asks.
- Update package version and lockfile to 1.1.0.
- Add changelog entry.
- Do not publish or tag.

Verify:

- Ruff check and format check.
- Tantra package and stress suites.
- All four store contracts against real PostgreSQL.
- Sarathi backend tests.
- Native Node reducer tests, UI lint, and production build.
- Strict MkDocs build, lock check, package build, and `git diff --check`.
- Live Brave run proves collapsed status, expansion replay, missing-finish notification, explicit finish, cancellation, and refresh.
- No child journal subscription occurs before explicit expansion.

Checklist:

- [ ] Public documentation
- [ ] Migration guide
- [ ] Release metadata
- [ ] Full automated verification
- [ ] Live browser verification

### Conventions (all phases)

- Preserve unrelated changes, including the existing `AGENTS.md` modification.
- Add no dependency, background worker, scheduler, event bus, or migration table.
- Keep UUID boundaries and stable compact JSON conventions from 1.0.
- Run focused checks plus the full phase verification before marking a phase done.
- **Contract freeze:** `ActorStatus`, lifecycle-message shape, root-only asks, and lazy subscription behavior freeze in Phase 0/1. Update this spec before changing them.

### Keeping this spec current

- Update the phase status marker and checklist during implementation.
- Record only deviations affecting behavior, contracts, scope, or phase boundaries.
- Keep routine implementation history out of the document.
- Put unresolved problems in Open Decisions or a follow-up note instead of implementing them across phase boundaries.

## Open Decisions

None.

## Risks

- `active` is process-local until the separate coordinator feature exists.
- Status polling is eventually consistent by up to two seconds.
- Parent notification spans two journals; deterministic delivery and activation reconciliation provide convergence, not a transaction.
- Expanding a long-running child may replay a large journal.
- The unpaginated tree-status response assumes the existing maximum depth and practical Sarathi tree sizes.
