# 014 — Session cleanup

## Goal

Give applications a safe Tantra API for permanently deleting root sessions and their descendants, individually or through bounded selectors.

## Scope

- In: PostgreSQL, SQLite, and memory stores; coordinated deletion; selectors; dry runs; cancellation; retries; stream termination; deterministic verification.
- Out: filesystem deletion, chat reset, partial journal pruning, keep-newest rules, grouping, schedulers, application UI, and Osuite integration.
- Shared memories, provider logs, telemetry, recordings, and backups remain outside session ownership.

## Decisions

- Delete complete trees. Retained sessions keep their journals and replay cursors unchanged.
- Select explicit root IDs, scalar metadata scope, and an age cutoff, combined with AND.
- Age is the latest header or journal change anywhere in the tree. Viewing history does not refresh it.
- Skip active trees unless allow_active=True. Running turns, queued inputs, pending approvals, and accepted mutating requests count as active.
- Applications authorize cleanup. It is independent of writer tokens and never an agent tool.
- Each tree deletion is atomic. A batch reports partial progress.
- Permanent ID-only markers prevent root and child UUID reuse.

## API contract

- `await runtime.delete(root_id, *, allow_active=False) -> bool`: True after deletion, False if absent; reject live child IDs and raise SessionBusy for active work unless allowed.
- `await runtime.cleanup(selector, *, dry_run=True, allow_active=False, limit=100, after=None) -> CleanupReport` ships in P1.
- CleanupSelector accepts optional root IDs, scalar metadata matches, and timezone-aware inactive_before. Missing keys differ from null; booleans differ from numbers; nested filters are rejected.
- Reject unscoped selectors. Explicitly empty ID collections match nothing.
- Process one page of 1–1,000 roots, oldest first by (created_at, id), using a value-based cursor that survives deletion.
- Reports contain IDs, outcomes, counts, continuation, and bounded errors: candidate, deleted, absent, active, changed, failed, unknown.
- Dry runs write nothing and call no provider. Execute an exact reviewed set using its returned IDs and original filters.
- Unsupported stores/coordinators fail before mutation. Required Store and Coordinator protocols stay unchanged.

## Deletion and selection behavior

- Follow durable parent relationships without listing limits, including legacy descendants lacking root_id.
- PostgreSQL selection/activity checks use headers and indexed lifecycle evidence, never journal bodies.
- Recheck eligibility transactionally; remote selector execution carries a content-free tree revision digest and skips changed trees.
- Uncertain state is unsafe by default. Forced deletion does not recover/resume model work.
- Lock order: root advisory lock, coordinator root row, actor/request rows.
- Once eligible, suppress activation and cancel tasks; await cooperative cleanup outside the root lock.
- Purge journals, headers/checkpoints, lifecycle indexes, cancellation pointers, activity/recovery state, and historical transport bodies.
- Commit markers and a content-free success receipt, then revoke writer/ownership, atomically. Retain minimal fencing data; never delete the coordinator root first.
- Streams/result waits terminate with SessionNotFound; shared observation detects deletion after missed notifications or expired transport history.
- Rollback preserves the tree and releases guards; cancelled work may recover interrupted. Timeouts are unknown: retry the same root IDs, never recreate them.
- Batch roots run sequentially. Keep completed deletions, report failures, and stop admission on infrastructure failure with continuation.

## Considered and rejected

- Hide/archive: retains stored history.
- Clear history under one ID: introduces reset semantics and invalidates command/replay guarantees.
- Application SQL: bypasses fencing, tasks, and readers.
- Cancel then delete separately: permits intervening recovery/work.
- Expiring markers with transport: permits resurrection.
- Filesystem now: needs a separate crash-safe file workflow.

## Implementation phases

### P0 — Safe individual deletion · deps: none · DONE

Deliver:
- Runtime.delete, delete control payload, SessionBusy, optional deletion capabilities for PostgreSQL/SQLite/memory.
- PostgreSQL migration 8 for markers/guards and equivalent durable SQLite markers.
- Atomic purge/reply/fence ordering, active checks, task cleanup, deletion-aware observation, and API documentation.

Verify:
- Idle/running/queued/approval trees, children/grandchildren, unrelated roots.
- Two runtimes: remote owner, takeover, send/delete races, owner death, lost reply, missed notices, transport expiry.
- Rollback, retries, UUID reuse denial, late writes, complete table purge.
- Cooperative task/reader/waiter/watcher cleanup without reactivation.
- Deterministic providers and the bench's durable Compose database.

Checklist:
- [x] API and migrations
- [x] Atomic deletion and fencing
- [x] Cross-worker recovery and reader termination
- [x] Focused tests, documentation, and independent review

Implementation notes:
- Optional store deletion/marker capabilities leave the required protocols unchanged. Uncoordinated stores retain the single-process ownership contract.
- Coordinated purge retains the root fence and one content-free delete receipt; receipt, permanent markers, and ownership revocation commit together. Normal transport retention can remove the receipt without removing markers.
- Cooperative task cleanup is bounded by the request timeout and happens outside the root lock. Noncooperative external work cannot be forcibly stopped.
- Refused/failed deletion relinquishes a delete-only ownership without recovery, preserving existing writer and history. Concurrent deletion returns False after another caller commits; read-only entry serializes with deletion.

Verification:
- 763 package tests and 120 stress/bench tests passed with zero skips on isolated Compose PostgreSQL (fsync and synchronous_commit on). Includes 51 deletion cases, real process death before/after commit, running-turn rollback, and missed-notification cleanup; no paid inference.
- just lint, strict documentation build, and independent Ponytail review passed. The owned workers/database/volume were removed.
- Evidence: [verification](../stress/bench/artifacts/014-p0-checks/verification.json), [package tests](../stress/bench/artifacts/014-p0-checks/package-tests.txt), [stress tests](../stress/bench/artifacts/014-p0-checks/stress-tests.txt), [review](../stress/bench/artifacts/014-p0-checks/review.md).

### P1 — Bounded selector cleanup · deps: P0 · —

Deliver:
- Selectors, dry runs, reports, deletion-safe pagination.
- PostgreSQL migration 9: native actor updated_at from headers, maintained with header writes; SQL tree age without an extra root write per child event.
- Matching SQLite/memory header semantics; named cleanup fault scenarios in the existing bench without changing baseline/scale identities.

Verify:
- Isolation, AND filters, empty IDs, invalid filters, cutoff equality, recent descendants.
- Dry-run immutability; activity/metadata/history races skip safely.
- Paging after deletes/retries/concurrent creation/partial failure.
- 10,000 sessions with 4,000/100,000-event journals: zero PostgreSQL historical-body reads; inspect plans and report deletion time, SQL activity, observer cleanup.
- Deliberate oracle errors fail reports and exit nonzero.

Checklist:
- [ ] Selectors and cursor contract
- [ ] Transactional rechecks and partial reports
- [ ] Store parity and read bounds
- [ ] Bench evidence, documentation, and independent review

### Conventions (all phases)

- One requested phase at a time; Ponytail full; no code comments.
- Focused checks, just lint, package/bench tests, PostgreSQL stress tests with zero skips, one independent review per code phase.
- Isolated durable Compose PostgreSQL, no paid inference. Stop writers for migrations; mixed-version writers unsupported.
- Contract freeze: P0 deletion guarantees and P1 selector/report types. Update this spec before changing them and notify dependent phases.

### Keeping this spec current

- Update markers/checklists only after verification. Record only material deviations and surprising implementation details.
- Keep unresolved follow-ups here; do not pull another phase into current work.

## Open Decisions

None blocking. Filesystem cleanup and application retention need separate specs.

## Risks

- Cancellation cannot undo committed external effects or forcibly stop arbitrary blocking code.
- Large deletes can exceed transaction/lease limits; roll back rather than weaken fencing.
- Live-record deletion leaves physical reclamation/backup expiry to database policy.
- Permanent markers grow, but retain no conversation content and prevent resurrection.
