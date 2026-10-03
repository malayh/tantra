# 014 — Safe chat deletion for Osuite retention

## Goal

Provide a Tantra operation that permanently removes a root session and its descendants, including active sessions. Osuite will use it to retain three chats per user/company/dashboard. This report proposes the library change; it does not implement it or delete existing chats.

## Findings

Inspected checkout and Osuite's installed dependency: `1258f12e00f93dfaf30ee8d9ec5ce7b506c279c3`, on 2026-10-02.

- `stores/base.py:Store` and `runtime.py:Runtime` expose no session deletion. `PostgresStore.patch_header` already supports titles; naming needs no new Tantra operation.
- In `stores/postgres.py:MIGRATIONS`, session events have no cascading foreign key to sessions. Deleting a header leaves its journal behind. Coordinator activity and requests cascade from coordinator roots; change notices do not.
- `coordinator.py:PostgresCoordinator.acquire` inserts a missing coordinator root before claiming ownership. Removing that row alone permits reenrollment and loses its generation fence.
- `runtime.py:_invalidate_root_locked` cancels local tasks and expires asks, but does not purge journals, terminate all streams, or clear all cached state. `_drain` can reactivate a task in its cleanup path; deletion must suppress that.
- `runtime.py:_stream` polls events indefinitely when the result is empty. Deleting rows alone does not terminate an existing reader.
- `coordinator.py:_apply_request` records the reply after running the command. `CoordinatedStore.reply` requires the ownership fence. Purging its request or invalidating ownership before recording the deletion reply would break acknowledgment.
- `coordinator_requests.envelope` contains user inputs, including Osuite draft snapshots. Existing cleanup retains these records for up to 24 hours. Deletion must remove old request bodies too, rather than purge only the event table.

Paths above are under `packages/tantra/src/tantra/`.

## Scope

- In: root deletion, descendant deletion, active generation cancellation, writer revocation, durable purge, stream termination, retries, and owner failure.
- Out: Osuite's three-chat policy, chat naming, HTTP routes, automatic retention schedules, and the separate journal performance work.
- Shared memory is outside session ownership: `MemoryRecord` has arbitrary metadata and no required session reference. Do not delete tenant-wide memories by guessing at metadata. Osuite does not enable Tantra memory.

## Proposed contract

- Add `await Runtime.delete(root_id: UUID) -> bool`. Return `True` when the tree is removed and `False` when already removed or absent. Reject a live child ID; callers must delete its root.
- This is an application control operation, independent of the chat writer token. Osuite must authorize the user and dashboard before calling it. Do not register it as an agent tool.
- Keep deletion idempotent by session ID. Repeating a call after a lost reply is safe.
- After commit, headers/listing no longer expose the tree; sends, answers, writer claims, recovery, and explicit reuse of deleted IDs fail. Existing streams terminate with `SessionNotFound` instead of waiting indefinitely. Frames already delivered or buffered cannot be recalled.
- Cancel running provider/tool tasks and expire pending asks. Deletion cannot undo an external tool side effect that already happened. Do not promise that Python cancellation can forcibly stop arbitrary blocking code.

## Recommended implementation

Use the existing owner and transaction fence, with one atomic durable purge. Avoid a separate background deletion queue or a durable multi-stage deletion workflow.

1. Route a new delete control request to the current owner. A caller may acquire an unowned root after lease expiry. Handle this request before normal journal loading and turn recovery; deletion must not resume a pending model turn just to remove it. Check the durable deletion marker before acquisition.
2. Hold the owner's root lock. Invalidate turn generations, cancel active tasks, and prevent further activation for this root. Do not await task cleanup while holding that lock: `_drain` also acquires it in `finally`.
3. In a fenced PostgreSQL transaction, enumerate the complete tree from durable headers without listing pagination. Record ID-only deletion markers for every removed actor, then delete events and headers, old coordinator request bodies, recovery state, and activity. Retain only the content-free receipt for the current delete request and the minimal coordination data needed for fencing/notification.
4. Record the successful delete reply while the ownership fence is still valid. Revoke the writer and invalidate ownership last, within the same transaction. Add a deleted change notice. The existing `_apply_request`/`reply` ordering needs an explicit deletion path; deleting `coordinator_roots` and relying on request cascades is incorrect.
5. After commit, clear local root/actor state and wake readers. Await cancellable task cleanup outside the root lock. Other instances observe the deletion notice; their periodic checks must also discover deletion when a notice is missed. Cached actors cannot reactivate or write after the durable fence changes.

Add durable ID-only deletion markers, checked transactionally by session creation and coordinator acquisition. They contain no prompt, title, dashboard snapshot, or telemetry. They prevent old connections or retries from recreating a deleted UUID even after coordination cleanup. Keep them until a separate policy deliberately changes ID-reuse guarantees.

Give the uncoordinated stores the same deletion semantics and protect their store-level operations. The PostgreSQL primitive must require a valid fence for enrolled roots; exposing unfenced SQL deletion through the public store would bypass runtime safety. Ordinary writes must check session existence/deletion under their existing locks.

On rollback, release the local deletion guard and leave the durable tree available for ordinary recovery; cancelled work may become interrupted. On owner crash after commit, deletion markers prevent recovery. A timeout is an unknown outcome: retry deletion by ID rather than creating a replacement under the same ID.

## Considered and rejected

- Hide older chats with metadata: leaves histories and snapshots stored.
- Raw SQL from Osuite: bypasses writer fencing, task cleanup, and streams; binds the application to library tables.
- Cancel, then separately delete rows: leaves a gap for another writer or recovery to submit more work.
- Delete coordinator roots first: cascades away the acknowledgment and allows reenrollment.
- Idle-only deletion: simpler, but an active oldest chat would block the requested New chat behavior.

## Implementation phase — Root deletion · deps: existing coordinator · —

- [ ] Add the root-only runtime API, delete control command, store primitive, deletion-marker migration, and documented errors.
- [ ] Implement atomic purge/reply/fence ordering and cleanup for all supplied stores. Keep pending-input payloads out of retained receipts and markers.
- [ ] Handle stream termination, active task cleanup, missed notices, and recovery without loading the full journal.
- [ ] Verify idle and active trees, repeated deletion, a live child ID, root/child ID reuse, late appends, descendant cleanup, and unrelated roots.
- [ ] Verify two runtimes: remote owner, writer takeover, send/delete races, owner crashes before/after commit, dropped acknowledgment, and missed deletion notices. Inject purge failure and assert transaction rollback. Check that old request snapshots are removed and no cancelled task reactivates.
- [ ] Run the repository's focused store/runtime/coordinator tests and checks; document the public API. No real provider is needed to establish deletion correctness.

## Osuite integration after upgrade

- Serialize chat creation/pruning within user/company/dashboard ownership. Only report a successful New chat once retention has finished; define failure handling so a failed purge does not silently leave an extra stored chat.
- Replace the selector with the authoritative retained list and drop expired browser records/outbox entries. A deleted chat in another tab must stop reconnecting and refresh the list.
- Derive the stable title from the first accepted user message, normalize whitespace, and cap at 10 characters through the existing header API. No naming endpoint or model call.

## Keeping this report current

- Update the phase marker/checklist when the deletion work starts. Record the shipped API and any change to the atomicity or ID-reuse guarantees.
- Keep this phase separate from the journal-scaling work and Osuite retention integration.
