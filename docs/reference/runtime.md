# Runtime and Connection

## Runtime

```python
Runtime(
    provider,
    store,
    agents,
    *,
    default_model=None,
    max_depth=3,
    deps_factory=None,
    retry=DEFAULT_RETRY,
    hooks=(),
    default_permission="allow",
    skills=None,
    memory=None,
    compactor=None,
    history_mode="full",
    telemetry=None,
    coordinator=None,
)
```

`history_mode="full"` preserves complete history for hooks, callable prompts, and custom compactors. Opt into `"compacted"` to expose only the latest compaction marker and its retained event window while building the same model request. This affects turn context only; event replay and cursors remain complete.

With `coordinator=None`, Runtime keeps its existing single-process behavior. A coordinated Runtime must use the same Store object as its coordinator and must be started with idempotent `await runtime.start()` before create, connect, status, or event operations. Startup failures should fail application readiness.

`create(agent, *, session_id=None, model=None, metadata=None) -> UUID` creates an idle root and records its header.

`await delete(root_id, *, allow_active=False) -> bool` permanently removes a root and every descendant. It returns `True` after deletion and `False` for an absent root. Passing a live child ID is rejected. Applications authorize this control operation; it needs no writer token and is never exposed as an agent tool.

Deletion raises `SessionBusy` while a tree has running or queued work, pending approvals, or accepted mutating requests. Pass `allow_active=True` to cancel work and delete without resuming it. Cancellation cannot undo external effects already committed or forcibly stop blocking code. Cooperative tasks are awaited outside the root lock, bounded by the coordinator's request timeout (10 seconds without a coordinator).

Deletion is atomic in PostgreSQL and SQLite, and locked in memory. It removes actor headers, journals, projections, and historical coordinator request bodies. Only permanent ID markers and content-free coordination evidence remain. UUID reuse raises `SessionExists`; readers, result waits, writer operations, and reconnects fail with `SessionNotFound`. Events already delivered or buffered cannot be recalled. A timeout has an unknown outcome: retry deletion using the same root UUID.

`MemoryStore`, `SQLiteStore`, and `PostgresStore` support deletion. Coordinated deletion requires `PostgresCoordinator` using that same store. Filesystem stores and unsupported custom stores/coordinators raise `NotImplementedError` before mutation. Uncoordinated runtimes retain their single-process ownership assumptions; use PostgreSQL coordination for multiple workers.

`connect(root_id, *, after=0, writable=False) -> Connection` validates a root. A connection iterates only the root journal.

`events(agent_id, *, after=0) -> AsyncIterator[LoggedEvent]` replays and tails any root or child journal without activating it.

`status(agent_id) -> ActorStatus` reads one actor's durable header. `tree_status(root_id) -> list[ActorStatus]` reads the root and descendants breadth-first without activating actors or reading journals.

With a coordinator, `active` reflects valid distributed ownership and actor activity; database uncertainty raises `CoordinatorUnavailable` instead of reporting inactivity. Read-only event and status operations never acquire execution ownership.

`ActorStatus.name` is the header's durable display name when present and otherwise the registered `agent` type. `state` is `queued`, `running`, `awaiting_input`, `idle`, `finished`, `failed`, `cancelled`, or `interrupted`. Without a coordinator, `active` reports only whether this Runtime process owns a live task. `current_turn_id`, `last_turn`, `last_seq`, and `updated_at` support polling without consuming actor journals.

`aclose()` stops new work, durably interrupts locally owned active turns, releases execution ownership, and closes coordinator resources. It leaves application-owned providers and stores open and never cancels work owned by another Runtime.

## Connection

Enter a connection with `async with`. Read-only connections iterate events. A writable connection claims the root tree and replaces any older writer across Runtime processes. Writer ownership is separate from execution ownership.

- `send(input, *, command_id) -> CommandReceipt` accepts a durable root input.
- `prompt(input, *, command_id) -> TurnResult` accepts the same input and waits for its terminal event.
- `answer(ask_id, response, *, command_id) -> CommandReceipt` resolves a live root ask.
- `cancel(*, command_id) -> CommandReceipt` cancels active and queued work in the live tree.

Cancelling the local wait for `prompt` does not cancel the accepted turn. Coordinated event readers replay Store pages and use notifications plus bounded catch-up polling, so a reconnect can observe work owned elsewhere without activating it.

`PostgresCoordinator` shares one periodic observation query across all roots watched by a worker. Notifications route immediate hints to the affected root; the shared check recovers missed notifications, expired ownership, actor activity, and transport replies. Journal pages remain independent per consumer, and idle observers do not read journals. Runtime uses this optional PostgreSQL capability when available. Custom coordinators keep the `watch()` fallback and need no new protocol methods.

Runtime reference-counts writable and read-only connections, event generators, and result waits independently. One watcher is retained per interested root and is removed after its last user exits, including cancellation and generator close. A result wait keeps its root subscribed after the originating connection closes. Unsubscribing does not stop accepted work or lease renewal, and reconnecting resumes from the caller's durable cursor.

## Results and errors

`LoggedEvent` contains `agent_id`, integer `seq`, and `event`. `CommandReceipt` contains the UUID and a `duplicate` flag. `TurnResult.outcome` is `completed`, `failed`, `cancelled`, or `interrupted`.

Writer misuse raises `WriterRequired` or `WriterReplaced`. `LeaseLost` fences stale owners. `CoordinatorUnavailable` reports database or ownership infrastructure failure. `CommandTimeout.command_id` identifies the durable command UUID when one exists; acceptance may be unknown, so retry that UUID. `RemoteExecutionError` reports a definitive remote execution or validation failure. Command UUID conflicts raise `InvalidCommandReuse`. Unknown sessions raise `SessionNotFound`; stale typed asks raise `AskExpired`.
