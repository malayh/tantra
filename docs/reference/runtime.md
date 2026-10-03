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

## Results and errors

`LoggedEvent` contains `agent_id`, integer `seq`, and `event`. `CommandReceipt` contains the UUID and a `duplicate` flag. `TurnResult.outcome` is `completed`, `failed`, `cancelled`, or `interrupted`.

Writer misuse raises `WriterRequired` or `WriterReplaced`. `LeaseLost` fences stale owners. `CoordinatorUnavailable` reports database or ownership infrastructure failure. `CommandTimeout.command_id` identifies the durable command UUID when one exists; acceptance may be unknown, so retry that UUID. `RemoteExecutionError` reports a definitive remote execution or validation failure. Command UUID conflicts raise `InvalidCommandReuse`. Unknown sessions raise `SessionNotFound`; stale typed asks raise `AskExpired`.
