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
    telemetry=None,
)
```

`create(agent, *, session_id=None, model=None, metadata=None) -> UUID` creates an idle root and records its header.

`connect(root_id, *, after=0, writable=False) -> Connection` validates a root. A connection iterates only the root journal.

`events(agent_id, *, after=0) -> AsyncIterator[LoggedEvent]` replays and tails any root or child journal without activating it.

`aclose()` stops new work, interrupts known active turns, releases writer authority, and leaves application-owned providers and stores open.

## Connection

Enter a connection with `async with`. Read-only connections iterate events. A writable connection claims the root tree and replaces any older writer in the same Runtime.

- `send(input, *, command_id) -> CommandReceipt` accepts a durable root input.
- `prompt(input, *, command_id) -> TurnResult` accepts the same input and waits for its terminal event.
- `answer(ask_id, response, *, command_id) -> CommandReceipt` resolves a live root or descendant ask.
- `cancel(*, command_id) -> CommandReceipt` cancels active and queued work in the live tree.

Cancelling the local wait for `prompt` does not cancel the accepted turn.

## Results and errors

`LoggedEvent` contains `agent_id`, integer `seq`, and `event`. `CommandReceipt` contains the UUID and a `duplicate` flag. `TurnResult.outcome` is `completed`, `failed`, `cancelled`, or `interrupted`.

Writer misuse raises `WriterRequired` or `WriterReplaced`. Command UUID conflicts raise `InvalidCommandReuse`. Unknown sessions raise `SessionNotFound`; stale typed asks raise `AskExpired`.
