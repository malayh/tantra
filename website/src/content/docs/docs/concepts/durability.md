# Durability and crashes

Durability means commands and events are appended before they are published. It does not mean in-flight model or tool work is reconstructed after process loss.

## Commands and cursors

Every mutation through a writable `Connection` is idempotent by UUID `command_id`: `send`, `prompt` acceptance, `answer`, and `cancel`. Reusing the UUID with different content raises `InvalidCommandReuse`. `send`, `answer`, and `cancel` return `CommandReceipt`, whose `duplicate` field reports an identical replay. `prompt` returns the original `TurnResult` for an identical replay and does not expose a duplicate flag. `Runtime.create` creates identity and does not take a command ID.

Journal sequence numbers start at 1. Cursor 0 means replay from the beginning. Readers save the last consumed `seq` and reconnect with `after=seq`.

The built-in PostgreSQL path batches provider text, reasoning and tool-argument deltas for at most 25 ms, 32 events or 64 KiB before starting a commit. Events keep their individual bodies and sequences and are delivered only after commit. Process death, cancellation, deletion, shutdown or ownership loss can discard received but uncommitted buffered output; the journal retains the committed prefix. Custom event hooks and other storage paths retain immediate commits. See the [1.4 upgrade guide](../guides/migration-1.4.md) for timing and compatibility details.

## Accepted work survives readers

A writable connection can call `send()` and disconnect immediately. The execution owner continues. Event readers replay from storage and do not buffer or throttle execution. Without a coordinator they wait on a process-local notification; `PostgresCoordinator` also publishes cross-process notices and performs bounded catch-up reads.

## Typed asks are live and root-only

Root `ctx.ask(...)` writes `AskRaised` and waits on an in-memory future. The current writable root connection may answer it. Ordinary input never answers an ask. A child cannot raise a human ask; it must use `send()` to ask its direct parent for help.

If the Runtime closes or the process dies, the future is gone. The unfinished turn is later marked `interrupted`; the old ask has expired. Send a new root command to continue the conversation.

## Child status and lifecycle delivery

Session headers retain the latest actor state, current turn, last terminal summary, and sequence. `status()` and `tree_status()` read these snapshots without activating actors or replaying journals. Existing headers load with defaults; historical state is not backfilled.

After a child turn ends without `finish()`, Tantra durably queues one deterministic status-only input for the direct parent. Activation-time reconciliation repairs a stop between recording the child terminal and enqueueing the parent input. Successful `finish()` uses its result-delivery input instead. Both paths preserve FIFO ordering, and child assistant text remains only in the child journal.

## Crash contract

Read-only connections, subscriptions, and status reads never activate an actor. Without a coordinator, a later root send recovers after a crash: it activates the root, records an unmatched started turn as interrupted, and drains inputs that were accepted but never started. With `PostgresCoordinator`, a writable connection or mutation can claim and recover an unowned or expired root. Recovery never repeats model or tool work from an interrupted turn.

A shared store alone does not coordinate live execution across processes. With `coordinator=None`, route one root tree to one Runtime process.

Tantra 1.2 can pair `PostgresStore` with `PostgresCoordinator`. Exactly one Runtime owns a root under a renewable lease, execution writes are fenced by its generation, and writers may reconnect through another process. Commands forward to the owner, while subscriptions replay journals and catch up across processes. After owner loss, the next writable reconnect or mutation waits for lease expiry, interrupts abandoned started turns, expires asks, repairs child lifecycle state, and drains accepted unstarted inputs.

Recovery never repeats model or tool work from an interrupted turn and cannot roll back an external side effect. PostgreSQL is the availability and ordering authority. See [Migrate to 1.2](../guides/migration-1.2.md) for setup and limits.
