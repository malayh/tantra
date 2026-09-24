# Durability and crashes

Durability means commands and events are appended before they are published. It does not mean in-flight model or tool work is reconstructed after process loss.

## Commands and cursors

Every mutation through a writable `Connection` is idempotent by UUID `command_id`: `send`, `prompt` acceptance, `answer`, and `cancel`. Reusing the UUID with different content raises `InvalidCommandReuse`. `send`, `answer`, and `cancel` return `CommandReceipt`, whose `duplicate` field reports an identical replay. `prompt` returns the original `TurnResult` for an identical replay and does not expose a duplicate flag. `Runtime.create` creates identity and does not take a command ID.

Journal sequence numbers start at 1. Cursor 0 means replay from the beginning. Readers save the last consumed `seq` and reconnect with `after=seq`.

## Accepted work survives readers

A writable connection can call `send()` and disconnect immediately. The process-owned actor continues. Event readers replay from storage and then wait on a process-local notification; they do not buffer or throttle execution.

## Typed asks are live and root-only

Root `ctx.ask(...)` writes `AskRaised` and waits on an in-memory future. The current writable root connection may answer it. Ordinary input never answers an ask. A child cannot raise a human ask; it must use `send()` to ask its direct parent for help.

If the Runtime closes or the process dies, the future is gone. The unfinished turn is later marked `interrupted`; the old ask has expired. Send a new root command to continue the conversation.

## Child status and lifecycle delivery

Session headers retain the latest actor state, current turn, last terminal summary, and sequence. `status()` and `tree_status()` read these snapshots without activating actors or replaying journals. Existing headers load with defaults; historical state is not backfilled.

After a child turn ends without `finish()`, Tantra durably queues one deterministic status-only input for the direct parent. Activation-time reconciliation repairs a stop between recording the child terminal and enqueueing the parent input. Successful `finish()` uses its result-delivery input instead. Both paths preserve FIFO ordering, and child assistant text remains only in the child journal.

## Crash contract

Connecting or subscribing never activates an actor. After a crash, a later root send activates the root, records an unmatched started turn as interrupted, and drains inputs that were accepted but never started. Model and tool work from the interrupted turn is never repeated automatically.

A shared store does not coordinate live execution across processes. Route one root tree to one Runtime process. Writer generations and subscriber wakeups are process-local; Tantra has no heartbeat, distributed lock, or cross-process event bus.
