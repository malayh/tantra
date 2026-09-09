# Sharp edges

## Accepted work is independent of readers

Disconnecting a reader or cancelling a local `prompt()` wait does not cancel accepted work. Use `Connection.cancel(command_id=...)` when cancellation is intended.

## Asks do not survive process loss

Only a live writable root connection can answer a current ask. After interruption, send a new root input; do not reuse the expired ask ID.

## One tree, one process

Writer replacement is process-local. Never drive the same root tree from multiple Runtime processes, even when they share a store.

## Actor streams are separate

A root cursor says nothing about a child cursor. Subscribe to every discovered child independently and persist each last sequence.

## Sync tool effects may outlive cancellation

Runtime discards a late worker-thread result, but it cannot undo the underlying side effect. Make side-effecting tools idempotent with their own operation keys.

## Store setup is application-owned

Call `setup()` for SQLite and PostgreSQL before creating Runtime work. Runtime does not own provider, store, memory, embedder, or telemetry cleanup.
