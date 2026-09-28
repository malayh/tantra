# Sharp edges

## Accepted work is independent of readers

Disconnecting a reader or cancelling a local `prompt()` wait does not cancel accepted work. Use `Connection.cancel(command_id=...)` when cancellation is intended.

## Asks do not survive process loss

Only a root actor can raise an ask, and only a live writable root connection can answer it. Children must message their parent. After interruption, send a new root input; do not reuse the expired ask ID.

## One tree, one execution owner

With `coordinator=None`, writer replacement is process-local; never drive the same root tree from multiple Runtime processes. `PostgresCoordinator` allows multiple Runtime processes to serve a shared root, but only while every one uses its own coordinator and the identical `PostgresStore` object supplied to that Runtime. Bare writes to an enrolled root are fenced.

Takeover waits for the old lease to expire. PostgreSQL uncertainty is an availability error, not proof that the actor is inactive. Started model and tool work is interrupted rather than replayed, and external side effects cannot be rolled back.

## Actor streams are separate and lazy

A root cursor says nothing about a child cursor. Poll `tree_status()` for lightweight tree state. Subscribe to a child only when its detailed events are needed, and persist that actor's last sequence independently.

## Sync tool effects may outlive cancellation

Runtime discards a late worker-thread result, but it cannot undo the underlying side effect. Make side-effecting tools idempotent with their own operation keys.

## Store setup is application-owned

Call `setup()` for SQLite and PostgreSQL before creating Runtime work. Runtime does not own provider, store, memory, embedder, or telemetry cleanup.
