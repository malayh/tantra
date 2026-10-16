# Migrate to 1.2

Tantra 1.2 keeps the 1.1 journals and non-PostgreSQL store formats. `PostgresStore.setup()` applies the coordinator schema additively; run it during normal application startup before starting Runtime.

## Enable coordination

Coordination is optional. Keep `coordinator=None` for the existing single-process contract. To let multiple processes serve the same roots, each process creates its own `PostgresStore`, passes that same object to its own `PostgresCoordinator` and Runtime, and starts Runtime during application lifespan.

```python
from tantra import PostgresCoordinator, PostgresStore, Runtime

store = PostgresStore(DATABASE_URL)
await store.setup()
coordinator = PostgresCoordinator(store)
runtime = Runtime(provider, store, [Bot], coordinator=coordinator)
await runtime.start()
```

Fail readiness if `runtime.start()` fails. On shutdown, call `await runtime.aclose()` before closing the application-owned provider and store. Runtime closes coordinator resources but does not close the provider or store.

## Guarantees

- One Runtime owns root execution under a renewable PostgreSQL lease; its generation fences execution writes in the same transaction.
- The newest writable connection owns mutations across processes. Commands forward to the execution owner and retain UUID deduplication and per-actor FIFO ordering.
- Readers replay durable journal pages and combine PostgreSQL notifications with periodic catch-up reads.
- A writable reconnect or mutation can recover an expired owner: abandoned started turns become interrupted, asks expire, child lifecycle state is reconciled, and accepted unstarted inputs drain without replaying started work.

## Limits

- Takeover waits for lease expiry, and PostgreSQL is the availability and ordering authority.
- Model and tool work from an interrupted turn is not replayed. External side effects already started cannot be rolled back, so side-effecting tools still need their own idempotency keys.
- Read-only subscriptions and status polling never acquire ownership or trigger recovery.
- There is no automatic startup scan, persistent browser outbox, or cross-process child worker pool.
