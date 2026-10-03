# Migrate to 1.3

Tantra 1.3 preserves journal bodies, event sequences, replay cursors, and the required Store/Coordinator interfaces. PostgreSQL gains indexed commands, operational checkpoints, pooled access, and shared observer catch-up. Filesystem, SQLite, and memory store formats stay unchanged.

## Upgrade PostgreSQL

1. Stop every application worker and other writer using the database. Mixed 1.2/1.3 writers are unsupported during migration.
2. Install `tantra-harness[postgres]==1.3.0` in the application environment. The extra includes the official `psycopg-pool` dependency.
3. Run setup once using the existing database and schema, before starting Runtime:

   ```python
   from tantra import PostgresStore

   store = PostgresStore(DATABASE_URL, schema=EXISTING_SCHEMA)
   await store.setup()
   await store.close()
   ```

   Use the schema already configured in your application; the default is `"tantra"`.

4. Start all workers with 1.3. Keep the existing startup order: Store setup, then `await runtime.start()`. Fail readiness if either operation fails.

Setup applies pending migrations through version 7. The command-index and operational-checkpoint backfills decode historical events in batches of at most 1,000; allow a maintenance window for large journals. Migration publication is transactional: failure rolls back without rewriting journal bodies or sequences, and setup can be retried while writers remain stopped.

Each store uses one to four pooled data connections. A coordinated worker also keeps a dedicated lease/control connection and a LISTEN connection, for at most six Runtime database connections per worker, excluding application tools and other application-owned connections. Preserve the [coordinator setup and shutdown order](migration-1.2.md).

## Optional compacted history

Full history remains the default. To load the latest committed summary and retained event window when preparing turns:

```python
runtime = Runtime(provider, store, [Bot], history_mode="compacted")
```

History mode controls loading; it does not enable summarization. Continue configuring a compactor when summaries are needed. Before the first compaction marker, compacted mode reads full history. Hooks, callable prompts, and custom compactors receive the retained window when this mode is selected. Custom stores without the optimized capability derive that window after a full read.

Public event replay and `connect(after=...)` still return the complete journal. Command identity, fencing, and recovery guarantees are unchanged; abandoned started turns are interrupted rather than replayed. Side-effecting tools still need application-level idempotency.

## Verification tooling

The repository includes `just bench baseline`, `live`, `replay`, `scale`, and `compare`, with an owned durable Compose database and standalone reports. Only explicit live runs make paid inference requests. See the [bench guide](https://github.com/malayh/tantra/tree/main/stress#automated-runtime-bench).
