# Migrate to 1.3

Tantra 1.3 preserves retained journal bodies, event sequences, replay cursors, and the required Store/Coordinator interfaces. PostgreSQL gains indexed commands, operational checkpoints, pooled access, shared observer catch-up, and session cleanup. SQLite setup adds permanent ID-only deletion markers; the filesystem format stays unchanged.

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

Setup applies pending migrations through version 9. Migration 8 adds permanent deletion markers and write guards; migration 9 backfills native actor update timestamps from existing headers and keeps them synchronized with header writes. The command-index and operational-checkpoint backfills decode historical events in batches of at most 1,000; allow a maintenance window for large journals. Migration publication is transactional: failure rolls back without rewriting journal bodies or sequences, and setup can be retried while writers remain stopped.

Each store uses one to four pooled data connections. A coordinated worker also keeps a dedicated lease/control connection and a LISTEN connection, for at most six Runtime database connections per worker, excluding application tools and other application-owned connections. Preserve the [coordinator setup and shutdown order](migration-1.2.md).

## Optional compacted history

Full history remains the default. To load the latest committed summary and retained event window when preparing turns:

```python
runtime = Runtime(provider, store, [Bot], history_mode="compacted")
```

History mode controls loading; it does not enable summarization. Continue configuring a compactor when summaries are needed. Before the first compaction marker, compacted mode reads full history. Hooks, callable prompts, and custom compactors receive the retained window when this mode is selected. Custom stores without the optimized capability derive that window after a full read.

Public event replay and `connect(after=...)` still return the complete journal. Command identity, fencing, and recovery guarantees are unchanged; abandoned started turns are interrupted rather than replayed. Side-effecting tools still need application-level idempotency.

## Session cleanup

`Runtime.delete(root_id)` deletes a complete root tree. `Runtime.cleanup(CleanupSelector(...))` selects one bounded page by root IDs, scalar root metadata, and/or a timezone-aware inactivity cutoff; supplied filters combine with AND. Cleanup defaults to a read-only dry run, and both APIs protect active trees unless `allow_active=True`. Applications authorize these operations independently of writer tokens; they are never agent tools. See the [Runtime reference](../reference/runtime.md#selector-cleanup) for reports, pagination, revision checks, and retry behavior.

PostgreSQL, SQLite, and memory stores support deletion and cleanup. Await SQLite setup after upgrading to install its marker table. Unsupported stores and coordinators fail before mutation. Deleted root and child UUIDs cannot be reused; retained sessions keep their journals and cursors. Session cleanup leaves shared memories, provider logs, telemetry, recordings, and backups intact.

## Verification tooling

The repository includes `just bench baseline`, `live`, `replay`, `scale`, and `compare`, with an owned durable Compose database and standalone reports. Only explicit live runs make paid inference requests. See the [bench guide](https://github.com/malayh/tantra/tree/main/stress#automated-runtime-bench).
