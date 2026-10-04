# Migrate to 1.4

Tantra 1.4 reduces PostgreSQL streaming and observation overhead. Public Runtime APIs, required Store/Coordinator interfaces, event bodies, sequences, fencing and replay cursors remain unchanged. Full history remains the default.

## Upgrade PostgreSQL

1. Stop every application worker and other writer using the database. Mixed-version writers are unsupported during migration.
2. Install `tantra-harness[postgres]==1.4.0` in the application environment.
3. Run setup once against the existing database and schema before starting Runtime:

   ```python
   from tantra import PostgresStore

   store = PostgresStore(DATABASE_URL, schema=EXISTING_SCHEMA)
   await store.setup()
   await store.close()
   ```

4. Start all workers with 1.4. Preserve the startup order: Store setup, then `await runtime.start()`. Fail readiness if either operation fails.

Setup applies pending migrations through version 11. Migration 10 adds a partial index for terminal events. Migration 11 adds stored `sessions.root_key` using the existing root expression and replaces its expression index. Adding the stored column rewrites session rows and requires a maintenance window; it does not rewrite journals. Migration publication is transactional: failure rolls back and setup can be retried while writers remain stopped.

Upgrading from before 1.3 also runs its projection, deletion-marker and actor-timestamp migrations. See the [1.3 guide](migration-1.3.md) for those backfills, session cleanup and optional compacted-history loading. SQLite, memory and filesystem storage formats have no new 1.4 migration.

## Streaming and readers

The built-in PostgreSQL append path automatically batches provider `TextDelta`, `ReasoningDelta` and `ToolCallDelta` events. It flushes at **25 ms, 32 events, or 64 KiB** of complete serialized delta payload, whichever comes first. The timer starts at the first buffered delta; an oversized event commits alone. Tool progress and lifecycle events remain immediate, and buffered deltas flush before tool execution and normal stream completion.

Every event retains its own body and sequence. Readers receive events after their transaction commits. Cancellation, deletion, shutdown and ownership loss discard uncommitted deltas; process death can lose received but uncommitted output. The durable journal retains an intact committed prefix. External tool effects still require application-level idempotency.

SQLite, memory, custom stores and custom coordination paths retain immediate commits. Any custom `Hook.on_event` implementation disables batching for the turn; hooks inheriting the base no-op remain eligible. No public batching option is added.

Journal-only observation hints coalesce for another 25 ms; control changes remain immediate. Batching and observation can therefore each contribute 25 ms before database and scheduler delay. Authoritative periodic catch-up still handles missed notifications and expired ownership, and terminal evidence gates result reads.

PostgreSQL Runtime streams share recent committed pages with independent cursors and event copies. The cache is capped at 256 events/1 MiB per actor and 64 actors/16 MiB globally, conservatively accounting for retained Python objects. Historical replay and slow readers retain authoritative SQL pagination. Runtime observes only streaming/waiting actors plus root control state; public coordinator observation remains root-wide. Closing streams, deletion and shutdown release cached evidence and unused observation interests without stopping accepted work.

The built-in compactor reuses unchanged request projections within one invocation. Direct/custom compactors and full/compacted history semantics remain compatible; default full-history decoding and historical replay remain proportional to history.

## Verification and performance

The final package, bench and durable PostgreSQL stress suite passed **1,090 tests with zero skips**, plus lint and one independent Ponytail review. Replay digests, commit-before-delivery checks, rollback, uncertain acknowledgments, cancellation, takeover, deletion, mutation isolation, compaction compatibility and observer cleanup passed. Scale checks covered 64 observers/16 turns and 1,000 observers/100 turns, with four data connections/dispatchers maximum per worker and zero unchanged journal/result-body reads.

Nine primary repository streaming cases, with five warmed trials each, reduced median Python CPU **72.8–93.3%** and PostgreSQL CPU **58.0–94.1%** against the pre-optimization baseline. These measure runtime overhead with deterministic providers. Individual regressions remain: incremental P4 eight-reader delivery P95 rose **42.19 → 73.77 ms**, and one 1,000-observer campaign increased active Python CPU **9.6%** versus P3. See [spec 016](https://github.com/malayh/tantra/blob/main/design/016_active_runtime_cpu.md) for measurements and remaining costs.

Use `just bench cpu` for repeatable streaming measurements and `just bench compare` for compatible JSON/HTML comparisons. The bench starts its own durable Compose PostgreSQL database; CPU mode makes no paid inference requests. See the [bench guide](https://github.com/malayh/tantra/tree/main/stress#automated-runtime-bench).
