# Stores

The Runtime store contract persists actor headers, UUID-addressed inputs, and per-actor event pages.

- `create` and `header` manage session identity and relationships.
- `append` assigns monotonically increasing sequence numbers.
- `enqueue` atomically accepts or deduplicates an `InputQueued` command.
- `read_page(after=...)` supports scalar replay cursors.
- pending and incomplete-turn queries support activation of one actor.
- `list(parent_id=...)` validates and traverses actor relationships.

`MemoryStore`, `FileSystemStore`, `SQLiteStore`, and `PostgresStore` implement the contract. SQLite and PostgreSQL need `setup()`.

`PostgresStore` owns a lazily opened pool of one to four autocommit connections. Each ordinary operation checks out a connection only for its database work, and a transaction keeps one checkout through commit or rollback. `close()` returns pool resources. A coordinated worker also keeps one lease/control connection and one `LISTEN` connection outside the pool, so it uses at most six PostgreSQL connections before application-owned connections.

PostgreSQL also provides optional `lookup_command(root_id, command_id)` and `lookup_finished(actor_id)` methods. They return original stamped events through a small transactional journal index; stores without these methods retain full-read behavior. Command lookup follows the actor tree in breadth-first order, then creation time and ID, and returns the earliest matching event within the selected actor.

Optional `lookup_ask(root_id, ask_id)` returns `(actor_id, Stamped)` for the original `AskRaised` anywhere in the durable tree, including legacy parent relationships. Duplicate matches raise `ValueError`. PostgreSQL migration 12 adds the ask projection and partial index, backfilling both journal envelopes through the existing codec in batches of at most 1,000. Stop writers for setup. Corrupt rows or migration failures roll back; version publication happens only after completion. Journal bodies/sequences stay unchanged. Runtime provides a full-read fallback when this capability is absent; required Store and Coordinator protocols are unchanged.

PostgreSQL provides three further optional reads:

- `read_operational(actor_id)` returns ordered pending inputs, the latest unmatched start, journal finish evidence, unresolved cancellation targets, and a versioned sequence watermark. Live-work flags and cancellation pointers commit with lifecycle events. Streaming appends advance the watermark in the existing header update. Valid stale state catches up from indexed lifecycle evidence; invalid state rebuilds from the unchanged journal inside a fenced transaction.
- `read_turn(actor_id, turn_id)` returns `None` before a terminal exists. Completed reads page only the first start through the first terminal, inclusive; terminals preceding their start need only their terminal row.
- `read_compacted(actor_id)` returns a `HistorySnapshot` containing the latest compaction marker and retained events through a frozen `last_seq`. A floor before the marker is retained; missing floors use marker plus suffix, and absent markers use full history.

Runtime discovers these capabilities without extending the required Store protocol. Unsupported stores use full-read reduction and result reconstruction; opted-in compacted history is derived from that full read. Public replay always reads the durable journal.

The journal-index, operational-checkpoint, and coordinator-index migrations run during `setup()`. Stop writers before upgrading an existing database; mixed-version writers are unsupported. Version 7 adds only indexes for root lookup, session pagination, request dispatch, and transport cleanup. Migration version publication is atomic with the index changes. Setup decodes historical events in batches of at most 1,000 inside the earlier data migrations. Failure rolls back the migration, and retrying setup starts it again. Event bodies and sequence numbers remain unchanged.

The package retains older store methods only for the transitional in-repository application until its migration. They are not part of the Runtime contract.

Migration 8 adds permanent ID-only deletion markers and PostgreSQL write guards. Stop writers before setup; the migration publishes atomically and does not rewrite retained journals. SQLite setup adds its marker table idempotently. Session deletion is performed through `Runtime.delete`, which handles tasks, fencing, and readers. The optional store primitives do not extend the required Store protocol; direct unfenced deletion of a coordinated root is rejected. Filesystem deletion is unsupported.

In 1.4, migration 10 adds the partial terminal-event index and migration 11 replaces the root expression index with stored `sessions.root_key`. The generated column rewrites session rows; allow a maintenance window with all writers stopped. Journal bodies and sequences stay unchanged. Eligible delta-only appends update header sequence, timestamp and operational watermark in SQL without hydrating the header in Python. See the [1.4 upgrade guide](../guides/migration-1.4.md).

## Cleanup selection

Memory, SQLite, and PostgreSQL provide optional cleanup selection and revision-guarded deletion. These capabilities are not additions to the required Store protocol. PostgreSQL migration 9 adds a native actor `updated_at` synchronized with header writes. Selection computes tree age and activity from headers and lifecycle evidence in SQL, without reading historical event bodies or updating the root for every child event. Run migrations with writers stopped; mixed-version writers are unsupported.
