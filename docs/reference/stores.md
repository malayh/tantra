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

PostgreSQL provides three further optional reads:

- `read_operational(actor_id)` returns ordered pending inputs, the latest unmatched start, journal finish evidence, unresolved cancellation targets, and a versioned sequence watermark. Live-work flags and cancellation pointers commit with lifecycle events. Streaming appends advance the watermark in the existing header update. Valid stale state catches up from indexed lifecycle evidence; invalid state rebuilds from the unchanged journal inside a fenced transaction.
- `read_turn(actor_id, turn_id)` returns `None` before a terminal exists. Completed reads page only the first start through the first terminal, inclusive; terminals preceding their start need only their terminal row.
- `read_compacted(actor_id)` returns a `HistorySnapshot` containing the latest compaction marker and retained events through a frozen `last_seq`. A floor before the marker is retained; missing floors use marker plus suffix, and absent markers use full history.

Runtime discovers these capabilities without extending the required Store protocol. Unsupported stores use full-read reduction and result reconstruction; opted-in compacted history is derived from that full read. Public replay always reads the durable journal.

The journal-index, operational-checkpoint, and coordinator-index migrations run during `setup()`. Stop writers before upgrading an existing database; mixed-version writers are unsupported. Version 7 adds only indexes for root lookup, session pagination, request dispatch, and transport cleanup. Migration version publication is atomic with the index changes. Setup decodes historical events in batches of at most 1,000 inside the earlier data migrations. Failure rolls back the migration, and retrying setup starts it again. Event bodies and sequence numbers remain unchanged.

The package retains older store methods only for the transitional in-repository application until its migration. They are not part of the Runtime contract.
