# Stores

The Runtime store contract persists actor headers, UUID-addressed inputs, and per-actor event pages.

- `create` and `header` manage session identity and relationships.
- `append` assigns monotonically increasing sequence numbers.
- `enqueue` atomically accepts or deduplicates an `InputQueued` command.
- `read_page(after=...)` supports scalar replay cursors.
- pending and incomplete-turn queries support activation of one actor.
- `list(parent_id=...)` validates and traverses actor relationships.

`MemoryStore`, `FileSystemStore`, `SQLiteStore`, and `PostgresStore` implement the contract. SQLite and PostgreSQL need `setup()`.

PostgreSQL also provides optional `lookup_command(root_id, command_id)` and `lookup_finished(actor_id)` methods. They return original stamped events through a small transactional journal index; stores without these methods retain full-read behavior. Command lookup follows the actor tree in breadth-first order, then creation time and ID, and returns the earliest matching event within the selected actor.

The journal-index migration runs during `setup()`. Stop writers before upgrading an existing database; mixed-version writers are unsupported. Setup decodes historical events in batches of at most 1,000 inside the migration transaction. Failure rolls back the migration, and retrying setup starts it again. Event bodies and sequence numbers remain unchanged.

The package retains older store methods only for the transitional in-repository application until its migration. They are not part of the Runtime contract.
