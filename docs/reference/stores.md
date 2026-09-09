# Stores

The Runtime store contract persists actor headers, UUID-addressed inputs, and per-actor event pages.

- `create` and `header` manage session identity and relationships.
- `append` assigns monotonically increasing sequence numbers.
- `enqueue` atomically accepts or deduplicates an `InputQueued` command.
- `read_page(after=...)` supports scalar replay cursors.
- pending and incomplete-turn queries support activation of one actor.
- `list(parent_id=...)` validates and traverses actor relationships.

`MemoryStore`, `FileSystemStore`, `SQLiteStore`, and `PostgresStore` implement the contract. SQLite and PostgreSQL need `setup()`.

The package retains older store methods only for the transitional in-repository application until its migration. They are not part of the Runtime contract.
