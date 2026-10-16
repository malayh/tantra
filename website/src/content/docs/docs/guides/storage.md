# Storage backends

A `Store` persists session headers, UUID-addressed inputs, and per-session sequenced journals. Tantra ships `MemoryStore`, `FileSystemStore`, `SQLiteStore`, and `PostgresStore`.

```python
store = SQLiteStore("sessions.db")
await store.setup()
runtime = Runtime(provider, store, [Bot], default_model="openai/gpt-5")
```

`SQLiteStore` and `PostgresStore` require `setup()` at application startup. The application owns store lifetime and cleanup.

Each actor journal is independent and gap-free. `append` returns the assigned sequence, `read_page` reads after a scalar cursor, and `enqueue` atomically accepts or deduplicates a command. The store also exposes the pending input and incomplete-turn data needed to activate one actor.

A shared store is persistence, not execution coordination by itself. With the default `coordinator=None`, do not drive the same live root tree from multiple Runtime processes.

Tantra 1.2 can pair `PostgresStore` with `PostgresCoordinator` for leased execution ownership, fenced writes, cross-process commands and notifications, and takeover recovery. The coordinator must receive the identical store object passed to Runtime, and the Runtime must be started during application lifespan. See [Migrate to 1.2](migration-1.2.md) for the setup pattern and failure limits.
