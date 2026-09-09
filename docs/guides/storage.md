# Storage backends

A `Store` persists session headers, UUID-addressed inputs, and per-session sequenced journals. Tantra ships `MemoryStore`, `FileSystemStore`, `SQLiteStore`, and `PostgresStore`.

```python
store = SQLiteStore("sessions.db")
await store.setup()
runtime = Runtime(provider, store, [Bot], default_model="openai/gpt-5")
```

`SQLiteStore` and `PostgresStore` require `setup()` at application startup. The application owns store lifetime and cleanup.

Each actor journal is independent and gap-free. `append` returns the assigned sequence, `read_page` reads after a scalar cursor, and `enqueue` atomically accepts or deduplicates a command. The store also exposes the pending input and incomplete-turn data needed to activate one actor.

A shared store is persistence, not execution coordination. Do not drive the same live root tree from multiple Runtime processes. Cross-process notifications and automatic recovery are outside the 1.0 contract.
