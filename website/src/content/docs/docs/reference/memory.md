# Memory

`Memory` is the protocol used by `memory_write` and `memory_recall`. Pass an implementation through `Runtime(memory=...)`.

`BuiltinMemory` stores `MemoryWrite` rows, returns ranked `MemoryHit` values, supports supersession and deletion, and can backfill embeddings. Nothing is recalled automatically.
