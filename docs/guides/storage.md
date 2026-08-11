# Storage backends

A `Store` is an append-only event log plus a mutable session header, with optimistic concurrency (`append(..., expect_seq=)`) and a single-writer lease per session. Four implementations ship.

| Backend | Constructor | Layout | Notes |
|---|---|---|---|
| `MemoryStore` | `MemoryStore()` | dicts | In-process. Test default, zero config; nothing survives the process. |
| `FileSystemStore` | `FileSystemStore(root)` | `<root>/<sid>/session.json` + `events.jsonl` | Lease is a TTL'd record in `<sid>/.lock` guarded by `fcntl.flock` — a bare flock dies with the process, which would contradict durable suspend. |
| `SQLiteStore` | `SQLiteStore(path)` | `sessions`, `events(session_id, seq)` | WAL mode, one connection per operation, writes under `BEGIN IMMEDIATE` with a 30 s busy timeout. |
| `PostgresStore` | `PostgresStore(dsn, schema="tantra")` | same tables in a dedicated schema | psycopg 3 via `pip install "tantra-harness[postgres]"`; metadata is JSONB + GIN; pgvector used for memory when the extension is available. |

Importing `tantra` without psycopg works; constructing `PostgresStore` without it raises.

## `setup()` is mandatory — and nothing calls it for you

!!! danger "`SQLiteStore` and `PostgresStore` create no schema until you call `setup()`"
    `Harness` never calls it. Call it once at startup, before the first session. `MemoryStore` and `FileSystemStore` work without it.

```python
store = SQLiteStore("sessions.db")
await store.setup()
```

`setup()` is idempotent and versioned — running it twice, or from several workers against a fresh Postgres schema at once, is safe (Postgres serialises it under an advisory lock keyed on the schema name).

## Using one

```python
import asyncio

from tantra import Agent, FakeProvider, Harness, Sample, SQLiteStore, collect
from tantra.events import TurnCompleted


class Bot(Agent):
    prompt = "You are helpful."


async def main() -> None:
    store = SQLiteStore("sessions.db")
    await store.setup()

    harness = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model="fake/model")
    session = await harness.create_session(Bot, metadata={"company": 42})
    events = await collect(harness.run(session.id, "hello"))

    print([e.event.stop_reason for e in events if isinstance(e.event, TurnCompleted)])
    print([header.id == session.id for header in await store.list(metadata={"company": 42})])


asyncio.run(main())
```

```text
['completed']
[True]
```

## Choosing

- **`MemoryStore`** for tests and throwaway runs.
- **`FileSystemStore`** for a single-machine CLI. Cheap per-step writes, human-readable logs.
- **`SQLiteStore`** for a single-machine app that wants queries and one file. The slow one under heavy write load — one fsync per commit.
- **`PostgresStore`** for anything multi-process or multi-pod. It is the backend cross-instance resume and lease contention are actually exercised against.

Every sample round-trips the store, so a remote Postgres adds real per-step latency to a chatty loop. That is the durability tax.

## Conformance

All four pass one shared suite. A third-party store runs the same one:

```python
from tantra.testing import store_conformance

await store_conformance(lambda: SQLiteStore("conformance.db"))
```

The factory must return a store over the same underlying storage on every call — separate instances stand in for separate processes, and the suite calls it from worker threads to check lease contention and stale-`expect_seq` behaviour.

## Memory rows

Session storage and [memory](memory.md) rows are separate concerns, but the row methods are **part of** the `Store` protocol:

```python
async def memory_put(self, row: MemoryRecord) -> None
async def memory_get(self, mid: str) -> MemoryRecord | None
async def memory_all(self, *, metadata=None, include_dead=False) -> list[MemoryRecord]
async def memory_search(self, vector: list[float], k: int) -> list[tuple[MemoryRecord, float]] | None
```

`memory_all` filters on `metadata` as a subset and leaves out deleted and superseded rows unless `include_dead`. Only Postgres has a real `memory_search`; the others return `None` and recall degrades to keyword-only. A custom store that skips the row methods is still a perfectly good session store — `BuiltinMemory` just refuses it at construction.

## Next

- [Stores reference](../reference/stores.md), [Durability & resume](../concepts/durability.md).
