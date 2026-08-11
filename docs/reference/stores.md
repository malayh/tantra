# Stores

```python
from tantra import FileSystemStore, MemoryStore, PostgresStore, SQLiteStore, Store
```

A store is an append-only session event log plus a mutable session header. Everything durable about tantra lives here.

## `Store` (protocol)

```python
async def setup(self) -> None
async def create(self, header: SessionHeader) -> None
async def header(self, sid: str) -> SessionHeader | None
async def put_header(self, h: SessionHeader) -> None
async def patch_header(self, sid: str, *, title=UNSET, status=UNSET, pending_ask=UNSET,
                       usage=UNSET, metadata=UNSET) -> SessionHeader
async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int
def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]
async def list(self, *, metadata: dict[str, Any] | None = None, parent_id: str | None = None,
               limit: int = 50, before: str | None = None) -> list[SessionHeader]
async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool
async def release_lease(self, sid: str, holder: str) -> None
async def memory_put(self, row: MemoryRecord) -> None
async def memory_get(self, mid: str) -> MemoryRecord | None
async def memory_all(self, *, metadata: dict[str, Any] | None = None,
                     include_dead: bool = False) -> list[MemoryRecord]
async def memory_search(self, vector: list[float], k: int) -> list[tuple[MemoryRecord, float]] | None
```

| Method | Semantics |
|---|---|
| `setup` | Prepare the backend. Idempotent. |
| `create` | Register a new session. Raises `SessionExists` when the id is taken. |
| `header` | The header, or `None` for an unknown session. `lease` is reported as stored, expired or not — compare `lease.expires_at` against now to spot a turn abandoned by a dead worker. |
| `put_header` | Overwrite the header. `last_seq` and `lease` are store-owned and preserved regardless of what you pass. Raises `SessionNotFound`. |
| `patch_header` | Apply one atomic edit and return the result: only the fields passed change, so a concurrent writer touching other fields is not lost. `metadata` merges shallowly — the given keys overwrite, the rest survive, nothing is deleted. `updated_at` is stamped on every patch; `last_seq` and `lease` are left alone. Raises `SessionNotFound`. The `UNSET` default sentinel and the `apply_patch(header, **fields)` helper both live in `tantra.stores.base`; apply the patch inside whatever exclusion primitive the backend already uses for `append`. |
| `append` | Append events, return the new last seq. **Optimistic concurrency:** raises `SeqConflict` unless `expect_seq` equals the session's current last seq; `expect_seq=None` skips the check and appends onto whatever is current. The first event of a session gets seq 1. Raises `SessionNotFound` for an unknown session. |
| `read` | Yield every `Stamped` with `seq > from_seq`, in order. Raises `CorruptLog` rather than skipping an undecodable event — a gap in the suffix would silently rewrite history. |
| `list` | Headers newest first. `metadata` matches as a subset, `before` is a session-id cursor for paging. |
| `acquire_lease` | Take or refresh the single-writer lease for `ttl` seconds. `False` when someone else holds a live one. An expired lease is acquirable by anyone and is never cleared on expiry. Raises `SessionNotFound`. |
| `release_lease` | Drop the lease when `holder` owns it, otherwise do nothing. |
| `memory_put` | Store a memory row, overwriting any row already held under its id. |
| `memory_get` | One row by id, deleted and superseded ones included, or `None`. |
| `memory_all` | Rows matching `metadata` as a subset — a key the row lacks never matches. Deleted and superseded rows are left out unless `include_dead`. |
| `memory_search` | Up to `k` live rows nearest `vector` with their distances, or `None` from a backend with no vector path. |

`append`'s `expect_seq` is the whole concurrency story: two writers racing the same session means one of them gets `SeqConflict`, and the lease is what stops that from happening in normal operation.

### `select_headers(headers, *, metadata=None, parent_id=None, limit=50, before=None)`

```python
from tantra.stores.base import select_headers
```

The in-memory implementation of `list()`'s filtering, paging and ordering — sort by `(created_at, id)` descending, apply the `before` cursor, then `parent_id`, then the `metadata` subset match, then `limit`. `MemoryStore`, `FileSystemStore` and `SQLiteStore` all call it; only `PostgresStore` pushes the equivalent down into SQL.

A third-party store that can hold every header in memory should call it rather than reimplement the semantics, because `store_conformance` pins them exactly:

```python
async def list(self, **kwargs) -> list[SessionHeader]:
    return select_headers(self._all_headers(), **kwargs)
```

## Implementations

| Store | Constructor | Layout |
|---|---|---|
| `MemoryStore` | `MemoryStore()` | In-process dicts under a lock. The test default; nothing survives the process. |
| `FileSystemStore` | `FileSystemStore(root: str \| Path)` | `<root>/<sid>/session.json` + `events.jsonl`; the lease is a TTL'd record in `<sid>/.lock` guarded by `fcntl.flock`. |
| `SQLiteStore` | `SQLiteStore(path: str \| Path)` | `sessions` / `events(session_id, seq)` / `memories` tables, WAL mode. |
| `PostgresStore` | `PostgresStore(dsn: str, schema: str = "tantra")` | The same tables in a dedicated schema, versioned migrations, `metadata` as JSONB + GIN. Needs `tantra-harness[postgres]`; without psycopg the constructor raises `TantraError`. |

!!! danger "`setup()` is mandatory and nothing calls it"
    `SQLiteStore` and `PostgresStore` create their schema in `await store.setup()`. `Harness` never calls it. Skip it and the first append fails on a missing table.

    ```python
    store = SQLiteStore("sessions.db")
    await store.setup()
    ```

    `MemoryStore` and `FileSystemStore` work without it (`FileSystemStore.create` makes its own directories), but calling it is harmless and keeps the code backend-agnostic.

## Memory rows

The row methods [`BuiltinMemory`](memory.md) uses are part of the protocol, and all four stores implement them. `memory_search` is where they differ: `PostgresStore` runs pgvector similarity, the others return `None` and recall degrades to keyword-only.

Keep `memory_all` filter values scalar: `PostgresStore` matches with jsonb containment, which reads a list or dict value as a recursive subset rather than an equality test.

`BuiltinMemory` still checks for `memory_put` / `memory_get` / `memory_all` at construction and raises `TantraError` when a store predating 0.2.0 lacks them.

## Testing a third-party store

```python
from tantra.testing import store_conformance

async def test_my_store(tmp_path):
    await store_conformance(lambda: MyStore(tmp_path / "db"))
```

`store_conformance(store_factory)` runs the shared suite and raises `AssertionError` on the first violation. Requirements on the factory:

- it must return a store over the **same underlying storage** on every call — separate instances stand in for separate processes;
- it is called from worker threads, so it must build a store without touching a running event loop.

The suite covers create/header/append/read, stale `expect_seq`, blind `expect_seq=None` appends, `put_header` field ownership, `patch_header` field isolation and metadata merging, unknown-session errors, `list` filtering and paging, lease acquisition, expiry and contention, and memory rows — a store without `memory_put` skips the row checks rather than failing them. `setup()` is called twice on purpose: it must be idempotent. The lease-contention check races real threads and shortens the interpreter's switch interval, so an unguarded critical section actually loses.

!!! warning "No isolation is enforced"
    `list()` with no filter returns every session in the store, across every tenant and user. Scoping is the application's job — put your keys in `SessionHeader.metadata` and filter every read.

## See also

- [Events](events.md) — `Stamped`, `SessionHeader`, `Lease`.
- [Guides: storage backends](../guides/storage.md).
