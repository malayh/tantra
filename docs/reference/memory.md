# Memory

```python
from tantra import BuiltinMemory, Memory, MemoryHit, MemoryRecord, MemoryWrite, memory_recall, memory_write
```

Durable rows an agent writes and recalls across turns, by verbs rather than injection. **Nothing is recalled automatically** — the model asks through the `memory_recall` and `memory_write` tools. Wire it with `Harness(memory=...)`; it reaches tools as `ctx.memory`.

## `Memory` (protocol)

```python
async def write(self, m: MemoryWrite) -> str
async def get(self, mid: str) -> MemoryRecord | None
async def recall(self, q: str, *, k: int = 5, kind: str | None = None,
                 tags: list[str] | None = None, entity: str | None = None,
                 metadata: dict[str, Any] | None = None) -> list[MemoryHit]
async def supersede(self, old_id: str, new: MemoryWrite) -> str
async def delete(self, mid: str) -> None
```

| Method | Semantics |
|---|---|
| `write` | Store a new row, return its id. |
| `get` | One row by id, **including** deleted and superseded ones; `None` when unknown. |
| `recall` | Up to `k` live rows matching `q`, best first. Deleted and superseded rows never match. `tags` requires every listed tag; `metadata` matches as a subset. Each hit reports the `mode` that produced it, so a backend degrading to keyword-only says so. |
| `supersede` | Write `new` and point `old_id` at it. The old row stays readable via `get`. |
| `delete` | Soft-delete: `recall` stops returning it, `get` still does. |

`metadata` scopes rows the way session metadata scopes sessions — recall filters on it and tantra enforces nothing.

## Data types

**`MemoryWrite`** — the input. `extra="forbid"`, so an unknown field raises.

| Field | Type | Default |
|---|---|---|
| `kind` | `str` | required |
| `title` | `str` | required |
| `body` | `str` | required |
| `tags` | `list[str]` | `[]` |
| `entities` | `list[str]` | `[]` |
| `metadata` | `dict[str, Any]` | `{}` |

**`MemoryRecord`** — the stored row: everything in `MemoryWrite` plus `id: str`, `embedding: list[float] | None`, `created_at: datetime`, `deleted: bool = False`, `superseded_by: str | None`.

**`MemoryHit`** — frozen dataclass: `memory: MemoryRecord`, `score: float`, `mode: str` (`"keyword"` or `"vector"`).

## `BuiltinMemory(store, embedder=None)`

```python
harness = Harness(provider, store, [Bot], memory=BuiltinMemory(store))
```

The shipped implementation. It stores rows through duck-typed methods on whatever object you hand it, so any object with `memory_put`, `memory_get` and `memory_all` works; missing any of them raises `TantraError` at construction. `MemoryStore`, `FileSystemStore`, `SQLiteStore` and `PostgresStore` all qualify — see [Stores](stores.md).

Recall is hybrid:

- **Keyword** always runs: score is the fraction of the query's tokens present in the row's title, body, tags and entities. Tokens are lowercase alphanumeric runs.
- **Vector** runs only when an `embedder` was passed **and** the store also implements `memory_search(vector, k) -> list[tuple[MemoryRecord, float]] | None`. Score is `1.0 - distance`.

Both passes are merged per row keeping the higher score, sorted by `(score, created_at)` descending, cut to `k`.

!!! warning "Vector recall degrades silently"
    Only `PostgresStore` (with pgvector available) implements `memory_search`; on the other stores an embedder is used to *write* vectors but recall stays keyword-only. Embedding failures are swallowed too — the row is stored with `embedding=None`. Check `hit.mode` if you need to know which pass answered.

**`async backfill() -> int`** embeds live rows that have no vector and returns how many were repaired. Raises `TantraError` without an embedder.

## The model-facing tools

Add them to `Agent.tools` like any other tool.

```python
from tantra import memory_recall, memory_write

class Assistant(Agent):
    tools = [memory_write, memory_recall]
```

| Tool | Parameters |
|---|---|
| `memory_write` | `kind: str`, `title: str`, `body: str`, `tags: list[str] \| None = None`, `entities: list[str] \| None = None` → returns the new id |
| `memory_recall` | `query: str`, `k: int = 5`, `kind: str \| None = None`, `tags: list[str] \| None = None`, `entity: str \| None = None` → returns a list of dicts with `id`, `kind`, `title`, `body`, `tags`, `entities`, `score`, `mode` |

!!! note "No `metadata` parameter"
    Neither tool exposes `metadata` to the model: `memory_write` always writes `{}`, and `memory_recall` never filters on it. Scoping by metadata is an application concern — write those rows through your own `Memory` calls, not the model's.

Both raise a self-describing error when the harness was built without `memory=`.

## See also

- [Stores](stores.md) — which backends hold memory rows.
- [Providers](providers.md) — `Embedder` and `OpenAICompatibleEmbedder`.
- [Guides: memory](../guides/memory.md).
