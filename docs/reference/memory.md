# Memory

```python
from tantra import BuiltinMemory, Memory, MemoryHit, MemoryRecord, MemoryWrite, memory_recall, memory_tools, memory_write
```

Durable rows an agent writes and recalls across turns, by verbs rather than injection. **Nothing is recalled automatically** — the model asks through the `memory_recall` and `memory_write` tools. Wire it with `Harness(memory=...)`; it reaches tools as `ctx.memory`.

## `Memory` (protocol)

```python
async def write(self, m: MemoryWrite) -> str
async def get(self, mid: str) -> MemoryRecord | None
async def recall(self, q: str, *, k: int = 5, kind: str | None = None,
                 tags: list[str] | None = None, entity: str | None = None,
                 metadata: dict[str, Any] | None = None) -> list[MemoryHit]
async def supersede(self, old_id: str, new: MemoryWrite, *, scope: dict[str, Any] | None = None) -> str
async def delete(self, mid: str, *, scope: dict[str, Any] | None = None) -> bool
```

| Method | Semantics |
|---|---|
| `write` | Store a new row, return its id. |
| `get` | One row by id, **including** deleted and superseded ones; `None` when unknown. |
| `recall` | Up to `k` live rows matching `q`, best first. Deleted and superseded rows never match. `tags` requires every listed tag; `metadata` matches as a subset. Each hit reports the `mode` that produced it, so a backend degrading to keyword-only says so. |
| `supersede` | Write `new` and point `old_id` at it. The old row stays readable via `get`. A row whose metadata does not match `scope` is refused as unknown, so a caller holding the wrong scope cannot learn that the id exists. |
| `delete` | Soft-delete, reporting whether the row is now gone: `recall` stops returning it, `get` still does. Never raises — an unknown id and a row outside `scope` both return `False`, and deleting an already-deleted row returns `True` again. |

`metadata` scopes rows the way session metadata scopes sessions — recall filters on it and tantra enforces nothing. Matching fails closed: a key the row lacks never matches, and `None` matches only a stored `None`.

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

The shipped implementation. It stores rows through the `Store` row methods, and checks for `memory_put`, `memory_get` and `memory_all` on whatever object you hand it; missing any of them raises `TantraError` at construction. `MemoryStore`, `FileSystemStore`, `SQLiteStore` and `PostgresStore` all qualify — see [Stores](stores.md).

Filtering happens in the store: `recall` passes its `metadata` down to `memory_all(metadata=...)`, which also leaves out deleted and superseded rows unless asked for them with `include_dead=True`.

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

!!! note "The model never sets `metadata`"
    Neither tool exposes `metadata` to the model. The unscoped pair shipped at module level writes `{}` and filters on nothing. To scope rows, build the pair with `memory_tools(scope=...)`.

### `memory_tools(scope=None) -> tuple[Tool, Tool]`

```python
from tantra import memory_tools

memory_write, memory_recall = memory_tools(lambda ctx: {"user": ctx.deps["user_id"]})
```

`scope` is a `MemoryScope` — `Callable[[Context], dict[str, Any]]`, called per tool invocation. Its result is written as the row's `metadata` and passed as `recall`'s `metadata` filter, so one tenant's tools cannot read or write another's rows. `memory_write` and `memory_recall` imported from `tantra` are `memory_tools()` with no scope.

Keep scope values scalar and always set: a key the row lacks never matches, so a missing key fails closed, and `PostgresStore` reads a list or dict value as a jsonb subset rather than an equality test.

Both raise a self-describing error when the harness was built without `memory=`.

## See also

- [Stores](stores.md) — which backends hold memory rows.
- [Providers](providers.md) — `Embedder` and `OpenAICompatibleEmbedder`.
- [Guides: memory](../guides/memory.md).
