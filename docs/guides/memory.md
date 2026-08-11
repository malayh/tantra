# Memory

Memory is **tools only**. `Harness(memory=...)` injects nothing into the prompt and recalls nothing automatically — the model writes a better query than any heuristic, and an injected-but-irrelevant memory actively misleads.

Two moving parts, and you need both:

```python
from tantra import BuiltinMemory, memory_recall, memory_write

class Librarian(Agent):
    prompt = "You remember things."
    tools = [memory_write, memory_recall]

harness = Harness(provider, store, [Librarian], default_model=..., memory=BuiltinMemory(store))
```

`Agent.tools` supplies the verbs the model can call; `Harness(memory=)` supplies the implementation they reach through `ctx.memory`. Neither works alone.

Listing the tools without `Harness(memory=)` gives an `is_error` result the model can read ("no memory configured"), not a crash.

## The model

Thin by design: `kind`, `title`, `body`, plus `tags`, `entities` and `metadata`. Rows are soft-deleted and superseded, never mutated in place — `recall` stops returning a dead row while `get(id)` still does. `supersede` of a deleted or already-superseded row raises, so chains cannot fork. No document ingestion, no extraction, no reconcile: that is your application's job.

`BuiltinMemory(store, embedder=None)` keeps rows on the **concrete store**, via the `memory_put` / `memory_get` / `memory_all` protocol methods that `MemoryStore`, `FileSystemStore`, `SQLiteStore` and `PostgresStore` all implement. A store without them is refused at construction. Sessions and memories need not share a backend — sessions on SQLite with memories on the filesystem is a normal setup.

## Recall

Keyword matching everywhere: score is matched query tokens over total query tokens, `> 0` required, sorted by score then recency. An empty query or `k <= 0` returns `[]` — never "everything".

Hybrid keyword + vector recall runs only where the store exposes a vector search **and** an `Embedder` is configured: today that is Postgres with pgvector. Elsewhere the vector pass is silently absent — but honestly reported: every hit carries `mode`, `"keyword"` or `"vector"`, so a degraded backend says so rather than pretending.

```python
memory = BuiltinMemory(store, embedder=OpenAICompatibleEmbedder(base_url=..., api_key=..., model=...))
```

Embedding is best-effort: a failed embed never fails the write (the row lands with `embedding=None`), and `await memory.backfill()` repairs the gaps later.

## Multi-tenancy

`MemoryRecord.metadata` is the scoping field `recall` filters on, and the model never sets it. Build the tools with a scope instead of shipping the unscoped pair:

```python
from tantra import memory_tools

memory_write, memory_recall = memory_tools(lambda ctx: {"user": ctx.deps["user_id"]})
```

The callable runs per invocation: its result is stamped onto every row the agent writes and used as the recall filter, so one tenant's tools cannot reach another's rows. `memory_write` and `memory_recall` imported straight from `tantra` are the unscoped pair — they write `{}` and filter on nothing.

!!! warning "Keep scope values scalar and always set"
    Matching fails closed on a missing key, so a scope that sometimes omits its key simply matches nothing. `PostgresStore` matches with jsonb containment, so a list or dict value is read as a recursive subset rather than an equality test. Nothing enforces isolation beyond the filter you supply.

## A turn

```python
import asyncio
import json

from tantra import (
    Agent,
    BuiltinMemory,
    FakeProvider,
    Harness,
    MemoryStore,
    Sample,
    collect,
    memory_recall,
    memory_write,
)
from tantra.events import ToolCallCompleted
from tantra.providers.base import ToolCall


class Librarian(Agent):
    prompt = "You remember things."
    tools = [memory_write, memory_recall]


async def main() -> None:
    store = MemoryStore()
    memory = BuiltinMemory(store)
    written = json.dumps(
        {
            "kind": "fact",
            "title": "The p99 panel reads from Mimir",
            "body": "The p99 latency panel queries the Mimir datasource, not Prometheus.",
            "tags": ["dashboard"],
            "entities": ["mimir"],
        }
    )
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="w1", name="memory_write", args=written)]),
                Sample(
                    tool_calls=[
                        ToolCall(
                            id="r1",
                            name="memory_recall",
                            args=json.dumps({"query": "where does the p99 panel get its data"}),
                        )
                    ]
                ),
                Sample(text="noted."),
            ]
        ),
        store,
        [Librarian],
        default_model="fake/model",
        memory=memory,
    )
    sid = (await harness.create_session(Librarian)).id

    events = await collect(harness.run(sid, "remember where p99 comes from"))
    results = {e.event.call_id: e.event for e in events if isinstance(e.event, ToolCallCompleted)}

    print((await memory.get(results["w1"].result)).title)
    for row in results["r1"].result:
        print(row["mode"], round(row["score"], 2), row["title"])


asyncio.run(main())
```

```text
The p99 panel reads from Mimir
keyword 0.38 The p99 panel reads from Mimir
```

`memory_write` returns the new row's id. `memory_recall` returns `id`, `kind`, `title`, `body`, `tags`, `entities`, `score`, `mode` — embeddings and metadata never reach the model.

## Next

- [Memory reference](../reference/memory.md) — the `Memory` protocol, for your own implementation.
- [Storage backends](storage.md) — where rows live.
