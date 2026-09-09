# Memory

Memory is exposed through tools and is never injected automatically. Add `memory_write` and `memory_recall` to an agent, and supply the implementation to the Runtime.

```python
from tantra import BuiltinMemory, Runtime, memory_recall, memory_write


class Librarian(Agent):
    tools = [memory_write, memory_recall]


memory = BuiltinMemory(store, embedder=embedder)
runtime = Runtime(provider, store, [Librarian], memory=memory)
```

Without `Runtime(memory=...)`, the tools return a self-describing error result. `BuiltinMemory` supports writes, recall, supersession, deletion, and embedding backfill. Stores without vector search fall back to keyword results.

Memory rows are shared infrastructure; actor journals only record the tool calls and results that used them.
