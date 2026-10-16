# Quickstart

This offline example exercises durable command acceptance, a tool call, and a terminal result.

```python
import asyncio
from uuid import uuid4

from tantra import Agent, FakeProvider, MemoryStore, Runtime, Sample, tool
from tantra.providers.base import ToolCall


@tool
async def search_metrics(query: str) -> list[str]:
    """Return matching synthetic metrics."""
    return [f"metric:{query}"]


class Bot(Agent):
    tools = [search_metrics]


async def main() -> None:
    provider = FakeProvider([
        Sample(tool_calls=[ToolCall(id="c1", name="search_metrics", args='{"query":"p99"}')]),
        Sample(text="p99 is fine."),
    ])
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="fake/model")
    root_id = await runtime.create(Bot)
    async with runtime.connect(root_id, writable=True) as connection:
        result = await connection.prompt("How is p99?", command_id=uuid4())
    print(result.text)
    await runtime.aclose()


asyncio.run(main())
```

For live streaming, accept the command with `connection.send(...)`, then iterate the connection. Each yielded `LoggedEvent` contains `agent_id`, integer `seq`, and the durable event. Save `seq` after consumption and reconnect with `after=seq`.

Swap `FakeProvider` for `OpenAICompatible`, and `MemoryStore` for `SQLiteStore`, `FileSystemStore`, or `PostgresStore`. Applications own provider and store setup and cleanup.
