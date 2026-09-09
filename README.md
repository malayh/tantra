<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/malayh/tantra@main/assets/logo.svg" width="140" alt="tantra logo">
</p>

# tantra

The agent actor runtime as a Python library. Install name `tantra-harness`, import name `tantra`.

Tantra gives each agent a durable FIFO inbox, an independent event journal, and one active turn. A process-wide `Runtime` owns providers, stores, tools, hooks, permissions, skills, memory, compaction, telemetry, actor tasks, and writer connections.

## Install

| Command | Adds |
|---|---|
| `pip install tantra-harness` | Core runtime and shell tools |
| `pip install "tantra-harness[web]"` | Brave web search and web fetch |
| `pip install "tantra-harness[doc]"` | PDF and Word document reading |
| `pip install "tantra-harness[postgres]"` | PostgreSQL storage |
| `pip install "tantra-harness[telemetry]"` | OpenTelemetry tracing |

The unrelated PyPI project `tantra` installs the same import name. Do not install both projects in one environment.

## Basic usage

```python
import asyncio
from uuid import uuid4

from tantra import Agent, OpenAICompatible, Runtime, SQLiteStore
from tantra.extratools.web import web_search


class Researcher(Agent):
    model = "gpt-5"
    prompt = "Answer with current, sourced information."
    tools = [web_search(api_key=BRAVE_API_KEY)]


async def main() -> None:
    store = SQLiteStore("sessions.db")
    await store.setup()
    runtime = Runtime(
        OpenAICompatible("https://api.openai.com/v1", OPENAI_API_KEY),
        store,
        [Researcher],
    )
    root_id = await runtime.create(Researcher)
    async with runtime.connect(root_id, writable=True) as connection:
        result = await connection.prompt(
            "What changed in Python 3.13?",
            command_id=uuid4(),
        )
    print(result.text)
    await runtime.aclose()


asyncio.run(main())
```

`Connection.prompt()` accepts a durable command and waits for its terminal result. For streaming, call `send()` and iterate the connection. Reconnect with the last scalar `seq` as `after`; use `Runtime.events()` for a child journal. Command IDs are UUIDs and make acceptance idempotent.

Subagents are independent actors. Models use the injected `spawn`, `send`, and `finish` tools; no parent stream merges child events. A live typed ask suspends only its actor turn and must be answered through the current writable root connection.

See the [documentation](https://malayh.github.io/tantra/docs/) and the [1.0 migration guide](https://malayh.github.io/tantra/docs/guides/migration-1.0/).

## Reference app

`apps/sarathi` is the repository's interactive web application.

## License

Apache-2.0
