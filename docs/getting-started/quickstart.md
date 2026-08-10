# Quickstart

Two steps: run a turn offline against `FakeProvider`, then swap in a real provider.

## 1. A turn with no network

`FakeProvider` replays scripted samples, so the loop, tool dispatch and event stream all run without an API key. `MemoryStore` keeps the session in process.

```python
import asyncio

from tantra import Agent, FakeProvider, Harness, MemoryStore, Sample, tool
from tantra.events import ToolCallCompleted, TurnCompleted
from tantra.providers.base import TextDelta, ToolCall


@tool
async def search_metrics(query: str) -> list[str]:
    """Search for relevant metrics based on query."""
    return [f"metric:{query}"]


class Bot(Agent):
    prompt = "You are helpful."
    tools = [search_metrics]


async def main() -> None:
    provider = FakeProvider(
        [
            Sample(tool_calls=[ToolCall(id="c1", name="search_metrics", args='{"query": "p99"}')]),
            Sample(text="p99 is fine."),
        ]
    )
    harness = Harness(provider, MemoryStore(), [Bot], default_model="fake/model")
    session = await harness.create_session(Bot)

    async for emitted in harness.run(session.id, "how is p99?"):
        match emitted.event:
            case ToolCallCompleted() as done:
                print(f"[tool result] {done.result}")
            case TextDelta() as delta:
                print(delta.text, end="", flush=True)
            case TurnCompleted():
                print()


asyncio.run(main())
```

Output:

```text
[tool result] ['metric:p99']
p99 is fine.
```

What each piece does:

- `@tool` turns a function into a tool. The docstring is the description the model sees; the signature becomes the JSON schema.
- `Agent` is declarative — values and function references only, no live I/O. It is looked up by name on resume.
- `Harness(provider, store, [Bot], default_model=...)` wires the engine. `default_model` covers agents that do not set `model`.
- `harness.run(...)` is an async iterator of `Emitted`. **The turn only advances while you consume the stream.** `tantra.collect` drains it into a list if you do not want to iterate.

## 2. Swap in a real provider

Same shape, real model. This needs an OpenAI-compatible API key (and a Brave key for `web_search`), plus `pip install "tantra-harness[web,doc]"`.

```python
import asyncio

from tantra import Agent, Harness, OpenAICompatible, SQLiteStore
from tantra.extratools.doc import read_doc
from tantra.extratools.shell import ShellGuard, bash
from tantra.extratools.web import web_fetch, web_search
from tantra.providers.base import TextDelta


class Researcher(Agent):
    model = "gpt-5"
    prompt = "You answer questions using the web, local documents and the shell."
    tools = [bash(), web_search(api_key=BRAVE_API_KEY), web_fetch(), read_doc()]
    permissions = {"bash": "allow"}


async def main() -> None:
    store = SQLiteStore("sessions.db")
    await store.setup()
    harness = Harness(
        OpenAICompatible("https://api.openai.com/v1", OPENAI_API_KEY),
        store,
        [Researcher],
        hooks=[ShellGuard()],
    )
    session = await harness.create_session(Researcher)
    async for emitted in harness.run(session.id, "What changed in Python 3.13?"):
        if isinstance(emitted.event, TextDelta):
            print(emitted.event.text, end="", flush=True)


asyncio.run(main())
```

What changed:

- Tools are built by factories (`web_search(api_key=...)`) in your own module, so keys are per-agent and a misconfiguration fails at construction, not mid-turn. The library never reads the environment.
- `bash` declares `permission="ask"`, so a headless run must override it — hence `permissions = {"bash": "allow"}`. Globs work (`"web_*": "allow"`).
- `ShellGuard()` denies destructive commands with a reason the model sees. `ShellGuard(on_trip="ask")` escalates to the human approval flow instead. It is a guardrail, not a sandbox.
- `SQLiteStore` persists the session, so a suspended turn can resume in another process.

## Next

- [Concepts](../concepts/turn-loop.md) — what the loop actually does per turn, and how suspend/resume works.
- [Guides](../guides/tools.md) — writing your own tools, permissions and hooks, skills, memory, storage.
