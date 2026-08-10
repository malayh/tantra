# tantra

The agent turn loop as a library. Install name `tantra-harness`, import name `tantra`.

## What it is

- The harness owns the loop — sampling, tool dispatch, permissions, approval/suspend-resume, subagents, compaction, persistence. You supply an `Agent` (a declarative class of values and function references) and run turns against it.
- Tools, hooks and skills plug in. An `Agent` declares `tools`; a `Harness` takes `hooks` and a `Skills` source.
- Agents hold no live I/O. A resume can run in another process and look the agent up by name from the persisted session header.
- Configuration is explicit construction: tools are built by factories (`web_search(api_key=...)`) in your own module, so keys are per-agent and a misconfiguration fails at construction, not mid-turn. The library never reads the environment.
- Shipped tools live under `tantra.extratools.*` and their dependencies are **extras**, so the base install stays light — `pydantic`, `httpx`, `openai` only. The shell tools are stdlib and need no extra.

## Install

| Command | Adds |
|---|---|
| `pip install tantra-harness` | core + `tantra.extratools.shell` (`bash`, `ShellGuard`) |
| `pip install "tantra-harness[web]"` | `web_search` (Brave) + `web_fetch` |
| `pip install "tantra-harness[doc]"` | `read_doc` for PDF / docx |
| `pip install "tantra-harness[postgres]"` | the psycopg driver that `PostgresStore` needs at use time; the class itself imports without it |
| `pip install "tantra-harness[web,doc]"` | combine freely |

**Import-name collision.** The unrelated PyPI project `tantra` also installs an `import tantra`. Installing both into one environment clobbers the import silently. Do not co-install them.

## Basic usage

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

Notes on the snippet:

- `harness.run(...)` is an async iterator of `Emitted`. The turn only advances while the stream is consumed; `tantra.collect` drains it into a list.
- `bash` declares `permission="ask"`, so a headless run must override it — hence `permissions = {"bash": "allow"}`. Globs work (`"web_*": "allow"`).
- `ShellGuard()` denies destructive commands with a reason the model sees. `ShellGuard(on_trip="ask")` escalates to the human approval flow instead. It is a guardrail, not a sandbox.

## Docs

- https://malayh.github.io/tantra/
- https://malayh.github.io/tantra/docs/

## Reference app

`apps/agni` in https://github.com/malayh/tantra — a terminal coding agent built on this library: REPL, permission prompts, skills, memory, compaction.

## License

Apache-2.0
