# tantra

tantra is the agent turn loop as a library. The harness owns sampling, tool dispatch, permissions, approval and suspend/resume, subagents, compaction and persistence; you supply an `Agent` — a declarative class of values and function references — and run turns against it.

Turns are durable and re-entrant: a turn can suspend on a question and resume in a different process. One `Harness` drives a CLI, an HTTP server and a background worker without changing agent code.

Install name is `tantra-harness`, import name is `tantra`.

## Where to go

- [Install](getting-started/install.md) — Python version, the extras matrix, store setup.
- [Quickstart](getting-started/quickstart.md) — a full offline turn with `FakeProvider`, then a real provider.
- [Concepts](concepts/turn-loop.md) — the turn loop, how `Agent` / session / `Harness` relate, and what durability buys you.
- [Guides](guides/tools.md) — task-shaped pages: defining tools, the shipped tool pack, permissions and hooks, skills, memory, subagents, storage, providers, compaction, telemetry.
- [Reference](reference/agent.md) — the public API surface, one page per module.
- [Sharp edges](sharp-edges.md) — the behaviours that surprise people. Read this before you ship.
