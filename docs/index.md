# tantra

Tantra is a process-wide actor runtime for Python agents. Every root and child agent owns a durable FIFO inbox, one active turn, an independent journal, and a scalar replay cursor.

- [`Runtime`](reference/runtime.md) owns providers, stores, agent resolution, active actors, typed asks, writer generations, and local subscriber notifications.
- [`Connection`](reference/runtime.md#connection) is the root-facing read/write boundary.
- [`Agent`](reference/agent.md) declares prompts, tools, subagents, permissions, skills, and output schemas.
- [`Store`](reference/stores.md) persists session headers, commands, and sequenced events.

Start with the [quickstart](getting-started/quickstart.md), then read [actor architecture](concepts/architecture.md), [durability and crashes](concepts/durability.md), and [subagents](guides/subagents.md).
