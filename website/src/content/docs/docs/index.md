---
title: tantra
---

Tantra is a process-wide actor runtime for Python agents. Every root and child agent owns a durable FIFO inbox, one active turn, an independent journal, and a scalar replay cursor.

- [`Runtime`](/tantra/docs/reference/runtime/) owns providers, stores, agent resolution, active actors, typed asks, writer generations, and local subscriber notifications.
- [`Connection`](/tantra/docs/reference/runtime/#connection) is the root-facing read/write boundary.
- [`Agent`](/tantra/docs/reference/agent/) declares prompts, tools, subagents, permissions, skills, and output schemas.
- [`Store`](/tantra/docs/reference/stores/) persists session headers, commands, and sequenced events.

Start with the [quickstart](/tantra/docs/getting-started/quickstart/), then read [actor architecture](/tantra/docs/concepts/architecture/), [durability and crashes](/tantra/docs/concepts/durability/), and [subagents](/tantra/docs/guides/subagents/).

Tantra 2.0 adds [`tantra.acp`](/tantra/docs/reference/acp/), which serves a Runtime to any ACP client over an ACP v2–inspired protocol plus the `tantra` profile: journal cursors, client command IDs, one fenced writer per root and lazy child transcripts. `tantra-acp-client` is its TypeScript client. Existing PostgreSQL deployments should follow the [2.0 upgrade guide](/tantra/docs/guides/migration-2.0/) with writers stopped.
