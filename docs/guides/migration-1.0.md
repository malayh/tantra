# Migrate to 1.0

Tantra 1.0 uses a process-wide actor `Runtime` and fresh storage. It does not read pre-1.0 logs or recover in-flight work across processes.

| Before 1.0 | 1.0 |
|---|---|
| `create_session(...)` | `Runtime.create(...)`, returning a UUID |
| synchronous coordinator invocation | `Connection.prompt(...)` returns `TurnResult` |
| merged execution streaming | `Connection.send(...)`, then iterate the `Connection` or `Runtime.events(...)` independently |
| `replay(...)` | `Runtime.events(agent_id, after=seq)` |
| process-loss recovery | answer a live ask on the current writer; after interruption, use a later `send(...)` |
| blocking `ctx.spawn(...)` and `ctx.fan_out(...)` | model tools `spawn`, `send`, and `finish` |
| one merged descendant stream | subscribe to each actor journal with its own scalar cursor |
| cross-process session claims | one root tree per Runtime process; newest local writer connection wins |

Every writable `Connection` mutation now requires a UUID `command_id`: `send`, `prompt`, `answer`, and `cancel`. `Runtime.create` does not. Save the last integer `seq` separately for each subscribed actor.

Start applications on empty 1.0 storage. A process crash does not restart model calls or tools. A later root send marks an unmatched active turn interrupted and drains only commands that never started.
