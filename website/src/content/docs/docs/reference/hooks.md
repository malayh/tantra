---
title: Hooks
---

Subclass `Hook`, override the needed async callbacks, and pass instances to `Runtime(hooks=[...])`.

Callbacks cover turn start/end, sample start, tool before/after, durable events, and cancels. Hooks execute in list order. `before_tool` may transform a call or return `Denial`/`Escalation`.

Every hook is awaited. The turn waits until a turn, sample, tool or event hook returns, and `cancel` or `cancel_agent` waits until `on_cancel` returns. Slow work, such as network calls, killing processes or heavy compute, belongs outside the hook, in a job or a task the host starts.

## on_cancel

```python
async def on_cancel(self, root_id: UUID, agent_ids: list[UUID]) -> None
```

Called after a cancel that stops agents is saved, with every agent it covers, so the host can stop their work outside tantra.

| Cancel | Queued event inputs | `agent_ids` | Runs on |
| --- | --- | --- | --- |
| Tree cancel (`descendants=True`) | Removed | Every agent in the tree | The Runtime that called `Connection.cancel` |
| Root-only cancel (`descendants=False`) | Kept | Not called | — |
| `cancel_agent` | Removed for the child and its descendants | The child and its descendants | The tree's owner, inside the parent's tool call |

- The list comes from the tree, not from the running turns: idle agents waiting on background work are included, and so are finished agents.
- It never runs on a coordinator owner's request handler, so it may call tantra for the same tree, for example `runtime.deliver`.
- A retried cancel calls it again. Make it idempotent.
- An error reaches the requester: `Connection.cancel` raises, and `cancel_agent` returns a tool error. The cancel stays saved, and repeating it calls the hook again.
- It is missed when the requester stops between the saved cancel and the hook; a retried cancel calls it. During a rolling deploy, a 3.0 owner's cancel reply carries no agent list, and the hook isn't called.
