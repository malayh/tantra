# Actor subagents

Subagents are independent session actors with their own UUIDs, contexts, FIFO inboxes, journals, and integer cursors. Tantra injects three model-visible tools.

| Tool | Effect |
|---|---|
| `spawn(agent_name, input)` | Create a declared child, queue its first input, and return its UUID immediately |
| `send(agent_id, input)` | Durably queue a message across one direct parent-child edge |
| `finish(result)` | Permanently close a child and deliver its result to its parent |

```python
class Worker(Agent):
    prompt = "Research one claim. Send updates, then finish with your finding."


class Lead(Agent):
    prompt = "Spawn workers in parallel and synthesize their finished messages."
    subagents = [Worker]
```

Multiple `spawn` calls in one assistant response start in parallel. The parent is not blocked and may become idle while children work. Assistant text stays in the actor's own journal; information crosses an edge only through `send` or `finish`.

`finish` fails while any descendant remains unfinished. `Runtime.max_depth` defaults to 3 and rejects an over-depth spawn before creating a session. Human messages target only the root, while the root writer may answer live asks from any descendant.

Subscribe to child activity with `runtime.events(child_id, after=cursor)`. Root and child sequence numbers are unrelated.
