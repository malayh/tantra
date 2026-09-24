# Actor subagents

Subagents are independent session actors with their own UUIDs, contexts, FIFO inboxes, journals, and integer cursors. Tantra injects four model-visible tools where applicable.

| Tool | Effect |
|---|---|
| `spawn(agent_name, input, name=None)` | Create a declared child, optionally name it, queue its first input, and return its UUID immediately |
| `send(agent_id, input)` | Durably queue a message across one direct parent-child edge |
| `status(agent_id)` | Read durable status for one direct child without activating it or reading its journal |
| `finish(result)` | Permanently close a child and deliver its result to its parent |

```python
class Worker(Agent):
    prompt = "Research one claim. Send updates, then finish with your finding."


class Lead(Agent):
    prompt = "Spawn workers in parallel and synthesize their finished messages."
    subagents = [Worker]
```

Multiple `spawn` calls in one assistant response start in parallel. The parent is not blocked and may become idle while children work. Assistant text stays in the actor's own journal; information crosses an edge through `send` or `finish`. A child can send only to its direct parent or children, and a parent can send only to direct children.

Ordinary child completion leaves the actor reusable. Every child terminal outcome other than successful `finish` queues `[agent <child-uuid> turn ended] <stable-json>` for the direct parent. The JSON contains `child_id`, `turn_id`, `outcome`, `stop_reason`, and optional `error`; it contains no child assistant text, tool output, or result. The parent may inspect the child with `status`, send more work, or wait for an explicit result. `finish` permanently closes the child and delivers its result, and it fails while any descendant remains unfinished.

`Runtime.max_depth` defaults to 3 and rejects an over-depth spawn before creating a session. Human messages and typed asks are root-only. A child calling `ctx.ask()` receives an ordered tool error directing it to `send()` its parent. Runtime rejects static child permissions whose effective verdict is `ask`; a dynamic escalation to `ask` also fails without emitting `AskRaised`.

The optional child `name` is trimmed, and a blank name is omitted. It is a durable display label only: `agent_name` remains the actor type and the child UUID is unchanged. A deterministic retry must reuse the same normalized name.

Applications can poll `runtime.tree_status(root_id)` without activating actors or reading journals. Subscribe to child activity with `runtime.events(child_id, after=cursor)` only when detailed output is needed. Root and child sequence numbers are unrelated.
