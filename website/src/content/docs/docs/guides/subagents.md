---
title: Actor subagents
---

Subagents are independent session actors with their own UUIDs, contexts, FIFO inboxes, journals, and integer cursors. Tantra injects four model-visible tools where applicable.

| Tool | Effect |
|---|---|
| `spawn(agent_name, input, name=None)` | Create a declared child, optionally name it, queue its first input, and return its UUID immediately |
| `send(agent_id, input)` | Durably queue a message across one direct parent-child edge |
| `status(agent_id)` | Read durable status for one direct child without activating it or reading its journal |
| `cancel_agent(agent_id)` | Cancel the active and queued work of one direct child and its descendants; returns `{agent_id, cancelled}` |

```python
class Worker(Agent):
    prompt = "Research one claim and reply with your finding."


class Lead(Agent):
    prompt = "Spawn workers in parallel and synthesize their replies."
    subagents = [Worker]
```

Multiple `spawn` calls in one assistant response start in parallel. The parent is not blocked and may become idle while children work. Assistant text stays in the actor's own journal, except a child's final message each turn, which becomes its reply. Information also crosses an edge through `send`. A child can send only to its direct parent or children, and a parent can send only to direct children.

Every child turn end, except a cancel by its parent, wakes the direct parent with one queued input, and the child stays reusable through `send`:

- A completed turn queues `[agent <child-uuid> replied] <text>`. The text is the turn's final assistant message, or the canonical JSON of `submit_output` for a child with an `output_schema`. An empty message becomes `(no text reply)`, and text over 32 KiB of UTF-8 is cut at a character boundary and ends with `[reply truncated at 32 KiB]`.
- `max_steps`, failed, cancelled and interrupted turns queue `[agent <child-uuid> turn ended] <stable-json>`, where the JSON contains `child_id`, `turn_id`, `outcome`, `stop_reason`, and optional `error`.
- `cancel_agent` ends the subtree's turns as cancelled with `stop_reason` `cancelled_by_parent` and wakes no parent.

A child may reply several times as its parent sends more work, and a mid-level child may reply while its own children still run. `tree_status` is the authoritative view of the whole tree. `finish` stays a reserved tool name for child agents so that journals written before 3.0, where a child finished explicitly, project unambiguously.

`Runtime.max_depth` defaults to 3 and rejects an over-depth spawn before creating a session. Human messages and typed asks are root-only. A child calling `ctx.ask()` receives an ordered tool error directing it to `send()` its parent. Runtime rejects static child permissions whose effective verdict is `ask`; a dynamic escalation to `ask` also fails without emitting `AskRaised`.

The optional child `name` is trimmed, and a blank name is omitted. It is a durable display label only: `agent_name` remains the actor type and the child UUID is unchanged. A deterministic retry must reuse the same normalized name.

Applications can poll `runtime.tree_status(root_id)` or follow `runtime.watch_tree(root_id)` without activating actors or reading journals. Subscribe to child activity with `runtime.events(child_id, after=cursor)` only when detailed output is needed. Root and child sequence numbers are unrelated.
