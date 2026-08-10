# Permissions & hooks

Two mechanisms. Permissions are declarative and answer "may this tool run at all"; hooks are code and answer everything else.

## The ruleset

`Agent.permissions` is `dict[glob, "allow" | "ask" | "deny"]` over tool **names**.

```python
class Build(Agent):
    tools = [read_file, write_file, bash()]
    permissions = {"*": "deny", "read_*": "allow", "write_*": "ask"}
```

Resolution order:

1. The **longest matching glob** wins. `{"*": "deny", "write_*": "ask", "write_dashboard": "allow"}` gives `write_dashboard` → allow, `write_panel` → ask, everything else → deny.
2. Equal-length patterns resolve to the **strictest** value (`deny` > `ask` > `allow`).
3. No matching rule → the tool's own `permission=` declaration.
4. Still nothing → `Harness(default_permission=...)`, which defaults to `"allow"`.

An `ask` verdict suspends the turn with an `Approval` request carrying `extra={"permission": <tool name>}`; a `deny` verdict becomes an `is_error` result reading `denied by permissions: <tool>` that the model can adapt to — never a suspend. Invalid values are rejected when the `Harness` is built.

## Sub-agent permissions are derived, never widened

A child's effective verdict is the strictest of its own decision and **every ancestor's** decision for the same tool name. The chain is rebuilt from `parent_id` on resume; a missing ancestor header fails closed.

!!! warning "A rule-less ancestor still votes"
    An ancestor with no matching rule contributes `Harness(default_permission=)`. Under `default_permission="ask"`, a child that explicitly declares `{"skill": "allow"}` still asks, because its parent's silent vote is `ask` and `ask` is stricter. This is the never-widen rule working as designed, and it surprises everyone once. To grant a child a silent allow, give the *parent* the allow rule too.

## Hooks

Subclass `Hook` and override only what you need; pass instances as `Harness(hooks=[...])`.

| Hook | When | Return |
|---|---|---|
| `before_turn(turn)` | once per `run()`, after `TurnStarted` is persisted (**not** on `resume()`) | — |
| `before_sample(turn)` | before every model call, including on a resumed turn | — |
| `before_tool(call, turn)` | after `ToolCallRequested` is persisted, before the permission check | `None` / `ToolCallRequested` / `Denial` / `Escalation` |
| `after_tool(call, result, is_error, turn)` | only for tools that actually executed | a replacement result, or `None` to keep it |
| `after_turn(turn, event)` | with the terminal `TurnCompleted` or `TurnFailed` | — |
| `on_event(emitted)` | every emitted event, live deltas included | — |

`before_tool` returns:

- `None` — pass through.
- a `ToolCallRequested` (use `call.model_copy(update={"args": ...})`) — **transform the executed arguments only**. The log keeps what the model asked for; `name` and `call_id` changes are ignored. It re-fires on resume for the same call, so keep it deterministic.
- `Denial(reason)` — the tool is never invoked; the model sees `denied by hook: <reason>`. The first denial in the chain wins and stops it.
- `Escalation(reason)` — forces the verdict to at least `ask`. The chain keeps running (a later `Denial` still wins), the reason is prepended to the approval body, and exactly one `AskRaised` is produced even when the tool is already `permission="ask"`.

A hook that raises aborts the turn the way abandonment does — the turn stays incomplete and `resume()` re-enters.

## Guardrails belong in `before_tool`

Globs match tool *names* only: "allow `ls`, deny `rm`" is inexpressible as a rule. Argument-level policy is a hook.

```python
import asyncio

from tantra import Agent, Denial, FakeProvider, Harness, Hook, MemoryStore, Sample, TurnContext, collect, tool
from tantra.events import ToolCallCompleted, ToolCallRequested, TurnCompleted
from tantra.providers.base import ToolCall


@tool
async def write_dashboard(path: str) -> str:
    """Write a dashboard."""
    return f"wrote {path}"


class Editor(Agent):
    tools = [write_dashboard]
    permissions = {"write_*": "allow"}


class Guard(Hook):
    async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial | ToolCallRequested | None:
        if call.name != "write_dashboard":
            return None
        if call.args["path"].startswith("/"):
            return Denial("absolute paths are off in this run; write inside the project instead")
        return call.model_copy(update={"args": {"path": f"build/{call.args['path']}"}})


async def main() -> None:
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="c1", name="write_dashboard", args='{"path": "/etc/p99.json"}')]),
                Sample(tool_calls=[ToolCall(id="c2", name="write_dashboard", args='{"path": "p99.json"}')]),
                Sample(text="done."),
            ]
        ),
        MemoryStore(),
        [Editor],
        default_model="fake/model",
        hooks=[Guard()],
    )
    sid = (await harness.create_session(Editor)).id

    events = await collect(harness.run(sid, "write the dashboard"))
    for done in [e.event for e in events if isinstance(e.event, ToolCallCompleted)]:
        print(done.is_error, done.result)
    print([e.event.args for e in events if isinstance(e.event, ToolCallRequested)])
    print([e.event.stop_reason for e in events if isinstance(e.event, TurnCompleted)])


asyncio.run(main())
```

```text
True denied by hook: absolute paths are off in this run; write inside the project instead
False wrote build/p99.json
[{'path': '/etc/p99.json'}, {'path': 'p99.json'}]
['completed']
```

The log records what the model asked for (`p99.json`); the tool received `build/p99.json`.

[`ShellGuard`](shell.md) is the worked example that ships — a `before_tool` hook that parses shell command lines and returns `Denial` or `Escalation`.

## Next

- [Shell & ShellGuard](shell.md), [Hooks reference](../reference/hooks.md), [Permissions reference](../reference/permissions.md), [Ask reference](../reference/ask.md).
