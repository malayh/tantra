# Hooks

```python
from tantra import Denial, Escalation, Hook
```

Lifecycle callbacks on the turn loop. Subclass `Hook`, override only what you need, and pass instances to `Harness(hooks=[...])`. They run in list order, and every method is `async`.

## The six methods

```python
async def before_turn(self, turn: TurnContext) -> None
async def before_sample(self, turn: TurnContext) -> None
async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> ToolCallRequested | Denial | Escalation | None
async def after_tool(self, call: ToolCallRequested, result: Any, is_error: bool, turn: TurnContext) -> Any
async def after_turn(self, turn: TurnContext, event: SessionEvent) -> None
async def on_event(self, emitted: Emitted) -> None
```

| Method | When | Notes |
|---|---|---|
| `before_turn` | Once per `run()`, after `TurnStarted` is persisted | **Not called on `resume()`.** `turn.history` / `model` / `limits` / `provider` are still `None` here — see [Context](context.md). |
| `before_sample` | Before every model call | Including on resumed turns and retried samples in another process. |
| `before_tool` | Before a tool executes | The gate. Contract below. |
| `after_tool` | After the tool returns or raises, before the result is persisted | Return `None` to keep the result unchanged, anything else to replace it. `is_error` tells you which case you are in; it is not changeable. |
| `after_turn` | Once the terminal `TurnCompleted` or `TurnFailed` is persisted | The `event` argument is that terminal event. |
| `on_event` | For every emitted event | Includes the live `TextDelta` / `ReasoningDelta` / `ToolCallDelta` that are never persisted. Each event notifies once, from the session that produced it. |

## `Denial` and `Escalation`

```python
@dataclass(frozen=True)
class Denial:
    reason: str


@dataclass(frozen=True)
class Escalation:
    reason: str
```

## The `before_tool` contract

Return one of four things:

| Return | Effect |
|---|---|
| `None` | Pass through unchanged. |
| a `ToolCallRequested` | Replaces the **arguments** the tool executes with. The log keeps what the model asked for. |
| `Denial(reason)` | The tool never runs. The model gets `denied by hook: <reason>` as an error result and adapts. |
| `Escalation(reason)` | Forces the verdict to at least `ask`, so the call goes through the normal approval suspend/resume flow with the reason shown to the human. |

```python
class NoProduction(Hook):
    async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial | None:
        if call.name == "deploy" and call.args.get("target") == "prod":
            return Denial("production deploys are approved out of band")
        return None
```

Rules the chain follows:

- **Only `args` survive a replacement.** The returned object's `name`, `call_id` and `sample_id` are ignored — the tool was already resolved from the original call's name, and the permission verdict is computed from it too.
- **A `Denial` stops the chain immediately.** Later hooks do not run.
- **An `Escalation` does not stop the chain.** The first escalation's reason is kept, later hooks still run, and a later `Denial` still wins. The result is one merged verdict — `strictest(verdict, "ask")` — so an already-`ask` tool raises one approval, not two.
- **Hooks re-fire on resume.** A suspended turn re-executes the tool call from the start, and `before_tool` runs again for it. Keep it a pure decision; put side effects in `after_tool` or `on_event`.
- Denial and escalation both happen **before** permission resolution. A denial still writes `ToolCallStarted` ahead of the error `ToolCallCompleted` — the tool never runs, but the pair stays intact for readers.

## See also

- [Permissions](permissions.md) — how the verdict an `Escalation` raises is resolved.
- [Ask](ask.md) — the approval flow it lands in.
- [Extra tools](extratools.md) — `ShellGuard` is a `Hook` built on this contract.
- [Guides: permissions & hooks](../guides/permissions-hooks.md).
