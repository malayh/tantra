---
title: Defining tools
---

`@tool` turns a typed function into a model-visible JSON-schema tool. Its docstring is the description.

```python
from tantra import Context, tool


@tool
async def lookup(metric: str, ctx: Context) -> str:
    """Read one metric."""
    await ctx.emit(f"reading {metric}")
    return await ctx.deps.metrics.read(metric)
```

The bare `Context` annotation is injected and omitted from the model schema. `ctx.emit()` appends durable progress, and `ctx.ask()` raises a typed live question. `ctx.deps` comes from `Runtime(deps_factory=...)` for each turn.

Tools declare `permission="allow"`, `"ask"`, or `"deny"`. The longest matching `Agent.permissions` rule replaces that declaration and may widen or narrow it; without a match, the tool declaration applies, then the Runtime default. Hooks may transform or deny a validated call before execution.

All calls in one assistant response are validated and authorized before execution. Accepted async calls start on the event loop; sync callables start with `asyncio.to_thread`. Sibling failures become individual error results, and provider-visible results preserve the assistant's original call order.

Actor coordination uses the injected `spawn`, `send`, `status`, and `cancel_agent` tools described in [Actor subagents](/tantra/docs/guides/subagents/).

## Long-running tools

A tool whose work outlives the turn, such as a build or a sandbox task, starts the work, keeps its own task record with the agent's id (`UUID(hex=ctx.session_id)`), and returns at once. The agent gets the result later as a new input, and one conditional state change on the task decides between delivering and cancelling:

```python
async def task_finished(task_id: UUID, output: str) -> None:
    async with db.transaction():
        if await tasks.finish(task_id, output):
            await jobs.enqueue("deliver_task", task_id)


async def deliver_task(task_id: UUID) -> None:
    task = await tasks.get(task_id)
    await runtime.deliver(task.agent_id, task.output, command_id=task_id, source="build")


class StopTasks(Hook):
    async def on_cancel(self, root_id: UUID, agent_ids: list[UUID]) -> None:
        task_ids = await tasks.cancel_running(agent_ids)
        await jobs.enqueue("kill_tasks", task_ids)
```

- `tasks.finish` and `tasks.cancel_running` are one conditional update each, such as `UPDATE tasks SET state = 'done' WHERE id = $1 AND state = 'running'`. Whichever changes the row first wins, so a cancelled task never delivers and a finished task is never killed.
- The host's task watcher calls `task_finished` outside any turn. The delivery job commits with the state change, so a crash can't lose the wake-up, and the job retries until `runtime.deliver` succeeds. The task id as `command_id` makes a repeated delivery a duplicate. [`deliver`](/tantra/docs/reference/runtime/#deliver) needs no connection or writer.
- The agent sees `[event build] <output>` as a new input, queued behind any inputs already waiting.
- [`on_cancel`](/tantra/docs/reference/hooks/#on_cancel) only marks tasks cancelled and queues the slow kill: the cancel waits for the hook.
