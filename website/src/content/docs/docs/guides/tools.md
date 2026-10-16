# Defining tools

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

Actor coordination uses the injected `spawn`, `send`, and `finish` tools described in [Actor subagents](subagents.md).
