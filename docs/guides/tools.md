# Defining tools

A tool is a decorated function. The signature becomes the JSON schema, the docstring becomes the description the model reads.

```python
from tantra import Context, tool


@tool
async def search_metrics(query: str, ctx: Context, limit: int = 5) -> list[str]:
    """Search for relevant metrics based on query."""
    await ctx.emit(f"searching for {query}")
    return [f"{query}:{index}" for index in range(limit)]
```

- Type hints drive the schema; parameters without a default are required. Sync and async functions both work.
- `@tool(name=..., description=..., permission=...)` overrides the derived name, the docstring, and the tool's default permission verdict.
- The decorated symbol is a `Tool`, **not a callable**. `search_metrics("p99")` fails; the loop calls `await tool.invoke(args, ctx)`.

!!! tip "The docstring is a prompt, not documentation"
    It ships in every sample request. Write it for the model: when to use the tool, when *not* to, hard constraints, what the output looks like. The shipped tools ([`bash`](shell.md), [`web_search`](web-search.md)) run 15–25 lines each — read them as templates.

## `ctx` is injected by annotation

A parameter annotated exactly `ctx: Context` is stripped from the model-facing schema and injected at call time. The name does not matter; the annotation does.

!!! warning
    Write `ctx: Context`, nothing else. `ctx: Context | None` is a union, not a type, so the parameter is never recognised or stripped — and the schema builder then blows up on `Context` itself with `PydanticSchemaGenerationError`, at decoration time (i.e. at import). An unannotated `ctx` survives decoration but is rejected when the `Harness` is built. Because Python forbids a non-default parameter after a defaulted one, put `ctx` before your optional arguments (as above).

## Factories for configured tools

Anything a tool needs at construction — an API key, a timeout, a base URL — goes in a closure. This is how the shipped pack works, and it means a misconfiguration fails at construction rather than mid-turn. The library never reads the environment.

```python
from tantra import Tool, tool


def status_check(base_url: str) -> Tool:
    @tool(permission="ask")
    async def status_check(path: str) -> str:
        """Report the health of one service by path."""
        return f"{base_url}{path} is healthy"

    return status_check
```

Per-request values (a tenant-scoped client, a DB session) belong in `ctx.deps` instead — `Harness(deps_factory=...)` rebuilds them on every `run`/`resume`, including one in another process.

## The `Context` surface

| Attribute | What it gives you |
|---|---|
| `deps` | whatever `deps_factory(header)` returned |
| `store` | the live `Store` |
| `memory` | the `Memory` passed to `Harness`, or `None` |
| `session_id`, `turn_id`, `call_id`, `depth` | identity of the running call |
| `await emit(message)` | progress, **persisted** as `ToolProgress` |
| `await ask(request)` | suspend the turn for a human ([durability](../concepts/durability.md)) |
| `await spawn(agent, input)` | run a child session ([subagents](subagents.md)) |
| `await fan_out(tasks, max_concurrency=4)` | run several children concurrently |

`ctx.emit` progress lands in the log between `ToolCallStarted` and `ToolCallCompleted`, so it survives a reconnect. `ask`, `spawn` and `fan_out` are only available inside a running tool call.

## Errors are your only error channel

An exception raised by a tool becomes an `is_error` tool result whose content is **`str(exc)` and nothing else** — no type, no traceback, and `is_error` itself is dropped on the OpenAI-compatible wire. The message is the entire contract with the model.

```python
raise RuntimeError(
    f"read_doc found no file at {path!r} — check the path against the working directory, "
    "or list the directory first to find the document's real name"
)
```

Name what went wrong, then what to do next. "Invalid input" teaches the model nothing and it will retry the same call.

## Reserved names

- `skill` — injected automatically when the harness has a `Skills` catalogue; a user tool of that name raises at construction.
- `submit_output` — the synthetic tool `Agent.output_schema` adds; the loop intercepts calls by that name, so a tool of your own with it is unreachable on an agent that sets `output_schema`.

## A full turn

```python
import asyncio

from tantra import Agent, Context, FakeProvider, Harness, MemoryStore, Sample, Tool, collect, tool
from tantra.events import ToolCallCompleted, ToolProgress, TurnCompleted
from tantra.providers.base import ToolCall


@tool
async def search_metrics(query: str, ctx: Context, limit: int = 5) -> list[str]:
    """Search for relevant metrics based on query."""
    await ctx.emit(f"searching for {query}")
    return [f"{query}:{index}" for index in range(limit)]


def status_check(base_url: str) -> Tool:
    @tool(permission="ask")
    async def status_check(path: str) -> str:
        """Report the health of one service by path."""
        return f"{base_url}{path} is healthy"

    return status_check


class Analyst(Agent):
    prompt = "You investigate metrics."
    tools = [search_metrics, status_check("https://internal.example")]
    permissions = {"status_check": "allow"}


async def main() -> None:
    harness = Harness(
        FakeProvider(
            [
                Sample(
                    tool_calls=[
                        ToolCall(id="c1", name="search_metrics", args='{"query": "p99", "limit": 2}'),
                        ToolCall(id="c2", name="status_check", args='{"path": "/api"}'),
                    ]
                ),
                Sample(text="all good."),
            ]
        ),
        MemoryStore(),
        [Analyst],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Analyst)).id

    for emitted in await collect(harness.run(sid, "how is p99?")):
        match emitted.event:
            case ToolProgress() as progress:
                print(f"[progress] {progress.message}")
            case ToolCallCompleted() as done:
                print(f"[result] {done.result}")
            case TurnCompleted() as end:
                print(f"[turn] {end.stop_reason}")

    print(list(search_metrics.schema.parameters["properties"]))


asyncio.run(main())
```

```text
[progress] searching for p99
[result] ['p99:0', 'p99:1']
[result] https://internal.example/api is healthy
[turn] completed
['query', 'limit']
```

Note the last line: `ctx` never reaches the model. Note also that the two calls ran serially — tool calls within a sample always do.

## Next

- [Permissions & hooks](permissions-hooks.md) — gating what a tool is allowed to do.
- [Tools reference](../reference/tools.md), [Context reference](../reference/context.md).
