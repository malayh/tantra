# Tools

```python
from tantra import Context, Tool, tool
```

## `@tool`

```python
def tool(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    permission: str | None = None,
) -> Tool
```

Turns a function into a `Tool`. Works bare or called:

```python
@tool
async def search_metrics(query: str) -> str:
    """Search for metrics."""
    return f"found {query}"


@tool(permission="ask")
async def write_file(path: str, body: str) -> str:
    """Write a file to disk."""
    ...
```

| Argument | Default | Meaning |
|---|---|---|
| `name` | `fn.__name__` | The name the model calls and permission globs match. |
| `description` | the function's docstring (`""` if absent) | The model-facing description. Write it for the model: what it is for, when not to use it, what the failure text means. |
| `permission` | `None` | This tool's own verdict, used when no agent rule matches. See [Permissions](permissions.md). |

Sync and async functions both work. The parameter list becomes a pydantic model and its JSON schema; defaults become optional parameters.

!!! warning "The decorated symbol is not callable"
    `@tool` returns a `Tool` object, not a function. `await search_metrics("p99")` fails. Invoke it as `await search_metrics.invoke({"query": "p99"}, ctx)` — which is how the tests exercise tools.

## `Tool`

Constructed by `@tool`; the class is public mainly so factories can return it (`def bash(*, timeout=120.0) -> Tool`).

| Attribute | Meaning |
|---|---|
| `fn` | The wrapped function. |
| `name` / `description` / `permission` | As above. |
| `args_model` | The generated pydantic model for the parameters. |
| `schema` | A [`ToolSchema`](providers.md) sent to the provider. |
| `ctx_param` / `takes_ctx` | Name of the injected context parameter, and whether there is one. |

**`async invoke(args: dict, ctx: Context) -> Any`** validates `args` against `args_model`, injects `ctx`, calls the function and awaits an awaitable result. A validation failure raises `ValidationError`; inside a turn that becomes an error result the model reads.

Return anything JSON-serializable. Non-`str` results are `json.dumps`-ed (with `default=str`) when assembled into the next request.

## `Context`

```python
@tool
async def deploy(target: str, ctx: Context) -> str:
    """Deploy to a target."""
    await ctx.emit(f"deploying {target}")
    return "done"
```

Annotate a parameter **exactly** `ctx: Context` to receive it. The annotated parameter is stripped from the model-facing schema and injected at call time.

!!! danger "The annotation must be exact"
    Injection tests `issubclass(annotation, Context)`, so only the bare class is stripped. `ctx: Context | None` is a union, not a class — it is treated as a model-facing parameter and raises `PydanticSchemaGenerationError` at decoration time, i.e. when the module is imported. An unannotated `ctx` builds a schema with a typeless `ctx` property and is rejected at `Harness` construction: *"tool 't' has an unannotated 'ctx' parameter; annotate it `ctx: Context`"*.

### Attributes

| Attribute | Type | Meaning |
|---|---|---|
| `deps` | `Any` | Whatever `Harness(deps_factory=...)` built for this turn. `None` without a factory. |
| `store` | `Store` | The session store, for tools that read history or other sessions. |
| `memory` | `Memory \| None` | The configured [Memory](memory.md), or `None`. |
| `session_id` | `str` | Session running this call. |
| `turn_id` | `str` | Turn running this call. |
| `call_id` | `str` | This tool call's id. Stable across a resume. |
| `depth` | `int` | Session depth. Root is 0. |

### Methods

**`async emit(message: str) -> None`** — record progress as a persisted `ToolProgress` event. It is a real store append, not a live-only signal, so it survives a reconnect and shows up in `replay`.

**`async ask(request: AskRequest) -> AskResponse`** — suspend the turn until a human answers, then return the response. See [Ask](ask.md).

!!! warning "The whole tool re-runs on resume"
    The process may die while suspended. On resume the tool is re-executed **from its first line**, and every already-answered `ask` returns its recorded response without prompting again. Side effects before an `ask` therefore happen twice; nothing may be captured in a closure across the suspend. Raises `TantraError` if called outside a running tool call.

**`async spawn(agent: type[Agent] | str, input: str) -> Any`** — run `agent` as a child session and return its final text, or — when the child declares an `output_schema` — its parsed output **as a plain dict** (`model_dump(mode="json")`), not a model instance. Re-entrant: a resumed turn attaches to the child already recorded for this call instead of creating a twin. A child that asks suspends this turn too. Raises `TantraError` beyond `Harness(max_depth=...)`, or when the harness wired no spawner.

**`async fan_out(tasks: Sequence[tuple[type[Agent] | str, str]], max_concurrency: int = 4) -> list[Any]`** — run `(agent, input)` pairs as concurrent child sessions. Results are positionally aligned with `tasks`; a task that fails contributes its **exception object** in place of a result rather than failing the turn, so check `isinstance(result, Exception)`.

## Error contract

A tool that raises is caught by the loop: **only `str(exc)` reaches the model**, as a `ToolCallCompleted` with `is_error=True`. The exception type, the traceback and even the `is_error` flag are dropped before the provider sees it. The message is the entire contract — make every raise name what went wrong and what to do next.

## See also

- [Context](context.md) — `TurnContext`, the hook-facing object (not this one).
- [Extra tools](extratools.md) — the shipped tool pack.
- [Guides: defining tools](../guides/tools.md).
