# Agent

```python
from tantra import Agent, agent_name, build_name_table
```

## `Agent`

A declarative class: values and function references only, never live I/O. A resume runs in another process and looks the agent up **by name** from the persisted header, so an agent holding a client, a pool or a request-scoped object could not survive it. Per-process objects belong in `deps_factory` (see [Harness](harness.md)).

Subclass it and override class attributes. Never instantiated — the harness works with the class itself.

| Attribute | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str \| None` | `None` | Registered name. `None` derives it from the class name (see `agent_name`). |
| `model` | `str \| None` | `None` | Model id passed to the provider. `None` falls back to `Harness(default_model=...)`; if both are unset, the turn raises. |
| `prompt` | `str` or `(TurnContext) -> str \| Awaitable[str]` | `""` | System prompt. A callable is resolved before **every** sample, so it can vary with turn state. |
| `tools` | `list[Tool]` | `[]` | Tools built with [`@tool`](tools.md). Anything else raises at `Harness` construction. |
| `skills` | `list[str] \| None` | `None` | Skill filter. `None` = every skill in the catalogue, `[]` = opt out (no `skill` tool, no index block). |
| `subagents` | `list[type[Agent]]` | `[]` | Child agents. Each becomes a delegate tool named after the agent, taking one `task: str`. |
| `permissions` | `dict[str, str]` | `{}` | Glob → `"allow"` / `"ask"` / `"deny"`. See [Permissions](permissions.md). |
| `max_steps` | `int` | `40` | Samples allowed per turn. Must be ≥ 1. Hitting the cap ends the turn with `stop_reason="max_steps"`. |
| `output_schema` | `type[BaseModel] \| None` | `None` | When set, a `submit_output` tool is exposed; calling it validates the args and ends the turn with the parsed output on `TurnCompleted.output` — dumped to a plain dict, not a model instance. |

```python
class Researcher(Agent):
    model = "gpt-5"
    prompt = "You answer questions using the web."
    tools = [web_search(api_key=BRAVE_API_KEY), web_fetch()]
    permissions = {"web_*": "allow"}
```

!!! warning "Mutable class attributes"
    `tools`, `subagents` and `permissions` are plain class attributes. Subclasses that do not redeclare them share the base class's object.

## `agent_name(agent: type[Agent]) -> str`

Returns the registered name: a `name` declared **on that class** wins, otherwise the class name is lowercased with `_` inserted at case boundaries (`ResearchLead` → `research_lead`, `HTTPProbe` → `http_probe`).

The lookup is `agent.__dict__.get("name")`, so a `name` inherited from a base class is ignored — a subclass gets a name derived from its own class name unless it declares one.

## `build_name_table(agents: Iterable[type[Agent]]) -> dict[str, type[Agent]]`

Walks `agents` and their `subagents` transitively and returns the name → class table. `Harness` calls this for you; call it directly to validate a registry.

Raises `TantraError` when an entry is not an `Agent` subclass, or when two different classes resolve to the same name. The same class reached twice is fine.

## See also

- [Harness](harness.md) — what happens to an `Agent` at construction and at run time.
- [Tools](tools.md) — building the values that go in `tools`.
- [Sharp edges](../sharp-edges.md) — `skills = ()` is not an opt-out; `[]` is.
