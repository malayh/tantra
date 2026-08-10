# Agent, Session, Harness

Three things, deliberately separate. Two of them are classes; a session is not — it is a `SessionHeader` plus an event log in the store, and there is no `Session` object to hold.

| | What it is | Lifetime | Holds |
|---|---|---|---|
| `Agent` | declaration | process, immutable | `model`, `prompt`, `tools`, `skills`, `subagents`, `permissions`, `max_steps`, `output_schema` |
| Session | one conversation | rows in the store | event log + `SessionHeader` (agent **name**, depth, parent_id, metadata) |
| `Harness` | the runtime | process, one or more per app | provider, `default_model`, store, `deps_factory`, hooks, skills, memory, compactor, the loop, the name→agent table |

## Agent holds no I/O

No provider, no client, no DSN, no pool — values and function references only.

```python
class DashboardEditor(Agent):
    model = "google/gemini-3-pro"
    prompt = "You edit dashboards. Explore before you change anything."
    tools = [search_metrics, get_label_values]
    subagents = [PromQLWriter]
    permissions = {"search_*": "allow", "write_*": "ask"}
    max_steps = 40
    output_schema = Dashboard
```

`prompt` is a `str`, or a callable taking the `TurnContext` (sync or async). Anything dynamic that needs I/O — a git status block, a per-tenant preamble — is computed by your app and baked into the class it constructs.

This is forced by the durable loop, not by taste. A resume runs in a different process, which rebuilds the `Harness` from config and looks the agent up **by name** from the persisted session header. An `Agent` holding a live client could not survive that. The same constraint produced `deps_factory(header)` — per-request clients are built there, not welded onto the class.

Cardinality is the other half: many agents, one set of infrastructure. Merged, every agent would need a store and a provider passed in, and swapping in `FakeProvider` for tests would touch every agent class instead of one line.

!!! tip "The analogy"
    `Harness` : `Agent` :: FastAPI app : router. The app owns the server, middleware and lifespan; the router owns paths and handlers and is inert on its own.

## No registry

There is no decorator, no global table, no import-time side effect. `Harness(agents=[...])` **is** the name→agent table:

```python
harness = Harness(provider, store, [Build, Explore, DashboardEditor], default_model="google/gemini-3-pro")
```

- The list is walked **transitively through `subagents`**, so a sub-agent never passed to `Harness` is still resolvable.
- Names derive from the class name (`DashboardEditor` → `dashboard_editor`) unless the class sets `name`.
- A duplicate name raises `TantraError` at construction.
- Anywhere an agent is named — `create_session`, `ctx.spawn`, `ctx.fan_out` — the class and its name string are both accepted.

## Construction-time validation

`Harness.__init__` walks every agent's tools and fails loudly there rather than mid-turn:

- a tool that is not a `@tool`, or two tools with the same name;
- a generated JSON schema that is not an object, a required parameter with no inferable type, or an unannotated `ctx` parameter;
- an invalid permission value in a ruleset, on a tool, or in `default_permission`;
- `max_steps < 1`;
- a user tool named `skill` when a `Skills` catalogue is configured.

Schema inference misfiring on a union or a forward ref is a confusing provider-side 400 if it reaches the wire. Catching it at construction is why the check exists.

## Sessions

A session is rows in a store: an append-only event log plus a mutable header (`id`, `agent`, `parent_id`, `depth`, `title`, `status`, `metadata`, `last_seq`, `usage`, `lease`, `pending_ask`).

```python
session = await harness.create_session(DashboardEditor, metadata={"company": 42, "user": 7})
```

`metadata` is the only tenancy hook, and tantra enforces nothing with it — `store.list(metadata=...)` filters, nothing scopes automatically. A sub-agent is a child session (`parent_id`, `depth + 1`) run by the same harness: same store, same provider, same deps.

## Next

- [Durability & resume](durability.md) — what the split buys you.
- [Harness reference](../reference/harness.md), [Agent reference](../reference/agent.md).
