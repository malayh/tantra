# Permissions

```python
from tantra.permissions import PERMISSIONS, check_permission, decide, strictest
```

Every tool call is resolved to one of three verdicts before it runs.

| Verdict | Effect |
|---|---|
| `"allow"` | The tool runs. |
| `"ask"` | The turn raises an `Approval` request and suspends. Approved → runs; refused → `"denied by user"` as an error result. |
| `"deny"` | The tool never runs; the model gets `"denied by permissions: <name>"` as an error result. |

`PERMISSIONS = ("allow", "ask", "deny")`. Strictness order is `deny` > `ask` > `allow`.

Denials and refusals are normal `ToolCallCompleted` events with `is_error=True` — the model reads them and adapts. They do not fail the turn.

## Resolution

For a tool call named `name` in an agent with `permissions` rules:

1. **Agent ruleset.** The longest matching glob (`fnmatch`, case-sensitive) wins. Equal-length patterns resolve to the strictest value.
2. **Tool's declared permission.** With no matching rule, `Tool.permission` applies (e.g. `bash` declares `"ask"`).
3. **Harness default.** With neither, `Harness(default_permission=...)` applies — `"allow"` unless you change it.

Then, for a child session, the same name is resolved against **each ancestor's ruleset in turn** and merged with `strictest`. Ancestors contribute rules only: their step 2 is skipped, so a rule-less ancestor contributes the harness default.

!!! warning "A child can never widen what its ancestors granted"
    Under `default_permission="ask"`, a parent with no matching rule contributes `ask`, which beats a child tool's declared `allow`. This is why sub-agent `skill` calls ask while the same call at depth 0 does not. Grant it explicitly on the parent: `permissions = {"skill": "allow"}`.

## `decide(name, rules, tool_permission, default) -> str`

The single-ruleset resolution above (steps 1–3). `rules` is a `Mapping[str, str]`, `tool_permission` is `str | None`.

```python
decide("web_fetch", {"web_*": "allow", "*": "deny"}, None, "allow")  # "allow" — longest glob wins
decide("web_fetch", {"web_*": "ask", "web_fet*": "deny"}, None, "allow")  # "deny"
decide("bash", {}, "ask", "allow")  # "ask" — the tool's own declaration
```

## `strictest(*verdicts) -> str`

Returns the most restrictive of the given verdicts. Used to merge the ancestor chain, and to apply a hook's [`Escalation`](hooks.md) (`strictest(verdict, "ask")`).

## `check_permission(label, value) -> str`

Validates one verdict string and returns it; raises `TantraError` naming `label` otherwise. `Harness` runs it at construction over `default_permission` and every value in every agent's `permissions`, so a typo fails at startup rather than mid-turn.

## See also

- [Hooks](hooks.md) — `Denial` and `Escalation`, which run before permission resolution.
- [Ask](ask.md) — what an `"ask"` verdict puts on the wire.
- [Guides: permissions & hooks](../guides/permissions-hooks.md).
