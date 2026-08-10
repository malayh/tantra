# Ask

```python
from tantra import (
    Approval, ApprovalResponse,
    Choice, ChoiceResponse,
    FreeText, FreeTextResponse,
    AskRequest, AskResponse,
)
```

Three request types, three matching responses. Both unions are discriminated on `kind`, and every model allows extra fields.

## Requests

| Class | `kind` | Fields |
|---|---|---|
| `Approval` | `approval` | `title: str`, `body: str = ""`, `extra: dict = {}` |
| `Choice` | `choice` | `title: str`, `options: list[str] = []`, `extra: dict = {}` |
| `FreeText` | `free_text` | `prompt: str`, `extra: dict = {}` |

`AskRequest = Approval | Choice | FreeText`.

## Responses

| Class | `kind` | Fields |
|---|---|---|
| `ApprovalResponse` | `approval` | `allow: bool` |
| `ChoiceResponse` | `choice` | `selected: str` |
| `FreeTextResponse` | `free_text` | `text: str` |

`AskResponse = ApprovalResponse | ChoiceResponse | FreeTextResponse`.

`extra` is free space for the UI — anything you put there rides along on the persisted `AskRaised` and comes back to whoever renders the prompt.

## Asking from a tool

```python
@tool
async def rename_branch(name: str, ctx: Context) -> str:
    """Rename the current branch."""
    answer = await ctx.ask(FreeText(prompt=f"Rename to {name}? Type a different name to override."))
    return f"renamed to {answer.text}"
```

`ctx.ask` persists `AskRaised(ask_id, call_id, request)`, suspends the turn and returns control to whoever is iterating the stream. The header settles to `status="awaiting_input"` with `pending_ask=<ask_id>`.

The caller answers with a matching response:

```python
async for emitted in harness.resume(sid, ask_id, FreeTextResponse(text="release/2.1")):
    ...
```

`resume` appends `AskAnswered(ask_id, response)` and re-drives the turn.

!!! warning "The tool re-executes from its first line"
    Resume does not resume a paused coroutine — it replays the tool call. Asks are matched **positionally per `call_id`**, so already-answered ones return their recorded responses without prompting, and execution reaches the point after the last answered ask. Everything before that ask runs again. Keep pre-ask side effects idempotent, and carry nothing across the suspend in a closure.

If the replayed tool asks in a different order or count than the log records, the loop raises `TantraError` naming the unanswered ask rather than guessing.

Only the permission flow is type-checked on resume: a request carrying `extra={"permission": ...}` must be answered with an `ApprovalResponse`. Other requests hand the tool whatever response object you supplied, so match the kind yourself.

## Permission asks

An `"ask"` verdict raises the same machinery without any tool code. The harness builds:

```python
Approval(
    title=f"Run {call.name}?",
    body=json.dumps(effective.args, default=str),
    extra={"permission": call.name},
)
```

Answer with `ApprovalResponse(allow=True)` to run the call, or `allow=False` to complete it as `"denied by user"` with `is_error=True`. A [hook `Escalation`](hooks.md) prepends its reason to `body`.

## Reconnecting

`resume(sid)` with no `ask_id` while an ask is pending yields the stored `AskRaised` again and returns without running anything — the way to re-present a question to a client that dropped.

## See also

- [Events](events.md) — `AskRaised` / `AskAnswered`.
- [Harness](harness.md) — `resume` semantics and what it raises.
- [Permissions](permissions.md) · [Hooks](hooks.md)
