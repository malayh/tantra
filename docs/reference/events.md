# Events

```python
from tantra import CompactionApplied, Lease, SessionEvent, SessionHeader, SessionStatus, Stamped, Usage
from tantra.events import TurnCompleted, ToolCallCompleted  # concrete classes live here
```

The session log is append-only. Everything the harness knows about a session is derived from it.

## The persisted events

All seventeen are pydantic models carrying `version: int = 1` and a literal `type` discriminator, with `extra="allow"` so a newer writer's fields survive a round trip through an older reader.

| Class | `type` | Fields | Meaning |
|---|---|---|---|
| `SessionCreated` | `session_created` | `agent`, `parent_id`, `depth`, `metadata` | First event of every session. |
| `TurnStarted` | `turn_started` | `turn_id`, `input` | A turn began with this input. Also the boundary compaction and turn-state derivation cut on. |
| `SampleStarted` | `sample_started` | `turn_id`, `sample_id`, `model` | A model call is about to run. Counts against `max_steps`. |
| `TextPart` | `text_part` | `sample_id`, `text` | Assistant text from one sample. |
| `ReasoningPart` | `reasoning_part` | `sample_id`, `text`, `signature` | Reasoning block, replayed back to providers that want it. |
| `ToolCallRequested` | `tool_call_requested` | `sample_id`, `call_id`, `name`, `args` | The model asked for a tool. `args` is the parsed object. |
| `ToolCallStarted` | `tool_call_started` | `call_id` | The call left the requested state. Always paired with the `ToolCallCompleted` that follows it — a denial, a refusal or unparseable arguments get one too, so a reader never sees a completion for a call it never saw start. |
| `ToolProgress` | `tool_progress` | `call_id`, `message` | One `ctx.emit` from inside the tool. |
| `ToolCallCompleted` | `tool_call_completed` | `call_id`, `result`, `is_error` | The call's outcome. Denials, refusals and interruptions are also this, with `is_error=True`. |
| `ChildSessionSpawned` | `child_session_spawned` | `call_id`, `child_session_id`, `agent` | A `spawn`/`fan_out` child was created. This is the record a resume re-attaches to. |
| `AskRaised` | `ask_raised` | `ask_id`, `call_id`, `request` | The turn suspended on a question. `call_id` is `None` only for asks outside a tool call. |
| `AskAnswered` | `ask_answered` | `ask_id`, `response`, `answered_by` | A human answered. Written by `resume`. |
| `SampleCompleted` | `sample_completed` | `sample_id`, `usage`, `finish_reason` | The sample finished; `usage` is the provider's report. |
| `CompactionApplied` | `compaction_applied` | `strategy`, `tokens_before`, `tokens_after`, `summary`, `floor_turn_id` | Context was summarized. See below. |
| `CancelRequested` | `cancel_requested` | `turn_id` | Someone called `Harness.cancel`. |
| `TurnCompleted` | `turn_completed` | `turn_id`, `stop_reason`, `output` | Terminal. `stop_reason` is `completed`, `output`, `max_steps` or `cancelled`. |
| `TurnFailed` | `turn_failed` | `turn_id`, `error` | Terminal. The provider failed after its retries, or compaction could not summarize. |

A turn is *incomplete* when the log's last `TurnStarted` is not followed by a `TurnCompleted` or `TurnFailed`. That is what `run` refuses and `resume` picks up.

## `SessionEvent`

A discriminated union over the seventeen classes, keyed on `type`. Being an `Annotated` union, it is a type — not a class — so `isinstance(event, SessionEvent)` does not work.

Two ways to branch:

```python
if emitted.event.type == "tool_call_completed":
    ...
```

```python
from tantra.events import ToolCallCompleted

if isinstance(emitted.event, ToolCallCompleted):
    ...
```

The `type` string is the stable, serialization-friendly filter — use it across a wire. The concrete classes are importable from `tantra.events` (not from top-level `tantra`) when you want the typed field access.

`tantra.events.SESSION_EVENT_ADAPTER` is a public `TypeAdapter[SessionEvent]` for parsing a bare stored event back into the union. The shipped stores do not use it — they persist and parse whole `Stamped` records via `Stamped.model_validate_json` — but it is there for callers who hold a lone event payload.

## `Stamped`

What a store yields from `read`.

| Field | Meaning |
|---|---|
| `seq` | Position in the log. The first event of a session is seq 1. |
| `event` | The `SessionEvent`. |

## `SessionHeader`

The mutable summary beside the log.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `id` | `str` | — | Session id. |
| `agent` | `str` | — | Registered agent name; how a resume finds the class. |
| `parent_id` | `str \| None` | `None` | Parent session, for children. |
| `depth` | `int` | `0` | Root is 0. |
| `created_at` / `updated_at` | `datetime` | now (UTC) | Timestamps. |
| `title` | `str \| None` | `None` | Free for the application; tantra never sets it. |
| `status` | `SessionStatus` | `"idle"` | See below. |
| `metadata` | `dict[str, Any]` | `{}` | Your scoping keys. `store.list(metadata=...)` matches it as a subset. |
| `last_seq` | `int` | `0` | Store-owned. Preserved across `put_header` and `patch_header`. |
| `usage` | `Usage` | zeros | Accumulated across every sample of the session. |
| `lease` | `Lease \| None` | `None` | Store-owned; reported as stored, expired or not. |
| `pending_ask` | `str \| None` | `None` | The `ask_id` the session waits on while `awaiting_input`. |

## `SessionStatus`

`Literal["idle", "running", "awaiting_input", "failed"]`. The harness settles the header to `awaiting_input` (with `pending_ask`) when a turn suspends, `failed` after `TurnFailed`, and `idle` otherwise.

!!! note "Status is not a liveness check"
    A worker killed mid-turn leaves `status="running"`. The reliable signal that a turn was abandoned is an expired `lease` on a session whose last turn is incomplete.

## `Usage`

| Field | Meaning |
|---|---|
| `input_tokens` | Prompt tokens **excluding** cached reads — the OpenAI-compatible provider subtracts `cached_tokens` before reporting. |
| `output_tokens` | Completion tokens. |
| `cache_read_tokens` | Prompt tokens served from cache. |
| `cache_write_tokens` | Tokens written to cache. |

Total prompt size is `input_tokens + cache_read_tokens + cache_write_tokens`. Adding `input_tokens` to a vendor dashboard's prompt count will not match.

## `Lease`

`holder: str`, `expires_at: datetime`. The single-writer claim on a session. An expired lease is acquirable by anyone and is never cleared on expiry — it stays on the header as evidence of who held the session last and when it lapsed.

## `CompactionApplied`

| Field | Meaning |
|---|---|
| `strategy` | Compactor name, e.g. `prune_then_summarize`. |
| `tokens_before` / `tokens_after` | Estimates, not counts. See [Compaction](compaction.md). |
| `summary` | The brief that replaces everything before the floor. |
| `floor_turn_id` | Assembly starts at this `TurnStarted`. `None` means "everything after this event". |

Nothing is ever rewritten: this event *is* the compaction. Assembly derives the compacted view from the latest `CompactionApplied`, while `replay` still returns the full history.

## See also

- [Loop & events flow](loop.md) — the `Emitted` envelope events arrive in.
- [Stores](stores.md) — how they are persisted and read back.
