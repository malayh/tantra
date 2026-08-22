# Context (`TurnContext`)

```python
from tantra import TurnContext
```

The turn-scoped dataclass. Every [Hook](hooks.md) callback receives it, and a callable `Agent.prompt` and a [`Compactor`](compaction.md) are given it too. It is **not** `tantra.tools.Context` — that one is the per-call handle a tool receives.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `session_id` | `str` | — | Session running this turn. |
| `turn_id` | `str` | — | Turn id. Stable across a resume — a resumed turn keeps the original. |
| `agent` | `str` | — | Registered agent name. |
| `depth` | `int` | — | Session depth. Root is 0. |
| `input` | `str` | — | The turn's input text. |
| `metadata` | `dict[str, Any]` | `{}` | The session header's metadata. |
| `deps` | `Any` | `None` | What `Harness(deps_factory=...)` built for this turn. |
| `history` | `list[SessionEvent] \| None` | `None` | The **live** event log of the session, appended to as the turn runs. |
| `model` | `str \| None` | `None` | Model resolved for this turn. |
| `limits` | `ModelLimits \| None` | `None` | Context window and max output for that model. |
| `provider` | `Provider \| None` | `None` | The harness's provider, so a compactor can sample. |
| `tracer` | `Tracer` | `NULL_TRACER` | What `Harness(telemetry=...)` was given, so a compactor can trace its own model call. See [Telemetry](telemetry.md#turncontexttracer). |

## Who sets what, and when

`Harness` builds the object with the first seven fields. The loop then fills in `history`, `model`, `limits`, `provider` and `tracer` when it is constructed — which happens **after** `before_turn` runs.

| Callback | `history` / `model` / `limits` / `provider` / `tracer` |
|---|---|
| `Hook.before_turn` | defaults — the loop does not exist yet |
| `Hook.before_sample` | populated |
| `Hook.before_tool` / `after_tool` | populated |
| `Hook.after_turn` | populated |
| `Agent.prompt(turn)` | populated |
| `Compactor.compact(ctx)` | populated |

Read `history` rather than snapshotting it: it aliases the log the loop appends to, so a hook holding an old slice goes stale mid-turn.

```python
class Auditor(Hook):
    async def before_sample(self, turn: TurnContext) -> None:
        print(f"{turn.agent} depth={turn.depth} events={len(turn.history)}")
```

## Assembly helpers

`tantra.context` also holds the functions that turn an event log into a provider request. They are not exported at the top level, but they are public: a custom [`Compactor`](compaction.md) needs them to measure or rebuild a window, and `PruneThenSummarize` is written on top of all three.

```python
from tantra.context import assemble_messages, build_messages, compaction_window
```

| Function | Signature | Returns |
|---|---|---|
| `compaction_window` | `(events: Sequence[SessionEvent]) -> tuple[str, list[SessionEvent]]` | The latest `CompactionApplied` summary (`""` when there is none) and the events from its floor onward. This is the compacted view of the log. |
| `assemble_messages` | `(summary: str, events: Sequence[SessionEvent]) -> list[Message]` | Provider messages for that window: the summary as a leading `UserMessage`, then each turn's input, assistant text/reasoning/tool calls grouped by `sample_id`, and one `ToolResultMessage` per answered call. |
| `build_messages` | `(events: Sequence[SessionEvent]) -> list[Message]` | `assemble_messages` applied to `compaction_window(events)` — the whole log to the request the loop sends. |

Two properties matter if you build on them: a `ToolCallCompleted` whose `call_id` was never requested is dropped, and a later result for the same `call_id` **overwrites** the earlier message in place rather than appending. That second rule is what lets stage-1 pruning replace a bulky result by emitting a new `ToolCallCompleted` instead of editing history.

## See also

- [Tools](tools.md) — `Context`, the per-call handle with `emit`, `ask`, `spawn`, `fan_out`.
- [Hooks](hooks.md) · [Compaction](compaction.md)
