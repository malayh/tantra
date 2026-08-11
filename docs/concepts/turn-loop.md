# The turn loop

A **turn** is one user request. A turn contains N **samples** — one model API call each. The loop is the product: you configure its plug points, you do not wire a graph.

## The spine

```
run(session_id, input):
  acquire session lease (single writer per session)
  append TurnStarted
  loop until stop:
    hooks.before_sample
    compactor.check() -> maybe CompactionApplied
    build SampleRequest  (system + skills index + history + tool schemas)
    append SampleStarted
    provider.stream(req):
      TextDelta / ReasoningDelta / ToolCallDelta  -> emit live
      accumulate into TextPart / ReasoningPart / ToolCallRequested
    append parts + Usage
    if no tool calls or submit_output called or max_steps hit: stop
    for each tool call, in order:
      resolve tool; hooks.before_tool (may deny/transform/escalate)
      permissions.decide(tool name) -> allow | ask | deny
      if ask: append AskRaised, release lease, RETURN (turn suspended)
      append ToolCallStarted  (deny and refusal paths write it too, then an is_error result)
      execute; ToolProgress from ctx.emit; exceptions -> is_error result
      hooks.after_tool (may transform)
      append ToolCallCompleted
  append TurnCompleted
  release lease
```

Points worth knowing:

- **Tool calls within a sample run serially.** A provider can emit several per sample; they execute in order. Parallel execution would multiply durable-suspend cases for little gain. `ctx.fan_out` is the explicit parallelism primitive.
- The lease is re-acquired at every sample boundary. Losing it aborts the turn without touching the header.
- Transient provider errors retry inside the sample step (`RetryConfig`, 3 attempts by default) before becoming `TurnFailed`. A partially streamed sample is discarded, never persisted.
- `max_steps` (default 40, per agent) caps samples. Hitting it still synthesizes results for every requested call — see [sharp edges](../sharp-edges.md).

## Two event streams

They are deliberately different.

**Persisted — the log.** Ordered, append-only, `seq` per session, source of truth. Seventeen types:

| Event | Carries |
|---|---|
| `SessionCreated` | `agent`, `parent_id`, `depth`, `metadata` |
| `TurnStarted` | `turn_id`, `input` |
| `SampleStarted` | `turn_id`, `sample_id`, `model` |
| `TextPart` | `sample_id`, `text` |
| `ReasoningPart` | `sample_id`, `text`, `signature` |
| `ToolCallRequested` | `sample_id`, `call_id`, `name`, `args` |
| `ToolCallStarted` | `call_id` |
| `ToolProgress` | `call_id`, `message` |
| `ToolCallCompleted` | `call_id`, `result`, `is_error` |
| `ChildSessionSpawned` | `call_id`, `child_session_id`, `agent` |
| `AskRaised` | `ask_id`, `call_id`, `request` |
| `AskAnswered` | `ask_id`, `response`, `answered_by` |
| `SampleCompleted` | `sample_id`, `usage`, `finish_reason` |
| `CompactionApplied` | `strategy`, `tokens_before`, `tokens_after`, `summary`, `floor_turn_id` |
| `CancelRequested` | `turn_id` |
| `TurnCompleted` | `turn_id`, `stop_reason`, `output` |
| `TurnFailed` | `turn_id`, `error` |

**Emitted only — never persisted:** `TextDelta`, `ReasoningDelta`, `ToolCallDelta`. Replay reconstructs a turn's structure, not its keystrokes.

Every event you receive — persisted or live — arrives wrapped in `Emitted`:

```python
class Emitted(BaseModel):
    session_id: str
    depth: int = 0
    seq: int | None = None
    event: SessionEvent | TextDelta | ReasoningDelta | ToolCallDelta
```

`seq` is `None` for the live-only deltas. `session_id` and `depth` are what let one stream carry a sub-agent's output alongside its parent's, so a client can nest or collapse it.

## Consuming

`run()`, `resume()` and `replay()` are async generators. The turn advances only while you iterate.

```python
async for emitted in harness.run(session.id, "fix the p99 panel"):
    match emitted.event:
        case TextDelta() as delta:
            print(delta.text, end="", flush=True)
        case TurnCompleted() as end:
            print(end.stop_reason)
```

`collect` drains a stream into a list when you do not want to iterate:

```python
from tantra import collect

events = await collect(harness.run(session.id, "go"))
```

Stopping consumption pauses the turn durably — see [durability & resume](durability.md).

## Next

- [Events reference](../reference/events.md) — every field on every event.
- [Loop reference](../reference/loop.md) — `Emitted`, `RetryConfig`, stop reasons.
