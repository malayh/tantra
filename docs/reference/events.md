# Events

Every durable event belongs to one actor journal. `LoggedEvent(agent_id, seq, event)` is yielded only after storage assigns its integer sequence.

| Group | Events |
|---|---|
| Session/input | `SessionCreated`, `InputQueued`, `TurnStarted` |
| Provider | `SampleStarted`, text/reasoning/tool-call deltas, parts, `SampleCompleted` |
| Tools | `ToolCallRequested`, `ToolCallStarted`, `ToolProgress`, `ToolCallCompleted` |
| Actors | `ChildCreated`, `AgentFinished` |
| Human control | `AskRaised`, `AskAnswered`, `CancellationRequested` |
| Terminal | `TurnCompleted`, `TurnFailed`, `TurnCancelled`, `TurnInterrupted` |
| Context | `CompactionApplied` |

A terminal event carries the accepted command ID as its turn ID. `TurnResult` reduces one turn's text, output, usage, stop reason, error, and outcome. Session headers reduce terminal events into `last_turn` for status polling.

Root and child sequence numbers are independent. Clients discover a child from `ChildCreated` and subscribe to its journal separately when detailed events are needed. A child terminal other than successful `finish()` also creates a deterministic status-only `InputQueued` in the direct parent's journal. Its frozen form is `[agent <child-uuid> turn ended] <stable-json>`, where the JSON contains `child_id`, `turn_id`, `outcome`, `stop_reason`, and `error` only when present. It contains no child assistant text, tool output, or result.

Child `SessionHeader`, `SessionCreated`, and `ChildCreated` records may carry an optional durable display `name`. The `agent` field remains the registered actor type; older records without `name` remain valid.
