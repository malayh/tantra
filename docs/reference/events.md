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

A terminal event carries the accepted command ID as its turn ID. `TurnResult` reduces one turn's text, output, usage, stop reason, error, and outcome.

Root and child sequence numbers are independent. Clients discover a child from `ChildCreated` and subscribe to its journal separately.
