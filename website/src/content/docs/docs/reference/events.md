---
title: Events
---

Every durable event belongs to one actor journal. `LoggedEvent(agent_id, seq, event)` is yielded only after storage assigns its integer sequence.

| Group | Events |
|---|---|
| Session/input | `SessionCreated`, `InputQueued`, `TurnStarted` |
| Provider | `SampleStarted`, text/reasoning/tool-call deltas, parts, `SampleCompleted` |
| Tools | `ToolCallRequested`, `ToolCallStarted`, `ToolProgress`, `ToolCallCompleted` |
| Actors | `ChildCreated`, `AgentFinished` (journals written before 3.0) |
| Human control | `AskRaised`, `AskAnswered`, `CancellationRequested` |
| Terminal | `TurnCompleted`, `TurnFailed`, `TurnCancelled`, `TurnInterrupted` |
| Context | `CompactionApplied` |

A terminal event carries the accepted command ID as its turn ID. `TurnResult` reduces one turn's text, output, usage, stop reason, error, and outcome. Session headers reduce terminal events into `last_turn` for status polling.

Root and child sequence numbers are independent. Clients discover a child from `ChildCreated` and subscribe to its journal separately when detailed events are needed. Every child terminal except a cancel by its parent also creates one deterministic `InputQueued` in the direct parent's journal, with `origin` `turn_ended` and command ID `uuid5(child_id, turn_id)`. A completed turn queues `[agent <child-uuid> replied] <text>`, where the text is the turn's final assistant message, or the canonical JSON of `submit_output`, capped at 32 KiB of UTF-8. Other terminals queue `[agent <child-uuid> turn ended] <stable-json>`, where the JSON contains `child_id`, `turn_id`, `outcome`, `stop_reason`, and `error` only when present. `AgentFinished` and the `finished` origin appear only in journals written before 3.0.

`InputQueued` also records where an input came from. `origin` is `user`, `spawn`, `message`, `finished` or `turn_ended`, and agent inputs carry the sending actor's journal id in `sender_id`. The input text and its prefixes are unchanged. Events written before these fields existed read both as None. A retry compared with such an event ignores both fields, so it stays a duplicate; when both sides carry `origin`, every field must match.

Child `SessionHeader`, `SessionCreated`, and `ChildCreated` records may carry an optional durable display `name`. The `agent` field remains the registered actor type; older records without `name` remain valid.
