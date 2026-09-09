# The actor turn loop

One queued input produces one complete turn:

1. Append or deduplicate `InputQueued` using its UUID command ID.
2. Activate the actor if it is idle.
3. Select the oldest unhandled input and append `TurnStarted`.
4. Assemble context and call the provider.
5. Validate and authorize tool calls, then start accepted calls together.
6. Append tool progress and results in execution order while returning results to the model in call order.
7. Append exactly one terminal event: completed, failed, cancelled, or interrupted.
8. Drain the next input or become idle.

Streamed text, reasoning, and tool-call deltas are durable before publication. Sync tools use worker threads; async tools stay on the event loop. A failed tool call becomes that call's error result and does not cancel siblings.

Inputs received during a turn remain FIFO and never steer the active sample. Cancellation targets the live root tree and queued work; a later root command can activate the root again.
