# Sharp edges

The behaviours that surprise people. Each one is load-bearing: it is how the library works, not a bug waiting to be fixed. Read this before you ship.

## Running turns

**The turn advances only while someone consumes the stream.**
`run()` and `resume()` are async generators. A client that disconnects mid-turn leaves the turn parked at the last persisted event — durable and resumable, but nothing re-drives it by itself. Detect it as an expired `lease` on a session whose last turn is incomplete, and re-enter with `resume(sid)` and no `ask_id`. Server adapters must own that sweep; the library will not. If you do not want to iterate, drain with [`collect`](reference/loop.md).

**`run`, `resume` and `replay` raise on the first iteration, not at the call.**
`SessionNotFound`, `SessionBusy` and `TurnIncomplete` are raised inside the generator body, so `stream = harness.run(...)` never throws. Put the `try` around the `async for`, not around the call that builds the stream.

**Cancel is a persisted flag, not `task.cancel()`.**
`cancel()` appends `CancelRequested` and nothing else. The loop may be running in another process; it notices at its next store boundary — before a sample, and between tool calls — then ends the turn with `stop_reason="cancelled"`. **A tool already executing is not interrupted**: cancelling a session sitting inside `bash()` waits for that command to finish or time out. Cancelling a *suspended* turn takes effect at the next `resume`, which completes it without sampling. If you need a hard stop, give the tool its own timeout.

**Every `tool_call_id` must be answered before the next sample — `max_steps` is the sneaky one.**
OpenAI-compatible APIs reject an assistant message whose tool calls have no results, so any early stop mid-batch (suspend, denial, cancel, cap) still writes `ToolCallCompleted(is_error=True)` for the calls that never ran. Expect `"not executed: max steps reached"` and `"denied by user"` in your logs — they are real events, not noise. Anything you write that rewrites or filters the log must preserve the pairing.

**Replaying a parent does not reproduce the child events you saw live.**
Child sessions write to their own logs; a parent forwards their events live only. A client rebuilding history must fetch children with `store.list(parent_id=sid)` and replay each. The asymmetry is deliberate — the alternative doubles every child write.

## Writing tools

**Only `str(exc)` reaches the model.**
The exception type, the traceback and even the `is_error` flag are dropped before the provider sees the result. The message is the entire error contract: make every raise name what went wrong *and* what to do next ("the file is a scan with no text layer — ask the user for a text version"), never `ValueError: bad input`.

**`ctx.ask` re-executes the whole tool on resume.**
Suspension is not a paused coroutine — the tool is replayed from its first line, and already-answered asks return their recorded responses without prompting. Everything before the ask therefore happens twice. Ask first, act after; make any pre-ask side effect idempotent.

**Nothing may be captured in a Python closure across a suspend.**
The process can die while the turn waits for an answer. Anything the tool needs afterwards comes from `ctx.deps` — rebuilt per process by `deps_factory`, which is a *factory* for exactly this reason — or from the event log. A captured connection pool will not survive a resume on another pod.

**Use `ctx.spawn` / `ctx.fan_out` for children; never create your own.**
They record `ChildSessionSpawned`, which is what lets a replayed turn attach to the existing child instead of creating a twin. Calling `harness.run` yourself from inside a tool has no such record, and a resume will start the work over.

**The `@tool` symbol is not callable.**
`@tool` returns a `Tool` object, not a function, so `await my_tool(x=1)` fails. Invoke it as `await my_tool.invoke({"x": 1}, ctx)` — which is how the test suite exercises every tool.

**`ctx` must be annotated exactly `ctx: Context`.**
Injection tests whether the annotation *is* the `Context` class, so only the bare form is stripped from the model-facing schema. `ctx: Context | None` is a union and is treated as a real parameter — it blows up with `PydanticSchemaGenerationError` when the module is imported. An unannotated `ctx` survives decoration and is rejected at `Harness` construction instead. Both fail loudly; neither reaches a turn.

**`skill` and `submit_output` are reserved tool names.**
`skill` is injected whenever the harness has a skills catalogue and collides loudly at construction. `submit_output` is intercepted by the loop whenever the agent declares an `output_schema`, so a tool of that name is silently shadowed.

**`ctx.emit` progress is persisted.**
Each call is a real store append that shows up in `replay` — which is the point, live-only progress is lost on reconnect. It also means a chatty tool writes to the store on every message. Emit milestones, not a log stream.

## Permissions, hooks and skills

**A rule-less ancestor's harness default beats a child's explicit `allow`.**
Child verdicts are merged with every ancestor's ruleset using the strictest value, and an ancestor with no matching rule contributes `default_permission`. Under `default_permission="ask"`, sub-agent `skill` calls suspend even though the tool declares `allow` — one permission engine, no framework-tool exemption. Grant it on the parent: `permissions = {"skill": "allow"}`.

**`Agent.skills = ()` is not an opt-out; `[]` is.**
The check is `agent.skills == []`, and a tuple does not equal a list. `skills = ()` registers the `skill` tool, indexes nothing, and rejects every name the model asks for.

**`before_tool` re-fires on resume, and only `args` survive a replacement.**
A resumed turn re-runs the tool call, hooks included, so keep `before_tool` a pure decision and put side effects in `after_tool`. If you return a modified `ToolCallRequested`, its `name` and `call_id` are ignored — the tool and the permission verdict were already resolved from the original call.

## Context and compaction

**Compaction must never orphan a `tool_call` / `tool_result` pair.**
The shipped compactor replaces result *content* and never removes a message, and cuts a prefix only at turn boundaries. A custom compactor that cuts mid-turn produces an assistant message whose tool call has no result — a 400 on every OpenAI-compatible provider.

**Token counts are estimates.**
The number driving compaction is the provider's reported usage on the previous sample plus `len(text) // 4` for everything added since. There is no local tokenizer and no attempt to match one; `CompactionConfig.buffer` exists to absorb the error. Do not bill from it.

**Compaction will stub large tool output, including fetched pages.**
A `web_fetch` result that survives past `tail_turns` becomes `[pruned: web_fetch output, N chars omitted]`. Only `skill` output is exempt, and no mechanism exists to exempt anything else. If a page's content must persist, have the tool return a summary or write it somewhere durable.

## Storage and operations

**`store.setup()` is mandatory for SQLite and Postgres, and nothing calls it.**
`Harness` never runs it. Skip it and the first append fails on a missing table. Call it once at startup — it is idempotent, and harmless on `MemoryStore` / `FileSystemStore`.

**Tantra enforces no isolation.**
`store.list(metadata={...})` with the wrong filter — or no filter — returns every session in the store, across every tenant. Scoping is entirely the application's job: put your keys in `SessionHeader.metadata` and filter every read.

**`O_APPEND` is not atomic above `PIPE_BUF` (4096 bytes), and any real tool result exceeds it.**
The filesystem store does not rely on append atomicity: correctness comes from the **single-writer lease per session**. Fan-out children write to their own logs and never contend. Any store you write yourself needs the same guarantee — run [`store_conformance`](reference/stores.md) against it.

## Imports and types

**Not everything is exported at the top level.**
Wire types (`ToolCall`, `StreamEnd`, `TextDelta`, `ToolSchema`, …) come from `tantra.providers`. Concrete event classes (`TurnCompleted`, `ToolCallCompleted`, …) come from `tantra.events`; of that module only `SessionEvent`, `SessionHeader`, `SessionStatus`, `Stamped`, `Usage`, `Lease` and `CompactionApplied` are re-exported from `tantra`. `SessionExists` and `CorruptLog` live in `tantra.errors`.

**`SessionEvent` is a union, not a class.**
`isinstance(event, SessionEvent)` does not work. Branch on the discriminator string — `event.type == "tool_call_completed"` — which is also what survives a trip over a wire, or import the concrete class from `tantra.events` and use `isinstance` against that.
