# Context and TurnContext

`Context` is injected into tool functions annotated with the bare `Context` class. It exposes the actor ID, turn ID, call ID, depth, per-turn dependencies, store, memory, and durable `emit` and live root-only `ask` operations.

`TurnContext` is the hook and prompt context. Runtime populates history, resolved model, provider limits, provider, tracer, dependencies, and the current actor header. History is complete by default. With `Runtime(history_mode="compacted")`, it contains the latest `CompactionApplied` marker and the retained event window. Before compaction it also holds `sample_request`, the complete prepared `SampleRequest` including prompt blocks, execution environment, messages, tools, and parameters.

Actor coordination is model-visible through Runtime's injected `spawn`, `status`, `send`, and `finish` tools rather than methods on `Context`.

Both contexts expose `submitted_by`, taken from the accepted input command. It defaults to None for legacy and internal actor inputs. Applications may use it for audit or execution checks; Runtime does not treat it as authorization or automatically include it in the model request.
