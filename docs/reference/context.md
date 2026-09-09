# Context and TurnContext

`Context` is injected into tool functions annotated with the bare `Context` class. It exposes the actor ID, turn ID, call ID, depth, per-turn dependencies, store, memory, and durable `emit` and live `ask` operations.

`TurnContext` is the hook and prompt context. Runtime populates history, resolved model, provider limits, provider, tracer, dependencies, and the current actor header.

Actor coordination is model-visible through Runtime's injected `spawn`, `send`, and `finish` tools rather than methods on `Context`.
