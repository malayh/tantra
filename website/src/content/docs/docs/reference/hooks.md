# Hooks

Subclass `Hook`, override the needed async callbacks, and pass instances to `Runtime(hooks=[...])`.

Callbacks cover turn start/end, sample start, tool before/after, and durable events. Hooks execute in list order. `before_tool` may transform a call or return `Denial`/`Escalation`.
