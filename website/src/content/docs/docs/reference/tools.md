---
title: Tool and tool decorator
---

`@tool` builds a `Tool` from a typed callable. The signature becomes an object JSON schema; the docstring becomes the model description. A bare `Context` parameter is injected. Nested Pydantic models are inlined without `$ref`; recursive models keep `$ref`.

`Tool.invoke(args, ctx)` validates input and executes the callable. Runtime starts accepted tool calls in parallel, offloading synchronous functions to threads, and returns results to the provider in call order.

Framework actor tools are reserved when applicable: `spawn(agent_name, input, name=None)`, `status(agent_id)` and `cancel_agent(agent_id)` on declarations with children, and `send(agent_id, input)` across a direct parent-child edge. Child agents also reserve the name `finish`, which journals written before 3.0 use. A child delivers its result by ending its turn: the final message becomes its reply. A spawn name is an optional trimmed display label and does not change actor identity. Human asks remain root-only.
