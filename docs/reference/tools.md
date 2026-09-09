# Tool and tool decorator

`@tool` builds a `Tool` from a typed callable. The signature becomes an object JSON schema; the docstring becomes the model description. A bare `Context` parameter is injected.

`Tool.invoke(args, ctx)` validates input and executes the callable. Runtime starts accepted tool calls in parallel, offloading synchronous functions to threads, and returns results to the provider in call order.

Framework actor tools are reserved when applicable: `spawn(agent_name, input)`, `send(agent_id, input)`, and child-only `finish(result)`.
