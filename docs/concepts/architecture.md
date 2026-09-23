# Runtime, Connection, and Agent

| Component | Lifetime | Responsibility |
|---|---|---|
| `Agent` | declaration | Prompt, model override, tools, subagents, permissions, skills, output schema |
| actor session | durable | UUID identity, parent/root relationship, FIFO inputs, independent journal, scalar cursor |
| `Runtime` | one process | Provider, store, registry, actors, local notifications, live asks, writer generations |
| `Connection` | client attachment | Root event iteration and optional writer authority |

`Runtime` receives the root agent registry. Subagents are discovered transitively from `Agent.subagents`; names are durable, so later activation resolves the same declaration.

```python
runtime = Runtime(provider, store, [Researcher], default_model="openai/gpt-5")
root_id = await runtime.create(Researcher, metadata={"tenant": 42})
```

A root or child actor runs at most one turn at a time. Different actors and independent tool calls may overlap. An idle actor has no permanent worker: a durable input activates a drain task, and the task exits when the inbox is empty.

Only a writable root connection accepts human commands. Opening a newer writer atomically invalidates the previous writer. Readers never own execution, and disconnecting does not cancel accepted work.

Every actor has its own journal. `ChildCreated` reveals a child UUID, after which clients subscribe to that child explicitly. There is no merged tree stream or composite cursor.

## Model guidance layers

Model guidance has four layers:

1. The application behavior prompt comes from `Agent.prompt` and remains the first system block.
2. The execution environment is one final system block assembled from active capabilities, such as available skill names and child lifecycle semantics. It is omitted when no guidance applies.
3. Tool schemas and descriptions define tool arguments and ordinary usage.
4. Skill bodies provide detailed instructions only after the model loads them on demand with the skill tool.
