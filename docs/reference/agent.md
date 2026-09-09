# Agent

`Agent` is a declarative class used by Runtime, never an instance holding live I/O.

| Attribute | Default | Meaning |
|---|---|---|
| `name` | derived class name | Durable registry name |
| `model` | `None` | Model override |
| `prompt` | empty | System prompt string or callable |
| `tools` | `[]` | `Tool` objects |
| `subagents` | `[]` | Child declarations available to `spawn` |
| `permissions` | `{}` | Name or glob rules |
| `skills` | `None` | All, selected, or no skills |
| `max_steps` | implementation default | Sample limit per turn |
| `output_schema` | `None` | Pydantic terminal-output schema |

`build_name_table(agents)` walks subagents transitively and rejects duplicate names. Runtime uses that durable registry to resolve actor headers.

Model precedence is agent override, root session model, then Runtime default.
