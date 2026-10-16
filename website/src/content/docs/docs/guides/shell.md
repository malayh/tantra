# Shell tools

`bash()` is a synchronous tool and therefore executes in a worker thread under Runtime. It defaults to permission `ask`.

```python
from tantra.extratools.shell import ShellGuard, bash


class ShellAgent(Agent):
    tools = [bash()]
    permissions = {"bash": "allow"}


runtime = Runtime(provider, store, [ShellAgent], hooks=[ShellGuard()])
```

`ShellGuard` rejects destructive command shapes before execution. `ShellGuard(on_trip="ask")` raises a live approval request instead. It is a guardrail, not an operating-system sandbox.

Cancellation abandons the awaited thread result, but a command already started by the operating system may still finish. Design side-effecting tools to be idempotent.
