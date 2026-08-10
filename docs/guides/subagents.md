# Subagents & fan-out

A sub-agent is a **child session** — `parent_id` set, `depth + 1` — driven by the same loop, the same harness, the same store and provider. There is no second abstraction.

## Two ways to start one

**Declarative.** `subagents = [Researcher]` auto-generates a tool named after the agent, taking a single `task: str`. Its description is the sub-agent class's own docstring.

```python
class Researcher(Agent):
    """Researches one narrow question and reports back."""

    tools = [look]


class Boss(Agent):
    subagents = [Researcher]
```

**Imperative.** `await ctx.spawn(Researcher, "dig into the p99 regression")` from inside a tool — the class or its name string both work. It returns the child's `TurnCompleted.output` when the child has an `output_schema`, otherwise its final text.

`Harness(max_depth=3)` caps recursion. Both the name lookup and the depth check run *before* any child session is created, so a rejected spawn leaves no orphan.

## Fan-out

```python
results = await ctx.fan_out([(Worker, "a"), (Worker, "b"), ("ghost", "c")], max_concurrency=4)
```

Results are positionally aligned with `tasks`. A task that fails contributes its exception in that slot instead of failing the turn — an unknown agent, a `TurnFailed` child, a cancelled child, or a child that hit `max_steps` with no output all land as errors beside the successes.

## Child events ride the parent's stream, never its log

Forwarding is **live only**. A child's events are emitted on the parent's stream tagged with the child's `session_id` and `depth`, but they persist to the child's own log. The parent's log contains just the delegating tool call plus a `ChildSessionSpawned`.

```python
import asyncio

from tantra import Agent, FakeProvider, Harness, MemoryStore, Sample, collect, tool
from tantra.events import ChildSessionSpawned, ToolCallCompleted, ToolCallRequested
from tantra.providers.base import ToolCall


@tool
async def look(q: str) -> str:
    """Look something up."""
    return f"found {q}"


class Researcher(Agent):
    """Researches one narrow question and reports back."""

    tools = [look]


class Boss(Agent):
    subagents = [Researcher]


async def main() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="p1", name="researcher", args='{"task": "dig"}')]),
                Sample(
                    tool_calls=[
                        ToolCall(id="k1", name="look", args='{"q": "a"}'),
                        ToolCall(id="k2", name="look", args='{"q": "b"}'),
                    ]
                ),
                Sample(text="child answer"),
                Sample(text="parent answer"),
            ]
        ),
        store,
        [Boss],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Boss)).id

    events = await collect(harness.run(sid, "go"))
    child_sid = next(e.event.child_session_id for e in events if isinstance(e.event, ChildSessionSpawned))

    for emitted in events:
        if emitted.session_id == child_sid and isinstance(emitted.event, ToolCallRequested):
            print(f"depth={emitted.depth} {emitted.event.name}({emitted.event.args})")

    parent_log = [stamped.event async for stamped in store.read(sid)]
    print([e.name for e in parent_log if isinstance(e, ToolCallRequested)])
    print([e.result for e in parent_log if isinstance(e, ToolCallCompleted)])
    print([header.id == child_sid for header in await store.list(parent_id=sid)])


asyncio.run(main())
```

```text
depth=1 look({'q': 'a'})
depth=1 look({'q': 'b'})
['researcher']
['child answer']
[True]
```

!!! warning "Replay will not reproduce what you saw live"
    A UI built on the live stream cannot rebuild the same view from `replay()`. To reconstruct history including children, fetch them with `store.list(parent_id=sid)` and replay each. The asymmetry is deliberate — the alternative doubles every child write.

## Bubbled asks and the two-resume dance

A child's `ctx.ask` (or an `ask` verdict inside the child) suspends the **whole ancestry**: the child suspends durably, the parent's delegating tool call stays incomplete, and `AskRaised` is forwarded live. The ask lives in the *child's* log, so answering takes two calls:

```python
async for emitted in harness.resume(child_sid, ask_id, ApprovalResponse(allow=True)):
    ...
async for emitted in harness.resume(root_sid):
    ...
```

The first call answers the ask in the child's own log. The second is a **bare** resume of the root, which re-drives the chain top-down: it re-executes the delegating tool from the start; spawn is re-entrant and attaches to the existing child recorded in `ChildSessionSpawned` rather than creating a twin. It recurses over any depth.

## Permissions

Child verdicts are the strictest of the child's own decision and every ancestor's — a parent `deny` beats a child `allow`, and a rule-less ancestor still votes with `Harness(default_permission=)`. See [permissions & hooks](permissions-hooks.md).

## Next

- [Durability & resume](../concepts/durability.md), [Context reference](../reference/context.md).
