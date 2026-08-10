# Durability & resume

The loop holds no state between samples that isn't in the store. That single rule is what makes a turn survive an approval prompt, a dropped WebSocket, a `^C`, or the process dying.

## Consumption-driven generators

`run()`, `resume()` and `replay()` are async generators. Nothing runs detached; the loop advances as you iterate.

!!! warning "Errors surface on the first iteration, not at call time"
    `SessionBusy`, `TurnIncomplete`, `SessionNotFound` and "agent sets no model" are raised when the generator is first advanced. `stream = harness.run(sid, "go")` never raises — `async for` over it does. Wrap the iteration, not the call.

## Leases

One writer per session. `run()`/`resume()` acquire a lease (`Harness(lease_ttl=60.0)`) and re-acquire it at every sample boundary; a second writer gets `SessionBusy`. A worker that dies holds the lease until it expires, after which anyone may take it — the expired lease stays readable on the header as evidence of who held the session last.

## Suspend on ask

`await ctx.ask(...)` and an `ask` permission verdict both suspend the turn durably: `AskRaised` is appended, the lease is released, the generator returns. The header goes to `status="awaiting_input"` with `pending_ask` set. A child's ask suspends the whole ancestry.

This runs offline against `FakeProvider` and `MemoryStore` — the `Harness` that started the turn is discarded before the answer arrives:

```python
import asyncio

from tantra import Agent, ApprovalResponse, FakeProvider, Harness, MemoryStore, Sample, collect, tool
from tantra.events import AskRaised, ToolCallCompleted, TurnCompleted
from tantra.providers.base import ToolCall


@tool
async def write_dashboard(path: str) -> str:
    """Write a dashboard."""
    return f"wrote {path}"


class Editor(Agent):
    tools = [write_dashboard]
    permissions = {"write_*": "ask"}


async def main() -> None:
    store = MemoryStore()
    first = Harness(
        FakeProvider([Sample(tool_calls=[ToolCall(id="c1", name="write_dashboard", args='{"path": "p99.json"}')])]),
        store,
        [Editor],
        default_model="fake/model",
    )
    sid = (await first.create_session(Editor)).id

    opening = await collect(first.run(sid, "write the p99 dashboard"))
    raised = next(e.event for e in opening if isinstance(e.event, AskRaised))
    header = await store.header(sid)
    print(header.status, header.pending_ask == raised.ask_id)

    del first
    second = Harness(FakeProvider([Sample(text="dashboard written.")]), store, [Editor], default_model="fake/model")
    resumed = await collect(second.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    print(next(e.event.result for e in resumed if isinstance(e.event, ToolCallCompleted)))
    print(next(e.event.stop_reason for e in resumed if isinstance(e.event, TurnCompleted)))


asyncio.run(main())
```

```text
awaiting_input True
wrote p99.json
completed
```

## The two resumes

`resume(sid, ask_id, response)` answers an ask: it appends `AskAnswered`, synthesizes or executes the asked-about call, and continues at the next call in the batch. A permission ask requires an `ApprovalResponse`; anything else is rejected before the answer is persisted.

Bare `resume(sid)` is the safe sweep entry point, and it does one of two different things:

- **Turn suspended on an ask** → it *replays*: the original `AskRaised` is re-yielded at its own `seq`, no tool runs, the log is untouched. Present the prompt again.
- **Turn abandoned mid-flight** (nobody consumed the stream) → it *re-drives* the turn from replayed state.

!!! danger "A tool re-executes from the start after `ctx.ask`"
    On resume the tool function runs again from its first line. Already-answered asks return their recorded responses without prompting (matched by ask-index per `call_id`), but **every side effect before the ask happens twice** — including `ctx.emit` progress. Do the writes after the ask, make them idempotent, or expect duplicates. Nothing may be captured in a Python closure across a suspend either: the process can die there, so post-resume state comes from `ctx.deps` (rebuilt per process by `deps_factory`) or the event log.

## Abandonment

Stop consuming the stream — a `^C`, a client disconnect — and the turn pauses at the last persisted event. Nothing re-drives it by itself. The next `run()` on that session raises `TurnIncomplete`; bare `resume(sid)` picks it up.

```python
from tantra import TurnIncomplete

try:
    async for emitted in harness.run(sid, text):
        ...
except TurnIncomplete:
    async for emitted in harness.resume(sid):
        ...
```

There is no public "is this turn incomplete?" helper. Catching `TurnIncomplete` is the cheap route; a server sweep that wants to check before taking the lease reads the log and looks for a `TurnStarted` with no matching `TurnCompleted` / `TurnFailed`, which is what `apps/agni` does.

## Cancel

`await harness.cancel(sid)` appends `CancelRequested` and returns; it is a persisted flag, not `task.cancel()`. The loop may be running in another process, so it checks the store at sample and tool-call boundaries, synthesizes results for unexecuted calls, and appends `TurnCompleted(stop_reason="cancelled")`. Cancelling a *suspended* turn takes effect at the next `resume`, which ends the turn without sampling.

## Wiring a server

No WebSocket or SSE adapter ships. The primitives already carry the transport, and a public wire protocol would freeze a shape most servers rewrite anyway. An adapter is roughly twenty-five lines of your own app code, against your own auth and tenancy:

- **Out:** `Emitted.model_dump_json()` per event.
- **In:** `harness.resume(sid, ask_id, response)` when the human answers.
- **Sweep:** a dropped client leaves a durably paused turn. Detect it — lease expired with the turn incomplete — and re-drive with bare `resume(sid)`.

Cross-process resume is pinned at library level: two `Harness` instances over one store, one starting a turn and the other finishing it.

## Next

- [Storage backends](../guides/storage.md) — where sessions actually live.
- [Permissions & hooks](../guides/permissions-hooks.md) — what turns a call into an ask.
- [Sharp edges](../sharp-edges.md).
