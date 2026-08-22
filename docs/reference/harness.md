# Harness

```python
from tantra import Harness
```

The runtime: provider, store, deps and the name → agent table. Many agents, one harness. It holds no per-turn state, so one instance serves a CLI, an HTTP server and a worker at the same time.

## Constructor

```python
Harness(
    provider: Provider,
    store: Store,
    agents: Iterable[type[Agent]],
    *,
    default_model: str | None = None,
    deps_factory: Callable[[SessionHeader], Any] | None = None,
    retry: RetryConfig = DEFAULT_RETRY,
    lease_ttl: float = 60.0,
    hooks: Sequence[Hook] = (),
    default_permission: str = "allow",
    max_depth: int = 3,
    skills: Skills | None = None,
    memory: Memory | None = None,
    compactor: Compactor | None = None,
    telemetry: Tracer | None = None,
)
```

| Parameter | Meaning |
|---|---|
| `provider` | Model transport. See [Providers](providers.md). |
| `store` | Session log and header storage. See [Stores](stores.md). **Call `await store.setup()` yourself** — the harness never does. |
| `agents` | Agent classes to register. `subagents` are walked transitively, so listing the roots is enough. |
| `default_model` | Model for agents that set none. Read fresh on every `run`/`resume`, so reassigning `harness.default_model` switches the model for subsequent turns. |
| `deps_factory` | `(SessionHeader) -> deps` (may be async), called once per `run`/`resume` and surfaced as `ctx.deps` / `TurnContext.deps`. This is the only correct home for connections, clients and pools: a resume in another process rebuilds them. |
| `retry` | Provider retry policy. See [RetryConfig](loop.md#retryconfig). |
| `lease_ttl` | Seconds the single-writer lease is held, refreshed before every sample. A worker that dies leaves the lease to expire, after which another process may `resume`. |
| `hooks` | [Hook](hooks.md) instances, run in order. |
| `default_permission` | Verdict for tool calls no rule and no tool declaration covers. Validated at construction. See [Permissions](permissions.md). |
| `max_depth` | Deepest child session. `ctx.spawn` at a greater depth raises `TantraError` inside the tool, which reaches the model as an error result. |
| `skills` | A [Skills](skills.md) catalogue. Enabling it injects the `skill` tool into every agent that has not opted out with `skills = []`. |
| `memory` | A [Memory](memory.md) backend, exposed to tools as `ctx.memory`. `None` makes `memory_write` / `memory_recall` raise a self-describing error. |
| `compactor` | A [Compactor](compaction.md), consulted before every sample. `None` disables compaction. |
| `telemetry` | A [Tracer](telemetry.md) — normally `Telemetry()` from the `[telemetry]` extra. `None` installs `NullTracer` and records nothing. |

### Validation at construction

Everything below raises `TantraError` before a single turn runs:

- an entry that is not an `Agent` subclass, or two classes resolving to the same name;
- an entry in `Agent.tools` that is not a `Tool` (i.e. not decorated with `@tool`);
- a tool whose JSON schema is not an object, has an **unannotated `ctx` parameter**, or has a required parameter with no inferable JSON type;
- an invalid permission string in `default_permission` or in any agent's `permissions`;
- `max_steps < 1`;
- duplicate tool names within one agent — including a subagent delegate colliding with a tool, or a user tool named `skill` when `skills` is configured.

Useful attributes after construction: `harness.agents` (name → class) and `harness.tools` (agent name → tool name → `Tool`).

## `agent_for(name: str) -> type[Agent]`

Returns the registered class. Raises `TantraError` listing the known names when `name` is unknown.

## `async create_session(agent, metadata=None) -> SessionHeader`

`agent` is a class or a registered name. Creates the session and appends `SessionCreated` at seq 1. `metadata` is copied onto the header and is the only handle for scoping later `store.list(metadata=...)` queries — tantra enforces no isolation of its own.

## `run(sid: str, input: str) -> AsyncIterator[Emitted]`

Starts a new turn. Persists `TurnStarted`, calls `before_turn` hooks, then samples until the model stops, the output schema is submitted, `max_steps` is hit, a tool suspends, or the turn is cancelled.

Raises, **on the first iteration**:

| Exception | Cause |
|---|---|
| `SessionNotFound` | Unknown `sid`. |
| `SessionBusy` | Another process holds a live lease. |
| `TurnIncomplete` | The previous turn never terminated — use `resume(sid)`. |

On exit the header is settled: `status` becomes `idle`, `failed`, or `awaiting_input` with `pending_ask` set, and the lease is released.

## `resume(sid, ask_id=None, response=None) -> AsyncIterator[Emitted]`

Re-drives the session's incomplete turn. Three shapes:

- **`resume(sid)` with an unanswered ask** — yields the pending `AskRaised` again and returns without running anything. Use it to re-present a question after a reconnect. The re-emitted frame carries `seq=None`, marking it a replay of an event already in the log rather than a new one. The exception is a session that was cancelled while suspended: the replay is skipped and the turn is re-driven straight to `TurnCompleted(stop_reason="cancelled")`.
- **`resume(sid, ask_id, response)`** — appends `AskAnswered`, then continues the turn. The tool that asked is **re-executed from its first line**; already-answered asks return their recorded responses without prompting.
- **`resume(sid)` with no pending ask** — re-drives a turn abandoned mid-flight (dropped stream, dead worker, cancelled session).

Raises `SessionNotFound`, `SessionBusy`, or `TantraError` when there is no incomplete turn, when `ask_id` and `response` are not supplied together, when `ask_id` is unknown or already answered, or when a permission ask is answered with anything but an `ApprovalResponse`. All on the first iteration. Agent, model, permission chain, skill index and deps are all resolved **before** `AskAnswered` is written, so a broken configuration does not burn the ask.

`before_turn` hooks do **not** fire on resume; `before_sample` does.

## `async cancel(sid: str, *, recursive: bool = False) -> bool`

Appends `CancelRequested` for the running turn and returns `True`. Returns `False` when there is no incomplete turn. With `recursive=True`, every descendant session is flagged deepest-first before the target, and the return is `True` when at least one of them had a turn to cancel. Raises `SessionNotFound`.

The append is **blind** (`expect_seq=None`), so it lands in one attempt against a log the running turn is still writing to — a busy session cannot livelock the race and refuse to be cancelled.

Cancellation is a persisted flag, not `task.cancel()` — the loop notices at its next store boundary (before a sample, between tool calls) and ends the turn with `stop_reason="cancelled"`, synthesizing error results for any tool calls left unanswered. A **suspended** turn takes the flag at the next `resume`, which completes it without sampling.

## `replay(sid, *, from_seq=0) -> AsyncIterator[Emitted]`

Yields every persisted event of the session in seq order, as `Emitted` with its `seq`. Raises `SessionNotFound` on the first iteration.

Replay reads one session's log. Child sessions have their own logs, so a replayed parent does **not** reproduce the child events you saw live — find them with `store.list(parent_id=sid)`.

## See also

- [Agent](agent.md) · [Loop & events flow](loop.md) · [Events](events.md)
- [Sharp edges](../sharp-edges.md) — the turn advances only while you consume the stream.
