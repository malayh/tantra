# Tantra v1 — Agent Harness Framework

## Goal

- A Python library that ships **the agent turn loop** and exposes its decision points as protocols, so an application supplies tools, skills, memory, storage and a provider and gets a working harness.
- One engine, many drivers: the same `Harness` runs behind FastAPI/WebSocket, an HTTP/SSE endpoint, or a terminal CLI, because the boundary is a typed event stream rather than a callback.
- The loop is **durable and re-entrant** — a turn suspended for human approval resumes from persisted state in a different process.

## Scope

**In (v1)**
- Turn loop: context assembly → sample → tool dispatch → repeat, with `max_steps` and usage accounting.
- Tools (`@tool` + inferred schema), permissions, `ctx.ask` suspend/resume, cancellation, lifecycle hooks.
- Sub-agents (child sessions) and `fan_out` parallel orchestration.
- Skills (`SKILL.md` on disk, progressive disclosure via a `skill` tool).
- Memory protocol + one built-in implementation.
- Context compaction (prune-then-summarize, configurable).
- Store protocol + four backends: in-memory, filesystem, SQLite, Postgres.
- Provider protocol + one OpenAI-compatible implementation (OpenRouter) + `FakeProvider` with record/replay.
- Adapters: CLI renderer, WebSocket, SSE, event collector.
- `apps/agni` — a real, usable terminal coding agent built on the library, in-repo.

**Out (v1)**
- MCP client. ACP adapter. OTel instrumentation. Evals framework.
- Artifact / `editable_object` as a core concept — stays application-level.
- Handoff and blackboard multi-agent patterns.
- Native Anthropic provider (the protocol is shaped to accept one; the impl is later).
- Cross-process **live** event tailing (see Open Decisions).
- Sandboxing / git-worktree isolation for sub-agents.
- Any tenancy or authorization enforcement.

## Decisions

- **The loop is the product, not a graph.** Tantra ships the loop; you configure its plug points. Rejected LangGraph: `observability_ui/backend/app/agents/dashboard_editor.py` is 6 nodes and 5 edges where 4 nodes are "call an LLM" — the graph bought nothing and cost a regex router (`re.search(r"nothing_to_do", ...)`, line 176) plus `thread_id` string-mangling for sub-agents (`services/agents.py:113`). Graphs pay off for genuine DAGs; a harness is one loop plus a stack.
- **Vocabulary.** A **turn** is one user request. A turn contains N **samples** (model API calls). Taken from Grok Build; opencode's `SessionPrompt.loop` is the same thing unnamed.
- **Posture: FastAPI-style library.** Protocols + explicit construction. No settings module, no app registry, no import-time side effects, no scaffolding command. It must embed inside an existing FastAPI app.
- **Durable, re-entrant loop.** The loop holds no state between samples that isn't in the store. Rejected in-process generators: an approval wait would pin a worker and a WS reconnect to a different pod would lose the live turn.
- **Append-only event log is the source of truth**, plus a small mutable header. State (including tool-call progress) is derived by replay, never by in-place update. Rejected mutable CRUD (opencode's `PartTable` with UPDATEs): the filesystem backend would need atomic in-place mutation inside a file — locking and read-modify-write, exactly where an FS backend gets racy.
- **Live stream ≠ persisted log.** Token deltas are emitted, not persisted; a completed `TextPart` is persisted. Replay reconstructs the turn's structure, not its keystrokes.
- **Transport is a typed event stream**, adapters serialize it. Rejected ACP-as-spine (Grok Build's choice): free Zed interop isn't worth a coding-agent-shaped, file-path-centric schema when the target agents edit dashboards. The *lesson* from ACP is kept — the boundary is a protocol, which is what kills the in-process `WSConnectionManager` dict at `services/agents.py:19`.
- **One HITL primitive.** Everything needing a human is `await ctx.ask(...)`, which suspends the turn durably. The declarative `permissions` ruleset is an auto-responder for permission-shaped asks. This unifies the two disconnected mechanisms in the current code — LangGraph `interrupt()` (`simple_chat.py:32`) and the `APPROVAL`/`CHOICE` wire union (`schema.py:35-49`).
- **Agents are declarative classes.** Class attributes for model/prompt/tools/subagents/permissions.
- **No agent registry concept.** `Harness(agents=[...])` *is* the name→class table. It is built by walking the declared list **transitively through `subagents`**, so a sub-agent never passed to `Harness` is still resolvable. Names must be unique; a collision raises at construction. Rejected decorator/global registry — import-order side effects are what forced the function-local imports in `services/agents.py:61-63`.
- **Tools: decorator + inferred schema; `ctx` injected by annotation** and stripped from the model-facing schema. Same idea as LangGraph's `InjectedState`, no LangGraph.
- **Tool calls within a sample run serially.** Providers emit several per sample; parallel execution multiplies the durable-suspend cases (two pending approvals at once) for little gain on DB/HTTP tools. `ctx.fan_out` is the explicit parallelism primitive.
- **Skills: progressive disclosure.** Name + description in the system prompt (~30 tokens each); the model calls `skill(name)` to pull the body. Rejected eager injection — 10 skills × 2k tokens burns 20k before the user speaks.
- **Memory: tools only.** `memory_recall` / `memory_write` are ordinary tools. No auto-recall injection — the model writes a better query than any heuristic, and an injected-but-irrelevant memory actively misleads. Matches how `kalki/backend/kalki/memory/registry.py` already exposes memory as verbs.
- **Memory implementation is thin**: rows with kind/title/body, tags, entities, soft delete, supersede, hybrid keyword+vector recall. No document ingestion, no extraction, no reconcile — kalki keeps owning those.
- **Sessions carry `metadata: dict` and nothing else.** Tantra performs no tenancy or authorization checks. Rejected first-class `tenant_id`/`user_id`: it imposes a two-level model on a single-user CLI, and a framework that half-enforces isolation is worse than one that clearly doesn't.
- **Provider protocol models cache markers and reasoning blocks natively**, not lowest-common-denominator, so a native Anthropic implementation drops in without a schema change. Rejected LiteLLM — inheriting someone else's bugs at the tool-call-streaming layer is the one place that must be exact.
- **Compaction: prune-then-summarize**, all thresholds in `CompactionConfig`, behind a `Compactor` protocol.
- **Structured final output via a synthetic tool.** `Agent.output_schema` appends a `submit_output` tool; the turn ends when the model calls it. Rejected the current two-pass approach (`dashboard_editor.py:134-144` runs a second LLM with `with_structured_output` and routes on a regex) — one model call, no regex, and the schema is enforced by the provider.
- **License: Apache-2.0.** Patent grant; what a framework is expected to ship under.
- **Python 3.13, `uv` workspace, `just`, ruff line-length 120, pytest `asyncio_mode = "auto"`.** Follows `odin/pyproject.toml` and `odin/justfile`.

## The turn loop

```
run(session_id, input):
  acquire session lease (single writer per session)
  append TurnStarted
  loop until stop:
    hooks.before_sample
    compactor.check() -> maybe CompactionApplied
    build SampleRequest  (system + skills index + history + tool schemas)
    append SampleStarted
    provider.stream(req):
      TextDelta / ReasoningDelta / ToolCallDelta  -> emit live
      accumulate into TextPart / ReasoningPart / ToolCallRequested
    append parts + Usage
    if no tool calls or submit_output called or max_steps hit: stop
    for each tool call, in order:
      resolve tool; hooks.before_tool (may deny/transform)
      permissions.decide(tool, args) -> allow | ask | deny
      if ask: append AskRaised, release lease, RETURN (turn suspended)
      append ToolCallStarted
      execute; ToolProgress from ctx.emit; exceptions -> is_error result
      hooks.after_tool (may transform)
      append ToolCallCompleted
  append TurnCompleted
  release lease
```

`resume(session_id, ask_id, response)` re-enters this loop from replayed state: it appends `AskAnswered`, synthesizes the result for the asked-about call, and continues at the next tool call in the batch.

## Event model

Two streams, deliberately different.

**Persisted (the log).** Ordered, append-only, `seq` per session.

| Event | Fields |
|---|---|
| `SessionCreated` | `agent`, `parent_id`, `depth`, `metadata` |
| `TurnStarted` | `turn_id`, `input` |
| `SampleStarted` | `turn_id`, `sample_id`, `model` |
| `TextPart` | `sample_id`, `text` |
| `ReasoningPart` | `sample_id`, `text`, `signature` |
| `ToolCallRequested` | `sample_id`, `call_id`, `name`, `args` |
| `ToolCallStarted` | `call_id` |
| `ToolProgress` | `call_id`, `message` |
| `ToolCallCompleted` | `call_id`, `result`, `is_error` |
| `ChildSessionSpawned` | `call_id`, `child_session_id`, `agent` |
| `AskRaised` | `ask_id`, `call_id`, `request` |
| `AskAnswered` | `ask_id`, `response`, `answered_by` |
| `SampleCompleted` | `sample_id`, `usage`, `finish_reason` |
| `CompactionApplied` | `strategy`, `tokens_before`, `tokens_after`, `summary` |
| `CancelRequested` | `turn_id` |
| `TurnCompleted` | `turn_id`, `stop_reason` |
| `TurnFailed` | `turn_id`, `error` |

**Emitted only (never persisted):** `TextDelta`, `ReasoningDelta`, `ToolArgsDelta`.

Every emitted event carries `session_id` and `depth` so a client can nest or collapse sub-agent output on a single stream.

### Session header (mutable projection)

`id`, `agent`, `parent_id`, `depth`, `created_at`, `updated_at`, `title`, `status` (`idle|running|awaiting_input|failed`), `metadata`, `last_seq`, `usage`, `lease`, `pending_ask`.

## Protocols

```python
class Store(Protocol):
    async def setup(self) -> None: ...
    async def create(self, header: SessionHeader) -> None: ...
    async def header(self, sid: str) -> SessionHeader | None: ...
    async def put_header(self, h: SessionHeader) -> None: ...
    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int) -> int: ...
    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]: ...
    async def list(self, *, metadata: dict | None = None, parent_id: str | None = None,
                   limit: int = 50, before: str | None = None) -> list[SessionHeader]: ...
    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool: ...
    async def release_lease(self, sid: str, holder: str) -> None: ...

class Provider(Protocol):
    def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]: ...
    async def embed(self, texts: list[str]) -> list[list[float]]: ...
    @property
    def limits(self) -> ModelLimits: ...   # context_window, max_output

class Skills(Protocol):
    async def index(self, names: list[str] | None) -> list[SkillRef]:  ...   # name + description
    async def load(self, name: str) -> SkillBody: ...                        # body + file listing

class Memory(Protocol):
    async def write(self, m: MemoryWrite) -> str: ...
    async def recall(self, q: str, *, k: int = 5, kind: str | None = None,
                     tags: list[str] | None = None, entity: str | None = None) -> list[MemoryHit]: ...
    async def supersede(self, old_id: str, new: MemoryWrite) -> str: ...
    async def delete(self, mid: str) -> None: ...

class Compactor(Protocol):
    async def compact(self, ctx: TurnContext) -> list[SessionEvent]: ...
```

`append(..., expect_seq=)` is optimistic concurrency: two workers cannot both advance one session.

## Public API

```python
harness = Harness(
    provider=OpenAICompatible(base_url=OPENROUTER, api_key=..., model="google/gemini-3-pro"),
    store=PostgresStore(dsn, schema="tantra"),
    agents=[Build, Explore, DashboardEditor],
    skills=FileSystemSkills("./skills"),
    memory=BuiltinMemory(store),
    deps_factory=lambda: Deps(mimir=MimirClient(), db=async_session),
    hooks=[audit_hook],
    default_permission="ask",
)

s = await harness.create_session(agent="build", metadata={"company": 42, "user": 7})
async for ev in harness.run(s.id, "fix the p99 panel"):
    ...
async for ev in harness.resume(s.id, ask_id, Approve(allow=True)):
    ...
await harness.cancel(s.id)
async for ev in harness.replay(s.id, from_seq=0):
    ...
```

```python
class DashboardEditor(Agent):
    model = "google/gemini-3-pro"
    prompt = load_prompt("prompts/dashboard_editor.md")
    tools = [search_metrics, get_label_values]
    subagents = [PromQLWriter]
    permissions = {"search_*": "allow", "write_*": "ask"}
    max_steps = 40
    output_schema = Dashboard
```

```python
@tool
async def search_metrics(query: str, ctx: Context) -> list[dict]:
    """Search for relevant metrics based on query."""
    await ctx.emit(f"searching for {query}")
    return await ctx.deps.mimir.search(query)
```

`Context` exposes: `session_id`, `turn_id`, `call_id`, `depth`, `deps`, `memory`, `store`, `emit()`, `ask()`, `spawn()`, `fan_out()`.

`prompt` accepts `str | Callable[[TurnContext], str | Awaitable[str]]`. `load_prompt(path)` reads a file. No template engine.

## Permissions and hooks

- Ruleset is `dict[glob, "allow" | "ask" | "deny"]`; longest matching glob wins; unmatched falls back to `Harness(default_permission=)`.
- A tool may also declare `permission=` at definition; the agent ruleset overrides it.
- **Sub-agent permissions are derived, never widened**: child effective = child declared ∩ parent effective. Mirrors opencode's `deriveSubagentSessionPermission`.
- Hooks: `before_turn`, `before_sample`, `before_tool`, `after_tool`, `after_turn`, `on_event`. `before_tool` returns `None` (pass), a modified `ToolCall` (transform), or `Denial(reason)` (deny — becomes an error tool result, loop continues).

## Sub-agents and fan-out

- A sub-agent is a **child session** with `parent_id` and `depth+1`, run by the same loop. No second abstraction.
- `subagents = [PromQLWriter]` exposes each as a tool named after the agent. `ctx.spawn(Agent, input)` is the imperative form.
- `max_depth` on `Harness` (default 3) prevents runaway recursion.
- `ctx.fan_out(tasks, max_concurrency=4)` spawns N children concurrently and returns `list[Result | Error]` — one failure does not fail the turn.
- Child events are forwarded onto the parent's **live** stream tagged with the child `session_id` and `depth`.

## Compaction

Two stages, thresholds in `CompactionConfig` with the defaults below (all overridable).

```
usable          = context_window - max_output - buffer      # buffer default 20_000
prune_pool_min  = 40_000   # only prune if tool output totals at least this
prune_gain_min  = 20_000   # only prune if it reclaims at least this
tail_turns      = 2        # never touch the last N turns
summarize_at    = 0.95     # of usable, after pruning
```

- **Stage 1 (free):** replace bulky tool-result content with metadata stubs, newest-first, protecting `tail_turns`. Never prunes `skill` tool output — that would silently drop a capability the model believes it has.
- **Stage 2 (one LLM call):** summarize the pruned prefix into a structured brief — Goal / Constraints / Progress / Key Decisions / Next Steps / Critical Context — and replace those messages. Emits `CompactionApplied`.

## Storage backends

| Backend | Layout | Notes |
|---|---|---|
| `MemoryStore` | dicts | Test default; zero config. |
| `FileSystemStore` | `<root>/<sid>/session.json` + `events.jsonl` | Lease is an `fcntl.flock` on a lockfile. |
| `SQLiteStore` | `sessions`, `events(session_id, seq)` PK | WAL mode. |
| `PostgresStore` | same tables in a dedicated `tantra` schema | `setup()` is idempotent and versioned; `metadata` is JSONB + GIN. Mirrors what `core/langgraph.py` already does with `search_path=langgraph` and `saver.setup()`. |

All four pass one shared conformance suite in `tantra.testing`.

## Sharp edges

Things that will surprise an implementer. Each is load-bearing.

- **Cancel is a persisted flag, not `task.cancel()`.** The loop may be running in another process. `cancel()` appends `CancelRequested`; the loop checks the store at sample and tool-call boundaries. An in-flight tool in *your* process also gets an asyncio cancellation, but that is an optimization, not the mechanism.
- **Nothing may be captured in a Python closure across a suspend.** After `ctx.ask` the process can die. Anything a tool needs post-resume comes from `ctx.deps` (rebuilt per process by `deps_factory`) or from the event log. `deps_factory` must be a factory for exactly this reason — a captured connection pool will not survive a resume on another pod.
- **`O_APPEND` is not atomic above `PIPE_BUF` (4096 bytes).** A single JSON line longer than that can interleave under concurrent append, which any tool result will exceed. The FS backend does not rely on append atomicity — correctness comes from the **single-writer lease per session**. Fan-out children write to their own logs, so they never contend.
- **Replaying a parent session does not reproduce the child events you saw live.** Child events persist only to the child's log; forwarding is live-only. A client reconstructing history must fetch child sessions via `list(parent_id=...)`. This asymmetry is deliberate — the alternative doubles every child write.
- **OpenAI-compatible APIs require every `tool_call_id` in an assistant message to be answered before the next sample.** So a suspend mid-batch must, on resume, still emit results for the calls that were never executed — `"denied by user"` or `"not executed: turn interrupted"`. These are real `ToolCallCompleted` events with `is_error=True`, not omissions. Getting this wrong produces a 400 that only reproduces after a denial.
- **Compaction must never orphan a tool_call/tool_result pair.** Stage 1 replaces result *content* and never removes a message. Stage 2 cuts only at turn boundaries. A prefix cut mid-turn yields an assistant message with a `tool_call` whose result was dropped, which is a 400 on every OpenAI-compatible provider.
- **Token counts come from the provider's reported usage on the previous sample**, plus a `len(text) // 4` estimate for content added since. There is no local tokenizer and no attempt to match one; the compaction `buffer` exists to absorb the error.
- **`ctx.emit` progress is persisted** as `ToolProgress`. The existing `AgentProgress` writer (`dashboard_editor.py:60-61`) is live-only and its output is lost on reconnect — this fixes that.
- **Tantra enforces no isolation.** `list(metadata={...})` with the wrong filter, or no filter, returns every session in the store. Multi-tenant callers must scope every read themselves.

## Implementation phases

```
P0 store ─┬─ P2 loop ─┬─ P3 control ─┬─ P4 subagents ─┐
P1 provider ┘         │              └─ P9 adapters ──┤
                      ├─ P5 skills ───────────────────┤
                      ├─ P6 memory ───────────────────┤
                      └─ P7 compaction ───────────────┤
P0 ───────── P8 sqlite+postgres ──────────────────────┴─ P10 agni
```

Where the graph lies: **P5, P6 and P7 all edit context assembly** (`context.py:build_sample_request`). They are independent on paper and will conflict in one function in practice — land them in sequence or agree the insertion order up front. **P6 on Postgres needs P8** for pgvector; build it against SQLite/FS first.

### Conventions (all phases)
- `uv` workspace at the repo root, `package = false`, members `["packages/tantra", "apps/agni"]`. Follows `odin/pyproject.toml`.
- Python 3.13. ruff, line-length 120. `just lint`, `just test`, `just sync`.
- pytest with `asyncio_mode = "auto"`; tests in `packages/tantra/tests/`.
- **No comments in code.** Docstrings on public protocols and tools only — a tool's docstring *is* its model-facing description.
- No network in tests. Every test uses `FakeProvider` or a recorded cassette.
- Run `just lint` + `just test` before marking a phase done.
- **Contract freeze after P0/P1:** the `SessionEvent` union, `SessionHeader`, the `Store` protocol, and the `Provider`/`SampleRequest`/`ProviderEvent` types. Changing them means updating this spec first, then telling dependent phases.

### Keeping this spec current
- Update the status marker on the heading and tick the checklist as you go.
- When the build deviates from the plan, **strike the original line and say why it changed** — `~~original~~ **Cut in P4.** <reason>`. Never silently rewrite; the reason a plan changed is worth more than the plan.
- After a phase lands, add only detail that would surprise the next reader — a constant whose value is load-bearing, a behavior that isn't what the name suggests, an ordering that matters. Skip anything the code already says plainly.
- Problems found but not fixed go to Open Decisions or a Follow-up note, with enough detail to act on later. Don't fix them inline and don't leave them unrecorded.

### Phase 0 — Event log + Store protocol + memory/FS backends · deps: none · ∥ P1 · blocks all · —
- `events.py`: the `SessionEvent` discriminated union and `SessionHeader`, as tabled above.
- `stores/base.py`: `Store` protocol with `expect_seq` optimistic concurrency and leases.
- `stores/memory.py`, `stores/fs.py`. FS lease via `fcntl.flock` on `<sid>/.lock`.
- `testing.py`: `store_conformance(store)` — one suite every backend must pass.
- **Verify:** conformance suite green on both backends. Specifically: appending with a stale `expect_seq` raises; two concurrent `acquire_lease` calls, exactly one wins; `read(from_seq=n)` returns a contiguous suffix; a JSONL file with a truncated final line still reads every complete event before it.
- Checklist:
  - [ ] Event union + header
  - [ ] Store protocol
  - [ ] MemoryStore
  - [ ] FileSystemStore
  - [ ] Conformance suite

### Phase 1 — Provider protocol + OpenAI-compatible + FakeProvider · deps: none · ∥ P0 · —
- `providers/base.py`: `Provider`, `SampleRequest` (system blocks with `cache` markers, messages, tool schemas, params), `ProviderEvent` union, `ModelLimits`.
- `providers/openai_compat.py`: streaming chat completions against OpenRouter; accumulates fragmented `ToolCallDelta`s into complete calls; surfaces `usage`.
- `providers/fake.py`: `FakeProvider([Sample(text=...), Sample(tool_calls=[...])])` plus cassette record/replay.
- **Verify:** a recorded OpenRouter cassette with tool-call arguments split across 12 SSE chunks replays into exactly one `ToolCallRequested` with byte-identical JSON args. `FakeProvider` scripted with two samples yields the exact expected `ProviderEvent` sequence.
- Checklist:
  - [ ] Provider protocol + request/event types
  - [ ] OpenAI-compatible streaming + tool-call accumulation
  - [ ] FakeProvider
  - [ ] Cassette record/replay

### Phase 2 — The turn loop · deps: P0, P1 · —
First genuinely useful phase: non-interactive agents work end to end.
- `agent.py` (`Agent` + name derivation + transitive `subagents` name table), `tools.py` (`@tool`, schema inference from type hints + docstring, `ctx` stripping), `context.py` (system prompt + history + tool schemas), `loop.py`, `harness.py` (`create_session`, `run`, `replay`).
- Serial tool dispatch. Tool exceptions become `is_error` results the model can see. `max_steps` stop. Usage accumulation onto the header.
- `Agent.output_schema` → synthetic `submit_output` tool ends the turn.
- `adapters/collect.py`.
- **Verify:** with `FakeProvider` scripted to call `search_metrics` then answer, `collect(harness.run(...))` yields `ToolCallRequested → ToolCallStarted → ToolCallCompleted → TextPart → TurnCompleted` in that order, and `replay()` from a fresh `Harness` over the same store reconstructs identical events. A tool that raises produces `is_error=True` and the loop continues to a second sample.
- Checklist:
  - [ ] Agent class + name table
  - [ ] `@tool` schema inference + ctx injection
  - [ ] Context assembly
  - [ ] Loop with serial dispatch + max_steps
  - [ ] `output_schema` via submit_output
  - [ ] collect adapter

### Phase 3 — Control layer: ask/resume, permissions, cancel, hooks · deps: P2 · —
- `ctx.ask()` → `AskRaised`, lease released, turn returns. `harness.resume(sid, ask_id, response)` replays and continues mid-batch.
- Unexecuted calls in a suspended batch get synthesized `is_error` results on resume.
- `permissions.py`: glob ruleset, longest-match, `default_permission`, auto-answer of permission asks.
- `harness.cancel()` via `CancelRequested` + boundary checks.
- `hooks.py`: the six hook points; `before_tool` allow/deny/transform.
- **Verify:** run a turn that asks; **discard the `Harness` object entirely**, build a new one over the same store, call `resume()` — the turn completes with the tool executed and the log contiguous. A `deny` rule produces an error tool result without ever invoking the tool. `cancel()` from a second `Harness` instance stops a running turn at the next boundary.
- Checklist:
  - [ ] ask/resume across process boundary
  - [ ] Synthesized results for unexecuted calls
  - [ ] Permission ruleset + auto-answer
  - [ ] Cancellation
  - [ ] Hooks

### Phase 4 — Sub-agents + fan-out · deps: P3 · ∥ P5, P6, P7, P8 · —
- Child sessions (`parent_id`, `depth`), `max_depth`, derived permissions.
- `subagents = [...]` → auto-generated tools; `ctx.spawn`; `ctx.fan_out(max_concurrency)`.
- Live child-event forwarding onto the parent stream with `session_id` + `depth`.
- **Verify:** a parent whose sub-agent calls two tools shows those events on the parent stream at `depth=1`, and the parent's own log contains only the `task` call plus `ChildSessionSpawned`. `fan_out` of 3 where one raises returns 2 results + 1 error and the parent turn completes. Depth 4 with `max_depth=3` raises before the child session is created.
- Checklist:
  - [ ] Child sessions + depth limit
  - [ ] Derived permissions
  - [ ] subagents-as-tools + spawn
  - [ ] fan_out with partial failure
  - [ ] Event forwarding

### Phase 5 — Skills · deps: P2 · ∥ P4, P6, P7, P8 · —
- `skills.py`: `Skills` protocol, `FileSystemSkills(root)` parsing `SKILL.md` YAML frontmatter (`name`, `description`) — the format already used across `kalki/skills/` and `.agents/skills/`.
- Index (name + description) injected into the system prompt; `skill(name)` tool returns the body plus a listing of files in the skill directory.
- `Agent.skills` filters which skills are indexed (`None` = all).
- **Verify:** an agent with 3 skills adds 3 lines to the system prompt and nothing else; the assembled request is under 200 tokens larger than the same agent with no skills. Calling `skill("cold-email")` places the full body in context and the listing includes `references/`.
- Checklist:
  - [ ] Skills protocol
  - [ ] FileSystemSkills + frontmatter parsing
  - [ ] Index injection
  - [ ] skill tool + file listing

### Phase 6 — Memory · deps: P2 · ∥ P4, P5, P7 · needs P8 for pgvector · —
- `memory.py`: `Memory` protocol. `BuiltinMemory` over the store backends: kind/title/body, tags, entities, soft delete, supersede.
- Hybrid recall: keyword always; vector where the backend supports it (Postgres/pgvector), degrading to keyword-only elsewhere — **explicitly, with the degradation visible in `MemoryHit`**, not silently.
- `memory_recall` / `memory_write` tools.
- Embeddings via `Provider.embed`, best-effort: a failed embed never fails the write (kalki's `_embed_best_effort` pattern), with a `backfill` entrypoint to repair.
- **Verify:** write 3 memories, `recall` ranks the matching one first on SQLite (keyword) and on Postgres (hybrid); `supersede` removes the old row from recall while `get` still returns it; a write with the embedding provider unreachable still commits and is repaired by `backfill`.
- Checklist:
  - [ ] Memory protocol
  - [ ] BuiltinMemory + schema
  - [ ] Hybrid recall + honest degradation
  - [ ] Tools
  - [ ] Best-effort embed + backfill

### Phase 7 — Compaction · deps: P2 · ∥ P4, P5, P6, P8 · —
- `compaction.py`: `Compactor` protocol, `CompactionConfig`, `PruneThenSummarize`.
- Stage 1 stubbing with `tail_turns` protection and skill-output exemption; stage 2 structured brief; `CompactionApplied`.
- **Verify:** a synthetic session with 200k tokens of tool output compacts below `usable`; the last 2 turns are byte-identical afterwards; every `tool_call` in the compacted message list still has a matching result (assert this explicitly — it's the 400-producing failure); a session that is all conversation and no tool output skips stage 1 and goes straight to stage 2.
- Checklist:
  - [ ] Compactor protocol + config
  - [ ] Stage 1 prune
  - [ ] Stage 2 summarize
  - [ ] Pair-integrity assertion in tests

### Phase 8 — SQLite + Postgres stores · deps: P0 · ∥ P2–P7 · —
- `stores/sqlite.py` (WAL), `stores/postgres.py` (dedicated `tantra` schema, idempotent versioned `setup()`, JSONB+GIN on metadata, optional pgvector for P6).
- **Verify:** the P0 conformance suite passes unchanged against both. Two processes against one Postgres store: only one acquires the lease; the loser's `append` with a stale `expect_seq` raises rather than corrupting. `setup()` run twice is a no-op.
- Checklist:
  - [ ] SQLiteStore
  - [ ] PostgresStore + versioned setup
  - [ ] pgvector support
  - [ ] Conformance green on all four backends

### Phase 9 — Adapters · deps: P3 · ∥ P5–P8 · —
- `adapters/cli.py` (terminal renderer, prompts on ask), `adapters/ws.py`, `adapters/sse.py`.
- **Verify:** against a FastAPI `TestClient`, a WS session runs a turn, receives an ask, sends a response, and receives `TurnCompleted`. With a Postgres store and two separate `Harness` instances, instance A starts the turn and instance B resumes it — the pattern the current `WSConnectionManager` dict makes impossible.
- Checklist:
  - [ ] CLI renderer
  - [ ] WS adapter
  - [ ] SSE adapter
  - [ ] Two-worker resume test

### Phase 10 — `apps/agni` reference harness · deps: P3, P4, P5, P7, P9 · —
Real and usable, not elaborate. It exists to make gaps in the library show up.
- Tools: `read`, `write`, `edit`, `glob`, `grep`, `bash`.
- Agents: `build` (full access) and `explore` (read-only, available as a sub-agent).
- Permissions: reads allow, writes and bash ask. FS skills from `./skills`. Session resume. Compaction on.
- `main.py` over `adapters/cli.py`, ~30 lines.
- **Verify:** on a scratch git repo, ask it to add a function to an existing file — it greps, reads, prompts before writing, and produces a correct edit. `^C` mid-turn then re-running with the same session id resumes rather than restarting. A task that fills the context window compacts and continues instead of erroring.
- Checklist:
  - [ ] Six tools
  - [ ] build + explore agents
  - [ ] Permission prompts
  - [ ] Session resume
  - [ ] End-to-end run on a scratch repo

## Open Decisions

- **Cross-process live tailing.** v1 ships `replay(from_seq)` (finite) only. A client reconnecting to a pod that isn't running the turn must poll. Resolving it needs a pub/sub — Postgres `LISTEN/NOTIFY` for the PG backend, or a `Bus` protocol with a Redis implementation. Decide once a real deployment hits it.
- **Artifact / `editable_object`.** Kept out of core; `observability_ui`'s dashboard editor needs it and will implement it application-side first. Promote to core only if a second consumer needs the same thing.
- **MCP client.** Deferred. Nothing in the current agents needs it. Revisit when an external tool server is actually wanted.
- **OTel instrumentation.** Deferred despite the obvious fit. Spans for turn/sample/tool with token and cost attributes are ~100 LOC; the question is whether they belong in core or in a `tantra-otel` package.
- **Native Anthropic provider.** The protocol accommodates cache markers and thinking blocks. Explicit prompt caching is the single largest cost lever on a loop that replays a growing history every sample — worth measuring before deciding.
- **ACP adapter.** Would give editor integration. Only worth it if someone wants tantra agents inside Zed.
- **Handoff pattern.** ~50 LOC (swap the agent on the session). Left out because nobody has asked for triage-style routing yet.
- **Migrating `observability_ui` agents.** Blocked on the artifact decision above.

## Risks

- **Durable-loop tax on the CLI.** Every sample round-trips the store. With `FileSystemStore` that's cheap; with a remote Postgres it's a real per-step latency. Mitigation: batch appends per sample rather than per event; `MemoryStore` for throwaway runs.
- **Sub-agent event forwarding + replay asymmetry confuses clients.** A UI built against the live stream will not reproduce the same view from `replay`. Mitigation: document it loudly, and have `replay` emit `ChildSessionSpawned` prominently so a client knows to go fetch.
- **Compaction constants are guesses until real sessions exist.** The defaults are borrowed from opencode's numbers for a coding agent; a dashboard agent's distribution is different. Mitigation: `CompactionApplied` records before/after tokens, so the data to tune with accumulates automatically.
- **Schema inference from type hints will misfire** on unions, generics, and forward refs, and the failure will be a confusing provider-side 400. Mitigation: validate every tool's generated JSON Schema at `Harness` construction and fail loudly there, not at call time.
- **No isolation enforcement is a real footgun.** A missing `metadata` filter leaks sessions across tenants. Mitigation: documented sharp edge; consumers should wrap `Store` with a scoping decorator rather than pass filters at each call site.
- **`apps/agni` grows into a second product.** Mitigation: it exists to exercise the library. If a feature doesn't stress a library surface, it doesn't go in.

## Success criteria

- One `Harness` runs unchanged behind a WebSocket, an SSE endpoint, and a terminal — only the adapter differs.
- A turn suspended for approval on worker A resumes on worker B and completes correctly.
- `apps/agni` performs a real multi-file edit on a scratch repo, prompts before writing, survives `^C` + resume, and compacts without erroring.
- The store conformance suite passes against in-memory, filesystem, SQLite and Postgres.
- Every test in `packages/tantra/tests/` runs with no network access.
- A sub-agent's tool calls appear live on the parent's stream, correctly tagged with depth.
