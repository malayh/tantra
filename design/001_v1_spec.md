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
- Multimodal turn input (`TurnStarted.input` is `str`).
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
- ~~**Hand-rolled SSE/accumulation over the official `openai` SDK**~~ **Reversed post-P1.** The `openai` SDK now owns SSE decode and tool-call accumulation (`AsyncOpenAI` with `max_retries=0` — the loop owns retry; exceptions translate to `ProviderError` with `status_code`). Deciding factor: 3 of the 8 P1 review defects (mid-stream error frames, index-less accumulation, missing ids) sat exactly in the layer the SDK has already hardened. The tantra↔chat mapping, disjoint usage semantics, reserved-key policy and cassette seam stay ours; `http_client=` keeps the injectable-transport tests. Caveats: the SDK's `.stream()` helper rejects non-strict tool schemas, so we use `chat.completions.create(stream=True)` + `ChatCompletionStreamState` (P2 schema inference need not satisfy OpenAI strict mode); OpenRouter extras ride `extra_body`; a server omitting tool-call `index` now raises `ProviderError` instead of being tolerated.
- **Model rides the request, not the provider.** `Provider` holds transport only (base_url, key); `SampleRequest.model` names the model, sourced from `Agent.model` with `Harness(default_model=)` fallback; `limits(model)` replaces the `limits` property. Rejected model-in-provider — `dashboard_editor.py:29-43` needs pro for the loop and flash for parsing; one model per harness can't express a cheaper sub-agent. Rejected a named provider registry — a naming layer taxing the single-vendor common case.
- **`run()`/`resume()` are consumption-driven.** The loop advances as the caller iterates; nothing runs detached. A disconnect mid-turn pauses the turn durably at the last persisted event; any process re-enters with `resume(sid)`. Rejected a background task: `Harness` would own task lifecycles, and reconnect needs the live tailing already deferred to Open Decisions.
- **Typed ask vocabulary.** `Approval`, `Choice`, `FreeText` requests with matching responses, each carrying `extra: dict`. The permission auto-responder answers `Approval`; adapters render all three with no app code; custom payloads ride `extra`. Rejected opaque dicts — the auto-responder and the CLI renderer both need a recognizable shape.
- **`deps_factory(header)` takes the `SessionHeader`**, sync or async, called at each `run`/`resume` entry. `metadata` is how deps get tenant-scoped (per-company clients, as the current agents do per request). Cleanup is the app's own lifecycle — no managed teardown in v1.
- **Embeddings are a separate `Embedder` protocol**, not `Provider.embed`. OpenRouter serves no embeddings endpoint — the one shipped provider couldn't implement its own protocol. `BuiltinMemory(store, embedder=...)`.
- **Memory tools are ordinary tools.** `memory_recall`/`memory_write` are imported and listed in `Agent.tools`; setting `Harness(memory=)` injects nothing.
- **`TurnCompleted` carries `output`.** The parsed `output_schema` value lands on the terminal event; callers never fish `submit_output` args out of the stream.
- **Child asks bubble.** A sub-agent's `ctx.ask` (or `ask` permission) suspends the child durably and forwards `AskRaised` onto the parent's live stream; the whole ancestry suspends. The answer targets the child session; bare `resume(root)` re-drives the chain. Rejected ask→deny in children (Grok Build style) — it silently changes what the one HITL primitive means at depth≥1.
- **The loop owns provider retry.** Transient failures (429, 5xx, timeout) retry with capped exponential backoff — `RetryConfig` on `Harness`, default 3 attempts — then `TurnFailed`. A partial stream is discarded, never persisted. Hand-rolled backoff, not tenacity: one call site, and the discard-partial-sample semantics don't fit a generic decorator; not worth a dependency.
- **Events carry a version int**; readers tolerate unknown fields. The log outlives the code that wrote it.
- **`TurnStarted.input` is `str`.** Multimodal input is out of v1.
- **Memory rows carry `metadata: dict`** exactly like sessions — recall filters on it, tantra enforces nothing. Without it a multi-tenant app leaks memories across tenants with no way to filter.
- **Unattended guardrails are `before_tool` hooks, not declarative arg rules.** No-HITL runs use `allow`/`deny` rulesets plus a hook that denies or transforms calls by args; `deny` is an `is_error` result the model adapts to, never a suspend. Declarative arg-level rules stay parked in Open Decisions. Demonstrated in `apps/agni` (P10).
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

Transient provider errors (429, 5xx, timeout) are retried inside the sample step per `RetryConfig` before becoming `TurnFailed`; a partially streamed sample is discarded, nothing persisted.

`resume(session_id, ask_id=None, response=None)` re-enters any incomplete turn from replayed state. With `ask_id` + `response` it appends `AskAnswered`, synthesizes the result for the asked-about call, and continues at the next tool call in the batch. With neither, it re-drives a turn abandoned mid-flight (caller stopped consuming). `run()` with a turn incomplete raises `TurnIncomplete`; `run()` while the lease is held raises `SessionBusy` — typed errors, no queuing.

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
| `AskRaised` | `ask_id`, `call_id`, `request: AskRequest` |
| `AskAnswered` | `ask_id`, `response: AskResponse`, `answered_by` |
| `SampleCompleted` | `sample_id`, `usage`, `finish_reason` |
| `CompactionApplied` | `strategy`, `tokens_before`, `tokens_after`, `summary` |
| `CancelRequested` | `turn_id` |
| `TurnCompleted` | `turn_id`, `stop_reason`, `output` |
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
    def limits(self, model: str) -> ModelLimits: ...

class Embedder(Protocol):
    async def embed(self, texts: list[str]) -> list[list[float]]: ...

class Skills(Protocol):
    async def index(self) -> Sequence[SkillInfo]: ...   # name + description; filtering moved to Harness (P5)
    async def load(self, name: str) -> Skill: ...       # body + file listing

class Memory(Protocol):
    async def write(self, m: MemoryWrite) -> str: ...
    async def get(self, mid: str) -> MemoryRecord | None: ...   # added in P6: Verify needs get() on dead rows
    async def recall(self, q: str, *, k: int = 5, kind: str | None = None, tags: list[str] | None = None,
                     entity: str | None = None, metadata: dict | None = None) -> list[MemoryHit]: ...
    async def supersede(self, old_id: str, new: MemoryWrite) -> str: ...
    async def delete(self, mid: str) -> None: ...

class Compactor(Protocol):
    async def compact(self, ctx: TurnContext) -> list[SessionEvent]: ...
```

`append(..., expect_seq=)` is optimistic concurrency: two workers cannot both advance one session. `Stamped` is `(seq, SessionEvent)`. `SampleRequest` carries `model`; `ModelLimits` is `context_window` + `max_output`.

### Ask vocabulary

```python
class Approval(AskRequest):  title: str; body: str; extra: dict
class Choice(AskRequest):    title: str; options: list[str]; extra: dict
class FreeText(AskRequest):  prompt: str; extra: dict

await ctx.ask(Approval(title="Write dashboard?", body=diff))  # -> ApprovalResponse(allow=True)
```

Responses mirror requests (`ApprovalResponse(allow)`, `ChoiceResponse(selected)`, `FreeTextResponse(text)`). This is the wire contract the existing `APPROVAL`/`CHOICE` union in `observability_ui`'s `schema.py:35-49` maps onto.

## Agent vs Session vs Harness

| | What it is | Lifetime | Holds |
|---|---|---|---|
| `Agent` | declaration | process, immutable | `model`, `prompt`, `tools`, `subagents`, `permissions`, `max_steps`, `output_schema` |
| `Session` | one conversation | rows in the store | event log + header (agent **name**, depth, parent_id, metadata) |
| `Harness` | the runtime | process, one or more per app | provider, `default_model`, store, `deps_factory`, hooks, the loop, the name→class table |

- **`Agent` holds no I/O.** No provider, no client, no DSN, no pool — only values and function references. Forced by the durable loop: a resume runs in a different process, which rebuilds `Harness` from config and looks up the agent **by name** from the persisted header. An `Agent` holding a live client cannot survive that. Same constraint that produced `deps_factory`. This is the defect in `dashboard_editor.py:29-43` — `init_chat_model` at import time welds the agent to a provider and an API key.
- **Cardinality differs**: many agents, one set of infrastructure. Merged, every agent would need a store and a provider passed in, and `provider=FakeProvider()` for tests would touch every agent class instead of one line.
- **Sub-agents need no second mechanism.** `ctx.spawn(PromQLWriter)` re-enters the *same* `Harness` — same store, provider and deps — with a new session at `depth+1`. If `Agent` owned the runtime, each child would re-plumb infrastructure.
- Analogy: `Harness` : `Agent` :: FastAPI app : router. The app owns the server, middleware and lifespan; the router owns paths and handlers and is inert alone.
- **Prior art has the same split with the runtime half ambient.** opencode has first-class agent config (`Agent.Info`: name, `mode` primary/subagent, model, prompt, tools, permission) but no runtime object — `Session`, `SessionPrompt.loop`, `SessionProcessor`, `Provider`, `Storage` read process-global app state. Grok Build likewise: subagent definitions are config, the runtime is crates behind ACP. Both ship an *app*, so one runtime per process was always true. Tantra is a library, so the runtime must be constructible: two `Harness` instances over one Postgres store, one starting a turn and the other resuming it, is unrepresentable in either of theirs and is the Phase 9 verify.

## Public API

```python
harness = Harness(
    provider=OpenAICompatible(base_url=OPENROUTER, api_key=...),
    default_model="google/gemini-3-pro",
    store=PostgresStore(dsn, schema="tantra"),
    agents=[Build, Explore, DashboardEditor],
    skills=FileSystemSkills("./skills"),
    memory=BuiltinMemory(store, embedder=OpenAICompatibleEmbedder(base_url=..., api_key=..., model=...)),
    deps_factory=lambda header: Deps(mimir=MimirClient(company=header.metadata["company"]), db=async_session),
    hooks=[audit_hook],
    default_permission="ask",
)

s = await harness.create_session(agent="build", metadata={"company": 42, "user": 7})
async for ev in harness.run(s.id, "fix the p99 panel"):
    ...
async for ev in harness.resume(s.id, ask_id, ApprovalResponse(allow=True)):
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

`Agent.model` is optional; unset falls back to `Harness(default_model=)`. Anywhere an agent is named — `create_session`, `spawn`, `fan_out` — the class and its name are both accepted.

## Permissions and hooks

- Ruleset is `dict[glob, "allow" | "ask" | "deny"]`; longest matching glob wins; unmatched falls back to `Harness(default_permission=)`.
- A tool may also declare `permission=` at definition; the agent ruleset overrides it.
- **Sub-agent permissions are derived, never widened**: child effective = child declared ∩ parent effective. Mirrors opencode's `deriveSubagentSessionPermission`.
- Hooks: `before_turn`, `before_sample`, `before_tool`, `after_tool`, `after_turn`, `on_event`. `before_tool` returns `None` (pass), a modified `ToolCall` (transform), or `Denial(reason)` (deny — becomes an error tool result, loop continues).

## Sub-agents and fan-out

- A sub-agent is a **child session** with `parent_id` and `depth+1`, run by the same loop. No second abstraction.
- `subagents = [PromQLWriter]` exposes each as a tool named after the agent. `ctx.spawn(Agent, input)` is the imperative form.
- `max_depth` on `Harness` (default 3) prevents runaway recursion.
- `ctx.fan_out(tasks, max_concurrency=4)` — `tasks` is `list[tuple[Agent | str, input]]` — spawns N children concurrently and returns `list[Result | Error]`; one failure does not fail the turn.
- `spawn` of an agent absent from the transitive name table raises at spawn, before the child session is created.
- `spawn` returns the child's final text, or its parsed `output` when the child has an `output_schema`.
- Child events are forwarded onto the parent's **live** stream tagged with the child `session_id` and `depth`.
- A child's ask suspends the whole ancestry: the child suspends durably, the parent's spawn tool call stays incomplete, `AskRaised` is forwarded live. Answer with `resume(child_sid, ask_id, response)`, then bare `resume(root_sid)` re-drives the chain top-down.

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
- Nothing is rewritten in the log — context assembly treats the latest `CompactionApplied` as the floor: its summary plus every event after it. `replay()` still returns full history.

## Storage backends

| Backend | Layout | Notes |
|---|---|---|
| `MemoryStore` | dicts | Test default; zero config. |
| `FileSystemStore` | `<root>/<sid>/session.json` + `events.jsonl` | ~~Lease is an `fcntl.flock` on a lockfile.~~ **Changed in P0.** Lease is a TTL'd record inside `<sid>/.lock`, with `fcntl.flock` as the mutex guarding it — bare flock can't express TTL and dies with the process, contradicting durable suspend. |
| `SQLiteStore` | `sessions`, `events(session_id, seq)` PK | WAL mode. |
| `PostgresStore` | same tables in a dedicated `tantra` schema | `setup()` is idempotent and versioned; `metadata` is JSONB + GIN. Mirrors what `core/langgraph.py` already does with `search_path=langgraph` and `saver.setup()`. |

All four pass one shared conformance suite in `tantra.testing`.

## Sharp edges

Things that will surprise an implementer. Each is load-bearing.

- **The loop advances only while someone consumes the iterator.** `run()`/`resume()` are generators; a WS client that drops mid-turn pauses the turn at the last persisted event — durable and resumable, but nothing re-drives it by itself. Detection: lease expired with the turn incomplete. Re-entry: `resume(sid)` with no `ask_id`. Server adapters must own this sweep; the library will not.
- **Cancel is a persisted flag, not `task.cancel()`.** The loop may be running in another process. `cancel()` appends `CancelRequested`; the loop checks the store at sample and tool-call boundaries. An in-flight tool in *your* process also gets an asyncio cancellation, but that is an optimization, not the mechanism. Cancelling a *suspended* turn takes effect at the next `resume`, which appends `TurnCompleted(stop_reason="cancelled")` without sampling.
- **Nothing may be captured in a Python closure across a suspend.** After `ctx.ask` the process can die. Anything a tool needs post-resume comes from `ctx.deps` (rebuilt per process by `deps_factory`) or from the event log. `deps_factory` must be a factory for exactly this reason — a captured connection pool will not survive a resume on another pod.
- **`O_APPEND` is not atomic above `PIPE_BUF` (4096 bytes).** A single JSON line longer than that can interleave under concurrent append, which any tool result will exceed. The FS backend does not rely on append atomicity — correctness comes from the **single-writer lease per session**. Fan-out children write to their own logs, so they never contend.
- **Replaying a parent session does not reproduce the child events you saw live.** Child events persist only to the child's log; forwarding is live-only. A client reconstructing history must fetch child sessions via `list(parent_id=...)`. This asymmetry is deliberate — the alternative doubles every child write.
- **OpenAI-compatible APIs require every `tool_call_id` in an assistant message to be answered before the next sample.** So *any* early stop mid-batch — suspend, deny, cancel, `max_steps` — must still emit results for the calls that were never executed: `"denied by user"` or `"not executed: turn interrupted"`. These are real `ToolCallCompleted` events with `is_error=True`, not omissions. `max_steps` is the sneaky one: the cap hitting on a sample that requested tools orphans the batch. Getting this wrong produces a 400 that only reproduces after a denial.
- **`spawn` must be re-entrant.** If the process dies (or the child suspends) mid-spawn, resume replays the parent and re-executes the spawn tool call — it must attach to the existing child found via `ChildSessionSpawned`, never create a twin. This same mechanism is what makes bubbled child asks resumable.
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

### Phase 0 — Event log + Store protocol + memory/FS backends · deps: none · ~~∥ P1~~ · blocks all · **done**

~~∥ P1~~ **Run sequentially in P0.** Empty repo — both phases would create the same scaffolding (workspace pyproject, justfile); parallel worktrees would conflict there.
- `events.py`: the `SessionEvent` discriminated union and `SessionHeader`, as tabled above; version int on the envelope, unknown fields tolerated on read.
- `stores/base.py`: `Store` protocol with `expect_seq` optimistic concurrency and leases.
- `stores/memory.py`, `stores/fs.py`. FS lease via `fcntl.flock` on `<sid>/.lock`.
- `testing.py`: `store_conformance(store)` — one suite every backend must pass.
- **Verify:** conformance suite green on both backends. Specifically: appending with a stale `expect_seq` raises; two concurrent `acquire_lease` calls, exactly one wins; `read(from_seq=n)` returns a contiguous suffix; a JSONL file with a truncated final line still reads every complete event before it.
- Checklist:
  - [x] Event union + header
  - [x] Store protocol
  - [x] MemoryStore
  - [x] FileSystemStore
  - [x] Conformance suite
- Landed notes (P0):
  - `store_conformance(store_factory)`, not `(store)` — multi-instance/multi-thread lease checks need fresh instances.
  - `Store.read` is `def` returning `AsyncIterator[Stamped]` (structural match for async generators), not `async def`.
  - Typed errors in `errors.py`: `SeqConflict`, `SessionExists` (`create` on existing id raises), `SessionNotFound`, `CorruptLog`.
  - `read()` tolerates only a torn (newline-less) final line; a complete line that fails validation raises `CorruptLog` — unknown event *types* are never silently dropped.
  - FS `append` reconciles `session.json` from the log tail seq under flock (log is source of truth after a crash between fsync and header write).
  - `put_header` preserves store-owned `last_seq` and `lease`; expired leases are returned unfiltered on headers (callers compare `expires_at`).
  - `Usage` (token counts) and `Lease` (holder, expires_at) models added; both `extra="allow"`.
  - `AskRaised.request` / `AskAnswered.response` are `dict` until P3 lands the typed ask classes — P3 swaps them in; noted here since the union freezes after P0/P1.
  - `SessionCreated` is in the union but nothing appends it yet — P2's job.

### Phase 1 — Provider protocol + OpenAI-compatible + FakeProvider · deps: none · ∥ P0 · **done**
- `providers/base.py`: `Provider`, `Embedder`, `SampleRequest` (`model`, system blocks with `cache` markers, messages, tool schemas, params), `ProviderEvent` union, `ModelLimits` via `limits(model)`.
- `providers/openai_compat.py`: streaming chat completions against OpenRouter; accumulates fragmented `ToolCallDelta`s into complete calls; surfaces `usage`; constructor takes `limits: dict[str, ModelLimits]` with a conservative fallback for unknown models.
- `providers/fake.py`: `FakeProvider([Sample(text=...), Sample(tool_calls=[...])])` plus cassette record/replay.
- **Verify:** a ~~recorded~~ **hand-authored (P1: no network/keys in CI; recorder exists for dev-time capture)** OpenRouter cassette with tool-call arguments split across 12 SSE chunks (landed: 15) replays into exactly one `ToolCallRequested` with byte-identical JSON args. `FakeProvider` scripted with two samples yields the exact expected `ProviderEvent` sequence.
- Checklist:
  - [x] Provider protocol + request/event types
  - [x] OpenAI-compatible streaming + tool-call accumulation
  - [x] FakeProvider
  - [x] Cassette record/replay
- Landed notes (P1):
  - `ProviderEvent` = `TextDelta | ReasoningDelta | ToolCallDelta` (live fragments) `| ToolCall` (complete; `args` is the raw accumulated JSON **string**) `| StreamEnd` (terminal; carries full accumulated text/reasoning/tool_calls/usage/finish_reason). Complete `ToolCall`s appear both standalone and inside `StreamEnd` — P2 must consume exactly one of the two or it will duplicate `ToolCallRequested`.
  - `ToolCall.args: str` vs `ToolCallRequested.args: dict` — the loop does the `json.loads`; P2 must decide what invalid JSON from the model produces (likely an `is_error` result, not a crash).
  - Usage fields are disjoint: `input_tokens` excludes `cache_read_tokens` (Anthropic-native semantics; `prompt_tokens - cached_tokens`).
  - `SystemBlock.cache` is modeled but `openai_compat` flattens system blocks to one string and drops the flag — the wire-level `cache_control` emission waits for the native Anthropic provider (Open Decisions).
  - `AssistantMessage.reasoning` is modeled but not serialized upstream by `openai_compat` (OpenAI-compat endpoints don't accept it; a native Anthropic provider will need it for thinking signatures).
  - Mid-stream `error` frames and zero-data-frame streams raise `ProviderError` — a partial stream is never a success-shaped `StreamEnd`. Reserved payload keys (`model`, `messages`, `stream`, `stream_options`, `tools`) cannot be overridden via `params`.
  - `ProviderError` also signals FakeProvider script/cassette exhaustion — P2's retry must not blindly retry it 3× in tests.
  - ~~Tool-call slots key by `index`, else by `id`, else continue the last slot~~ **Changed in the SDK swap.** Accumulation is the SDK's; a delta without `index` raises `ProviderError`. Missing ids are still synthesized as `call_{index}`.
  - `ProviderError` carries `status_code: int | None` — P2's `RetryConfig` keys retryability (429/5xx/timeout) off it, no message parsing.

### Phase 2 — The turn loop · deps: P0, P1 · **done**
First genuinely useful phase: non-interactive agents work end to end.
- `agent.py` (`Agent` + name derivation + transitive `subagents` name table), `tools.py` (`@tool`, schema inference from type hints + docstring, `ctx` stripping), `context.py` (system prompt + history + tool schemas), `loop.py`, `harness.py` (`create_session`, `run`, `replay`).
- Serial tool dispatch. Tool exceptions become `is_error` results the model can see. `max_steps` stop with orphan-call synthesis. Usage accumulation onto the header. `RetryConfig` backoff around the sample step.
- `Agent.output_schema` → synthetic `submit_output` tool ends the turn.
- `adapters/collect.py`.
- **Verify:** with `FakeProvider` scripted to call `search_metrics` then answer, `collect(harness.run(...))` yields `ToolCallRequested → ToolCallStarted → ToolCallCompleted → TextPart → TurnCompleted` in that order, and `replay()` from a fresh `Harness` over the same store reconstructs identical events. A tool that raises produces `is_error=True` and the loop continues to a second sample.
- Checklist:
  - [x] Agent class + name table
  - [x] `@tool` schema inference + ctx injection
  - [x] Context assembly
  - [x] Loop with serial dispatch + max_steps
  - [x] `output_schema` via submit_output
  - [x] collect adapter
- Landed notes (P2):
  - Emitted envelope: `Emitted(session_id, depth, seq: int | None, event)` in `loop.py`; deltas carry `seq=None`. `run()`/`replay()` are async generators — `SessionBusy`/`TurnIncomplete`/`SessionNotFound`/missing-model errors surface on first iteration, not at call time.
  - `ProviderError` gained `retryable: bool | None` (errors.py is not frozen); `openai_compat` sets it for `APIConnectionError` (timeouts have no status). Retryable = flag OR 429 OR ≥500; FakeProvider exhaustion (neither) is never retried.
  - Lease is re-acquired (same holder, TTL extended) at every sample boundary; a stolen lease raises mid-turn with nothing further appended and the header left untouched — the loser must not clobber the new holder's `status`.
  - `SampleStarted` is appended once per sample before the first attempt; a failed sample's log tail is exactly `SampleStarted → TurnFailed`. Partial-stream parts are buffered and discarded on retry.
  - `ToolCallStarted` only for calls that actually execute; invalid-JSON / unknown-tool / capped / post-submit orphans go `ToolCallRequested → ToolCallCompleted(is_error=True)` with no Started.
  - `submit_output` dispatches before the `max_steps` cap check (submitting costs no sample) and is only special when `output_schema` is set.
  - `ctx.emit` is queue-mediated — only the loop generator ever appends, keeping `expect_seq` single-threaded; `ToolProgress` lands between Started and Completed. The append happens at the tool's next await point, not inside `emit` itself.
  - Tool tasks are cancelled and awaited when the consumer abandons the stream (`aclosing` throughout) — abandonment leaves status `idle`, lease released, turn incomplete; the next `run()` raises `TurnIncomplete` (P3's `resume` entry point).
  - Follow-up (P9): a retried sample re-yields its live deltas with no reset marker on the stream — adapters double-render partial text; `Emitted` is `extra="allow"`, an attempt marker would do.
  - Schema validation at `Harness` construction rejects unannotated `ctx` params and required properties with no inferable JSON type; `max_steps < 1` also rejected there.

### Phase 3 — Control layer: ask/resume, permissions, cancel, hooks · deps: P2 · **done**
- `ask.py`: `Approval`/`Choice`/`FreeText` + response types.
- `ctx.ask()` → `AskRaised`, lease released, turn returns. `harness.resume(sid, ask_id=None, response=None)` replays and continues mid-batch; bare `resume(sid)` re-drives an abandoned turn.
- Unexecuted calls in a suspended batch get synthesized `is_error` results on resume.
- `permissions.py`: glob ruleset, longest-match, `default_permission`, auto-answer of permission asks.
- `harness.cancel()` via `CancelRequested` + boundary checks.
- `hooks.py`: the six hook points; `before_tool` allow/deny/transform.
- **Verify:** run a turn that asks; **discard the `Harness` object entirely**, build a new one over the same store, call `resume()` — the turn completes with the tool executed and the log contiguous. A `deny` rule produces an error tool result without ever invoking the tool. `cancel()` from a second `Harness` instance stops a running turn at the next boundary.
- Checklist:
  - [x] ask/resume across process boundary
  - [x] Synthesized results for unexecuted calls
  - [x] Permission ruleset + auto-answer
  - [x] Cancellation
  - [x] Hooks
- Landed notes (P3):
  - Two ask flavors. Permission asks (`Approval` with `extra={"permission": name}`) are framework-consumed: suspend before `ToolCallStarted`, `resume(sid, ask_id, ApprovalResponse)` executes on allow / synthesizes `"denied by user"` on deny; a non-`ApprovalResponse` answer is rejected at `resume()`. `ctx.ask` suspends mid-tool; on resume the tool **re-executes from the start** with recorded answers replayed by ask-index per `call_id` — pre-ask side effects (incl. `ToolProgress`) repeat. A permission ask consumes index 0 for its call.
  - A suspended batch stays half-answered in the log (legal — no sample until resume answers the rest); resume executes the remaining calls. Synthesis (`is_error=True`) covers deny, cancel (`"not executed: turn cancelled"`) and post-submit orphans.
  - Invalid-JSON calls are rejected **in the log** at parts time — `ToolCallCompleted(is_error=True)` lands in the same append batch right after `SampleCompleted` — so the rejection survives suspend/resume (it used to be in-memory only, which executed the tool with default args after a resume). Tool results are no longer strictly in `tool_calls` order; every id is still answered before the next sample.
  - Bare `resume(sid)` is the safe sweep entry point: it re-drives an abandoned turn but **replays** a suspended one (yields the original `AskRaised` at its seq, no tool re-run, log untouched). `resume(ask_id)` accepts only the current turn's single pending ask; stale ask_ids from terminated turns are rejected. `_pending_ask` assumes ≤1 unanswered ask per turn — P4's bubbled child asks live in the child's log so this holds; revisit if that changes.
  - `cancel(sid)` appends `CancelRequested` lease-less under `expect_seq` (5-attempt read-retry; `False` when no turn in flight). The loop absorbs foreign events at sample tops, before each tool call, and in `_append`'s single-shot `SeqConflict` retry — foreign **non-cancel** events raise instead of being absorbed. Once `submit_output` has stopped a batch, cancel no longer relabels the turn. The in-process asyncio-cancellation optimization was not built.
  - Hooks: `before_tool` runs after `ToolCallRequested` is persisted — a transform changes executed args only, the log keeps what the model asked (`name`/`call_id` changes are ignored); it re-fires on resume for the same call. `after_tool` fires only for tools that actually executed. `before_turn` only on `run()`. A raising hook aborts the turn like abandonment (`resume` re-enters). `on_event` sees live deltas too.
  - `TurnState.observe` (loop.py) is the single turn-state mutation point — `_append`, `_absorb` and `derive_turn_state` all feed it; P4 must teach it `ChildSessionSpawned` for re-entrant spawn.
  - Follow-up (P9): `AskAnswered.answered_by` is never populated — `resume()` exposes no parameter for it.

### Phase 4 — Sub-agents + fan-out · deps: P3 · ∥ P5, P6, P7, P8 · **done**
- Child sessions (`parent_id`, `depth`), `max_depth`, derived permissions.
- `subagents = [...]` → auto-generated tools; `ctx.spawn` (re-entrant, attaches via `ChildSessionSpawned`); `ctx.fan_out(max_concurrency)`.
- Live child-event forwarding onto the parent stream with `session_id` + `depth`; child asks bubble and suspend the ancestry.
- **Verify:** a parent whose sub-agent calls two tools shows those events on the parent stream at `depth=1`, and the parent's own log contains only the `task` call plus `ChildSessionSpawned`. `fan_out` of 3 where one raises returns 2 results + 1 error and the parent turn completes. ~~Depth 4 with `max_depth=3`~~ **Tested as depth 2 with `max_depth=1` (P4)** — same single code path (`parent.depth + 1 > max_depth`), far fewer scripted samples — raises before the child session is created. A child that asks suspends both sessions; `resume(child_sid, ask_id, response)` then `resume(parent_sid)` — from a fresh `Harness` — completes both turns without a duplicate child.
- Checklist:
  - [x] Child sessions + depth limit
  - [x] Derived permissions
  - [x] subagents-as-tools + re-entrant spawn
  - [x] fan_out with partial failure
  - [x] Event forwarding
  - [x] Bubbled ask + two-step resume
- Landed notes (P4):
  - Spawn/fan_out are queue-mediated through `_execute` like ask; only the loop appends `ChildSessionSpawned`. Attach-or-create is by spawn index per call_id (`TurnState.children` + a per-call cursor). `Spawner.resolve` — name-table lookup **and** the `max_depth` check, both pure functions of `(agent, parent depth)` — runs *before* the cursor is read, so a failing spawn never consumes an attachment slot; ordering it after created twin children on resume (caught in review). Store-level `create()` failures propagate out of the turn instead of filling a slot, for the same reason.
  - Child outcome mapping: `TurnCompleted.output` → result; else the final sample's text. `TurnFailed`, a cancelled child, and `max_steps` with no output are all **errors** (spawn raises → `is_error` result; the fan_out slot holds the exception). `SessionBusy` while driving a child propagates out of the parent turn, leaving it incomplete and resumable — it must never terminally answer the spawn call while the child sits suspended.
  - Bubbling: the parent suspends with `loop.suspended` = the child's pending ask, so its header shows `awaiting_input`/`pending_ask` — but the ask lives in the **child's** log, so `resume(parent, ask_id, …)` rejects it and bare `resume(parent)` takes the re-drive path (re-executes the spawn tool from the start; attach makes it idempotent; recursive over depth). fan_out lets runnable children finish before bubbling the lowest waiting task index. The parent's `pending_ask` can go stale between resume cycles (child moved on to a second ask) — self-heals on the next bare resume.
  - Derived permissions: verdict = strictest of the child's `decide(...)` and each ancestor's `decide(name, rules, None, default_permission)`. A rule-less ancestor still contributes the harness default, so `default_permission="ask"` overrides a child's explicit `allow` (never-widening, by design). The chain is rebuilt on resume by walking `parent_id`; a missing ancestor header fails closed (raises), and resuming a child requires its ancestor agents to be registered in the harness.
  - Subagent tool: param is `task: str`; description = `sub.__doc__`, **not** `inspect.getdoc` (which inherits `Agent`'s base docstring).
  - `on_event` fires once per forwarded child event — the parent's `run()` skips non-parent session_ids because the child's own loop already fired the hooks. Child metadata is a copy of the parent's (`deps_factory` scoping).
  - `SessionBusy(sid)` now requires the sid and carries a real message (errors.py is not frozen).
  - Follow-up (P9): cancelling a parent that is suspended on a child ask synthesizes the spawn call and ends the parent, but the child stays `awaiting_input` forever — no ancestor will re-drive it; a server sweep must notice orphaned children via `list(parent_id=...)`.

### Phase 5 — Skills · deps: P2 · ∥ P4, P6, P7, P8 · **done**
- `skills.py`: `Skills` protocol, `FileSystemSkills(root)` parsing `SKILL.md` YAML frontmatter (`name`, `description`) — the format already used across `kalki/skills/` and `.agents/skills/`.
- Index (name + description) injected into the system prompt; `skill(name)` tool returns the body plus a listing of files in the skill directory.
- `Agent.skills` filters which skills are indexed (`None` = all).
- **Verify:** an agent with 3 skills adds 3 lines to the system prompt and nothing else; the assembled request is under 200 tokens larger than the same agent with no skills. Calling `skill("cold-email")` places the full body in context and the listing includes `references/`.
- Checklist:
  - [x] Skills protocol
  - [x] FileSystemSkills + frontmatter parsing
  - [x] Index injection
  - [x] skill tool + file listing
- Landed notes (P5):
  - `Skills.index()` takes no filter argument (spec sketch struck): filtering lives in `Harness._skill_index`, which validates `Agent.skills` names against the index and fails loudly on typos. Consequence: a DB-backed `Skills` cannot push the filter down; the harness fetches the full catalogue and filters in memory.
  - Index block = one extra `SystemBlock` (preamble + one `- name: description` line per skill), assembled per sample, never persisted. Token delta for 3 skills measured at 155 (< 200); ~100 of that is the `skill` tool schema — its docstring has ~180 chars of headroom before the Verify bound flips.
  - The `skill` tool is injected per agent at construction (`permission="allow"`; collision with a user tool named `skill` raises). Filter check runs before `Skills.load` (no I/O for a forbidden name). Unknown/filtered names are `is_error` results; the turn continues.
  - Sub-agent skill calls under `default_permission="ask"` DO ask: a rule-less ancestor contributes the harness default via P4 derived permissions, and declared `allow` wins only at depth 0. Deliberate — one permission engine, no framework-tool exemption; parents grant silent loads with a `"skill": "allow"` rule. Both behaviors test-pinned.
  - `resume()` now validates (agent, model, chain, skill index, deps) BEFORE appending `AskAnswered` — a broken skills root no longer burns the ask and strands a stale `pending_ask`; the same `resume(ask_id)` retries cleanly after the root is fixed.
  - Parser is hand-rolled (no pyyaml): top-level single-line `key: value` between `---` fences, quotes stripped, nested keys ignored, CRLF + UTF-8 BOM tolerated, `utf-8` pinned. YAML block-scalar markers (`>`, `|`, variants) as name/description raise instead of leaking `>` into the prompt.
  - Fail-loud by design: a malformed `SKILL.md` anywhere under the root (or a missing root) fails every agent's run/resume at entry, even agents filtered away from it. A skills root is config; a bad file should not be silently skipped.
  - Follow-ups: empty catalogue still advertises the `skill` tool (tool injection is construction-time, index is run-time); `Agent.skills = ()` (tuple) is not recognized as opt-out (`[]` is); index order is skill-directory order, not name order.

### Phase 6 — Memory · deps: P2 · ∥ P4, P5, P7 · needs P8 for pgvector · **done**
- `memory.py`: `Memory` protocol. `BuiltinMemory` over the store backends: kind/title/body, tags, entities, `metadata` scoping, soft delete, supersede.
- Hybrid recall: keyword always; vector where the backend supports it (Postgres/pgvector), degrading to keyword-only elsewhere — **explicitly, with the degradation visible in `MemoryHit`**, not silently.
- `memory_recall` / `memory_write` tools.
- Embeddings via the `Embedder` protocol, best-effort: a failed embed never fails the write (kalki's `_embed_best_effort` pattern), with a `backfill` entrypoint to repair.
- **Verify:** write 3 memories, `recall` ranks the matching one first on ~~SQLite (keyword) and on Postgres (hybrid)~~ MemoryStore and FileSystemStore — **SQLite/Postgres land in P8**; `supersede` removes the old row from recall while `get` still returns it; a write with the embedding provider unreachable still commits and is repaired by `backfill`.
- Checklist:
  - [x] Memory protocol
  - [x] BuiltinMemory + schema
  - [x] Hybrid recall + honest degradation *(keyword half + the degradation surface; the vector half is P8/pgvector)*
  - [x] Tools
  - [x] Best-effort embed + backfill
- Landed notes (P6):
  - Rows live on the **concrete** stores, not in `BuiltinMemory`: MemoryStore/FileSystemStore grew `memory_put`/`memory_get`/`memory_all` (the frozen `Store` protocol is untouched); `BuiltinMemory` duck-checks for them and raises `TantraError` otherwise. P8's pgvector extends the same seam. FS rows are one JSON file each under `<root>/_memory/` (tmp + `os.replace`, last-write-wins, no lease); `_memory` is invisible to `list()` because it has no `session.json`.
  - Recall is keyword-only for now: score = matched lowercase-alnum query tokens / total query tokens, `score > 0` required, sorted score desc then `created_at` desc, every hit `mode="keyword"`. Empty query, `k <= 0`, or no match → `[]`, never "everything". `kind`/`tags`/`entity` filters compare casefolded; `metadata` subset-match stays exact. Vectors are stored and backfilled but nothing reads them until P8.
  - The built-in tools expose **no `metadata` parameter** and always write `metadata={}` — tenancy is app-scoped (decision above) and unreachable through the shipped tools; a multi-tenant app writes its own tools over `ctx.memory` (which cannot see session metadata — one `Memory` per tenant, or wrap it). `embedding` and `metadata` never appear in tool output.
  - Embedder results are coerced to `list[float]` — at write time inside the best-effort try (junk vectors become the embed-failed → `embedding=None` case), at backfill time before any `memory_put` (raises loudly, nothing persisted). Without this a junk vector bricked every subsequent read.
  - `supersede` of a deleted or already-superseded row raises (no forked chains). Put-order is new-then-old, so a failure superseding leaves the old row live and un-pointed; the reverse half-failure (new landed, old put failed) leaves two live rows — accepted, callers retry.
  - `MemoryWrite` is `extra="forbid"` (callers can't smuggle `deleted`/`created_at`/`id` into rows); `MemoryRecord` stays `extra="allow"` as the persisted model. Corrupt FS row files raise `CorruptLog` naming the path.
  - Import shape: `stores/fs.py` → `memory.py` → `tools.py`, so `tools.py` now imports `Store` (and `Memory`) under `TYPE_CHECKING` only — a runtime import of `tantra.stores.base` there re-creates a real cycle currently masked by import order.
  - Follow-ups: same-score+same-timestamp ordering differs per backend (MemoryStore insertion order, FS filename sort — no id tie-break); `metadata={"k": None}` matches rows lacking the key; `memory_all` is a full scan per recall (fine now, P8 pushes down).

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
- `adapters/cli.py` (terminal renderer, prompts on ask), `adapters/ws.py`, `adapters/sse.py`; a typed inbound command envelope (`run` / `resume` / `cancel`) shared by WS and SSE.
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
- `--auto` mode: everything `allow`, no prompts; a `before_tool` bash guard denies destructive commands (`rm -rf`, `git push --force`, …) — the hook-based unattended-guardrail pattern, demonstrated.
- `main.py` over `adapters/cli.py`, ~30 lines.
- **Verify:** on a scratch git repo, ask it to add a function to an existing file — it greps, reads, prompts before writing, and produces a correct edit. `^C` mid-turn then re-running with the same session id resumes rather than restarting. A task that fills the context window compacts and continues instead of erroring. In `--auto`, a prompt that leads the model to `rm -rf` gets a denial the model recovers from, with zero human input for the whole turn.
- Checklist:
  - [ ] Six tools
  - [ ] build + explore agents
  - [ ] Permission prompts
  - [ ] `--auto` + bash guard hook
  - [ ] Session resume
  - [ ] End-to-end run on a scratch repo

## Open Decisions

- **Event timestamps.** The event table has no per-event timestamp and P0 added none. Adding one later is a log-format change; decide before anything depends on replay ordering by time. (Raised in P0 review.)
- **Cross-process live tailing.** v1 ships `replay(from_seq)` (finite) only. A client reconnecting to a pod that isn't running the turn must poll. Resolving it needs a pub/sub — Postgres `LISTEN/NOTIFY` for the PG backend, or a `Bus` protocol with a Redis implementation. Decide once a real deployment hits it.
- **Artifact / `editable_object`.** Kept out of core; `observability_ui`'s dashboard editor needs it and will implement it application-side first. Promote to core only if a second consumer needs the same thing.
- **MCP client.** Deferred. Nothing in the current agents needs it. Revisit when an external tool server is actually wanted.
- **OTel instrumentation.** Deferred despite the obvious fit. Spans for turn/sample/tool with token and cost attributes are ~100 LOC; the question is whether they belong in core or in a `tantra-otel` package.
- **Native Anthropic provider.** The protocol accommodates cache markers and thinking blocks. Explicit prompt caching is the single largest cost lever on a loop that replays a growing history every sample — worth measuring before deciding.
- **ACP adapter.** Would give editor integration. Only worth it if someone wants tantra agents inside Zed.
- **Handoff pattern.** ~50 LOC (swap the agent on the session). Left out because nobody has asked for triage-style routing yet.
- **Argument-level permission rules.** Globs match tool *names* only; "bash: allow `ls`, deny `rm`" is inexpressible. agni's bash tool will want it. Candidate: optional per-tool `permission_key(args) -> str` routed through the same ruleset.
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
