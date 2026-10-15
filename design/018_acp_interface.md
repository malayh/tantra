# 018 — ACP v2–inspired interface and the `tantra` profile

**Status:** P1–P6 ✅ DONE. P7–P8 not started.

## Goal

Replace the custom `SessionSocket` wire protocol from 017 with an interface **inspired by [ACP](https://agentclientprotocol.com) v2**. We follow v2 wherever it fits. We diverge only where tantra's durable, multi-client, server-run design makes clearly more sense, and we extend v2 through a negotiated `tantra` profile. Today's Runtime core is left untouched: `tantra.acp` is the only way clients interact with it.

Deliverables:
- `tantra.acp` (Python)
- `tantra-acp-client` (TS)
- Sarathi adoption
- Removal of the 017 transport
- An upstream proposal for the ACP v2 discussion, backed by the working implementation

Small additive Runtime changes come in the **final** phase only.

**We own the whole implementation:**
- Python: tantra's own JSON-RPC core, transports and wire models.
- TS: tantra's own client.
- No third-party ACP SDKs. ACP v2 is where we start, not a conformance target. Diverging is fine wherever it suits tantra.

## Scope

**In**
- `tantra.acp`: the v2 agent, the profile, stdio, and a native ASGI WebSocket endpoint.
- Subagents with two-way messaging.
- `packages/tantra-acp-client`.
- Sarathi adoption.
- Deleting `SessionSocket`, `tantra-session-client`, the socket tests, the socket bench and the socket docs.
- The upstream proposal.
- The final-phase Runtime additions: a structured `InputQueued` sender, and `Runtime.watch_tree`.

**Out**
- ACP v1. A shim can come later via `AgentProtocolRouter`.
- Osuite adoption (follow-up spec).
- Streamable HTTP/SSE.
- Client-supplied MCP servers.
- Plans and slash commands.
- Per-child cancel (Open Decisions).
- Any Runtime or journal change before the final phase.
- A load bench.
- Posting upstream. The user does that.

## Context

**ACP v2 status (2026-10)**
- The draft was announced on 2026-07-20 and has reached `schema-v2.0.0-alpha.8` (2026-10-09). There is no stabilization date.
- Merged so far:
  - Prompt responds at insertion with `messageId`.
  - `state_update` reports `running` / `idle` / `requires_action`, with an `error` stop reason.
  - Agent-initiated foreground work.
  - `session/resume` with a `replayFrom` tagged union. Only `start` exists so far.
  - Upserts for messages and tool calls.
  - `_`-prefixed extension values.
- `subagent_update`, `session_message` and `session_message_chunk` exist only in `schema.unstable.json`.
- Implementers:
  - The Claude and Codex ACP adapters support v2 behind a flag.
  - Zed still starts on v1 in production.
- **SDKs are not used.** The official SDKs disagree on the alpha: the TS SDK 1.8.0 is at alpha.8, while the Python SDK `1.0.0rc3` is at alpha.5, which lacks the subagent, session-message and error-stop-reason shapes. We vendor the alpha.8 schema only as a test reference.

**Upstream threads this work informs**

| Thread | Gap or open question | tantra's contribution |
|---|---|---|
| A#954 / A#2175, v2 prompt RFD | Its FAQ says a returned `messageId` "is an acknowledgment, not a client-supplied idempotency key"; a correlation token was deferred | `commandId` |
| A#2208, durable resumable transport (open) | Cursor kept in memory only | Durable per-session cursor |
| A#2394 / A#2286 (open) | Resume-from-message, and state after resume | Our resume rules |
| A#533, multi-client attach (open) | Controller/observer roles removed after pushback | Fenced writer |
| A#1992, subagents (merged 09-30) | Selective subscription "worth revisiting"; stranded requests and catch-up unsolved | Both solved by our design |
| A#1261, `session/inject` (open) | No way to see queued input | `_tantra_queue_update` |

No published implementation does fencing, durable cursor replay, or lazy child delivery.

**Surprises**
- `SessionSocket`, `tantra-session-client` and 017 P0 all landed after release 1.4.0 (`b6778a0`), so deleting them breaks no release. The 017 P0 Runtime additions stay: `submitted_by`, `lookup_ask`, and migration 12.
- Synthetic inputs can only be recognized by the prefixes the runtime formats until P8:
  - `[agent <uuid>] …` (`runtime.py:1716`)
  - `[agent <uuid> finished] …` (`runtime.py:1796`)
  - `[agent <uuid> turn ended] …` (`runtime.py:215`)
  - a child's first input (`runtime.py:1698`)
- The root journal holds no child state other than lifecycle inputs. Until P8, the mirror comes from `runtime.tree_status`, polled or triggered by coordinator notices.
- **Per-child cancel does not exist.** `Connection.cancel` cancels the whole tree (`runtime.py:2539`).
- `finish` is permanent (`runtime.py:1758`). ACP subagent associations have no terminal state.
- Writer loss is only detectable by iterating the writable `Connection`, which raises `WriterReplaced`. `acp/agent.py` catches it the same way.
- `session/cancel` is a notification, so it can never return an error.
- Children cannot ask humans (`loop.py:576`). A child that is not being observed therefore never strands a request.
- A cursor is only stable for one fixed projection: the same client capabilities and the same profile.
- Without a coordinator, `patch_header` (title or model) gives no local signal.
- **P8 risk:** retries compare whole `InputQueued` events (`runtime.py:1727`, `:1817`, `:2458`). New fields must not turn a legacy event plus its retry into `InvalidCommandReuse`.
- Extension method names must start with `_` on the wire. Assert the exact method names in tests.
- In stdio mode, stdout must carry only JSON-RPC.

## Decisions

**Principles**
- **ACP v2–inspired** (user decision). Use v2 shapes and semantics by default. Diverge only where tantra's design is clearly better, and record each divergence and its reason in the tables below. A client that doesn't negotiate the profile gets v2 behavior plus those divergences.
- **v2 only, `protocolVersion: 2`** (user decision).
- **Runtime core untouched until P8** (user decision). P1–P7 use only public Runtime surface:
  - `create`, `delete`, `events`, `status`, `tree_status`, `lookup_ask`, `connect`
  - `runtime.store`, `runtime.coordinator`, `runtime.provider`, `runtime.tools`

**Dependencies and transport**
- **Own wire layer, no SDKs** (user decision). `tantra/acp/` contains:
  - `jsonrpc.py`: JSON-RPC 2.0 with concurrent dispatch, `$/cancel_request` and batches.
  - `transport.py`: ndjson over stdio, and in-memory pairs.
  - `wire.py`: pydantic models.

  This needs no extra dependency beyond pydantic. Why: the SDKs are pre-release, disagree on the alpha, and would need workarounds for the shapes we use, and we diverge anyway.
- **Reference schema:** alpha.8 `schema.json` and `schema.unstable.json`, vendored with the Apache-2.0 notice under `packages/tantra/tests/fixtures/acp_v2_alpha8/`. A stdlib-only structural test checks every emitted message against it. Allowlisted divergences document where we deviate; they don't forbid deviating.
- **Own TS client** `tantra-acp-client` (user decision). It has no runtime dependencies, uses native WebSocket, and implements the profile. Why: Sarathi and Osuite both need cursor, `commandId` and writer handling.
- **The client ships the transcript reducer** (user decision, P5). It is pure and framework-free; apps keep only rendering and their own hooks. ~~Transcript reducers stay with each app~~: every app would rewrite the same generic reducer.
- **WebSocket endpoint:** a native ASGI wrapper around our `Transport`. It authenticates and checks Origin before `accept`.
- **IDs:**
  - Session IDs are canonical UUID strings.
  - Namespace: `_meta.tantra`, methods `_tantra/…`, custom variants `_tantra_…`.

**Writer and asks**
- **Fenced single writer** (user decision).
  - The writer holds a long-lived `runtime.connect(root, writable=True)` and tails through it.
  - When fenced, it downgrades to a `runtime.events` tail from its last applied seq.
- **Clients without the profile** (user decision):
  - The first mutation claims the writer implicitly.
  - Once fenced, the next mutation fails once with `writer_required`, and the one after that reclaims.
  - A reader's `session/cancel` claims, because a notification can't return an error.
- **Asks go only to the current writer.** When the writer changes, pending asks are re-issued to the new writer and the old writer's requests are cancelled with `$/cancel_request`.

**Subagents**
- Every client gets `subagent_update` plus two-way `session_message`s for `spawn` / `send` / `finish`. v2 has no client `subagents` capability, so there is no tool-call fallback.
- `finish` maps to `idle` with `_tantra_finished`, plus `_meta.tantra.finished: true`.
- `capabilities` stays empty, because there is no per-child cancel.
- **Child state, P1:** derived from the child-journal tails, since every descendant transcript is pushed.
- **Child state, P2 (profile clients):** every descendant is mirrored from `runtime.tree_status` as `subagent_update {sessionId, title, state}` on its parent's session id, BFS order so parents come first. Snapshot after resume, then on change; live-only. Refresh triggers: coordinator `journal`/`header`/`activity` notices for non-root actors (debounced 500 ms), a root `ChildCreated`, and without a coordinator a 1 s poll while the root or any descendant is `queued`/`running`/`awaiting_input`. One `coordinator.watch` per session feeds both this mirror and the header watch. Non-profile clients keep the P1 tail-derived mirror.
- **Pending asks** at resume and at claim come from `header.pending_ask` plus `runtime.lookup_ask`.

**Other standard updates**
- `usage_update`, `compaction_update` / `compaction_summary_chunk`, `session_info_update` (title) and `config_option_update` (model).
- Auth happens in the handshake via `bind`; stdio trusts the local process.
- `authMethods: []`. `mcpServers` is ignored.

## Divergences from v2

These apply to every client.

| v2 rule | tantra | Why |
|---|---|---|
| `session/prompt` responds only after insertion; queueing is not insertion | Respond on **durable acceptance** (`InputQueued` committed) with `{messageId}`. The `user_message` is still emitted at insertion (`TurnStarted`). | In tantra, acceptance is a durable commitment. Holding a request for minutes behind a queue makes lost responses likely and gives no benefit. Clients already MUST tolerate updates arriving before or after the response. |
| `session/close` MUST cancel ongoing work | `close` **detaches this connection only**. Cancelling is always explicit. | Sessions are durable and shared. One viewer leaving must not stop work that others started or are watching. |
| Clients MUST NOT load or resume a child; child transcripts are pushed automatically | Profile clients **may `session/resume` a child, read-only**, and `session/close` it, so transcripts are delivered lazily. Clients without the profile get the standard automatic push. | Pushing every transcript costs proportional to the tree size. Children can't ask, so nothing strands, and cursors give catch-up. This answers the subagents RFD's open question. |

## Application interface

```python
from tantra.acp import AcpAccess, AcpAuth, AcpServer

server = AcpServer(
    runtime,
    agent=Sarathi,
    scope=scope,
    policy=policy,
    encode_prompt=encode_prompt,
    decode_input=decode_input,
    view=None,
    models=settings.models,
    name="sarathi",
)
await server.serve_stdio(AcpAuth(principal="local"))
endpoint = server.websocket(bind=bind, allowed_origins={"https://app.example"})
```

| Hook | Signature | Contract |
|---|---|---|
| `scope` | `async (auth, meta) -> Mapping[str, str]` | Required. Stamped on `session/new` together with `acp_cwd`; used as the `session/list` filter. Raise `PermissionError` to deny. |
| `policy` | `async (AcpAccess) -> bool` | Required, and must return exactly `True`. Operations: `resume`, `prompt`, `cancel`, `claim_writer`, `answer`, `set_model`, `delete`. |
| `encode_prompt` | `async (auth, blocks, meta) -> str` | Turns the `ContentBlock`s into Runtime input. `ValueError` → `-32602`. Default: text only. |
| `decode_input` | `(str) -> list[ContentBlock]` | The inverse, used for `user_message`. Default: `[text]`. |
| `view` | `async (LoggedEvent) -> LoggedEvent \| None` | Optional redaction before projection. |
| `models` | `Sequence[str] \| None` | Enables the `model` config option. |

- `AcpAuth(principal, submitted_by=None, expires_at=None)`.
- `AcpAccess(operation, auth, root, actor, meta, ask, response)`:
  - `root` and `actor` are `SessionHeader`s loaded on the server.
  - `ask` is a `LocatedAsk`.
  - `answer` is checked twice: before a request is issued (`response=None`), then again with the mapped response.
- `websocket(*, bind, allowed_origins, allow_missing_origin=False) -> AcpWebSocket`, an ASGI callable.
  - `bind`: `async (scope: Mapping[str, Any]) -> AcpAuth`, called with the raw ASGI scope after the Origin check. `PermissionError`, a non-`AcpAuth` result, or an `expires_at` at or before now denies.
  - `AcpWebSocket.aclose()` closes every live socket with `1012`, waits for them to unwind, and refuses later handshakes. Call it before `runtime.aclose()`.
- Limits are fixed:
  - 8 client-opened sessions per connection. P1 counts roots only; P2 also counts lazily resumed children.
  - 1 MiB inbound frame. On stdio, an oversized frame ends the connection.
  - 10 s per outbound send, plus closing the socket at `expires_at`. Both are enforced by the P3 WebSocket transport.
- `Transport` carries text frames: `send(str)` and `receive() -> str | None`. The `Peer` owns parsing and `-32700`.

## Wire surface

`initialize` response:

```json
{"protocolVersion": 2,
 "capabilities": {"session": {"prompt": {}, "mcp": {}, "delete": {}},
   "_meta": {"tantra": {"version": 1}}},
 "info": {"name": "<name>", "version": "<tantra-harness version>"},
 "authMethods": []}
```

| Method | Behavior |
|---|---|
| `session/new` | Calls `scope`, then `runtime.create(agent, session_id=uuid4(), metadata={**scope, "acp_cwd": cwd})`, then starts the tail. Returns `{sessionId, configOptions?}`, followed by `session_info_update` and `state_update`. |
| `session/list` | Roots of `agent` matching `scope` (and `acp_cwd == cwd` if a cwd is given), newest created first, in `store.list` order. Pages of 50 with `cursor` and `nextCursor`. An unknown or child cursor → `-32602`. |
| `session/resume` | Checks `policy(resume)`, replays according to `replayFrom`, and returns `{configOptions?}` (roots only). Then sends a `state_update` snapshot, any pending-ask requests (if this connection is the writer), then the live tail. Resuming an already-open session replaces it. A child is allowed only for profile clients, and read-only. |
| `session/prompt` | Roots only, and the session must be open. Checks `policy(prompt)`, then `encode_prompt`, then claims the writer: implicitly, or by returning `writer_required` to a profile client that isn't writer. Then `writer.send(cid)`. Encoding comes before the claim, so an invalid prompt never fences the current writer. Returns **`{messageId: cid}` once the input is durably accepted**. |
| `session/cancel` | Checks `policy(cancel)`, claims implicitly if needed, then `writer.cancel()` for the whole tree. The tail then reports `idle` with `cancelled`. |
| `session/close` | Detaches this connection's tail and cancels its outstanding requests. Never cancels work. Returns `{}`. |
| `session/delete` | Checks `policy(delete)`, then `runtime.delete(root, allow_active=True)`. Tails stop everywhere. |
| `session/set_config_option` | `model` only. Checks `policy(set_model)`, then `patch_header(model=)`. `ModelChangeBusy` → `-32603` with `data.tantra.code: "model_busy"`. |

**Errors**

| Condition | Code | `data.tantra.code` |
|---|---|---|
| Session unknown, unauthorized, or not open | `-32002` | — |
| Invalid input, or a mutation on a child | `-32602` | — |
| Mutation from a connection that isn't writer | `-32603` | `writer_required` |
| Still failing after 3 retries with the same command ID | `-32603` | `unavailable` |
| Profile `commandId` reused with different content (`InvalidCommandReuse`) | `-32602` | `command_conflict` |

Error messages never carry exception details.

## Projection

`tantra/acp/projection.py::Projector` is pure. Per actor stream it tracks the current turn, the reasoning blocks of each sample, open tool calls, and pending asks. It is configured from the client's capabilities and profile.

Profile clients get `_meta.tantra.cursor = "<seq>.<index>"` on every output derived from the journal: `seq` is the journal seq, `index` the position in `Projector.feed()` output order (internal signals take indices, so gaps are normal). Live-only outputs get no cursor: requests, notices, writer state, the queue, the resume `state_update` snapshot and the profile subagent mirror.

**Profile replay is uncoalesced.** It is exactly the live projection filtered by cursor, including every `usage_update` at its position. Why: P1's replay coalescing shifts output indices, so a coalesced stream has no stable `<seq>.<index>`. Plain v2 clients keep coalesced replay.

**Turns and messages**

| Tantra event | v2 output |
|---|---|
| `TurnStarted` with user input | `user_message {messageId: cmd, content: decode_input}`, then `state_update running` |
| `TurnStarted` with synthetic input | An incoming `session_message` for `[agent X] …`, `[agent X finished] …`, or a child's first input. Nothing for `turn ended`. Then `state_update running`, as agent-initiated work. |
| `TextDelta` / `ReasoningDelta` | `agent_message_chunk` / `agent_thought_chunk`, with `messageId` = sample |
| `TextPart` | `agent_message {content: [text]}`. The upsert replaces the deltas. |
| `ReasoningPart` | `agent_thought {content: [blocks so far]}` |

**Tools, children and asks**

| Tantra event | v2 output |
|---|---|
| `ToolCallRequested` | `tool_call_update {toolCallId, name, title: name, kind: "other", status: "pending", rawInput}`. `spawn`, `send` and `finish` are not shown as tool calls. Instead they produce an outgoing `session_message` (`messageId` = `uuid5(sender, f"{sample_id}:{call_id}")`): `spawn` at `ChildCreated`, `send` and `finish` when they complete successfully. If one fails, it is emitted as a failed `tool_call_update`. |
| `ToolCallStarted` | `tool_call_update {status: "in_progress"}` |
| `ToolProgress` | `tool_call_content_chunk` |
| `ToolCallCompleted` | `tool_call_update {status: completed\|failed, rawOutput}`, plus a result content chunk |
| `ChildCreated` | `subagent_update {sessionId, title: name or agent}`, sent before any traffic from the child |
| `AskRaised` | `state_update requires_action` and `_meta.tantra.ask {state: "pending"}`. To the writer: `request_permission` (Approval; `subject: {type: "tool_call"}`; options `allow` / `deny`) or `elicitation/create` (Choice / FreeText, only if the client supports `elicitation.form`). |
| `AskAnswered` | `_meta.tantra.ask {state: "answered"}`, then `state_update running`. Cancels any outstanding requests for that ask. |

**Usage, compaction and turn ends**

| Tantra event | v2 output |
|---|---|
| `SampleCompleted` | `usage_update {used, size: provider.limits(model).context_window}` |
| `CompactionApplied` | `compaction_update {status: "completed"}` and `compaction_summary_chunk` |
| `TurnCompleted` | `state_update idle` with `end_turn`, or `max_turn_requests` when it hit `max_steps` |
| `TurnCancelled` | `state_update idle` with `cancelled` |
| `TurnFailed` | `state_update idle` with `error {code: -32603, message}` |
| `TurnInterrupted` | `state_update idle` with `_tantra_interrupted` |
| `ToolCallDelta`, `SampleStarted`, `CancellationRequested`, `AgentFinished`, `SessionCreated`, `InputQueued` | Nothing (`InputQueued` only feeds the queue) |

Child status is mirrored as the parent's `subagent_update.state`. In P1 it is derived from the child tail. For profile clients (P2) it comes from `tree_status` for every descendant.
- `queued` or `running` → `running`
- `idle` → `idle` with the last stop reason
- `finished` → `idle` with `_tantra_finished`
- `failed` → `idle` with `error`
- `cancelled` → `idle` with `cancelled`
- `interrupted` → `idle` with `_tantra_interrupted`

The P2 `tree_status` mirror refreshes on coordinator notices for child actors, debounced to at most one refresh per 500 ms, and on a root `ChildCreated`. With no coordinator, it polls once a second while the root or any descendant is queued, running or awaiting input.

## `tantra` profile

Negotiated with `_meta.tantra = {"version": 1}` in both directions: the client sends it in `initialize.params.capabilities._meta`, the agent answers in `capabilities._meta`.
- Profile-only output goes only to profile clients: `_tantra_writer`, `_tantra_queue_update`, `_tantra_notice`, `_meta.tantra.cursor`. The `_tantra_finished` / `_tantra_interrupted` stop reasons and `_meta.tantra.ask` / `finished` are v2-legal extension values and go to everyone (frozen P1 projection).
- From non-profile clients, profile inputs are ignored (`commandId`, `mode`) and profile-only requests are rejected: `_tantra_after` → `-32602`, `_tantra/session/claim_writer` → `-32601`.

| Feature | Wire | Semantics |
|---|---|---|
| Command IDs | `_meta.tantra.commandId` (canonical UUID) on `session/prompt` and `session/cancel` | Becomes the Runtime `command_id`. A retry is a duplicate and gets the same `messageId`. Reusing it with different content → `-32602`, `command_conflict`; on cancel, a `command_conflict` notice. A non-UUID → `-32602`. |
| Cursor replay | `replayFrom: {"type": "_tantra_after", "cursor": "<seq>.<index>"}` | Exclusive, like `Last-Event-ID`. A cursor beyond the end of the journal → `-32602`. If capabilities changed, replay from `start`. |
| Writer mode | `_meta.tantra.mode` on `session/new` / `session/resume` | Defaults to `write` on new and `read` on resume. `write` claims before responding, so a denied claim fails the request with `-32002`; pending-ask requests follow the response. Profile clients never claim implicitly: a non-writer's prompt → `writer_required` every time. |
| Claim | `_tantra/session/claim_writer {sessionId}` | Checks `policy(claim_writer)`, then fences the current writer. Returns `{}`. |
| Writer state | `_tantra_writer {writer, reason: "claimed" \| "replaced" \| "lost"}` session update | Sent only on this connection, never replayed. `claimed`: this connection became writer. `replaced`: fenced by another writer. `lost`: the writer connection dropped (`LeaseLost` / `CoordinatorUnavailable`); reclaim is explicit. |
| Notices | `_tantra_notice {level: "warning", code, message}` session update | Live-only. `writer_required` for a non-writer's cancel (a notification can't error); `command_conflict` for a cancel reusing a command ID; `unavailable` for a profile cancel still failing after retries. An invalid `commandId` on cancel is only logged. |
| Queue | `_tantra_queue_update {entries: [{commandId, content, submittedBy}]}` session update | Full list of accepted but not yet inserted root user inputs. Sent on every change and after resume. |
| Re-resume | `session/resume` on an already-open root | Replaces it: its open children close and the writer follows the new `mode` (`write` sends `claimed` again, `read` releases silently). A rejected `replayFrom` or a denied `write` claim leaves the open session untouched. |
| Lazy children | `session/resume` / `session/close` on a child ID | Read-only (`mode: write` → `-32602`). The child's root must be open on the same connection (else `-32002`) and the child must be in that tree. Counts toward the 8-session limit; closing the root closes its children. Policy `resume` gets `root` = root header, `actor` = child header. The child transcript gets its own cursor. Profile clients never receive child transcripts automatically; announcements and state always flow through the mirror. |

## Implementation phases

### Phase 1 — v2 agent over stdio · deps: none · ✅ DONE

**Deliverables**
- Vendor the alpha.8 reference schema with its notice into `tests/fixtures/acp_v2_alpha8/`. No new dependencies.
- New modules in `tantra/acp/`: `__init__.py`, `jsonrpc.py`, `transport.py`, `wire.py`, `projection.py`, `agent.py`, `server.py`. This phase covers:
  - The standard surface and the three divergences.
  - Reader and writer tails, implicit claims, and informed takeover.
  - Asks routed to the writer.
  - The child push and the tail-derived subagent mirror.
  - The title/model header watch.
  - `serve_stdio`, with `sys.stdout` redirected to stderr while it serves.
- Non-core fix: `extratools/shell.py` starts its subprocess with `stdin=DEVNULL`, so child shells can't read the stdio server's input.
- `docs/reference/acp.md`, plus its mkdocs nav entry.

**Verify**
- `tests/test_acp_projection.py`, using fixture journals:
  - every row of the projection tables
  - upserts replacing deltas
  - coalesced replay reduces to the same state as live
  - synthetic prefixes round-tripped through the runtime's own formatting
  - `view`
- `tests/test_acp_jsonrpc.py`:
  - concurrent dispatch
  - `$/cancel_request` in both directions
  - batches
  - every error code
  - no exception text leaks to the client
- `tests/test_acp_schema.py`: every emitted message is structurally valid against the vendored alpha.8 `$defs`, apart from allowlisted divergences.
- `tests/test_acp_agent.py`, using a raw recording client built on our `Peer` over `memory_pair`, with `MemoryStore` and `FakeProvider`:
  - **Lifecycle:**
    - `initialize` returns v2.
    - new, then list, then resume from `start`: the result matches a live capture.
    - A queued prompt responds on acceptance, and its `user_message` arrives at `TurnStarted`.
    - The `state_update` sequence and every stop reason.
    - `close` leaves work running; `delete` stops tails.
  - **Writer and asks:**
    - Two non-profile clients: one fences the other, which gets `writer_required` once and then reclaims.
    - Requests reach only the writer, and are re-issued after a takeover.
    - Elicitation, with and without client capability.
  - **Subagents:**
    - `subagent_update` and two-way `session_message`.
    - An idle child woken by `send`.
    - `_tantra_finished`.
    - A non-profile client's attempt to resume a child is rejected.
  - **Other updates:** `usage_update` and `compaction_update`.
  - **Disconnect:** after a disconnect, the turn still completes.
- `tests/test_postgres_acp.py`, two Runtimes, owned Compose PostgreSQL, zero skips:
  - fencing across runtimes
  - a prompt sent on A is observed on B
  - the tail-derived child-state mirror
  - title and model updates
- A stdio subprocess run: initialize, new, prompt, `idle end_turn`. Every stdout line is JSON-RPC.

**Checklist**
- [x] Vendored schema, JSON-RPC core, transports, wire models
- [x] Projector + fixtures
- [x] Lifecycle, divergences, writer, asks, subagents
- [x] PostgreSQL + stdio tests, docs

**Verified:**
- 1144 package tests passed with zero skips against an owned Compose PostgreSQL (`fsync=on`, `synchronous_commit=on`). The four PostgreSQL ACP tests also passed 5 of 5 repeat runs.
- 157 stress tests passed.
- `ruff check` and `ruff format --check` clean; `mkdocs build --strict` passes.
- The Runtime-boundary diff is empty.
- One independent review found 3 major and 7 minor defects. All are fixed, each with a regression test that fails without its fix.
- The database and its volume were removed afterwards.

**As built** (these details would surprise the next reader):
- **Entity IDs:**
  - `toolCallId = uuid5(actor, f"{sample_id}:{call_id}")`. The projector maps each `call_id` to the latest `ToolCallRequested`, because providers reuse ids like `call_0` (`openai_compat.py:235`).
  - Outgoing session-message ids use the same key.
- **Resume and claim state:**
  - Every tail reads from seq 0. With `replayFrom` omitted, the agent discards outputs up to the watermark to warm the projector.
  - The resume snapshot and the pending asks at claim come from projector state, with `lookup_ask` supplying the descriptor for policy.
  - Replay coalesces deltas into message upserts and sends only the latest `usage_update`. Child state changes inside replayed history are not mirrored.
- **Prefixes** count as synthetic only when the UUID is the actor's parent or a known child. Otherwise the input is a normal user message.
- **Asks:** `requires_action` holds while any ask is pending.
- **Failures:**
  - `TurnFailed.error` is passed through verbatim. Use `view` to redact provider details.
  - Reader tails retry `CoordinatorUnavailable` / `LeaseLost` with backoff. Writer tails fall back to reading.
  - Any other stream failure, or the runtime closing, closes the transport.
- **JSON-RPC:** a request id is forgotten as soon as its handler returns, and `Reply.after` follow-ups run in their own task. A late `$/cancel_request` therefore can't drop a response.
- **stdio:** serves through private dups of fds 0 and 1. While serving, fd 0 is `/dev/null`, and fd 1 plus `sys.stdout` go to stderr. Descriptors and `O_NONBLOCK` flags are restored on close and when an open fails.

**Follow-ups:**
- A plain v2 client that resumes a root with a pending ask gets no request until its first mutation claims the writer. P2's explicit `mode: write` covers profile clients.
- **Runtime issue (for P8 or its own spec):** `_internal_id` (`runtime.py:1611`) keys on `turn_id:call_id`. A provider that repeats `call_0` across samples within one turn makes spawn, send and finish alias each other.

### Phase 2 — `tantra` profile · deps: P1 · ✅ DONE

**Deliverables**
- In `agent.py` and `projection.py`:
  - profile negotiation
  - `commandId`
  - cursors and `_tantra_after`
  - writer mode and `claim_writer`
  - `_tantra_writer`
  - `_tantra_queue_update`
  - lazy child resume and close
- `tests/fixtures/acp_profile/*.json`, one fixture per profile message. P4 and P7 reuse them.
- A profile section in `docs/reference/acp.md`.

**Verify**
- `tests/test_acp_profile.py`:
  - **Command IDs:**
    - A duplicate `commandId` gives one turn and the same `messageId`.
    - Conflicting reuse → `command_conflict`.
    - A lost response is recovered by resuming with `_tantra_after`.
  - **Cursor resume:**
    - Cut the stream at every output index, then resume with `_tantra_after`. The reduced result must equal the uninterrupted stream, for both the root and a child.
    - A cursor beyond the end is rejected.
  - **Writer:**
    - A `read` connection never receives requests.
    - `claim_writer` sends `_tantra_writer replaced` to the old writer.
    - Pending asks move to the new writer, and `$/cancel_request` goes to the old one.
  - **Queue:** the queue update matches the Runtime's queue.
  - **Lazy children:**
    - An unresumed child sends no transcript, but still sends `subagent_update` (grandchildren on the child's id).
    - Closing a child leaves it running.
  - **Isolation:** a non-profile client never receives profile-only output (`_tantra_writer`, `_tantra_queue_update`, `_tantra_notice`, cursors).
- PostgreSQL:
  - a claim across runtimes
  - a cursor resume served by the other runtime
  - the mirror refreshing from coordinator notices for a child on the other runtime
- `just stress`, zero PostgreSQL skips.

**Checklist**
- [x] Command IDs + cursors
- [x] Writer mode, claim, queue
- [x] Lazy children
- [x] Fixtures + docs

**Verified:**
- 1177 package tests passed with zero skips against an owned Compose PostgreSQL (`fsync=on`). `test_acp_profile.py` (30) plus `test_postgres_acp.py` (7) passed 5 of 5 repeat runs.
- 157 stress tests passed.
- `ruff check` and `ruff format --check` clean; `mkdocs build --strict` passes; the Runtime-boundary diff is empty.
- Two review rounds: 1 major and 10 minor defects, then 4 minor. All fixed, each with a regression test that fails when its fix is reverted.

**As built** (surprising details):
- **Mirror and `view`:** the profile mirror passes synthetic `ChildCreated` (name) and `TurnFailed` (error) events through `view`. A hidden descendant drops its whole subtree; a hidden error becomes `"Subagent failed"`. `view` reruns only when a descendant's raw `(name, state, error)` changes.
- **Mirror triggers:** besides notices, the poll and root `ChildCreated`, any live `TurnState` marks the mirror stale. Without that, the no-coordinator poll can't restart once the tree goes idle. If the coordinator watch dies, the mirror falls back to the poll. A failing refresh closes the transport, as tail failures do.
- **Profile clients still get the journal-derived `subagent_update {sessionId, title}`** at `ChildCreated`, with a cursor. The mirror adds `state`. `awaiting_input` maps to `requires_action`, which children never reach.
- **Writer announcements:** `replaced` / `lost` go only to a connection that was told `claimed`. The update is spawned before the writer's exit is awaited, so a cancelled request can't skip it. `claim_writer` replies first, then sends `claimed`, then the pending asks.
- **Plain v2 changes, both intentional:**
  - A writer that hits `LeaseLost` / `CoordinatorUnavailable` is released at once; backoff applies only to reader retries.
  - Pending asks found at a claim before go-live are issued after the `state_update` snapshot.
- **A denied `mode: write` on `session/new`** deletes the root it just created.
- **Profile cancel** retries like prompt; plain v2 cancel keeps P1's single attempt.

**Follow-up:** when `view` hides a child's `ChildCreated`, that child's lifecycle inputs to its parent no longer count as synthetic. They project as `user_message`, and the raw `[agent <id> …]` text reaches the client. Closing this means the projector must know which children `view` hid; this is P1 projection, so it needs its own decision.

### Phase 3 — WebSocket transport · deps: P1 · ✅ DONE

**Deliverables**
- `tantra/acp/websocket.py`: `server.websocket(...)` returns `AcpWebSocket`, a native ASGI callable wrapping a `WebSocketTransport`.
  - The Origin check runs first, then `bind`, then `accept`. A bad Origin never reaches app code.
    - Exactly one `Origin` header, matched exactly against `allowed_origins`.
    - A missing Origin needs `allow_missing_origin=True`. Node's native WebSocket sends none.
    - A bare `str` or `"*"` is rejected at construction.
  - Every denial sends `websocket.close` before `accept`. uvicorn turns that into HTTP 403; returning without a close would give HTTP 500.
  - No subprotocol is selected, so clients must not offer one. Why: version negotiation happens in `initialize`, and Chrome fails the handshake when it offers subprotocols and none is selected.
  - Text frames only; binary frames are ignored.
  - Close codes (frozen after P3):

    | Code | When |
    |---|---|
    | `1008` | Before accept: Origin denied or missing, or `bind` denied. After accept: `expires_at` reached. |
    | `1009` | Inbound text frame over 1 MiB of UTF-8 (`MAX_FRAME`). |
    | `1011` | `bind` failed unexpectedly, or the agent closed the transport: a stream or mirror failure, or the runtime stopping. |
    | `1012` | `AcpWebSocket.aclose()`. uvicorn also sends `1012` on its own shutdown, before the lifespan shutdown. |
    | `1013` | An outbound send didn't finish within 10 s. |
  - **The endpoint sends the close frame after `connect()` returns.** `close(code)` only records the first code and wakes `receive()`. Why: `AcpConnection.run` cancels its spawned tasks during teardown, the agent's `transport.close()` among them, so a frame sent from inside `close()` would be lost and the client would see `1006`.
- Docs: how to mount it, auth and Origin, close codes, and shutdown order.

**Verify**
- ASGI fixtures:
  - allowed, wrong, duplicate and missing Origin; `bind` is never called on an Origin denial
  - `bind` denial before `accept`, including an expired `expires_at`
  - close codes `1008`, `1009`, `1011`, `1012` and `1013`; the 1 MiB check counts bytes, not characters
  - binary frames are ignored
  - a disconnect leaves the turn running
- uvicorn with a `websockets` client on our `Peer`, end to end. Both packages are already in `uv.lock` via Sarathi.
- `just lint`, `just test`, `just stress` (zero PostgreSQL skips), `mkdocs build --strict`.

**Checklist**
- [x] Transport, Origin, bind, limits
- [x] Tests + docs

**Verified:**
- 1205 package tests passed with zero skips against an owned Compose PostgreSQL (`fsync=on`). `test_acp_websocket.py` (28) passed 5 of 5 repeat runs.
- 157 stress tests passed.
- `ruff check` and `ruff format --check` clean; `mkdocs build --strict` passes.
- The Runtime-boundary diff is empty; `agent.py`, `jsonrpc.py` and the dependencies are unchanged.
- One review: 1 major and 2 minor defects plus doc nits, all fixed, each with a test that fails when its fix is reverted. The major defect was reproduced on real uvicorn.

**As built** (surprising details):
- **uvicorn can close a socket by itself** (keepalive timeout, protocol error, its own `ws_max_size`). After that, every ASGI `send` raises `RuntimeError` rather than `OSError`. `send()` maps both to `ConnectionError`, and `finish()` suppresses both, so `aclose()` can't hang on a socket uvicorn already closed.
- **The close frame is sent by `WebSocketTransport.finish()`**, under the send lock and within 10 s. The endpoint's `finally` always removes the connection from the live set.
- **The `aclose()` check runs both before the Origin check and after `bind`.** A late handshake never runs `bind`, and a handshake that was inside `bind` when `aclose()` ran is still refused.
- **Only the `accept` send treats `OSError` as a client disconnect.** An `OSError` from `connect()` is logged and closes with `1011`.
- **`1013` is best effort.** A client that has truly stalled usually sees `1006`, because the close frame waits on the same backpressure. The endpoint still returns within about 2× the send limit.

### Phase 4 — `tantra-acp-client` (TS) · deps: P2, P3 · ✅ DONE

**Deliverables**
- `packages/tantra-acp-client`, published as `tantra-acp-client`:
  - Native ESM with declarations.
  - No runtime dependencies: our own JSON-RPC over native WebSocket, mirroring `tantra/acp/jsonrpc.py`.
  - Tests use Node's test runner.
- `TantraAcpClient({url, cwd?, onUpdate, onRequest?, onState?, onError?, webSocketFactory?, timing?})`.
  - `url: () => string | Promise<string>`.
  - `cwd` defaults to `/`.
  - `webSocketFactory` and `timing` (reconnect base and cap) are test seams. Defaults: native `WebSocket`, 1 s and 30 s.
- Methods:
  - `connect`, `disconnect`
  - `newSession`, `listSessions(cursor?)`, `resume(id, {mode})`, `close(id)`
  - `prompt(id, blocks, {commandId?})`, which returns a promise with a synchronous `.commandId`
  - `cancel(id, {commandId?})`
  - `claimWriter(id)`, `setModel(id, model)`, `delete(id)`
- **Profile:** advertises the profile and `elicitation.form`. An agent without the profile → `profile_unsupported`, with no reconnect.
- **Updates:**
  - Every `session/update`, `_tantra_*` included, goes to `onUpdate(sessionId, update)`.
  - `onState` exposes the connection state, plus `writer`, `cursor` and `paused` per session.
  - Why one callback: the app's reducer switches on `sessionUpdate` anyway.
- **Ordering:**
  - All inbound frames are handled in one ordered chain.
  - The cursor advances only after `onUpdate` resolves.
  - A throwing handler → `failed`, and the cursors are kept.
  - `onUpdate` must not await client calls, because the response would queue behind it. `onRequest` is called in order but not awaited.
- **Replay:** from the applied cursor when the client already has the session open, otherwise from `start`.
- **Reconnect:**
  - Jittered backoff from 1 s to 30 s, at most 10 dials, with `url()` called on every dial.
  - Every open session then resumes `read` from its applied cursor, in open order (roots before children).
  - Reclaiming the writer is explicit.
- **Uncertain prompts are held until reclaim** (user decision):
  - A session that was writer when the connection dropped pauses its FIFO prompt outbox.
  - The outbox resends with the frozen `commandId` once this client is writer again, so dedupe returns the same `messageId`.
  - `close`, `delete`, `disconnect` or `failed` reject held prompts with `uncertain` (if sent) or `closed`, carrying the `commandId`.
  - `unavailable` rejects with no automatic retry, because the server already retried.
- `listSessions` sends no `cwd` filter. Why: Sarathi's existing sessions have no `acp_cwd`.
- Outbound frames over 1 MiB are rejected locally with `too_large`. Why: the server would close with `1009`, and a held retry would loop.
- Permission and elicitation requests are surfaced with a `respond()` function and an abort signal.
- ~~Transcript reduction is left to the app.~~ P5 adds the reducer to the client (user decision).
- Deviations:
  - ~~`identity`~~: no persistence, and `submittedBy` comes from server auth.
  - ~~`onWriter`, `onQueue`~~: folded into `onUpdate`.
  - ~~advertising `subagents`~~: alpha.8 `ClientCapabilities` has no such field.

**Verify**
- Unit tests with a fake stream:
  - cursor advances only after the handler succeeds
  - a retry reuses the same `commandId`
  - reconnect resumes from the cursor in read mode
  - writer loss, then reclaim
  - request abort
  - `url()` called on every dial
  - the hold rule
  - parity with the P2 request fixtures
- An integration test against the P3 endpoint: a Python fixture server with a deterministic provider, two runtimes and PostgreSQL. It runs through `tests/test_acp_client_integration.py` and covers:
  - prompt and resume
  - recovering a lost response
  - fencing across two clients
  - lazy child resume
  - an approval answered with `respond()`
  - every frame the client sent passing `VALIDATOR`
- Lint, typecheck, build, and an ESM/declaration consumer check.

**Checklist**
- [x] API + profile handling
- [x] Reconnect + cursors
- [x] Unit + integration tests

**Verified:**
- `npm run lint`, `typecheck`, `test:consumer` clean. `npm test`: 49 passed in each of 3 runs.
- `test_acp_client_integration.py` passed 5 of 5 runs against an owned Compose PostgreSQL.
- 1206 package tests passed with zero skips. 157 stress tests passed.
- `ruff check` and `ruff format --check` clean; `mkdocs build --strict` passes.
- Nothing changed under `packages/tantra/src`, and no dependencies were added.
- One review: 1 major, 5 minor and 6 nit defects plus 6 test gaps. All fixed, each with a test that fails when its fix is reverted.

**As built** (surprising details):
- **Updates for sessions that aren't open still reach `onUpdate`**, because the mirror reports grandchildren on child ids that were never resumed. Cursor and writer state are tracked only for open sessions. A cursored update at or before the applied cursor is dropped.
- **Response side effects run synchronously** while the response frame is handled: registering the session, setting the writer flag, dropping descendants. Otherwise the updates that follow could be handled before the session exists.
- **Parents are learned from `subagent_update`.** A root's close, delete or re-resume drops its descendants.
- **A failed re-resume closes the session on both sides.** The client drops it and sends a best-effort `session/close`. Why: the server may already have shut the old session down (`agent.py:474`).
- **Reconnect:**
  - Only `-32002` on a resume drops that session. Any other error retires the connection and counts as a failed dial.
  - The dial count resets only once the connection reaches `open`. Why: a server that closes during every resume would otherwise be redialled forever.
  - The first `connect()` dials at once; a reconnect waits `min(cap, base·2ⁿ) × U[0.5, 1]` before every dial.
- **Hold rule:**
  - A writer pauses at drop time even with an empty outbox, so prompts made while reconnecting are held too.
  - A retried prompt that gets `writer_required` is held again.
  - The inbound chain resets on every new connection, so a wedged handler can't block the next one.
- **Requests:** with no `onRequest`, both request methods answer `-32601`. A throwing or rejecting `onRequest` → `handler_failed` and `-32603`.
- **Fixture server** (`test/server.py`) stops through `POST /control/shutdown`, because uvicorn re-raises SIGTERM after `serve()`, which would skip `aclose()`. It simulates a lost response by swallowing the prompt response and closing with `1011`.

**Follow-up:** a server whose stream fails right after go-live (`1011` from `agent.py:829`) makes the client reconnect about once a second indefinitely, with no `onError`, because every cycle reaches `open`.

### Phase 5 — Sarathi on the new interface · deps: P4 · ✅ DONE

Deploy backend and UI together. Old pages need a refresh.

**Client: transcript reducer** (user decision; additive, and `client.ts` / `protocol.ts` stay unchanged)
- New `packages/tantra-acp-client/src/transcript.ts`, exported from `index.ts`. Pure, immutable and framework-free; it returns the same object when nothing changes.
- **`Transcript`:**
  - **`sessions`:** a `SessionView` per session id: `entries`, `state`, `title`, `updatedAt`, `configOptions`, `queue`, `writerReason`.
  - **`roster`:** `{parentId, title, state}` per child id. The parent is the session id the `subagent_update` arrived on, so grandchildren are covered.
  - **`requests`:** pending requests per session, keyed by `askId`.
- **Entries:** one ordered list per session, upserted by `(kind, id)`.
  - `user`: the raw content blocks. Apps map `resource_link`s themselves.
  - `agent` / `thought`: chunks append; the full update replaces the text and marks it final.
  - `tool`: fields merged. Content chunks before completion are progress; `rawOutput` is the result.
  - `message`: a `session_message`.
  - `status`: only for a **cursored** `state_update idle` with `cancelled`, `_tantra_interrupted` or `error`. Why: the resume snapshot `state_update` has no cursor and would otherwise add a duplicate.
- **Functions:**
  - `empty`, `apply(t, sessionId, update)`, `reset`
  - `addRequest`, `removeRequest`
  - `askOf(request)`: approval, choice or free_text, plus `respond`
  - selectors: `descendants`, `treeRunning`, `pendingRequest`
- `usage_update` and compaction updates are ignored.
- **Golden fixture:** `packages/tantra/tests/test_acp_transcript_fixture.py` (tests only).
  - It scripts one session through `AcpServer` with a `FakeProvider`, covering text, reasoning, tool progress, all three ask kinds, spawn, finish, waking the idle child, and cancel.
  - It records the live stream, a replay from `start`, a mid-cursor replay and the request params into `packages/tantra-acp-client/test/fixtures/session.json`.
  - UUIDs are normalized and timestamps stripped. `TANTRA_UPDATE_FIXTURES=1` rewrites the file.

**Backend**
- `api/acp.py` replaces `api/ws.py`:
  - `scope` → `{"user": id, "kind": "root"}`.
  - **`policy`:**
    - Ownership of both root and actor.
    - `resume` needs either capability. Every other operation needs `write` and the root actor.
    - `answer` keeps the existing ask, kind and permission-argument checks. The pre-issue check (`response=None`) only checks that the ask belongs to the root.
  - **`encode_prompt` / `decode_input`:**
    - Text plus `resource_link` `file://` URIs under `UPLOAD_DIR/<user>` map to and from the attachment markers.
    - An escape, a non-`file` URI, a newline in a name, or any other block type → `ValueError`.
  - Mounted as `WebSocketRoute("/api/acp", ...)`. The lifespan calls `aclose()` before `close_resources`.
- **User-scoped tickets:**
  - audience `sarathi-acp`
  - `read|write` capability
  - signed login expiry; the ticket is valid for `min(now+30s, login expiry)`
  - issued by `POST /api/acp/ticket` with `no-store`
  - **The UI always requests `write`**; the read-only view is `resume(mode: read)`. `read` tickets serve observers.
- **Revalidation is dropped:** `AcpAuth` has no `revalidate`. The socket closes at login expiry, and Sarathi has no user-deletion route.
- **Telemetry:** exclude `api/acp` (it was `api/ws`). Why: the ticket is in the query string.
- **Remove:**
  - `/api/sessions` (list and create), `PATCH /api/sessions/{id}`, the socket-ticket endpoint
  - `/api/meta/ws-types`, `/api/sessions/{root}/actors`, `/api/models`
  - `SessionInput`, `HeaderNotice` and the other session schemas
- **Tests:** `test_acp.py` replaces `test_ws.py`, `test_sessions.py` and `test_meta.py`. Sarathi backend tests run from `apps/sarathi/backend` on SQLite plus `MemoryStore`, not the root `just test`.

**UI**
- **Dependencies:** depend on the local `tantra-acp-client` and remove `tantra-session-client`. Keep the repo-root Docker/Compose context for the local package.
- **One ACP connection per tab:**
  - `chat/layout.tsx` provides one client and one zustand store keyed by session.
  - `state.ts` is a thin binding over the client's reducer, plus UI-only state.
- **Session lifecycle:**
  - Leaving a page closes its session. Coming back resets its entry and replays from `start`. Why: it stays under the 8-session limit, and cursor replay only applies to open sessions.
  - New chat: `newSession`, then `prompt`, then navigate. The page skips `resume` for a session that is already open.
- **URLs** use canonical UUIDs; `/chat/<hex>` redirects.
- **Delete:** `transport.ts`, the old outbox and cursor code, `pendingFirstMessage`, actor polling, and the Retry banner (the hold rule replaces it).
- **Components:**
  - **Reclaim control** calls `claimWriter`.
  - **Read-only view** uses `mode: read`. Ask details reach only the writer, so readers see "Awaiting input", and answered asks leave no card.
  - **Child drawer** calls resume/close on the child.
  - **Queue strip** shows the queue.
  - **Model picker** uses `configOptions`.
  - **Sidebar** uses list (every page) and new. It refreshes on `session_info_update`.
- Run `yarn typegen` afterwards.

**E2E and docs**
- `e2e/session-client.mjs` becomes `e2e/acp-client.mjs`, built on `tantra-acp-client`. It passes `ws` through `webSocketFactory`, because native WebSocket can't set Origin.
- The gate's `drop-next-send` now exercises the server's retry: same `messageId`, one `InputQueued`.
- Update the runbooks and the README's "Session transport" section.

**Verify**
- **Client:**
  - **Replay equals live:** the golden live stream and the replay from `start` reduce to the same transcript.
  - **Cursor resume:** the live prefix plus the mid-cursor replay equals the full live stream.
  - **Profile fixtures:** queue, writer `claimed`/`replaced`/`lost`, and the mirror.
  - **Asks:** all three kinds through `askOf` and `respond`.
  - **Subagents:** two-way subagent messages, and an idle child woken again.
  - **Duplicates:** no duplicate status entry from the snapshot; a duplicate update is a no-op.
  - **Checks:** the consumer check passes, and the fixture test is deterministic over 5 runs.
- **Backend:**
  - tickets: purpose, expiry, tampering, read/write
  - the policy matrix
  - attachment-path escape
  - encode/decode round-trip
  - ACP over ASGI: list, new, prompt, title, model, `model_busy`, delete
  - the ported title tests
- **UI `yarn test`:** the store binding, Sarathi's helpers, and the golden fixture end to end.
- **Durable E2E across two backends:**
  - observing across backends
  - fencing and reclaim
  - answering an ask after a takeover
  - Stop, then sending another prompt
  - FIFO order
  - owner death
  - title and model updates
  - delete
  - recovering a lost prompt response
  - the ported approval, typed asks, child and document scenarios
- **Browser through Nginx:**
  - the full flow
  - read-only view
  - two tabs
  - reconnect after a backend restart
- **Checks:**
  - `just lint`, `just test`, Sarathi backend tests
  - `yarn lint`, `yarn tsc --noEmit`, `yarn build`
  - the client's npm checks
  - clean Docker builds
  - no diff under `packages/tantra/src`, `client.ts` or `protocol.ts`

**Checklist**
- [x] Client transcript reducer + golden fixture
- [x] Backend wiring, tickets, route removal
- [x] UI reducer + components
- [x] E2E + browser
- [x] Docs + builds

**Verified:**
- **Client:** lint, typecheck and the consumer check are clean; `npm test` passed 61/61 in each of 3 runs. The golden fixture test passed 5 of 5 runs. `test_acp_client_integration.py` passed 3 of 3 runs on owned PostgreSQL.
- **tantra:** 1207 passed with zero skips; ruff clean; `mkdocs build --strict` passes. Nothing under `packages/tantra/src` or in `protocol.ts` changed.
- **Sarathi backend:** 115 passed; ruff clean.
- **UI:** `yarn lint`, `tsc --noEmit` and `yarn build` are clean; `yarn test` passed 10/10. A second `yarn typegen` gives identical output.
- **Docker:** clean `--no-cache` builds. `e2e/acp-client.mjs` passed all 13 scenarios in each of 2 runs against two backends behind Nginx.
- **Browser (through Nginx):**
  - **Full flow:** signup, new chat, approval, subagent drawer, model switch, title.
  - **Two tabs:** fencing, ask handoff, reclaim both ways.
  - **Read-only:** the read-only view, reached through the hex URL redirect.
  - **Reconnect:** after restarting both backends, then reclaim and prompt.
- **Review:** one round, finding 6 minor defects and 5 nits. The fixes include every minor and three nits; each has a regression test where it can be tested.

**As built** (surprising details):
- **`client.ts` changed** (behavior only; the API is unchanged). Found by the P5 review:
  - `close()` always drops the session locally and sends `session/close` only while open. Before, a chat left during a reconnect was resumed again and leaked against the 8-session limit.
  - `resumeAll` closes sessions dropped mid-pass.
  - A failed `resume()` only cleans up the entry it created, so a stale failure no longer drops a newer resume.
- **Reducer:**
  - `reset` keeps the roster, because the mirror only sends on change.
  - Thought blocks are joined with a blank line.
  - `Ask` carries `askId`.
  - The ACP tool `kind` is stored as `toolKind`.
  - Mirror frames sit in the fixture's own `mirror` key, because their timing relative to the journal stream is racy.
- **A connection does not receive its own `config_option_update`:** `session.model` is updated synchronously. The model picker applies the `setModel` result.
- **Title and model pushes need a coordinator.** The Sarathi tests mount a second endpoint over a coordinated runtime proxy.
- **Attachments:**
  - `encode_prompt` resolves strictly (missing files and symlink loops are rejected).
  - It rejects ` path=` in names or paths, and rejects marker-shaped lines anywhere in the text.
  - Why: `decode_input` splits on the last ` path=`, so either one could forge a link to another user's file.
- **A `read` ticket can still call `session/new`:** `scope` can't see which operation called it. The empty session can't be prompted, and with the profile's default `mode: write` the claim fails and the session is deleted.
- **The `api/acp` tracing exclusion is a substring match,** so it also untraces `POST /api/acp/ticket`.
- **The E2E override lists two models** (`e2e-model,e2e-other`), because `set_config_option` only accepts configured models.
- **Owner-death E2E** uses `kill` (SIGKILL), so lease expiry is exercised.
- **UI:**
  - `hooks.ts` is deleted.
  - `useChat` lives in `client.ts`, because Next.js type-checks layout exports.
  - A session is ready only while it is open in the client state, so returning to a chat shows the loader rather than stale entries.

**Follow-up:** a user `resume()` that succeeds after `close()` was called for the same session on an open connection leaves it open on the server and untracked on the client. Closing it from the success path could close a newer resume of the same id.

### Phase 6 — Remove the 017 transport · deps: P5 · ✅ DONE

**Deliverables**
- Delete:
  - `tantra/websocket.py`, `tantra/_socket_protocol.py`, and their exports
  - `tests/test_socket*.py` and `tests/test_postgres_socket.py`
  - `packages/tantra-client/`
  - `docs/reference/websocket.md`, plus its nav entry
  - `stress/bench/socket.py`, `socket_worker.py`, `socket_client.mjs`, `stress/test_bench_socket.py`, and the matching README section
- Mark 017 "superseded for transport; P0 Runtime additions retained."

**Verify**
- `git grep -nE "SessionSocket|SocketAuth|SocketBinding|SocketAccess|SocketLimits|SocketViewContext|InputCodec|tantra-session-client|tantra-client/|_socket_protocol|socket-ticket|tantra\.websocket|stress\.bench\.socket|reference/websocket"` matches only `design/017` and `design/018`.
- All checks pass.

**Checklist**
- [x] Removal
- [x] Full checks

**Verified**
- `ruff` clean. tantra 1134 passed (was 1207; 73 socket tests removed) and stress 148 passed (was 157; 9 removed), both with zero skips on owned PostgreSQL.
- `mkdocs build --strict` passes. Sarathi backend 115 passed. Client 61/61, consumer passes.
- The Verify grep lists only `design/017` and `design/018`. Runtime, `tantra/acp/`, the client and the apps are untouched.
- One review, no defects.

**As built**
- `packages/tantra-client/` was removed from disk too, including its ignored `dist/` and `node_modules/`.
- No replacement transport bench: a load bench is out of scope.
- `CHANGELOG.md` is untouched. `SessionSocket` never shipped, so removing it needs no entry; no phase adds a `tantra.acp` entry yet.

### Phase 7 — Upstream proposal · deps: P6 · —

**Deliverables**
- `docs/acp/tantra-profile.md`, a public, RFD-shaped proposal: *a durable, multi-client profile for ACP v2*. It covers:
  - **Motivation:** server-run, multi-tenant, durable harnesses.
  - **Principles:** the v2 shape first.
  - **Each divergence and extension:** the problem, the wire format with examples taken from `tests/fixtures/acp_profile/`, and how plain v2 clients still conform.
  - **Mapping to upstream threads:**
    - A#954 correlation token
    - A#2208 durable cursor
    - A#2394 / A#2286 resume rules
    - A#533 fenced writer
    - A#1992 selective subscription
    - A#1261 queue visibility
  - **Reference implementation:** `tantra.acp`, `tantra-acp-client`, and Sarathi.
- `design/018_upstream_posts.md`: one short draft comment per thread, for the user to post.

**Verify**
- A test loads every example in the doc, validates it against the vendored alpha.8 schema plus the profile allowlist, and checks it matches the P2 fixture byte-for-byte.
- `mkdocs build --strict`.
- The user reviews it before anything is posted.

**Checklist**
- [ ] Proposal
- [ ] Draft posts
- [ ] Fixture parity test

### Phase 8 — Runtime additions · deps: P1 · —

> ⚠ **This phase changes tantra's Runtime.** Before planning or starting it, the orchestrator must explicitly tell the user that this phase changes Runtime internals. It must also get confirmation before any Runtime code is written. Keep the changes additive.

**Deliverables**
- `InputQueued` gains optional `sender_id: str | None` and `origin: Literal["user", "spawn", "message", "finished", "turn_ended"] | None`.
  - Set them in `_actor_spawn`, `_actor_send`, `_actor_finish` and `_lifecycle_input`. User inputs get `origin="user"`.
  - The input text and its prefixes stay unchanged.
  - When a legacy event (missing fields) is compared with its retry, treat the missing fields as matching, so legacy events plus retries never raise `InvalidCommandReuse`.
- `Runtime.watch_tree(root_id) -> AsyncIterator[TreeChange]`: actor status and root header changes, from local signals and coordinator notices. It does not read the journal.
  - Root header changes also cover `patch_header`, which must signal locally.
- `tantra.acp`:
  - The projector uses `origin` / `sender_id`, falling back to prefix parsing for legacy journals.
  - The subagent mirror and title/model watch use `watch_tree`; the polling is removed.

**Verify**
- Runtime tests:
  - fields set on every path
  - legacy events plus retry give no `InvalidCommandReuse`
  - `watch_tree` covers local, PostgreSQL across two runtimes, deletion and shutdown
  - no journal reads while idle
- Projector tests:
  - structured and legacy journals reduce to identical output
  - no `tree_status` polling remains
- Full package, stress, Sarathi and client suites pass unchanged.

**Checklist**
- [ ] User reminded + confirmed before starting
- [ ] Structured sender + legacy-safe comparison
- [ ] `watch_tree`
- [ ] ACP switched over, polling removed

### Conventions (all phases)
- **Workflow:**
  - No code comments.
  - One phase at a time.
  - Follow the `CLAUDE.md` orchestration, with one independent review per phase.
- **Testing:**
  - Use a deterministic `FakeProvider` and owned durable Compose PostgreSQL (`fsync=on`, `synchronous_commit=on`).
  - No paid inference.
  - Clean up resources you own.
- **Lint and format:**
  - Ruff, line length 120.
  - UI: ESLint + Prettier, 120 columns. Never edit `src/generated/` by hand.
- **Before marking a phase done:** run `just lint` + `just test`, plus `just stress` for phases that touch PostgreSQL.
- **Runtime boundary:** P1–P7 must not modify `runtime.py`, `loop.py`, `coordinator.py`, the stores or `events.py`. If something seems to need that, stop and report it.
- **Contract freeze:**
  - After P1: hooks, projection tables, divergences, error codes, message-ID rules.
  - After P2: the profile wire.
  - After P3: the `websocket()` signature and close codes.
  - After P4: the client API. P5 adds the transcript reducer exports; nothing existing changes.
  - Changing any of these means updating this spec first.
- **Reference schema bumps:** a separate change. Re-vendor the schema, update the wire models and the divergence allowlist, then rerun P1, P2 and P4 verification.
- **No third-party ACP packages** in Python or TS.

### Keeping this spec current
- Update the status marker and checklist ticks as you go.
- Keep the plan matching current truth. Add one short deviation note only when behavior, scope, contracts or phase boundaries materially change.
- After a phase lands, add only details that would surprise the next reader.
- Unresolved problems go in Open Decisions or a Follow-up note. Don't fix them outside the requested phase.

## Considered & rejected

- **Strict v2 conformance:**
  - The insertion-time response causes requests to sit pending for minutes.
  - Close-means-cancel breaks shared, durable sessions.
  - Pushing every child transcript costs bandwidth proportional to the tree.
- **ACP v1:** it holds the prompt open for the whole turn and has no state, upsert or session-message types.
- **Keep 017 and add an ACP adapter:** two protocols to maintain.
- **Fan asks out to every observer:** breaks attribution. Routing to the writer is simpler.
- **Custom `observe`/`unobserve` methods:** the standard `resume` and `close` on a child ID already express this.
- **Hide `spawn`/`send`/`finish` behind tool calls for every client:** the subagents RFD shows a child session is not a tool call.
- **Runtime changes in early phases:** the user wants the core untouched until P8.
- **ACP SDKs (Python `1.0.0rc3`, TS `@agentclientprotocol/sdk`):** pre-release, they disagree on the alpha (alpha.5 vs alpha.8), and they need workarounds for the alpha.8 shapes. Their servers `accept` before auth. We own the protocol instead.
- **A `tantra.dev` namespace:** implies owning a domain.
- **Plans, slash commands, `session/fork`:** tantra has no equivalents.

## Open Decisions

- **Reference schema bumps.** Default: stay on alpha.8 until a newer alpha brings something tantra wants.
- **Per-child cancel.** Needs a Runtime command scoped to one subtree, then `subagent_update.capabilities.cancel`. Separate spec.
- **v1 shim.** Default: none.
- **Osuite follow-up.** Needs a tantra release that includes 018.
  - `editor_context` goes through `encode_prompt` via `_meta`.
  - Redaction goes through `view`.
  - `dashboard_id` and retention go through `scope`.
  - It has no subagents or asks.
- **`usage_update` when `provider.limits` fails.** Default: omit it.
- **Profile v2.** Rename or drop features as upstream standardizes them, keeping `version` negotiation.

## Risks

- **v2 instability:** v2 is still an alpha, and `subagent_update` / `session_message` are unstable within it. We track it on our own schedule, re-vendoring the schema deliberately.
- **Owning the JSON-RPC layer:** we maintain framing, cancellation and batches ourselves in Python and TS. The conformance tests in P1 and P4 bound that risk.
- **Divergences:**
  - Generic v2 clients may assume close means cancel. With us, work keeps running after they close.
  - Generic clients may expect the response at insertion; they get it at acceptance instead.
- **Few v2 clients exist today,** and fencing may stay tantra-only.
- **Fragile prefix parsing:** until P8, classification breaks if the runtime's text formats change. Round-trip tests guard this.
- **Cursors depend on capabilities:** a cursor is only valid for the client capabilities it was issued under.
- **Replay cost:** a full replay is proportional to history length.
- **Security:**
  - Client-supplied MCP servers are ignored.
  - Read access isn't revalidated after the handshake.
- **stdio:** writing to stdout corrupts the stream.
- **P8 retries:** P8 changes event equality used by retries. The legacy-compatibility tests are mandatory.
