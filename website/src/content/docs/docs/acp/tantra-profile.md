# A durable, multi-client profile for ACP v2

- **Author(s):** the tantra project
- **Status:** Draft. Base: ACP v2, `schema-v2.0.0-alpha.8` (latest as of 2026-10-15).
- **Reference implementation:** [`tantra.acp`](https://github.com/malayh/tantra/tree/main/packages/tantra/src/tantra/acp) (Python agent), [`tantra-acp-client`](https://github.com/malayh/tantra/tree/main/packages/tantra-acp-client) (TypeScript client), and the Sarathi app. API reference: [ACP server](../reference/acp.md).

## Elevator pitch

- **A session is a durable journal on the server. A connection is an authenticated view of it.**
- From that one model:
    - One cursor, `<seq>.<index>`, is a journal position. It covers a network blip, a process restart and a switch to another worker.
    - `session/prompt` answers at durable acceptance. A client `commandId` makes retries exact and is the `messageId`.
    - One fenced writer per root and any number of readers. Agent→client requests go to the writer.
    - `session/close` detaches. Work belongs to the journal, not to a socket.
    - Child transcripts are delivered when asked for; child state always flows through a mirror.
- Negotiated with `_meta.tantra = {"version": 1}`. Two divergences from v2 apply to every client and two more to profile clients only. Everything else is v2 plus `_`-prefixed extensions.
- Every frame in the schema test, and every profile fixture, validates against the alpha.8 schema with an empty divergence allowlist. Benched at 1,000 observers plus 100 turns on two workers.

## Status quo

Upstream threads in `agentclientprotocol/agent-client-protocol`, re-read 2026-10-15.

| Thread | State | What it proposes today | Gap it leaves |
|---|---|---|---|
| [#2208](https://github.com/agentclientprotocol/agent-client-protocol/pull/2208) durable transport | Open RFD | Opaque per-stream `_meta["acp/cursor"]`; a bounded in-memory ring buffer behind `transport.resumable {retentionMessages, retentionSeconds}`; reconnect to the same `Acp-Connection-Id`; refuse with `409` / `stream/resume_failed`, then fall back to `session/resume`. Durable replay logs are rejected as "scope creep". | Two recovery layers. A restart, a retention miss or a different backend falls back to `session/resume`, which today means replay from `start`. |
| [#2394](https://github.com/agentclientprotocol/agent-client-protocol/pull/2394) last change | Open RFD | First, "the fix": `updatedAt` moves only when the retained conversation changes, never for a connect, a resume or a config write. Second, an optional inclusive cursor, `replayFrom: {"type": "message", "messageId"}`, behind `session.resume.replayFromMessage`; a message no longer retained (compaction, rewind) → `-32602`, then `start`. Motivation: 27,765 updates replayed from `start` to catch up about fifty turns. | The cursor names messages only and fails when compaction or rewind dropped the named message. |
| [#2286](https://github.com/agentclientprotocol/agent-client-protocol/issues/2286) state after resume | Open issue | Require the current `state_update` after `session/resume`, with and without `replayFrom`. | Allowed but not required in v2. |
| [#954](https://github.com/agentclientprotocol/agent-client-protocol/pull/954) / [#2175](https://github.com/agentclientprotocol/agent-client-protocol/pull/2175) correlation | Merged | `session/prompt` returns `messageId` at insertion. The v2 prompt RFD's FAQ calls it "an acknowledgment, not a client-supplied idempotency key"; a client `promptId` is deferred. | A lost response stays ambiguous; a retry can submit twice. |
| [#533](https://github.com/agentclientprotocol/agent-client-protocol/pull/533) multi-client attach | Open RFD | A proxy multiplexer. Controller/observer roles were removed so events need no per-client filtering. `request_permission` goes to every client; the first response wins and `permission_resolved` tells the rest. | Every attached client may answer; there is no designated answerer. |
| [#1992](https://github.com/agentclientprotocol/agent-client-protocol/pull/1992) subagents | Merged (unstable schema) | Child events are pushed automatically; clients MUST NOT load or resume a child. Selective subscription is "worth revisiting" if it strands nothing and defines catch-up. | Delivery cost scales with the size of the tree. |
| [#1261](https://github.com/agentclientprotocol/agent-client-protocol/pull/1261) `session/inject` | Open RFD | Queue and steer modes, with revoke and replace by `messageId`. It adds no shared pending-queue list. | Other clients cannot see what is queued. |

## What we propose: the model

- **The session is a durable journal on the server; a connection is an authenticated view of it.**
- The open proposals each address one part, at a different layer: a per-stream buffer (#2208), a message cursor (#2394), a multiplexing proxy (#533).
- Each profile rule follows from the model:

| Consequence | Rule on the wire |
|---|---|
| The cursor is a journal position | `_meta.tantra.cursor: "<seq>.<index>"` on every journal-derived update; `replayFrom: {"type": "_tantra_after", "cursor"}` |
| Acceptance is durable | `session/prompt` answers `{messageId}` once the input is committed to the journal; for profile clients `messageId` equals the `commandId` (a profile-only divergence) |
| One fenced writer per root | A lease in the store. Readers are any number of connections on any worker. |
| Asks go to the writer | Agent→client requests go to the writer only. Every notification goes to every connection. |
| `close` detaches | Cancelling is always explicit |

## Why it works better

| Thread | Upstream answer | tantra | Evidence |
|---|---|---|---|
| #2208 | Transport cursor plus an in-memory buffer, connection affinity, retention window; restart → full replay | The cursor is already durable, so there is one layer. No buffer, no retention window, no affinity. Any worker serves any resume, including after the owning worker is killed. | `test_profile_cursor_resume_is_served_by_the_other_runtime`; `test_cursor_resume_at_every_cut_equals_the_uninterrupted_stream`; Sarathi e2e `owner-death` (a backend is killed, the client resumes from its cursor on the other); the bench puts each root's writer and observer on different workers |
| #2394 | Inclusive `messageId` cursor that fails when compaction or rewind dropped the named message | An exclusive cursor that names any position, including between non-message updates (#2208's own argument against `messageId`). Compaction appends to the journal, so it never invalidates a cursor. An empty replay after the last cursor means nothing changed, so no `updatedAt` comparison is needed. tantra's `updatedAt` does not meet #2394's definition (see Costs). | `test_cursor_resume_at_every_cut_equals_the_uninterrupted_stream` (cuts at every output, with compaction, tools, asks and a child); `test_hidden_events_leave_cursor_gaps_and_cuts_still_hold` |
| #2286 | Proposes a MUST | Done: after every resume response, a `state_update` snapshot (with the last stop reason when idle) precedes live updates. Profile clients also get the queue, the mirror and, as writer, the pending asks. | `test_resume_without_replay_sends_only_the_current_snapshot`; `test_resume_in_write_mode_claims_before_responding_and_issues_asks_after` |
| #954 / #2175 | `messageId` is an acknowledgment; a correlation token is deferred | `commandId` becomes the Runtime command id and the `messageId` (a profile-only divergence from the Message ID RFD). A retry returns the same `messageId`. The dedupe record is the journal, so it holds on any worker and across restarts. | `test_duplicate_command_ids_are_one_turn_and_reuse_conflicts`; `test_a_lost_prompt_response_is_recovered_by_cursor_resume_and_retry` |
| #533 | Proxy broadcast; roles removed; first permission response wins | A fenced writer. Readers join freely. One routing rule for one message class: requests go to the writer, notifications to everyone. A claim moves pending asks and cancels the old writer's. `submittedBy` comes from server auth, is recorded with each input, and is shown in the queue. Native in the agent server; no proxy. | `test_a_read_connection_never_claims_and_claim_writer_moves_the_asks`; `test_requests_reach_only_the_writer_and_move_on_takeover`; `test_profile_claim_writer_fences_a_writer_on_the_other_runtime` |
| #1992 | Every child transcript pushed; selective subscription deferred | `session/resume` on a child, read-only, plus a state mirror. Announcements and state always flow; transcripts only when asked. Cursors and the resume snapshot give catch-up. Nothing strands because tantra children cannot raise asks; a general design would still have to deliver child requests while the transcript is hidden. | `test_children_are_mirrored_and_delivered_only_when_resumed`; `test_profile_mirror_follows_a_child_on_the_other_runtime` |
| #1261 | No shared view of pending input | `_tantra_queue_update`: the full FIFO list of accepted inputs not yet started, with `commandId` and `submittedBy`, after every resume and on every change | `test_queue_updates_follow_the_runtime_queue`; `test_an_input_cancelled_while_queued_leaves_the_queue` |

**Load:** 1,000 observers and 100 turns on two workers, with zero reconnects and zero idle journal reads. Numbers are under Implementation details and evidence.

### Why the interface is better

- **Fewer concepts.** One cursor, not a stream cursor plus a protocol cursor. One routing rule. One meaning of close. One id per submission.
- **Durable by default.** It needs no buffer or second store when the harness already keeps an ordered journal, as tantra does. The price is journal reads on resume (see Costs).
- **Graceful for plain v2 clients.** The first mutation claims the writer, children are pushed, and replay is coalesced. Plain clients meet only the two divergences that apply to every client (see Plain v2 clients).
- **Tenancy stays off the wire.** See the next section.
- **Schema-clean.** Extensions use v2's `_` and `_meta` rules. No frame needs a schema exception.

## One protocol for any tenancy

- **Tenancy is never on the wire.** It lives in three server hooks:
    - `bind`: identity, decided at the handshake.
    - `scope`: a metadata partition stamped on every new root and used as the `session/list` filter.
    - `policy`: every operation except `session/close`, checked against server-loaded session headers, never against client claims. `session/new` is checked against the header of the root it would create.
- **Unauthorized equals unknown.** A policy denial answers `-32002 "Session not found"`, exactly like an unknown id. An existing id the caller may not use answers exactly like an unknown one on every method.

| | Single tenant (local editor) | Multi tenant (Sarathi) |
|---|---|---|
| Transport | stdio; the identity is passed at start | WebSocket: Origin check, then `bind(ticket)`, then accept |
| `scope` | `{}` | `{"user": <id>, "kind": "root"}` |
| `policy` | always `True` | owner of root and actor; `resume` with a `read` or `write` ticket, everything else needs `write` on the root; answers re-check the ask's kind and arguments |
| Store | memory or SQLite | PostgreSQL plus a coordinator, N workers |
| Wire, projection, profile, client rules | identical | identical |

- On a networked deployment, one user on two devices and many users on one server are the same case: several connections, one writer.
- The reference TypeScript client speaks WebSocket only; a stdio client follows the same rules.

**Embedding hooks** (Python API, not wire):

| Hook | Contract |
|---|---|
| `bind(asgi_scope) -> AcpAuth` | WebSocket only. Runs after the Origin check and before accept. `PermissionError`, a wrong return type, or an `expires_at` already past denies. |
| `scope(auth, meta) -> {str: str}` | Stamped with `acp_cwd` on `session/new`; the subset filter for `session/list`. `PermissionError` denies. |
| `policy(access) -> bool` | Operations `new`, `resume`, `prompt`, `cancel`, `claim_writer`, `answer`, `set_model`, `delete`. Only exactly `True` allows. `new` sees a provisional root header (`id`, `root_id`, `agent`, `metadata`) before anything is created. `answer` is checked before a request is issued and again with the mapped response. |
| `encode_prompt` / `decode_input` | Content blocks to stored input and back. `ValueError` → `-32602`. Default: text only. |
| `view(event) -> event \| None` | Redaction before projection. `None` hides the event. Dropping or retyping a `ChildCreated` hides that child: its subtree leaves the mirror and cannot be resumed, its inputs to the parent project nothing, and the parent's calls that target it are withheld. |
| `AcpAuth(principal, submitted_by, expires_at)` | `submitted_by` is persisted with inputs, answers and cancels. The WebSocket closes at `expires_at`. |

```text
single = AcpServer(runtime, agent=Assistant, scope=no_scope, policy=allow_all, name="assistant")
await single.serve_stdio(AcpAuth(principal="local"))

multi = AcpServer(runtime, agent=Sarathi, scope=scope, policy=policy,
                  encode_prompt=encode_prompt, decode_input=decode_input, models=models, name="sarathi")
endpoint = multi.websocket(bind=bind, allowed_origins={"https://app.example"})
```

Sarathi's hooks: [`apps/sarathi/backend/src/sarathi/api/acp.py`](https://github.com/malayh/tantra/blob/main/apps/sarathi/backend/src/sarathi/api/acp.py).

## Specification (as implemented)

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. "Agent" is the server; "client" is the connected peer. This section is wire-only and describes the reference implementation today.

### Conventions

- JSON-RPC 2.0. The agent handles requests concurrently, so responses MAY arrive out of request order. Batches are supported. `$/cancel_request` works in both directions.
- Session ids are canonical lowercase UUID strings. Any other spelling is an unknown session (`-32002`).
- Extension names: `_meta.tantra.*`, methods `_tantra/*`, update kinds and stop reasons `_tantra_*`. Clients MUST ignore update kinds they do not know.
- Error messages are fixed strings. They never carry exception details.

### Negotiation and `initialize`

```json
{
  "jsonrpc": "2.0",
  "id": 0,
  "method": "initialize",
  "params": {
    "protocolVersion": 2,
    "info": {
      "name": "tantra-acp-client",
      "version": "0.1.0"
    },
    "capabilities": {
      "_meta": {
        "tantra": {
          "version": 1
        }
      }
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": 0,
  "result": {
    "protocolVersion": 2,
    "capabilities": {
      "session": {
        "prompt": {},
        "mcp": {},
        "delete": {}
      },
      "_meta": {
        "tantra": {
          "version": 1
        }
      }
    },
    "info": {
      "name": "assistant",
      "version": "1.5.0"
    },
    "authMethods": []
  }
}
```

- A client opts in with `capabilities._meta.tantra.version: 1`. Only the JSON integer `1` negotiates; `true`, `1.0` and `"1"` do not.
- The agent answers every valid `initialize` with `protocolVersion: 2`, the capabilities above, `info`, and `authMethods: []`. A request without an integer `protocolVersion` → `-32602`.
- A profile client MUST check that the answer advertises `_meta.tantra.version: 1`. The reference client fails with `profile_unsupported` otherwise and does not reconnect.
- `capabilities.elicitation.form` (an object) enables `elicitation/create`.
- Clients MUST send `initialize` before anything else. The agent does not enforce this: until then a connection is a plain v2 client without elicitation.
- Profile-only output goes only to profile connections: `_tantra_writer`, `_tantra_queue_update`, `_tantra_notice` and `_meta.tantra.cursor`.
- These v2-legal extension values go to every client: the stop reasons `_tantra_finished` and `_tantra_interrupted`, `_meta.tantra.ask` on `state_update`, `_meta.tantra.finished` on `session_message` and `subagent_update`, and `_meta.tantra.askId` on requests.
- Without the profile, `_meta.tantra.commandId` and `_meta.tantra.mode` are ignored, `_tantra_after` answers `-32602`, and `_tantra/session/claim_writer` answers `-32601`.

### Methods

| Method | Params used | Result | Behavior |
|---|---|---|---|
| `session/new` | `cwd` (absolute), `_meta.tantra.mode` | `{sessionId, configOptions?}` | Creates a root in the caller's scope, tagged with `cwd`. A denied `new` answers `-32002` and creates nothing. Profile: `mode` defaults to `write`; a denied claim deletes the new root and answers `-32002`. `mcpServers` is ignored. |
| `session/list` | `cwd?`, `cursor?` | `{sessions: [{sessionId, cwd, title?, updatedAt}], nextCursor?}` | Roots in the caller's scope (and with that `cwd`, when given), newest created first, 50 per page. `nextCursor` is opaque. A cursor that is not a root of this agent in the caller's scope → `-32602 "Invalid cursor"`. |
| `session/resume` | `sessionId`, `cwd` (absolute, unused), `replayFrom?`, `_meta.tantra.mode` | `{configOptions?}` on a root, `{}` on a child | Opens the session on this connection. `replayFrom` omitted or `null`: no replay. `{"type": "start"}`: all history. `{"type": "_tantra_after"}`: profile only. Any other type → `-32602`. Resuming an open root replaces it. |
| `session/prompt` | `sessionId`, `prompt`, `_meta.tantra.commandId` | `{messageId}` | Roots only, open on this connection. Needs the writer. Answers once the input is durably accepted. `messageId` is the `commandId`, or a fresh UUID without one. |
| `session/cancel` | `sessionId`, `_meta.tantra.commandId` (notification) | — | Cancels the running turn and every queued input in the tree. A session not open on this connection is ignored. |
| `session/close` | `sessionId` | `{}` | Detaches this connection's view: stops its tails, cancels its outstanding requests, releases its writer. Never cancels work. |
| `session/delete` | `sessionId` | `{}` | Deletes the tree, including active work. Needs neither an open session nor the writer. |
| `session/set_config_option` | `sessionId`, `configId: "model"`, `type: "id"`, `value` | `{configOptions}` | Only `model`, only to a configured value. Needs neither an open session nor the writer. Refused while coordinated work is active (`model_busy`). |
| `_tantra/session/claim_writer` | `sessionId` | `{}` | Profile only. See Writer claim. |

- `prompt` blocks MUST each carry a string `type`. Which types the agent accepts is server configuration (default: `text`); a rejected block → `-32602`.
- `session/new`, `session/resume`, `session/prompt`, `session/cancel`, claims, answers, `session/set_config_option` and `session/delete` are checked by policy. A denied request answers `-32002`. A denied cancel is dropped without a notice, and a denied answer is not applied. `session/close` needs no check: it only affects this connection.

**Order of responses and updates:**

| Method | Order on the wire |
|---|---|
| `session/new` | response → `session_info_update` → `state_update` snapshot → profile: `_tantra_queue_update` snapshot → writer: `_tantra_writer claimed` → live updates |
| `session/resume` on a root | replayed updates → response → `session_info_update` → `state_update` snapshot (plain clients: also every pushed child's `subagent_update` and `state_update`) → profile: `_tantra_queue_update` and the mirror snapshot → writer: `_tantra_writer claimed` (profile) → live updates. A writer's pending-ask requests follow `claimed` and MAY interleave with live updates. |
| `session/resume` on a child | replayed updates → `{}` → `state_update` snapshot → live updates |
| `session/prompt` | `{messageId}` after acceptance. The input's `_tantra_queue_update`, `user_message` and `state_update running` come from the journal and MAY arrive before or after it. |
| `_tantra/session/claim_writer` | `{}` → `_tantra_writer claimed` → pending-ask requests, which MAY interleave with live updates |

- Snapshots carry no cursor. Live updates of a session never precede its snapshots.

### Projection

Each journal event becomes zero or more updates on the session that owns the event.

| Journal event | Update |
|---|---|
| `InputQueued` (root user input) | none; profile: `_tantra_queue_update` |
| `TurnStarted`, user input | `user_message {messageId: commandId, content}`, then `state_update running` |
| `TurnStarted`, input from a parent or child agent | incoming `session_message {messageId, senderSessionId, recipientSessionId, content}`, with `_meta.tantra.finished: true` for a child's `finish`; nothing for a child's status-only "turn ended" input, and nothing for any input from a child that `view` hid. Then `state_update running` (agent-initiated work). |
| `TextDelta` / `ReasoningDelta` | `agent_message_chunk` / `agent_thought_chunk`, `messageId` = sample id |
| `TextPart` | `agent_message {content: [text]}`, an upsert that replaces the chunks |
| `ReasoningPart` | `agent_thought {content: [every reasoning block of the sample so far]}` |
| `ToolCallRequested` | `tool_call_update {toolCallId, name, title: name, kind: "other", status: "pending", content: [], rawInput}` |
| `ToolCallStarted` | `tool_call_update {toolCallId, status: "in_progress"}` |
| `ToolProgress` | `tool_call_content_chunk` |
| `ToolCallCompleted` | `tool_call_update {status: "completed" or "failed", rawOutput}`, then a `tool_call_content_chunk` with the result when it is not empty |
| `spawn`, `send`, `finish` calls | Not tool calls. Outgoing `session_message`: `spawn` at `ChildCreated`, `send` and `finish` on success. A failed one is a failed `tool_call_update`. Every update of a `spawn`, `send` or `status` call that targets a child hidden by `view` is withheld; its index is kept. |
| `ChildCreated` | `subagent_update {sessionId, title}` (the child's name, else its agent type), before any child traffic |
| `AskRaised` | `state_update requires_action` with `_meta.tantra.ask {askId, state: "pending"}`; the writer also gets a request (see Writer and asks) |
| `AskAnswered` | one `state_update` with `_meta.tantra.ask {askId, state: "answered"}`: `running`, or `requires_action` while other asks are pending |
| `SampleCompleted` | `usage_update {used, size}`; omitted when the model's context window is unknown |
| `CompactionApplied` | `compaction_update {compactionId, status: "completed", summary: []}`, then `compaction_summary_chunk` when there is a summary |
| `TurnCompleted` | `state_update idle` with `end_turn`, `max_turn_requests` (step limit) or `_tantra_finished` (`finish`) |
| `TurnCancelled` / `TurnFailed` / `TurnInterrupted` | `state_update idle` with `cancelled` / `error` and `error {code: -32603, message}` / `_tantra_interrupted` |
| End of a queued input that never started | none; profile: `_tantra_queue_update` |
| Anything else | none |

- **Ids are stable** across replays, reconnects and workers:
    - `user_message.messageId` is the command id, so it equals the prompt response's `messageId`.
    - `toolCallId = uuid5(sessionId, "<sampleId>:<callId>")`. An outgoing `session_message` uses the same key under the sender's id.
    - `compactionId = uuid5(sessionId, "compaction:<seq>")`.
- **A turn's end** withdraws outstanding requests for its asks.
- **Replay** for plain v2 clients is coalesced: deltas fold into one upsert per message, and only the latest `usage_update` is sent. Profile replay is uncoalesced: it is exactly the journal-derived live stream after the cursor.

### Errors

| Condition | `code` | `message` | `data.tantra.code` |
|---|---|---|---|
| Unknown, unauthorized, or not open on this connection; a child not open here; a denied claim or `new` | `-32002` | `Session not found` | — |
| Invalid params; a mutation or `mode: "write"` on a child open on this connection (a child resume: under a root open here, once policy allows); rejected prompt content; a bad cursor or `replayFrom`; an invalid `commandId` or `mode`; too many open sessions | `-32602` | fixed per case | — |
| `commandId` reused with different input or a different submitter | `-32602` | `Command conflict` | `command_conflict` |
| A mutation from a connection that is not the writer | `-32603` | `Writer required` | `writer_required` |
| Coordinator or lease unavailable. A prompt is first tried 4 times under the same command id. | `-32603` | `Service unavailable` | `unavailable` |
| A model change while coordinated work is active | `-32603` | `Model busy` | `model_busy` |
| Unknown method, or `_tantra/session/claim_writer` without the profile | `-32601` | `Method not found` | — |
| `$/cancel_request` while the handler is still running | `-32800` | `Request cancelled` | — |
| Anything else | `-32603` | `Internal error` | — |

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "error": {
    "code": -32602,
    "message": "Command conflict",
    "data": {
      "tantra": {
        "code": "command_conflict"
      }
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "error": {
    "code": -32603,
    "message": "Writer required",
    "data": {
      "tantra": {
        "code": "writer_required"
      }
    }
  }
}
```

- Clients SHOULD branch on `data.tantra.code` first, then on `code`.
- A `$/cancel_request` for `session/prompt` that arrives once acceptance has started does not abort it: the request still answers `{messageId}`.

### Command ids

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "session/prompt",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "prompt": [
      {
        "type": "text",
        "text": "Summarize the open issues."
      }
    ],
    "_meta": {
      "tantra": {
        "commandId": "3d9e5b7a-1f2c-4d6e-8a9b-7c5d3e1f2a4b"
      }
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "messageId": "3d9e5b7a-1f2c-4d6e-8a9b-7c5d3e1f2a4b"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "session/cancel",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "_meta": {
      "tantra": {
        "commandId": "b7e4c2a1-9d8f-4e6b-a5c3-2f1e0d9c8b7a"
      }
    }
  }
}
```

- `_meta.tantra.commandId` on `session/prompt` and `session/cancel` MUST be a canonical lowercase UUID. Anything else → `-32602` on a prompt; on a cancel it is logged and the cancel is dropped.
- It becomes the Runtime command id, unique within the tree. For a prompt it is also the `messageId`, which diverges from v2 (see Divergences from v2).
- The same id with the same input from the same submitter is a duplicate: one turn, the same `messageId`, on any worker.
- The same id with a different input or submitter → `command_conflict`. On a cancel, that is a `command_conflict` notice.
- Without one, the agent generates a fresh id.

### Cursors

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "agent_message",
      "messageId": "c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c",
      "content": [
        {
          "type": "text",
          "text": "There are three open issues."
        }
      ],
      "_meta": {
        "tantra": {
          "cursor": "12.3"
        }
      }
    }
  }
}
```

- Every update derived from the journal carries `_meta.tantra.cursor: "<seq>.<index>"`, merged into any existing `_meta`.
    - `seq` is the journal sequence number of the event, in the journal of the session the update is addressed to.
    - `index` is the position among that event's outputs. Internal signals take indices too, so gaps are normal.
- Within one resume, cursors on a session strictly increase.
- Live-only output carries no cursor: requests, `_tantra_*` updates, `session_info_update`, `config_option_update`, the `state_update` snapshots and the mirror's `subagent_update`s.
- A cursor is valid while the projection is unchanged: the same profile version and the same server configuration.

### Cursor replay

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "session/resume",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "cwd": "/work",
    "replayFrom": {
      "type": "_tantra_after",
      "cursor": "12.3"
    }
  }
}
```

- `replayFrom: {"type": "_tantra_after", "cursor"}` replays every journal-derived update strictly after the cursor (exclusive, like `Last-Event-ID`), then the snapshots, then the live tail.
- The cursor MUST match `(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)`, and its `seq` MUST NOT be beyond the session's last event. Otherwise → `-32602`.
- The live prefix up to a cursor plus the replay after it equals the uninterrupted stream.
- `{"type": "start"}` stays available to profile clients and replays uncoalesced, with cursors.

### Writer mode

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "session/resume",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "cwd": "/work",
    "_meta": {
      "tantra": {
        "mode": "write"
      }
    }
  }
}
```

- `_meta.tantra.mode` is `"read"` or `"write"`, on `session/new` (default `write`) and `session/resume` (default `read`). Any other value → `-32602`.
- `write` claims before the response. A denied claim answers `-32002`; on `session/new` the new root is deleted.
- Profile clients never claim implicitly.

### Writer claim

```json
{
  "jsonrpc": "2.0",
  "id": 8,
  "method": "_tantra/session/claim_writer",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b"
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": 8,
  "result": {}
}
```

- The session MUST be open on this connection. An open child → `-32602`; anything else → `-32002`.
- If this connection already writes, the claim is a no-op that answers `{}`.
- Otherwise policy decides (denied → `-32002`). The current writer is fenced: its outstanding requests get `$/cancel_request` and, if it was told `claimed`, it receives `_tantra_writer replaced`.
- After `{}`: `_tantra_writer claimed`, then every pending ask is re-issued here. Those requests MAY interleave with live updates.

### Writer state

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_writer",
      "writer": true,
      "reason": "claimed"
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_writer",
      "writer": false,
      "reason": "replaced"
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_writer",
      "writer": false,
      "reason": "lost"
    }
  }
}
```

- Sent on the root id, to this connection only. Never replayed, never cursored.
- `claimed`: this connection became the writer, through `mode: "write"` or a claim, once the session is live.
- `replaced`: another connection claimed.
- `lost`: the writer lease was lost. The agent released it at once; reclaiming is explicit.
- `replaced` and `lost` go only to a connection that was told `claimed`.

### Notices

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_notice",
      "level": "warning",
      "code": "writer_required",
      "message": "Writer required"
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_notice",
      "level": "warning",
      "code": "command_conflict",
      "message": "Command conflict"
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_notice",
      "level": "warning",
      "code": "session_deleted",
      "message": "Session deleted"
    }
  }
}
```

- `_tantra_notice {level: "warning", code, message}` is live-only, uncursored, and sent on the root.
- A failed `session/cancel` is reported this way, because a notification cannot return an error. A cancel denied by policy, or for a session not open here, produces no notice.
    - `writer_required`: this connection is not the writer; nothing was cancelled.
    - `command_conflict`: the `commandId` was used for something else.
    - `unavailable`: still failing after 4 attempts.
- `session_deleted`: the root was deleted, by any other connection on any worker or by the application (`Runtime.delete`, `Runtime.cleanup`). Sent once to every profile connection with the root open, once its `session/new` or `session/resume` has answered; the agent then closes the root and its open children there. A connection's own successful `session/delete` produces none; if that request errors or is cancelled after the deletion committed, the connection MAY still get it. Clients SHOULD drop the session and its descendants.

### Queue

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "_tantra_queue_update",
      "entries": [
        {
          "commandId": "4c8d2e6f-0a1b-4c3d-9e5f-6a7b8c9d0e1f",
          "content": [
            {
              "type": "text",
              "text": "Then draft a reply."
            }
          ],
          "submittedBy": "user-1"
        }
      ]
    }
  }
}
```

- The full FIFO list of accepted root user inputs that have not started. `submittedBy` is omitted when unknown.
- Sent after `session/new` and `session/resume`, and on every change. Live-only, on the root.

### Lazy children

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "method": "session/resume",
  "params": {
    "sessionId": "8a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
    "cwd": "/work",
    "replayFrom": {
      "type": "start"
    }
  }
}
```

- Profile clients never receive a child transcript unasked.
- `session/resume` on any descendant opens its transcript read-only, checked in this order:
    - Its root MUST be open on this connection, and neither it nor an ancestor may be hidden by `view`, else `-32002`.
    - Policy `resume` decides, seeing the root as the root and the child as the actor.
    - `mode: "write"` → `-32602`.
    - `replayFrom` rules are those of a root; the child has its own cursors. The result is `{}`.
- `session/close` on a child detaches its transcript; the child keeps running. Closing or re-resuming the root closes its children.
- Open children count toward the session limit.
- `session/prompt` or a claim on an open child → `-32602`.

### Re-resume

- `session/resume` on a root already open on this connection replaces it.
    - Its open children close; `session/close` on one then answers `-32002`.
    - The writer follows the new `mode`: `write` claims again and sends `claimed`; `read` releases it without a `_tantra_writer` update.
- A rejected `replayFrom` leaves the open session untouched.
- `write` claims before the open session is replaced:
    - A claim that is denied, or fails or is cancelled before it takes effect, leaves the open session untouched, including its writer.
    - A claim that takes effect but whose completion is lost (a timeout, or `$/cancel_request` mid-claim) fails the request, and the open session is fenced: it gets `_tantra_writer replaced`.
    - Once the claim succeeds, the swap completes and answers even if `$/cancel_request` arrives. The replaced session never sends `replaced`. A prompt racing the swap on it MAY fail with `writer_required`; a retry with the same `commandId` is exactly-once.
    - If the replay then fails (for example on store I/O), the request fails, the session is closed and no writer is left, as after a failed `read` re-resume.

### Child mirror

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "6f1c2a8e-3b4d-4e5f-9a7b-1c2d3e4f5a6b",
    "update": {
      "sessionUpdate": "subagent_update",
      "sessionId": "8a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
      "title": "scout",
      "state": {
        "state": "running"
      }
    }
  }
}
```

- For profile clients, every descendant is mirrored as `subagent_update {sessionId, title, state}` on its parent's session id, parents first. A finished child adds `_meta.tantra.finished: true`.
- A snapshot follows `session/new` and `session/resume`; after that, an update whenever the runtime's tree watch reports a change. The watch keeps only the latest tree, so a short-lived state can be skipped, and a new child's state may arrive before the parent's cursored announcement. Live-only.
- Child state maps to v2 state:

| Child | `state` |
|---|---|
| queued, running | `running` |
| awaiting input | `requires_action` |
| idle | `idle` with its last stop reason |
| finished | `idle`, `_tantra_finished` |
| failed | `idle`, `error` with `error {code: -32603, message}` |
| cancelled | `idle`, `cancelled` |
| interrupted | `idle`, `_tantra_interrupted` |

- The parent's transcript also carries the journal-derived `subagent_update {sessionId, title}`, with a cursor, at `ChildCreated`.

### Divergences from v2

| v2 rule | tantra | Applies to | Why |
|---|---|---|---|
| `session/prompt` answers only after insertion; holding input for later insertion is not acceptance | Answers on durable acceptance (the input is committed to the journal) with `{messageId}`. `user_message` is still emitted at insertion. | Every client | Acceptance is a durable commitment. Holding a request for minutes behind a queue makes lost responses likely. v2 clients already tolerate updates before or after the response. |
| "The Agent MUST cancel any ongoing work for that session" | Detaches this connection only. Cancelling is explicit. | Every client | Sessions are durable and shared. One viewer leaving must not stop work others started or watch. |
| Clients MUST NOT load or resume a child | Profile clients MAY resume a child read-only, and close it. Plain v2 clients get the standard automatic push. | Profile clients | Pushing every transcript costs in proportion to the tree; cursors give catch-up. Nothing strands only because tantra children cannot ask; a general design would still have to deliver child requests while the transcript is hidden. |
| "Only the Agent generates protocol message IDs" (Message ID RFD) | A prompt's `messageId` is the client's `commandId`. The agent validates it (canonical UUID) and rejects reuse with different content (`command_conflict`). Plain v2 clients get agent-generated ids. | Profile clients | Recovering a lost response needs an id the client knows before sending. One submission is one turn, so the correlation token and the message id coincide (see FAQ). |

### Writer and asks

- **One writer per root at a time.** The writer holds a lease in the store; a claim fences the previous one. A fenced writer keeps reading from its last applied event.
- **Needs the writer:** `session/prompt`, `session/cancel`, answering an ask. Nothing else does; policy still applies.
- **Profile clients** claim with `mode: "write"` or `_tantra/session/claim_writer`. A reader's prompt answers `writer_required` every time; its cancel yields a `writer_required` notice.
- **Plain v2 clients** claim with their first prompt or cancel. After being fenced, the next prompt fails once with `writer_required` and the one after reclaims. A cancel always claims.
- **Asks:**
    - Only the writer receives agent→client requests. Every notification goes to every connection.
    - An approval becomes `session/request_permission {sessionId, title, description?, subject?, options, _meta.tantra.askId}`. `subject` is `{type: "tool_call", toolCall: {toolCallId}}` when the ask belongs to a tool call. `options` are `allow` (`allow_once`) and `deny` (`reject_once`).
    - A choice or free-text ask becomes `elicitation/create {sessionId, mode: "form", message, requestedSchema, toolCallId?, _meta.tantra.askId}`, with one required property, `choice` (a string, with an `enum` when the choices are fixed) or `text`. It is sent only to a writer that advertised `elicitation.form`; otherwise it waits for another writer.
    - Accepted answers: `{outcome: {outcome: "selected", optionId: "allow" | "deny"}}`, `{action: "accept", content: {choice}}` with an allowed choice, or `{action: "accept", content: {text}}`. Anything else, including an error response, answers nothing, and so does an answer denied by policy. The ask stays pending, and this connection does not get the request again until it next becomes writer, for example by re-resuming with `mode: "write"`. A claim while already writer re-issues nothing.
    - Policy is checked before the request is issued and again before the answer is applied.
    - When the ask is answered elsewhere, its turn ends, the writer changes, or the session closes, the holder gets `$/cancel_request`.
    - On a claim, every pending ask is re-issued to the new writer.
    - Readers see `state_update requires_action` with `_meta.tantra.ask`, never the request.
    - tantra children cannot raise asks (a runtime rule), so a child never holds a request.

### Subagents

- **Plain v2 clients:** every descendant transcript is pushed on the same connection, announced by `subagent_update` before its traffic. Child state is mirrored on the parent's `subagent_update.state`. Resuming a child pushed on this connection answers `-32602`; any other child id answers `-32002`.
- **Profile clients:** lazy children plus the child mirror, as above.
- **Messages:** `spawn`, `send` and `finish` appear as two-way `session_message`s on both sessions.
- **Cancel** covers the whole tree. There is no per-child cancel, so `subagent_update` carries no `capabilities`.

### Transports

**stdio**

- Newline-delimited JSON-RPC in UTF-8: one message or batch per line. Blank lines are ignored.
- The agent's stdout carries only protocol frames.
- A line over 1 MiB ends the connection.
- The local process is trusted; its identity is fixed at start.

**WebSocket**

- One message or batch per text frame. Binary frames are ignored.
- Clients MUST NOT offer a subprotocol; the agent selects none. Versioning happens in `initialize`.
- **Handshake order:** Origin, then authentication, then accept.
    - Origin: exactly one `Origin` header, matched exactly against an allowlist. A missing Origin is denied unless the server allows it.
    - Every denial closes before accept, and a bad Origin never reaches authentication. On uvicorn the client sees HTTP 403.
- **Close codes:**

| Code | When |
|---|---|
| `1008` | Before accept: Origin denied or missing, or authentication denied. After accept: the credential expired. |
| `1009` | An inbound text frame over 1 MiB of UTF-8 |
| `1011` | Authentication failed unexpectedly, or the agent closed the connection: a session stream or the mirror failed, or the runtime stopped |
| `1012` | Server shutdown |
| `1013` | An outbound send did not finish within 10 s (best effort) |

- **Limits:** 1 MiB inbound frames, 10 s per outbound send, and 8 open sessions per connection (`-32602`). Plain v2 connections count roots; profile connections count roots plus open children.

### Client obligations

| Rule | Level | Why |
|---|---|---|
| Advance a session's cursor only after its update is applied | MUST | Replay starts strictly after the cursor |
| Ignore a cursored update at or before the applied cursor | SHOULD | A replay can overlap frames still queued locally |
| Resume an open session with `_tantra_after` its applied cursor; with no cursor, from `start` after clearing local state | SHOULD | Only the gap crosses the wire |
| Reconnect with jittered exponential backoff and a bounded number of dials. Reference: `min(30 s, 1 s × 2ⁿ) × U[0.5, 1]`, 10 dials. | SHOULD | Avoids reconnect storms |
| After a reconnect, resume every open session in `read` mode, roots before children; reclaim the writer explicitly | SHOULD | A reconnect must not fence another device silently |
| Drop a session whose resume answers `-32002` | SHOULD | It was deleted or access was revoked |
| On `_tantra_notice` `session_deleted`, drop the session and its descendants | SHOULD | The root was deleted; the agent has closed them here |
| Retry a prompt whose response was lost with the same `commandId` and content, only once this connection is the writer again | MUST | The agent then answers the original `messageId`; a new id could submit twice |
| Treat `writer_required` on such a retry as still uncertain, and hold it | SHOULD | The original may have been accepted |
| Do not retry `unavailable` | SHOULD | The agent already retried |
| Do not send frames over 1 MiB | MUST | The agent closes with `1009` |
| Generate canonical lowercase UUIDs for `commandId` | MUST | Anything else is `-32602` |

## Plain v2 clients

| Rule | Profile client | Plain v2 client |
|---|---|---|
| Writer | Explicit (`mode`, `claim_writer`), never implicit | The first prompt or cancel claims; after fencing, the next prompt fails once |
| `session/new` | Writer by default | Reader until its first mutation |
| Pending asks on resume | Issued after the response with `mode: "write"` | None until its first mutation claims |
| Children | Resumed on demand, plus the mirror | Every descendant pushed; resuming a pushed child → `-32602`, any other child → `-32002` |
| Replay | Uncoalesced, with cursors | Coalesced; latest `usage_update` only |
| `commandId`, `mode` | Used | Ignored; each prompt gets a fresh `messageId` |
| `_tantra_after`, `claim_writer` | Supported | `-32602`, `-32601` |
| `_tantra_writer`, `_tantra_queue_update`, `_tantra_notice`, cursors | Sent | Never sent |
| `_tantra_finished`, `_tantra_interrupted`, `_meta.tantra.*` values | Sent | Sent; v2 tolerates unknown stop reasons and `_meta` |
| `data.tantra.code` on errors | Sent | Sent |
| Divergences | All four | The two for every client: prompt timing and close |

## Costs and limits

- **A durable, ordered journal is required.** The memory store suits tests and one process.
- **Close is not cancel.** A generic v2 client that relies on close cancelling leaves work running. It must call `session/cancel`.
- **Answering at acceptance:** an accepted input cancelled before it starts never produces a `user_message`. Profile clients see it leave the queue; plain clients see nothing.
- **Cursors depend on the projection.** A change of profile version, server configuration or projection code can shift indices, and nothing on the wire detects it. A server that changes its projection SHOULD bump the profile version.
- **Resume cost on the server.** Every resume reads the session's journal from the start to rebuild projection state, even when only the gap is sent. A `start` replay costs time in proportion to history on both sides.
- **Cancel is coarse.** `session/cancel` stops the whole tree, queued inputs included. There is no per-child cancel, and queued inputs have no steer, revoke or replace.
- **Approving from a second device means taking the writer.** Under #533 a phone or dashboard can answer a permission request and the terminal is unaffected. Here the phone must claim, which fences the terminal: the turn keeps running, but the terminal must reclaim before it can prompt or answer again.
- **A dismissed request is gone for that connection.** An unmapped, declined or policy-denied answer leaves the ask pending; it comes back only when a connection next becomes writer.
- **Children cannot raise asks** in tantra. #1992 allows child permission and elicitation requests; lazy delivery would then have to route them while the child's transcript is not open.
- **`updatedAt` moves on every journal append and header patch**, including a model change through `session/set_config_option`. It does not meet #2394's proposed definition. Profile clients can resume with their last cursor instead: an empty replay means nothing changed.
- **Fencing assumes the agent owns the session**, as a server-run harness does. A stdio CLI agent still needs a proxy to serve several clients. Few v2 clients exist today, and fencing may stay tantra-only.
- **A plain v2 client that resumes a session with a pending ask** gets no request until its first mutation claims the writer.
- **Deletion is silent to plain v2 clients.** Their views stop, and later calls answer `-32002`. Profile clients get `session_deleted`.
- **Read access is not revalidated after `session/resume`.** A reader keeps receiving updates until it closes the session or the WebSocket closes at `expires_at`.
- **The child mirror is coalesced.** It follows the runtime's tree watch, which reads headers on each change and keeps only the latest tree, so a short-lived child state can be skipped.

## Implementation details and evidence

**Code**

| Part | Where |
|---|---|
| Agent, writer, asks, mirror | [`tantra/acp/agent.py`](https://github.com/malayh/tantra/blob/main/packages/tantra/src/tantra/acp/agent.py) |
| Projection | [`tantra/acp/projection.py`](https://github.com/malayh/tantra/blob/main/packages/tantra/src/tantra/acp/projection.py) |
| JSON-RPC, transports, wire models | [`jsonrpc.py`](https://github.com/malayh/tantra/blob/main/packages/tantra/src/tantra/acp/jsonrpc.py), [`transport.py`](https://github.com/malayh/tantra/blob/main/packages/tantra/src/tantra/acp/transport.py), [`websocket.py`](https://github.com/malayh/tantra/blob/main/packages/tantra/src/tantra/acp/websocket.py), [`wire.py`](https://github.com/malayh/tantra/blob/main/packages/tantra/src/tantra/acp/wire.py) |
| TypeScript client and transcript reducer | [`packages/tantra-acp-client/src`](https://github.com/malayh/tantra/tree/main/packages/tantra-acp-client/src) |
| Multi-tenant app | [`apps/sarathi/backend/src/sarathi/api/acp.py`](https://github.com/malayh/tantra/blob/main/apps/sarathi/backend/src/sarathi/api/acp.py) |

- No third-party ACP SDK in either language. The JSON-RPC layer, transports and wire models are tantra's own.
- The alpha.8 `schema.json` and `schema.unstable.json` are vendored as a test reference. A structural validator checks every update kind, request and response the schema test drives, and every example in this document, against them.

**Tests**

| Claim | Tests |
|---|---|
| Cursor resume at every cut equals the live stream, for a root and a child | `test_cursor_resume_at_every_cut_equals_the_uninterrupted_stream`, `test_hidden_events_leave_cursor_gaps_and_cuts_still_hold`, `test_a_hidden_child_leaks_no_lifecycle_input_and_cuts_still_hold` |
| A cursor resume is served by another runtime | `test_profile_cursor_resume_is_served_by_the_other_runtime` (PostgreSQL) |
| Command ids dedupe and conflict; a lost response is recovered | `test_duplicate_command_ids_are_one_turn_and_reuse_conflicts`, `test_a_lost_prompt_response_is_recovered_by_cursor_resume_and_retry` |
| Writer: read mode never claims; claims move asks; loss is explicit | `test_a_read_connection_never_claims_and_claim_writer_moves_the_asks`, `test_resume_in_write_mode_claims_before_responding_and_issues_asks_after`, `test_a_lost_writer_is_released_at_once_and_reclaiming_is_explicit` |
| A failed or cancelled write re-resume keeps the writer; a claimed one completes | `test_an_unavailable_write_resume_keeps_the_open_writer`, `test_a_cancelled_write_resume_keeps_the_open_writer`, `test_cancels_after_the_claim_still_complete_the_write_resume` |
| Fencing across runtimes | `test_fencing_across_runtimes`, `test_profile_claim_writer_fences_a_writer_on_the_other_runtime` (PostgreSQL) |
| Plain v2 behavior | `test_two_clients_fence_each_other_and_writer_required_fails_once`, `test_requests_reach_only_the_writer_and_move_on_takeover`, `test_a_plain_client_ignores_profile_inputs`, `test_a_fenced_plain_client_never_receives_profile_updates` |
| Divergences | `test_a_queued_prompt_responds_on_acceptance_and_its_user_message_follows_turn_start`, `test_close_detaches_without_cancelling_work` |
| Queue | `test_queue_updates_follow_the_runtime_queue`, `test_an_input_cancelled_while_queued_leaves_the_queue` |
| Lazy children and the mirror | `test_children_are_mirrored_and_delivered_only_when_resumed`, `test_lazily_resumed_children_count_toward_the_session_limit`, `test_profile_mirror_follows_a_child_on_the_other_runtime` (PostgreSQL) |
| Unauthorized equals unknown | `test_policy_denial_and_unknown_sessions_return_resource_not_found`, `test_a_child_not_open_here_answers_like_an_unknown_id`, `test_a_child_resume_checks_the_root_and_policy_before_the_mode`, `test_a_list_cursor_outside_the_callers_scope_is_invalid`, `test_a_read_only_policy_cannot_create_sessions`, `test_a_hidden_child_resumes_like_an_unknown_id` |
| Deletion notice | `test_only_other_profile_connections_are_told_about_a_deletion`, `test_a_lost_session_is_reported_deleted_only_while_open_and_its_root_is_gone`, `test_a_deletion_while_a_resume_replays_fails_it_without_a_notice` |
| Schema | `test_every_emitted_message_is_valid_against_the_alpha8_schema`, `test_profile_fixtures_are_valid_and_match_real_messages`, `test_the_proposal_examples_are_the_profile_fixtures` |
| WebSocket handshake and limits | `test_origin_denials_close_before_accept_without_calling_bind`, `test_bind_denials_close_1008_before_accept`, `test_frame_limit_counts_utf8_bytes`, `test_a_stalled_send_closes_1013`, `test_expiry_closes_1008_after_accept` |
| stdio | `test_stdio_carries_only_json_rpc_and_completes_a_turn`, `test_an_oversized_stdio_frame_ends_the_connection_and_restores_descriptors` |
| Client obligations | `the cursor advances only after onUpdate resolves`, `a prompt lost with the connection is held until the writer is reclaimed`, `a retried prompt that gets writer_required is held again`, `reconnect resumes open sessions in order, read-only, from their cursors`, `a server that drops every connection right after open fails after 10 dials`, `the session_deleted fixture drops the tree, reports not_found, then reaches onUpdate` |
| Client against two real endpoints | `test_typescript_client_against_two_real_endpoints` (PostgreSQL) |

- Python tests are in [`packages/tantra/tests`](https://github.com/malayh/tantra/tree/main/packages/tantra/tests); client tests are in [`packages/tantra-acp-client/test`](https://github.com/malayh/tantra/tree/main/packages/tantra-acp-client/test).
- Sarathi's end-to-end suite runs two backends behind Nginx: observing across backends, fencing and reclaim, answering an ask after a takeover, owner death, and recovering a lost prompt response.

**Bench** (`python -m stress.bench.acp`): two uvicorn workers on PostgreSQL, 1,000 observers idle for 35 s, then 100 concurrent turns with 10 slow readers. Observers are split across both workers; each root's writer runs on the other worker from its observer. One connection per root.

| | Previous custom protocol | ACP profile |
|---|---|---|
| Prompt-to-observed-idle p50 / p95, whole run | 3312 / 4486 ms | 3344 / 4557 ms |
| 35 s idle per worker: CPU, SQL calls | ~4.5 s, 848 | ~3.3 s, 348 |
| Idle journal or event-body SQL | 0 | 0 |
| Active per worker: CPU, journal queries | ~4.3 s, 1523 | ~4.4 s, 1482 |

- Full run: 57 s wall, 1,100 dials, 0 client errors, 0 reconnects, no server closes, 100 journals verified, every resource back to 0, inbound backlog peak 4.

**Known gaps in the reference implementation**

- Journals written before the runtime recorded input senders are classified by the text prefixes of their lifecycle inputs.

## Frequently asked questions

### Why not a transport cursor plus a buffer?

- With a journal, the cursor is already durable. A buffer is a second copy with its own retention window.
- The buffer misses the cases that hurt most: a process restart, another backend, a long disconnect. Each falls back to `session/resume`, today from `start`.
- Connection affinity ties recovery to one process. A journal cursor works on any worker.

### Why not `messageId` replay?

- A resume point has to sit between any two updates, not only at messages. #2208's FAQ makes the same argument.
- An inclusive cursor makes the client upsert the named message again. An exclusive one needs nothing.
- A message cursor fails when compaction or rewind dropped the named message. A position in an append-only journal stays valid.
- The trade-off: a tantra cursor is opaque to the client and tied to the projection.

### Why not broadcast asks?

- #533 sends every request to every client; first-writer-wins plus `permission_resolved` settles it deterministically. Any client can answer, which is its core use case: approve from a phone while the terminal keeps working.
- Routing requests to the writer is one rule for one message class. Notifications still go to everyone, so no per-event filtering is needed; that filtering was the concern that removed roles from #533.
- Handover is explicit: a claim moves the pending asks and cancels the old writer's requests.
- The trade-off: approving from a second device means claiming the writer, which fences the first until it reclaims.

### Why respond at acceptance?

- In tantra, acceptance is a durable commit, and insertion can come minutes later behind a queue. A request held that long is the one most likely to lose its response.
- The answer promises more than v2's: the input survives a crash.
- `user_message` still marks insertion, under the same `messageId`.

### Why let the client choose the `messageId`?

- Upstream says no. The Message ID RFD: "Only the Agent generates protocol message IDs." The v2 prompt RFD keeps its deferred correlation token "separate from letting the client choose the canonical `messageId`".
- The profile diverges anyway, for profile clients only:
    - Recovering a lost response needs an id the client knows before sending.
    - In tantra one submission is one turn, so a separate token would always map one-to-one onto the message id. The prompt RFD lists that mapping and the second identifier as the token's costs; merging them removes both.
    - The agent still owns validity: it accepts only canonical UUIDs, scopes them to one tree, and rejects reuse with different content (`command_conflict`).

## Revision history

- 2026-10-15: Initial draft, matching the reference implementation and the upstream threads as of this date.
- 2026-10-16: Policy sees `new`; `session_deleted` notice; existing ids the caller may not use answer like unknown ones; a failed `write` re-resume keeps the open session; children hidden by `view` leak no inputs.
