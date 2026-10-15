# ACP server

`tantra.acp` serves a Runtime over an interface inspired by [ACP](https://agentclientprotocol.com) v2 (`protocolVersion: 2`, reference schema `2.0.0-alpha.8`). It follows v2 shapes and semantics by default and diverges only where durable, multi-client, server-run sessions need it. tantra owns the JSON-RPC 2.0 layer, the transports, and the wire models; no ACP SDK is used and no dependency beyond pydantic is needed. The Runtime core is used only through its public API.

```python
import asyncio

from tantra.acp import AcpAccess, AcpAuth, AcpServer

async def scope(auth: AcpAuth, meta) -> dict[str, str]:
    return {"user": str(auth.principal)}

async def policy(access: AcpAccess) -> bool:
    return access.root.metadata.get("user") == str(access.auth.principal)

server = AcpServer(
    runtime,
    agent=Assistant,
    scope=scope,
    policy=policy,
    models=["gpt-5", "gpt-5-mini"],
    name="assistant",
)
asyncio.run(server.serve_stdio(AcpAuth(principal="local")))
```

Start and close the Runtime in the application. Closing or losing a client never cancels accepted work.

## Hooks

`AcpServer(runtime, *, agent, scope, policy, encode_prompt=encode_text, decode_input=decode_text, view=None, models=None, name)`

| Hook | Signature | Contract |
|---|---|---|
| `scope` | `async (auth, meta) -> Mapping[str, str]` | Required. Stamped on `session/new` together with `acp_cwd`, and used as the `session/list` filter. Raise `PermissionError` to deny. |
| `policy` | `async (AcpAccess) -> bool` | Required; must return exactly `True`. Operations: `resume`, `prompt`, `cancel`, `claim_writer`, `answer`, `set_model`, `delete`. |
| `encode_prompt` | `async (auth, blocks, meta) -> str` | Turns prompt `ContentBlock`s into Runtime input. `ValueError` becomes `-32602`. The default accepts text blocks only and joins them with newlines. |
| `decode_input` | `(str) -> list[ContentBlock]` | The inverse, used for `user_message`. The default returns one text block. |
| `view` | `async (LoggedEvent) -> LoggedEvent \| None` | Optional redaction before projection. It receives an independent copy; `None` hides the event. |
| `models` | `Sequence[str] \| None` | Enables the `model` select config option. |

`AcpAuth(principal, submitted_by=None, expires_at=None)` is the connection identity. `submitted_by` is persisted with prompts, answers, and cancels. stdio trusts the local process: pass the identity directly to `serve_stdio(auth)`, or serve any `Transport` with `await server.connect(transport, auth)`.

`AcpAccess(operation, auth, root, actor, meta, ask, response)` carries server-loaded `SessionHeader` copies, the request `_meta`, and for `answer` the durable `LocatedAsk`. `answer` is checked twice: before a request is issued (`response=None`) and again with the mapped `AskResponse`. Prompts check `prompt`; taking the writer additionally checks `claim_writer`.

Fixed limits: 8 open sessions per connection (roots, plus children resumed through the [profile](#tantra-profile)), 1 MiB inbound frames on stdio and [WebSocket](#websocket), 10 s per outbound WebSocket send, and WebSocket connections close at `expires_at`.

## Wire surface

`initialize` always answers version 2:

```json
{"protocolVersion": 2,
 "capabilities": {"session": {"prompt": {}, "mcp": {}, "delete": {}},
   "_meta": {"tantra": {"version": 1}}},
 "info": {"name": "<name>", "version": "<tantra-harness version>"},
 "authMethods": []}
```

| Method | Behavior |
|---|---|
| `session/new` | Calls `scope`, creates a root with metadata `{**scope, "acp_cwd": cwd}`, starts its tail, and returns `{sessionId, configOptions?}`. A `session_info_update` and `state_update` snapshot follow the response. `mcpServers` is ignored. |
| `session/list` | Roots of `agent` matching `scope` (and `acp_cwd == cwd` when given), newest created first, 50 per page. Pass the returned `nextCursor` as `cursor` for the next page. |
| `session/resume` | Checks `resume`. `replayFrom` omitted or `null` replays nothing; `{"type": "start"}` replays the root before responding, and for plain v2 clients every descendant too; profile clients replay only the session they resume. Returns `{configOptions?}`, then a current snapshot, then the live tail. Resuming an open session replaces it. Without the profile, children are rejected with `-32602`. |
| `session/prompt` | Roots only, on an open session. Checks `prompt`, encodes the blocks, takes the writer if needed, and returns `{messageId}` once the input is durably accepted. |
| `session/cancel` | Notification. Checks `cancel`, takes the writer if needed, and cancels the whole tree. The tail reports `idle` with `cancelled`. |
| `session/close` | Detaches this connection's tails, cancels its outstanding requests, and releases its writer. It never cancels work. Returns `{}`. |
| `session/delete` | Checks `delete` and deletes the tree with `allow_active=True`. Tails stop on every connection. |
| `session/set_config_option` | `configId: "model"` with `type: "id"` only. Checks `set_model` and patches the root header. `ModelChangeBusy` maps to `model_busy`. |

| Condition | Code | `data.tantra.code` |
|---|---|---|
| Session unknown, unauthorized, or not open | `-32002` | — |
| Invalid params, a child session, or too many open sessions | `-32602` | — |
| Mutation from a connection that is not writer | `-32603` | `writer_required` |
| Still unavailable after 3 retries of the same command ID | `-32603` | `unavailable` |
| Model change while work is active | `-32603` | `model_busy` |
| Profile `commandId` reused with different content | `-32602` | `command_conflict` |

Error messages are fixed strings and never carry exception details. If a session stream fails (including a raising `view` or `decode_input`) or the Runtime closes, the server closes the connection so the client reconnects and resumes; transient `CoordinatorUnavailable` and `LeaseLost` are retried from the last applied event. JSON-RPC parse errors, invalid requests, unknown methods, batches, and `$/cancel_request` in both directions follow JSON-RPC 2.0 and the ACP cancellation rules.

## Divergences from v2

| v2 rule | tantra | Why |
|---|---|---|
| `session/prompt` responds at insertion | Responds on durable acceptance (`InputQueued` committed). `user_message` is still emitted at `TurnStarted`. | Acceptance is a durable commitment; holding a request behind a queue invites lost responses. |
| `session/close` cancels ongoing work | `close` detaches this connection only; cancelling is explicit. | Sessions are durable and shared. |
| Clients must not resume a child | Profile clients may resume a child read-only and close it. Without the profile, children are pushed automatically and cannot be resumed. | Lazy child delivery keeps cost independent of tree size. |

## Projection

| Tantra event | Update |
|---|---|
| `TurnStarted`, user input | `user_message {messageId: command UUID, content: decode_input(input)}`, then `state_update running` |
| `TurnStarted`, agent input | An incoming `session_message` for `[agent X] …`, `[agent X finished] …` (with `_meta.tantra.finished`) or a child's spawn input; nothing for `[agent X turn ended] …`. Then `state_update running`. A prefix counts only when `X` is the session's parent or one of its children; anything else is an ordinary `user_message`. |
| `TextDelta`, `ReasoningDelta` | `agent_message_chunk`, `agent_thought_chunk` with the sample UUID as `messageId` |
| `TextPart`, `ReasoningPart` | `agent_message` / `agent_thought` upserts that replace the chunks |
| `ToolCallRequested`, `ToolCallStarted`, `ToolProgress`, `ToolCallCompleted` | `tool_call_update` (`pending`, `in_progress`, `completed` or `failed` with `rawInput` / `rawOutput`) and `tool_call_content_chunk`. `toolCallId = uuid5(session, "<sample>:<call_id>")`, so provider call IDs may repeat across samples and turns. |
| `spawn`, `send`, `finish` | Not tool calls. Outgoing `session_message`s with `messageId = uuid5(sender, "<sample>:<call_id>")`: `spawn` at `ChildCreated`, `send` and `finish` on success. A failure is shown as a failed `tool_call_update`. |
| `ChildCreated` | `subagent_update {sessionId, title}` before any child traffic |
| `AskRaised`, `AskAnswered` | `state_update requires_action` with `_meta.tantra.ask {askId, state}`; an answer reports `running` once no other ask is pending |
| `SampleCompleted` | `usage_update {used, size}`; omitted when `provider.limits` fails |
| `CompactionApplied` | `compaction_update {status: "completed"}` and `compaction_summary_chunk` |
| `TurnCompleted` | `idle` with `end_turn`, `max_turn_requests` for `max_steps`, or `_tantra_finished` for `finish` |
| `TurnCancelled`, `TurnFailed`, `TurnInterrupted` | `idle` with `cancelled`, `error {code: -32603, message}`, or `_tantra_interrupted` |

For plain v2 clients, replay coalesces streamed deltas into message upserts and reports only the latest `usage_update`, so a replayed transcript reduces to the same state as the live one. Profile replay is uncoalesced. For plain v2 clients, every descendant transcript is pushed on the same connection, and each child's `state_update` is mirrored on its parent's `subagent_update.state`. A finished child also carries `_meta.tantra.finished: true`. Profile clients never get child transcripts pushed; they resume a child to read it and see child state through the mirror. Title changes are sent as `session_info_update` and model changes as `config_option_update`; with a coordinator they follow header changes made anywhere, otherwise they are sent on `session/new` and `session/resume`.

## Writer and asks

Each root has one fenced writer. Without the profile, a connection starts as a reader; its first prompt or cancel takes the writer and fences the previous one, which keeps reading from its last applied event. A fenced connection's next prompt fails once with `writer_required`; the one after that takes the writer back. A cancel always takes the writer, because a notification cannot return an error.

Pending asks are sent only to the writer: an approval becomes `session/request_permission` with `allow` and `deny` options, and a choice or free-text ask becomes a form `elicitation/create` when the client advertised `capabilities.elicitation.form`. When the writer changes, outstanding requests are cancelled with `$/cancel_request` and re-issued to the new writer. Cancelled, declined, or unmapped responses answer nothing.

## `tantra` profile

A client opts in by sending `"_meta": {"tantra": {"version": 1}}` in `initialize.params.capabilities`; the agent always advertises the same value. Profile-only output (`_tantra_writer`, `_tantra_queue_update`, `_tantra_notice`, `_meta.tantra.cursor`) goes only to profile clients. The `_tantra_finished` / `_tantra_interrupted` stop reasons and `_meta.tantra.ask` / `finished` go to every client. Without the profile, `commandId` and `mode` are ignored, `_tantra_after` is rejected with `-32602`, and `_tantra/session/claim_writer` answers `-32601`.

| Feature | Wire | Semantics |
|---|---|---|
| Command IDs | `_meta.tantra.commandId` (canonical lowercase UUID) on `session/prompt` and `session/cancel` | Becomes the Runtime command ID. A retry with the same content is a duplicate and returns the same `messageId`. Different content → `-32602` with `command_conflict`; on cancel, a `command_conflict` notice. A non-canonical UUID → `-32602`. |
| Cursors | `_meta.tantra.cursor: "<seq>.<index>"` | On every update derived from the journal, merged into any existing `_meta`. `seq` is the journal seq and `index` the position in the projector's output for that event; gaps are normal. Live-only updates carry no cursor. |
| Cursor replay | `replayFrom: {"type": "_tantra_after", "cursor": "<seq>.<index>"}` | Replays everything after the cursor (exclusive), then the snapshot and the live tail. Malformed, or `seq` beyond the end of the journal → `-32602`. A cursor is valid only for the capabilities it was issued under; after a change, replay from `start`. |
| Writer mode | `_meta.tantra.mode: "read" \| "write"` on `session/new` and `session/resume` | Defaults to `write` on new and `read` on resume. `write` claims before responding, so a denied claim fails the request with `-32002`. Pending-ask requests follow the response. Profile clients never claim implicitly: a reader's prompt fails with `writer_required` every time, and its cancel yields a `writer_required` notice. |
| Claim | `_tantra/session/claim_writer {sessionId}` → `{}` | Checks `claim_writer` and fences the current writer. A no-op if this connection already writes. Pending asks move here; the old writer's requests are cancelled. |
| Writer state | `_tantra_writer {writer, reason}` | `claimed`: this connection became writer. `replaced`: fenced by another writer. `lost`: the writer lease was lost (`LeaseLost` / `CoordinatorUnavailable`); reclaiming is explicit. Sent on this connection only, never replayed. |
| Notices | `_tantra_notice {level: "warning", code, message}` | Sent for a cancel, since a notification cannot return an error: `writer_required` (this connection is not writer), `command_conflict` (the `commandId` was used for different content), `unavailable` (still failing after 3 retries). A cancel with an invalid `commandId` is only logged. |
| Queue | `_tantra_queue_update {entries: [{commandId, content, submittedBy?}]}` | Every accepted root user input that has not started. The full list is sent on each change and as a snapshot after `session/new` and `session/resume`. |
| Lazy children | `session/resume` / `session/close` on a child ID | Read-only: `mode: "write"` → `-32602`. The child's root must be open on the same connection, else `-32002`. Policy `resume` receives the root header as `root` and the child header as `actor`. The child gets its own cursors and `replayFrom` rules. Closing a child detaches its transcript and never stops it; closing the root closes its children. |
| Re-resume | `session/resume` on an open root | Replaces the session: its open children are closed (`session/close` on one then answers `-32002`), and the writer follows the new `mode`. `write` claims again and sends `claimed`; `read` releases the writer without a `_tantra_writer` update. A rejected `replayFrom` or a denied `write` claim leaves the open session untouched. |
| Child mirror | `subagent_update {sessionId, title, state}` | Every descendant is mirrored from `Runtime.tree_status` on its parent's session ID, parents first: a snapshot after resume, then each change. Child transcripts are never pushed. With a coordinator, refreshes follow child notices (at most one per 500 ms); without one, the mirror polls once a second while the tree is busy. |

A cursor-bearing update and a lost-response retry:

```json
{"sessionUpdate": "agent_message", "messageId": "c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c",
 "content": [{"type": "text", "text": "There are three open issues."}],
 "_meta": {"tantra": {"cursor": "12.3"}}}
```

After an uncertain prompt, reconnect, `session/resume` with `{"type": "_tantra_after", "cursor": "<last applied>"}` and `mode: "write"`, then retry the prompt with the same `commandId`: it is accepted once and answers the same `messageId`.

## WebSocket

`server.websocket(*, bind, allowed_origins, allow_missing_origin=False)` returns an `AcpWebSocket`, a raw ASGI callable that serves one JSON-RPC connection per socket, one message per text frame.

```python
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

from tantra.acp import AcpAuth

async def bind(scope) -> AcpAuth:
    user = await authenticate(scope)
    if user is None:
        raise PermissionError
    return AcpAuth(principal=user.id, submitted_by=user.id, expires_at=user.expires_at)

endpoint = server.websocket(bind=bind, allowed_origins={"https://app.example"})
app = Starlette(routes=[WebSocketRoute("/api/acp", endpoint)])
```

Any ASGI server or router that passes a `websocket` scope works; FastAPI takes the same `WebSocketRoute`, and `await endpoint(scope, receive, send)` mounts it by hand.

- **Handshake order:** Origin, then `bind`, then `accept`. Every denial sends `websocket.close` before `accept`, which uvicorn answers with HTTP 403. A rejected Origin never reaches `bind`.
- **Origin:** exactly one `Origin` header, matched exactly against `allowed_origins`. A bare string or `"*"` raises `ValueError`. A missing Origin is denied unless `allow_missing_origin=True`; Node's native WebSocket and other non-browser clients send none.
- **`bind`:** `async (scope) -> AcpAuth`, called with the raw ASGI scope. `PermissionError`, a result that is not an `AcpAuth`, or an `expires_at` at or before now denies with `1008`. Any other exception is logged and closes with `1011`.
- **Expiry:** the socket closes with `1008` at `auth.expires_at`. Clients reconnect with fresh credentials and resume.
- **No subprotocol:** none is selected, so clients must not offer one; Chrome fails the handshake when it offers subprotocols and the server selects none. Versioning happens in `initialize`.
- **Frames:** text only; binary frames are ignored. uvicorn's `ws_max_size` (16 MiB by default) must be at least 1 MiB, or the effective inbound limit shrinks.

| Close code | When |
|---|---|
| `1008` | Before `accept`: Origin denied or missing, or `bind` denied. After `accept`: `expires_at` reached. |
| `1009` | An inbound text frame over 1 MiB of UTF-8. |
| `1011` | `bind` failed unexpectedly, or the server closed the connection: a session stream or mirror failed, or the Runtime stopped. |
| `1012` | `AcpWebSocket.aclose()`. uvicorn also sends `1012` to open sockets on its own shutdown. |
| `1013` | An outbound send did not finish within 10 s. Best effort: a truly stalled client usually sees `1006`, because the close frame waits on the same backpressure; the endpoint still returns within about 20 s. |

**Shutdown:** `await endpoint.aclose()` closes every live socket with `1012`, waits for each connection to unwind, and denies later handshakes with `1012`. Call it before `await runtime.aclose()`, for example at the start of the lifespan shutdown. uvicorn already sends `1012` to open sockets and waits for them before the lifespan shutdown runs; `aclose()` still refuses late handshakes and covers other servers.

## stdio

`serve_stdio(auth)` exchanges newline-delimited JSON-RPC on stdin and stdout, which must be pipes or sockets. While it serves, protocol bytes use private duplicates of descriptors 0 and 1: descriptor 0 reads `/dev/null`, `sys.stdout` and descriptor 1 point at stderr, so stray prints and subprocesses cannot corrupt the stream, and `bash` subprocesses get no stdin. The original descriptors and their blocking flags are restored when serving ends or fails to start. A frame over 1 MiB ends the connection.

Clients that launch agent processes, such as Zed's `agent_servers` setting, can start it as a command. The client must speak ACP v2:

```json
{
  "agent_servers": {
    "Assistant": {
      "command": "python",
      "args": ["-m", "assistant.acp"]
    }
  }
}
```

## TypeScript client

`packages/tantra-acp-client` (`tantra-acp-client`) is an ESM client for the WebSocket endpoint. It has no runtime dependencies, uses the native `WebSocket`, always negotiates the `tantra` profile and `elicitation.form`, and owns cursors, command IDs, writer state and reconnects. It also ships a pure transcript reducer (see **Transcript** below).

```ts
import { TantraAcpClient } from "tantra-acp-client";

const client = new TantraAcpClient({
  url: async () => `wss://app.example/api/acp?ticket=${await ticket()}`,
  onUpdate: (sessionId, update) => reducer.apply(sessionId, update),
  onRequest: (request) => asks.show(request),
  onState: (state) => store.setConnection(state),
  onError: (error) => console.warn(error.code, error.sessionId),
});

await client.connect();
const { sessionId } = await client.newSession();
const prompt = client.prompt(sessionId, [{ type: "text", text: "Hello" }]);
console.log(prompt.commandId, (await prompt).messageId);
```

| Method | Wire |
|---|---|
| `connect()` / `disconnect()` | Dial, `initialize`, resume every known session / close with `1000` |
| `newSession()` | `session/new {cwd}`; the client becomes writer |
| `listSessions(cursor?)` | `session/list`, without a `cwd` filter |
| `resume(id, {mode})` | `session/resume`, `mode` defaults to `read`. A failed re-resume closes the session on both sides. |
| `close(id)` | Always drops the session (and a root's known descendants) locally and rejects its prompts. Sends `session/close` only while `open`; otherwise resolves `{}`, and a reconnect resume already in flight is closed once it returns. |
| `delete(id)` | `session/delete` |
| `prompt(id, blocks, {commandId?})` | `session/prompt`; returns a promise with a synchronous `.commandId` |
| `cancel(id, {commandId?})` | `session/cancel`; resolves once sent |
| `claimWriter(id)` / `setModel(id, model)` | `_tantra/session/claim_writer` / `session/set_config_option` |

- **Options:** `url` is called on every dial, so it can mint a fresh ticket. `cwd` defaults to `/`. `webSocketFactory` and `timing` (`reconnectBaseMs`, `reconnectCapMs`) are test seams.
- **State:** `onState({connection, sessions})` reports `closed`, `connecting`, `open` or `failed`, and `{writer, cursor, paused}` per open session. `paused` means prompts are held until this client is writer again.
- **Ordering:** every inbound frame (responses, updates, requests) is handled in one ordered chain per connection. `onUpdate` is awaited, and a session's cursor advances only after it resolves. `resume()` resolves after its replayed updates were handled, because replay precedes the response.
- **`onUpdate` must not await client calls.** The response would queue behind the running handler and never arrive. Start the call and let it finish later.
- **A throwing `onUpdate`** reports `handler_failed`, closes the socket and moves to `failed`. Cursors are kept, so a later `connect()` resumes after the last applied update on a fresh chain.
- **Replay:** an open session with a cursor resumes with `_tantra_after` that cursor; otherwise from `start`. Updates at or before the applied cursor are dropped.
- **Requests:** `onRequest({method, sessionId, params, respond, signal})` receives `session/request_permission` and `elicitation/create`. It is called in order but not awaited, so a dialog can stay open while updates keep flowing; answer later with `respond()`. `$/cancel_request` and disconnects abort `signal`, and `respond()` after that is a no-op. A throwing or rejecting `onRequest` reports `handler_failed` and answers `-32603`. Without `onRequest`, and for any other method, requests answer `-32601`.
- **Frames over 1 MiB** are rejected locally with `too_large` and never sent.

**Reconnect.** Any close not caused by `disconnect()` waits, then redials with jittered backoff: each wait is `min(30 s, 1 s × 2ⁿ) × [0.5, 1]`. Every open session then resumes `read` from its cursor, in open order, so roots come before their children. A session that answers `-32002` is dropped, its prompts are rejected, and it is reported; any other resume error closes the connection and counts as a failed dial. After 10 failed dials the client is `failed`; the count resets only once the connection is `open` again. Reclaiming the writer is explicit.

**Held prompts.** Each session has a FIFO outbox sent one prompt at a time. A session that was writer when the connection dropped is paused: its unanswered prompt goes back to the head, and nothing is sent until this client is writer again (`claimWriter`, `resume(id, {mode: "write"})`, or a `claimed` update). Prompts made while paused queue behind it. The retry reuses the same `commandId` and blocks, so the server answers the original `messageId`; a retry that gets `writer_required` is held again, since its outcome is still unknown. A prompt that was never sent, from a session that was not paused, sends normally and a non-writer gets `writer_required`. `close`, `delete`, `disconnect` and `failed` reject outstanding prompts with `uncertain` if they were sent, otherwise `closed`.

| `TantraAcpError.code` | Meaning |
|---|---|
| `writer_required`, `command_conflict`, `unavailable`, `model_busy` | The server's `data.tantra.code`. `unavailable` is not retried; the server already retried. |
| `not_found`, `invalid_params`, `internal` | `-32002`, `-32602`, any other server error |
| `uncertain` | A sent prompt was abandoned before its response; it may have been accepted |
| `closed` | Not connected, or the session is not open |
| `too_large`, `profile_unsupported`, `handler_failed`, `dial_failed` | Client-side failures; `profile_unsupported` never reconnects |

Errors carry `rpcCode`, `sessionId` and `commandId` when known. `crypto.randomUUID` requires a secure context (HTTPS or localhost).

**Transcript.** `src/transcript.ts` is pure, immutable and framework-free: each function returns a new `Transcript`, or the same object when nothing changed, so it suits React and zustand selectors. Call `apply` from `onUpdate` and `addRequest` from `onRequest`.

| Function | Effect |
|---|---|
| `empty()` | `{sessions: {}, roster: {}, requests: {}}` |
| `apply(t, sessionId, update)` | Folds one `session/update` into `sessions[sessionId]`, or into `roster` for `subagent_update` |
| `reset(t, sessionId)` | Drops the session's view and pending requests (before a replay from `start`); the roster stays |
| `addRequest(t, request)` / `removeRequest(t, sessionId, askId)` | Pending requests per session, keyed by `_meta.tantra.askId` |
| `askOf(request)` | `{kind: "approval" \| "choice" \| "free_text", askId, title, body, options, respond}` |
| `descendants(t, root)` / `treeRunning(t, root)` / `pendingRequest(t, id)` | Subagent ids breadth-first; whether the root or any descendant is not `idle`; the oldest pending request |

- **`SessionView`:** `entries`, `state` (the last `state_update`, with `_meta.tantra.ask` as `ask`), `title` and `updatedAt`, `configOptions`, `queue`, `writerReason`.
- **Entries** form one ordered list, upserted by `(kind, id)`:
  - `user`: the raw content blocks.
  - `agent` / `thought`: chunks append to `text`; the full message replaces it and sets `final`. Thought blocks are joined by a blank line.
  - `tool`: fields merged (the ACP `kind` is `toolKind`). Content chunks before `completed`/`failed` go to `progress`; `rawOutput` is the result.
  - `message`: a `session_message` with `sender`, `recipient`, `text` and `finished`.
  - `status`: a **cursored** `state_update idle` with `cancelled`, `_tantra_interrupted` or `error`, keyed by its cursor. The uncursored resume snapshot adds none.
- **Roster:** `{parentId, title, state}` per child id, where `parentId` is the session the `subagent_update` arrived on; missing fields are kept.
- `usage_update`, compaction updates, `_tantra_notice` and unknown kinds change nothing.
- `askOf(...).respond` sends exactly what the server accepts: `{outcome: {outcome: "selected", optionId: "allow" | "deny"}}` for approvals, `{action: "accept", content: {choice}}` or `{action: "accept", content: {text}}` for elicitations.
- `test/fixtures/session.json` is a golden recording of a scripted session (`tests/test_acp_transcript_fixture.py`; `TANTRA_UPDATE_FIXTURES=1` rewrites it). The client tests check that live, `start` replay and cursor resume reduce to the same transcript.
