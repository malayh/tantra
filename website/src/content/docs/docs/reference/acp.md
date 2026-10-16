---
title: ACP server
---

`tantra.acp` serves a Runtime over an interface inspired by [ACP](https://agentclientprotocol.com) v2 (`protocolVersion: 2`, reference schema `2.0.0-alpha.8`). It follows v2 shapes and semantics by default and diverges only where durable, multi-client, server-run sessions need it. tantra owns the JSON-RPC 2.0 layer, the transports, and the wire models; no ACP SDK is used and no dependency beyond pydantic is needed. The Runtime core is used only through its public API.

The wire protocol, including the `tantra` profile, is specified in the [ACP profile proposal](/tantra/docs/acp/tantra-profile/).

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
| `policy` | `async (AcpAccess) -> bool` | Required; must return exactly `True`. Operations: `new`, `resume`, `prompt`, `cancel`, `claim_writer`, `answer`, `set_model`, `delete`. |
| `encode_prompt` | `async (auth, blocks, meta) -> str` | Turns prompt `ContentBlock`s into Runtime input. `ValueError` becomes `-32602`. The default accepts text blocks only and joins them with newlines. |
| `decode_input` | `(str) -> list[ContentBlock]` | The inverse, used for `user_message`. The default returns one text block. |
| `view` | `async (LoggedEvent) -> LoggedEvent \| None` | Optional redaction before projection. It receives an independent copy; `None` hides the event. Dropping or retyping a `ChildCreated` hides that child: its subtree leaves the profile mirror and cannot be resumed, its messages, results and turn-ended inputs to the parent project nothing, and the parent's `spawn`, `send` and `status` calls that target it are withheld. |
| `models` | `Sequence[str] \| None` | Enables the `model` select config option. |

`AcpAuth(principal, submitted_by=None, expires_at=None)` is the connection identity. `submitted_by` is persisted with prompts, answers, and cancels. stdio trusts the local process: pass the identity directly to `serve_stdio(auth)`, or serve any `Transport` with `await server.connect(transport, auth)`.

`AcpAccess(operation, auth, root, actor, meta, ask, response)` carries server-loaded `SessionHeader` copies, the request `_meta`, and for `answer` the durable `LocatedAsk`. For `new`, `root` and `actor` are a provisional header of the root about to be created (`id`, `root_id`, `agent`, and `metadata` with `acp_cwd`); a denial creates nothing. `answer` is checked twice: before a request is issued (`response=None`) and again with the mapped `AskResponse`. Prompts check `prompt`; taking the writer additionally checks `claim_writer`.

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
| `session/new` | Calls `scope`, checks `new`, creates a root with metadata `{**scope, "acp_cwd": cwd}`, starts its tail, and returns `{sessionId, configOptions?}`. A `session_info_update` and `state_update` snapshot follow the response. `mcpServers` is ignored. |
| `session/list` | Roots of `agent` matching `scope` (and `acp_cwd == cwd` when given), newest created first, 50 per page. Pass the returned `nextCursor` as `cursor` for the next page. A cursor that is not a root of `agent` within the caller's `scope` answers `-32602`, like a malformed one. |
| `session/resume` | Checks `resume`. `replayFrom` omitted or `null` replays nothing; `{"type": "start"}` replays the root before responding, and for plain v2 clients every descendant too; profile clients replay only the session they resume. Returns `{configOptions?}`, then a current snapshot, then the live tail. Resuming an open session replaces it. Without the profile, a child pushed on this connection is rejected with `-32602`; any other child ID answers `-32002`. |
| `session/prompt` | Roots only, on an open session. Checks `prompt`, encodes the blocks, takes the writer if needed, and returns `{messageId}` once the input is durably accepted. |
| `session/cancel` | Notification. Checks `cancel`, takes the writer if needed, and cancels the whole tree. The tail reports `idle` with `cancelled`. |
| `session/close` | Detaches this connection's tails, cancels its outstanding requests, and releases its writer. It never cancels work. Returns `{}`. |
| `session/delete` | Checks `delete` and deletes the tree with `allow_active=True`. Tails stop on every connection; other profile connections first get a `session_deleted` notice. |
| `session/set_config_option` | `configId: "model"` with `type: "id"` only. Checks `set_model` and patches the root header. `ModelChangeBusy` maps to `model_busy`: only the PostgreSQL store and coordinator raise it, while coordinated work is active. |

| Condition | Code | `data.tantra.code` |
|---|---|---|
| Session unknown, unauthorized, or not open; a child ID not open on this connection; a denied claim or `new` | `-32002` | — |
| Invalid params; a mutation or `mode: "write"` on a child open on this connection (a child resume: under a root open here, once policy allows); too many open sessions | `-32602` | — |
| Mutation from a connection that is not writer | `-32603` | `writer_required` |
| Coordinator or lease unavailable, on any method. A prompt is first tried 4 times under the same command ID. | `-32603` | `unavailable` |
| Model change while coordinated work is active (PostgreSQL store and coordinator only) | `-32603` | `model_busy` |
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
| `TurnStarted`, agent input | An incoming `session_message` for an input whose `origin` is `message` (`[agent X] …`), `finished` (`[agent X finished] …`, with `_meta.tantra.finished`) or `spawn` (a child's spawn input); nothing for `turn_ended`. Then `state_update running`. The sender `X` must be the session's parent or one of its children; anything else, and every `origin: "user"` input, is an ordinary `user_message`. Inputs from a child hidden by `view` produce nothing and never enter the queue; they keep their output index, so cursors do not shift. Inputs without `origin`, from older journals, are classified by their prefixes under the same sender rule. |
| `TextDelta`, `ReasoningDelta` | `agent_message_chunk`, `agent_thought_chunk` with the sample UUID as `messageId` |
| `TextPart`, `ReasoningPart` | `agent_message` / `agent_thought` upserts that replace the chunks |
| `ToolCallRequested`, `ToolCallStarted`, `ToolProgress`, `ToolCallCompleted` | `tool_call_update` (`pending`, `in_progress`, `completed` or `failed` with `rawInput` / `rawOutput`) and `tool_call_content_chunk`. `toolCallId = uuid5(session, "<sample>:<call_id>")`. Call IDs are unique within a session: the turn engine rewrites a colliding provider ID to a deterministic `call_<24 hex>` before journaling (see [Providers](/tantra/docs/reference/providers/)). |
| `spawn`, `send`, `finish` | Not tool calls. Outgoing `session_message`s with `messageId = uuid5(sender, "<sample>:<call_id>")`: `spawn` at `ChildCreated`, `send` and `finish` on success. A failure is shown as a failed `tool_call_update`. Every update of a `spawn`, `send` or `status` call that targets a child hidden by `view` is an internal no-op that keeps its output index. |
| `ChildCreated` | `subagent_update {sessionId, title}` before any child traffic |
| `AskRaised`, `AskAnswered` | `state_update requires_action` with `_meta.tantra.ask {askId, state}`; an answer reports `running` once no other ask is pending |
| `SampleCompleted` | `usage_update {used, size}`; omitted when `provider.limits` fails |
| `CompactionApplied` | `compaction_update {status: "completed"}` and `compaction_summary_chunk` |
| `TurnCompleted` | `idle` with `end_turn`, `max_turn_requests` for `max_steps`, or `_tantra_finished` for `finish` |
| `TurnCancelled`, `TurnFailed`, `TurnInterrupted` | `idle` with `cancelled`, `error {code: -32603, message}`, or `_tantra_interrupted` |

For plain v2 clients, replay coalesces streamed deltas into message upserts and reports only the latest `usage_update`, so a replayed transcript reduces to the same state as the live one. Profile replay is uncoalesced. For plain v2 clients, every descendant transcript is pushed on the same connection, and each child's `state_update` is mirrored on its parent's `subagent_update.state`. A finished child also carries `_meta.tantra.finished: true`. Profile clients never get child transcripts pushed; they resume a child to read it and see child state through the mirror. Title changes are sent as `session_info_update` and model changes as `config_option_update`. They follow `Runtime.watch_tree`, so changes made through `Runtime.patch_header` always arrive; with a coordinator, header changes made anywhere arrive too. The connection that set the model normally gets no `config_option_update` for its own change. If a tree read was already in flight, it may get a transient one with the previous model, followed by one with the model it set.

## Writer and asks

Each root has one fenced writer. Without the profile, a connection starts as a reader; its first prompt or cancel takes the writer and fences the previous one, which keeps reading from its last applied event. A fenced connection's next prompt fails once with `writer_required`; the one after that takes the writer back. A cancel always takes the writer, because a notification cannot return an error.

Pending asks are sent only to the writer: an approval becomes `session/request_permission` with `allow` and `deny` options, and a choice or free-text ask becomes a form `elicitation/create` when the client advertised `capabilities.elicitation.form`. When the writer changes, outstanding requests are cancelled with `$/cancel_request` and re-issued to the new writer. Cancelled, declined, or unmapped responses answer nothing.

## `tantra` profile

A client opts in by sending `"_meta": {"tantra": {"version": 1}}` in `initialize.params.capabilities`; the agent always advertises the same value. Profile-only output (`_tantra_writer`, `_tantra_queue_update`, `_tantra_notice`, `_meta.tantra.cursor`) goes only to profile clients. The `_tantra_finished` / `_tantra_interrupted` stop reasons and `_meta.tantra.ask` / `finished` go to every client. Without the profile, `commandId` and `mode` are ignored, `_tantra_after` is rejected with `-32602`, and `_tantra/session/claim_writer` answers `-32601`.

| Feature | Wire | Semantics |
|---|---|---|
| Command IDs | `_meta.tantra.commandId` (canonical lowercase UUID) on `session/prompt` and `session/cancel` | Becomes the Runtime command ID. A retry with the same content is a duplicate and returns the same `messageId`. Different content → `-32602` with `command_conflict`; on cancel, a `command_conflict` notice. A non-canonical UUID → `-32602`. |
| Cursors | `_meta.tantra.cursor: "<seq>.<index>"` | On every update derived from the journal, merged into any existing `_meta`. `seq` is the journal seq and `index` the position in the projector's output for that event; gaps are normal. Live-only updates carry no cursor. |
| Cursor replay | `replayFrom: {"type": "_tantra_after", "cursor": "<seq>.<index>"}` | Replays everything after the cursor (exclusive), then the snapshot and the live tail. Malformed, or `seq` beyond the end of the journal → `-32602`. A cursor is valid while the projection is unchanged: the same profile version and server configuration. |
| Writer mode | `_meta.tantra.mode: "read" \| "write"` on `session/new` and `session/resume` | Defaults to `write` on new and `read` on resume. `write` claims before responding, so a denied claim fails the request with `-32002`. Pending-ask requests follow the response. Profile clients never claim implicitly: a reader's prompt fails with `writer_required` every time, and its cancel yields a `writer_required` notice. |
| Claim | `_tantra/session/claim_writer {sessionId}` → `{}` | Checks `claim_writer` and fences the current writer. A no-op if this connection already writes. Pending asks move here; the old writer's requests are cancelled. |
| Writer state | `_tantra_writer {writer, reason}` | `claimed`: this connection became writer. `replaced`: fenced by another writer. `lost`: the writer lease was lost (`LeaseLost` / `CoordinatorUnavailable`); reclaiming is explicit. Sent on this connection only, never replayed. |
| Notices | `_tantra_notice {level: "warning", code, message}` | Live-only, on the root. Sent for a cancel, since a notification cannot return an error: `writer_required` (this connection is not writer), `command_conflict` (the `commandId` was used for different content), `unavailable` (still failing after 4 attempts). A cancel with an invalid `commandId` is only logged. `session_deleted`: the root was deleted, by any other connection or by the application (`Runtime.delete`, `Runtime.cleanup`); sent once to every profile connection with it open, which then closes it and its open children. A connection's own successful `session/delete` produces none; if that request errors or is cancelled after the deletion committed, it may still get one. |
| Queue | `_tantra_queue_update {entries: [{commandId, content, submittedBy?}]}` | Every accepted root user input that has not started. The full list is sent on each change and as a snapshot after `session/new` and `session/resume`. |
| Lazy children | `session/resume` / `session/close` on a child ID | Read-only. Checked in order: the child's root must be open on the same connection, and neither the child nor an ancestor may be hidden by `view`, else `-32002`; policy `resume` receives the root header as `root` and the child header as `actor`; then `mode: "write"` → `-32602`. The child gets its own cursors and `replayFrom` rules. Closing a child detaches its transcript and never stops it; closing the root closes its children. |
| Re-resume | `session/resume` on an open root | Replaces the session: its open children are closed (`session/close` on one then answers `-32002`), and the writer follows the new `mode`. `write` claims before the open session is replaced and sends `claimed`. A claim that is denied, or fails or is cancelled before it takes effect, leaves the open session untouched, including its writer; a claim that takes effect but whose completion is lost (a timeout, or a cancel mid-claim) fails the request and fences the open session, which gets `replaced`. Once the claim succeeds, the swap completes and answers even if `$/cancel_request` arrives; the replaced session never sends `replaced`, and a prompt racing the swap on it may fail with `writer_required`. A replay that fails after the swap, for example on store I/O, fails the request, closes the session and leaves no writer. `read` releases the writer without a `_tantra_writer` update. A rejected `replayFrom` leaves the open session untouched. |
| Child mirror | `subagent_update {sessionId, title, state}` | Every descendant is mirrored from `Runtime.watch_tree` on its parent's session ID, parents first: a snapshot after resume, then each change. Child transcripts are never pushed. The mirror never polls or reads journals, and a new child's state may arrive before the parent's cursored `subagent_update` announces it. |

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
| `close(id)` | Always drops the session (and a root's known descendants) locally and rejects its prompts. Sends `session/close` only while `open`; otherwise resolves `{}`, and a reconnect resume already in flight is closed once it returns. While a `resume(id)` is pending it resolves `{}` at once and sends `session/close` after the last pending resume returns, unless a newer `resume(id)` reopened the session; a superseded `resume()` that succeeds rejects with `closed`. |
| `delete(id)` | `session/delete` |
| `prompt(id, blocks, {commandId?})` | `session/prompt`; returns a promise with a synchronous `.commandId` |
| `cancel(id, {commandId?})` | `session/cancel`; resolves once sent |
| `claimWriter(id)` / `setModel(id, model)` | `_tantra/session/claim_writer` / `session/set_config_option` |

- **Options:** `url` is called on every dial, so it can mint a fresh ticket. `cwd` defaults to `/`. `webSocketFactory` and `timing` (`reconnectBaseMs` 1 s, `reconnectCapMs` 30 s, `reconnectStableMs` 10 s) are test seams.
- **State:** `onState({connection, sessions})` reports `closed`, `connecting`, `open` or `failed`, and `{writer, cursor, paused}` per open session. `paused` means prompts are held until this client is writer again.
- **Ordering:** every inbound frame (responses, updates, requests) is handled in one ordered chain per connection. `onUpdate` is awaited, and a session's cursor advances only after it resolves. `resume()` resolves after its replayed updates were handled, because replay precedes the response.
- **`onUpdate` must not await client calls.** The response would queue behind the running handler and never arrive. Start the call and let it finish later.
- **A throwing `onUpdate`** reports `handler_failed`, closes the socket and moves to `failed`. Cursors are kept, so a later `connect()` resumes after the last applied update on a fresh chain.
- **Replay:** an open session with a cursor resumes with `_tantra_after` that cursor; otherwise from `start`. Updates at or before the applied cursor are dropped.
- **Requests:** `onRequest({method, sessionId, params, respond, signal})` receives `session/request_permission` and `elicitation/create`. It is called in order but not awaited, so a dialog can stay open while updates keep flowing; answer later with `respond()`. `$/cancel_request` and disconnects abort `signal`, and `respond()` after that is a no-op. A throwing or rejecting `onRequest` reports `handler_failed` and answers `-32603`. Without `onRequest`, and for any other method, requests answer `-32601`.
- **Frames over 1 MiB** are rejected locally with `too_large` and never sent.
- **Deletion:** a `_tantra_notice` `session_deleted` on an open session drops it and its known descendants, rejects their prompts, and reports `not_found` through `onError` before the notice reaches `onUpdate`. No `session/close` is sent; the agent already closed them.

**Reconnect.** Any close not caused by `disconnect()` waits, then redials with jittered backoff: each wait is `min(30 s, 1 s × 2ⁿ) × [0.5, 1]`. Every open session then resumes `read` from its cursor, in open order, so roots come before their children. A session that answers `-32002` is dropped, its prompts are rejected, and it is reported; any other resume error closes the connection and counts as a failed dial. A connection that closes less than `reconnectStableMs` (10 s) after reaching `open` also counts as a failed dial, so a server whose streams fail right after go-live does not cause an endless reconnect loop. After 10 failed dials the client is `failed` and reports `dial_failed`. The count resets only after a connection has stayed `open` for `reconnectStableMs`, or on `connect()`. Reclaiming the writer is explicit.

**Held prompts.** Each session has a FIFO outbox sent one prompt at a time. A session that was writer when the connection dropped is paused: its unanswered prompt goes back to the head, and nothing is sent until this client is writer again (`claimWriter`, `resume(id, {mode: "write"})`, or a `claimed` update). Prompts made while paused queue behind it. The retry reuses the same `commandId` and blocks, so the server answers the original `messageId`; a retry that gets `writer_required` is held again, since its outcome is still unknown. A prompt that was never sent, from a session that was not paused, sends normally and a non-writer gets `writer_required`. `close`, `delete`, `disconnect` and `failed` reject outstanding prompts with `uncertain` if they were sent, otherwise `closed`.

| `TantraAcpError.code` | Meaning |
|---|---|
| `writer_required`, `command_conflict`, `unavailable`, `model_busy` | The server's `data.tantra.code`. `unavailable` is not retried; the server already retried. |
| `not_found`, `invalid_params`, `internal` | `-32002` or a `session_deleted` notice, `-32602`, any other server error |
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
