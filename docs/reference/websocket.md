# SessionSocket

`SessionSocket` is a native ASGI WebSocket application. It uses committed Runtime streams and commands. Start and stop the Runtime in your application's lifespan; closing a socket leaves accepted work running. No FastAPI or Starlette dependency is required.

```python
from tantra import InputCodec, SessionSocket, SocketAuth, SocketBinding

async def bind(scope):
    session = await authenticate_application_request(scope)
    return SocketBinding(
        session.root_id,
        SocketAuth(
            principal=session.user,
            submitted_by=str(session.user.id),
            expires_at=session.login_expires_at,
            revalidate=session.revalidate,
            revoked=session.revoked,
        ),
    )

endpoint = SessionSocket(
    runtime,
    bind=bind,
    policy=access_policy,
    input_codec=InputCodec(MessageSchema, encode_input),
    views=open_event_view,
    notices=open_application_notices,
    allowed_origins={"https://app.example"},
)
```

Mount the callable in your ASGI router. For Starlette/FastAPI, `WebSocketRoute("/sessions/{id}/socket", endpoint)` accepts the native callable object. HTTP session creation, listing, history, deletion, quotas, credentials, and tool execution permissions remain application-owned.

## Authentication

`bind(scope) -> SocketBinding` is async. It resolves the authenticated identity and existing root session using application middleware, cookies, tickets, or headers. Never trust a client-supplied user ID as authentication.

`SocketAuth` is keyword-only:

| Field | Contract |
|---|---|
| `principal` | Opaque application identity. `None` requires explicit `anonymous=True`. |
| `revalidate` | Async zero-argument callback returning exactly `True` while the identity remains valid. Required even for explicitly public access. |
| `submitted_by` | Stable trusted audit identity, or None. Persisted by Runtime; never derived from a command frame. |
| `expires_at` | Optional timezone-aware credential expiry; enforced without client traffic. None allows non-expiring application credentials. |
| `revoked` | Optional `asyncio.Event`; setting it closes the socket promptly. |
| `anonymous` | Explicit application opt-in, default False. |

Authenticate and authorize root reading before accepting. Revalidate before subscriptions, writer acquisition, and every mutating command, plus every 30 seconds. Protected delivery pauses during revalidation. Denial or uncertain revalidation closes the socket; reconnect requires a new successful binding. Expiry and revocation interrupt stalled revalidation.

Origin is checked before acceptance against explicit strings; wildcards are rejected. Missing Origin is denied by default. `allow_missing_origin=True` permits authenticated non-browser clients only; anonymous connections still require an allowed Origin. Origin does not replace authentication.

## Policy

`policy(access: SocketAccess) -> bool` is async and must return exactly `True` to grant access. Its fields are:

- `operation`: `subscribe`, `acquire_writer`, `send`, `answer`, or `cancel`.
- `binding`: server-resolved root and authentication.
- `root`, `actor`: independently copied, server-loaded headers; descendants are checked through durable parent relationships.
- `input`: validated application input for `send`.
- `response`, `ask`: typed answer and original `LocatedAsk` descriptor for `answer`.

Periodic checks repeat read policy and writer-acquisition policy. Loss of writer permission downgrades the subscription to reading. Loss of read permission closes the socket. Commands always receive their own policy check; writer control is not authorization.

Approval policies use `ask.event.request`, including durable permission and transformed arguments in `extra`. Ignore display text for resource authorization. Deny legacy descriptors lacking required evidence. Runtime still decides pending state, expiry, typed response compatibility, and idempotency. Reauthorize sensitive effects in the tool/application execution path: disconnect or revocation cannot undo an accepted effect.

## Input and views

`InputCodec(schema, encode)` validates input with Pydantic strict validation. The synchronous encoder returns a deterministic Runtime input string and performs no business writes. Applications own schema bounds, domain validation, and snapshot semantics. Frozen retry input must encode identically across workers and deployments.

`views(context: SocketViewContext) -> EventView` is async. Context contains `binding`, copied `root` and `actor` headers, and `after`. Return an async mapper accepting an independently copied `LoggedEvent` and returning JSON or None. Each subscription has its own mapper state; seed correlation at the supplied cursor or redact conservatively. Writer transitions recreate the mapper at the queued committed cursor so cancelled mapping cannot corrupt correlation. An optional mapper `aclose()` is awaited on replacement and closure.

Every committed event produces a frame. A hidden event has `body: null` and still advances the cursor. No default view is supplied: tool arguments, skill bodies, metadata, and inputs may require redaction. No projection hook is installed in Runtime.

`notices(binding)` optionally returns an async iterator of application JSON. Notices have no journal sequence, no replay guarantee, and no cursor. Cancellation closes the iterator. Schedule durable application work independently of this socket lifecycle.

## Wire protocol

Offer WebSocket subprotocol `tantra.session.v1`. Frames are JSON text objects with `type`. UUIDs use lowercase hyphenated canonical form. Unknown fields, duplicate JSON keys, non-finite numbers, binary frames, and malformed values are rejected.

| Client type | Fields |
|---|---|
| `subscribe` | `subscription_id`, `actor_id`, `after` (default 0), `mode` (`read` default, or explicit `write`) |
| `unsubscribe` | `subscription_id` |
| `acquire_writer` | `subscription_id` |
| `send` | `subscription_id`, `command_id`, `input` (application JSON) |
| `answer` | `subscription_id`, `command_id`, `ask_id`, `response` |
| `cancel` | `subscription_id`, `command_id` |

Answer response is exactly one of `{"kind":"approval","allow":true}`, `{"kind":"choice","selected":"..."}`, or `{"kind":"free_text","text":"..."}`. No client approval descriptor or attribution is accepted.

One active subscription per actor per socket; use a fresh UUID for each subscription. Unsubscribe before replacing it. Descendant subscriptions are explicitly requested and read-only. Subscribe from the last successfully applied sequence; cursors beyond the current journal are rejected.

| Server type | Fields |
|---|---|
| `event` | `subscription_id`, `actor_id`, `seq`, `body` |
| `ready` | `subscription_id`, `actor_id`, `cursor`, `mode`, `active` |
| `receipt` | `subscription_id`, `command_id`, `operation`, `duplicate` |
| `writer_lost` | `subscription_id` |
| `unsubscribed` | `subscription_id` |
| `error` | `code`, `message`, optional `subscription_id`/`command_id`, `retryable`, `uncertain` |
| `notice` | `body` |

`ready` follows delivery of the initial committed watermark. Its cursor describes that replay boundary; new events can follow immediately. Root mutations require a delivered `ready` with `mode: "write"`. An explicit acquisition emits another `ready`. Writer replacement emits `writer_lost`, retains reading, and requires explicit reclaim. Reconnect has no remembered writer privilege; clients should initially subscribe read-only.

Receipts mean durable acceptance, not turn completion. Any disconnect or uncertain error before a receipt requires retrying the same command ID, input/response, and authenticated attribution. Never generate a new command ID to retry an unknown outcome. Ignore frames belonging to retired subscription IDs.

Command errors include `forbidden`, `invalid_input`, `invalid_request`, `command_conflict`, `ask_expired`, `writer_required`, `unknown_outcome`, `unavailable`, and `runtime_error`. Error messages expose no exception details. `unknown_outcome` and infrastructure command errors explicitly mark uncertain retryable outcomes.

Close codes: 1002 invalid protocol, 1008 authentication/read-policy denial, 1009 oversized frame, 1011 infrastructure/callback failure, 1012 Runtime shutdown, 1013 stalled client, and 4404 deleted/missing session. A denial before acceptance becomes a rejected handshake under ASGI; applications needing HTTP error details should provide their own HTTP entry point.

## Bounds and lifecycle

`SocketLimits` defaults:

- Eight subscriptions; 256 KiB inbound and 1 MiB outbound frames.
- Queue of at most 256 frames / 4 MiB, including the in-flight send.
- Ten-second queue/send capacity timeout; healthy replay applies backpressure. Application binding, policies, view creation, and event mapping use the same deadline.
- Thirty-second auth/read-policy revalidation interval.

Replay uses Runtime paging without a total-history cutoff. Applications may start from a projection snapshot watermark. Subscription cancellation and replacement discard obsolete queued frames. Disconnect closes streams, writer connections, and notice iterators without closing Runtime or cancelling accepted turns. Runtime deletion and shutdown terminate streams and release interests.

## TypeScript client

`tantra-session-client` is a native ESM package with declarations and no runtime dependencies. One `SessionClient` is permanently scoped by `serverScope`, canonical root UUID, and authenticated identity key. The identity key scopes local state and is not authentication. Its async `url()` callback runs before every dial so the application can issue a fresh ticket. Native `WebSocket` is the default; `webSocketFactory` supplies the same seam for tests or non-browser environments.

```ts
import { SessionClient } from "tantra-session-client";

const client = new SessionClient({
  serverScope: "https://api.example",
  rootId,
  identity: user.id,
  url: async () => socketUrl(await issueTicket()),
  initialCursors: { [rootId]: restoredProjection.cursor },
  onEvent: async (event) => applyEvent(event),
  onError: (error) => reportError(error),
});

const root = client.subscribe(rootId);
await client.connect();
await root.ready();
await client.acquireWriter();
const command = client.send({ message: "hello" });
console.log(command.commandId);
await command;
```

Subscriptions expose `state`, `ready()`, and `unsubscribe()`. They reconnect from the last successfully applied cursor with a fresh transport subscription ID. Async `onEvent`, `onNotice`, and `onReceipt` callbacks are serialized and awaited. An event, including a hidden event with `body: null`, advances its actor cursor only after `onEvent` succeeds and the optional persistence save completes. Handler failure blocks processing. `onState` reports connection, subscription, and outbox state; `onError` receives protocol, transport, command, storage, and application failures.

All subscriptions start read-only. Root mutation requires `acquireWriter()` after readiness. Writer loss and reconnect hold commands and require another explicit acquisition; descendants stay read-only. Writer-acquisition timeout retires the ambiguous transport and reconnects read-only for explicit reclaim. `send`, typed `answer`, and `cancel` return a promise with a synchronous `commandId` property and resolve only on a durable receipt. `onReceipt` observes the same acceptance separately.

Commands retain their UUID and frozen payload through the initial delivery and three automatic retries. Retry exhaustion rejects the promise with an uncertain `SessionClientError` whose code is `unknown_outcome`, leaves that command in manual state at the outbox head, and holds later commands. `retry(commandId)` starts another bounded cycle without changing its content. Definitive command rejection releases the next command; `writer_required` instead waits for explicit writer acquisition.

Persistence is optional and memory is the default. A `Persistence` adapter provides async `load()` and `save(PersistedState)` methods. State is versioned and bound to the exact server/root/identity scope. It stores desired actors, validated cursors, frozen commands, and attempt counts, never credentials, ticket URLs, transport IDs, writer grants, notices, or transcript data. Concurrent owners of one persisted outbox are unsupported. Storage failure blocks transmission and event advancement for manual recovery.

Persisted actor cursors are validated but never blindly restored as replay boundaries. The application must restore its projection first and pass exactly matching `initialCursors`; otherwise actors replay from zero. The adapter cannot make projection updates and cursor saves atomic, so applications needing durable resume must couple them in their own storage design.

`timing` overrides receipt timeout, the three retry delays, reconnect base/cap, and capacity timeout. Defaults are ten seconds for receipts and capacity, one/two/four seconds for command retries, and jittered reconnect backoff from one to 30 seconds for at most ten consecutive failed dials.

Sarathi supplies an application integration in `apps/sarathi`: root-scoped handshake tickets, explicit ownership/approval policies, a raw owner-only event view, header notices, and a React adapter around the client. See `apps/sarathi/README.md` for deployment and development. Osuite adoption remains separate. The endpoint and package do not install application HTTP routes or UI reducers automatically.

A root subscription with pending commands cannot be unsubscribed. Use `disconnect()` to leave safely while retaining its unresolved outbox.
