# Tantra session client

`tantra-session-client` is the framework-independent TypeScript client for Tantra's `tantra.session.v1` WebSocket endpoint. It is a native ESM package with declarations and no runtime dependencies. Build locally with `npm ci` and `npm run build` in this directory, then add the package as a local dependency in your application. Registry publication is separate.

```ts
import { SessionClient } from "tantra-session-client";

type Input = { message: string };
type ProjectedEvent = { type: string; text?: string };

const rootId = "12345678-1234-1234-1234-123456789abc";
const restoredCursor = 0;

const client = new SessionClient<Input, ProjectedEvent>({
  serverScope: "https://api.example",
  rootId,
  identity: "user-42",
  url: async () => {
    const ticket = await issueSocketTicket();
    return `wss://api.example/sessions/${rootId}/socket?ticket=${ticket}`;
  },
  initialCursors: { [rootId]: restoredCursor },
  onEvent: async (event) => {
    await applyProjectedEvent(event.actor_id, event.seq, event.body);
  },
  onError: (error) => reportSocketError(error),
});

const root = client.subscribe(rootId);
await client.connect();
await root.ready();
await client.acquireWriter();

const accepted = client.send({ message: "hello" });
console.log(accepted.commandId);
const receipt = await accepted;
```

`serverScope`, `rootId`, and `identity` permanently scope one client instance and any persisted state. `identity` is a stable storage key, not authentication. The `url` callback runs for every dial so it can return a fresh ticket. Browsers use native `WebSocket`; tests and non-browser runtimes can provide `webSocketFactory(url, "tantra.session.v1")`.

Subscribe to the root before sending commands. Subscriptions begin read-only and expose `state`, `ready()`, and `unsubscribe()`. `ready()` resolves after replay through the advertised cursor has been applied. Call `acquireWriter()` explicitly before `send`, `answer`, or `cancel`. Reconnects and writer loss return the root to read-only; writer access is never restored automatically. Descendants remain read-only. If writer acquisition times out, the client retires that transport and reconnects read-only; reclaim explicitly after the new subscription is ready.

`onEvent`, `onNotice`, and `onReceipt` may be async. The client serializes and awaits them. It advances an actor cursor only after `onEvent` succeeds, including events whose body is `null`. A callback failure blocks the client and is reported through `onError`. `onState` exposes connection, subscription, cursor, writer, and outbox state.

Commands resolve when the server returns a durable receipt, not when a turn finishes. Each returned promise has a synchronous `commandId` property. Pass `{ commandId }` to reuse an application-generated canonical UUID. Inputs and answers are cloned and validated before enqueueing, so later caller mutation cannot change a retry.

The client makes the initial delivery plus three automatic retries using the same command ID and frozen payload. If the outcome remains uncertain, the promise rejects with `SessionClientError` code `unknown_outcome`, the command stays at the head of the outbox with status `manual`, and later commands remain held. After investigating, call `client.retry(error.commandId)` to start another bounded attempt cycle with the same command.

State is memory-only unless a `Persistence` adapter supplies async `load()` and `save(state)`. Persisted state is versioned and must match `serverScope`, `rootId`, and `identity`; it contains actor identities, cursors, frozen commands, and attempt counts. It never contains credentials, ticket URLs, transport subscription IDs, writer grants, notices, or transcript data. Desired subscriptions are saved after loading and whenever one is added, even before it receives events. Use one owner per persisted outbox.

Saved cursors never select the replay boundary by themselves. Restore the application projection first, then pass its matching actor cursors through `initialCursors`. If no initial cursor is supplied, the actor replays from zero even when persisted state contains a later cursor. This avoids skipping history into an empty reducer. A persistence load or save failure blocks processing for manual recovery.

`timing` can override receipt timeout, three retry delays, reconnect base/cap, and socket capacity timeout for deterministic tests or deployment tuning. Production defaults are a ten-second receipt timeout, retry delays of one, two, and four seconds, reconnect backoff from one to 30 seconds, and a ten-second capacity timeout.

A root subscription with pending commands cannot be unsubscribed. Use `disconnect()` to leave safely while retaining its unresolved outbox.
