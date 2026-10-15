# tantra-acp-client

TypeScript client for tantra's ACP v2-inspired WebSocket endpoint (`tantra.acp`), with the `tantra` profile: cursors, command IDs, the fenced writer, and reconnects. Native ESM with declarations, no runtime dependencies, native `WebSocket`.

```ts
import { TantraAcpClient } from "tantra-acp-client";

const client = new TantraAcpClient({
  url: async () => `wss://app.example/api/acp?ticket=${await ticket()}`,
  onUpdate: (sessionId, update) => reducer.apply(sessionId, update),
  onRequest: (request) => asks.show(request),
  onState: (state) => store.setConnection(state),
  onError: (error) => console.warn(error.code),
});

await client.connect();
const { sessionId } = await client.newSession();
const prompt = client.prompt(sessionId, [{ type: "text", text: "Hello" }]);
const { messageId } = await prompt;
```

- Updates, responses and requests are handled in one ordered chain; a session's cursor advances only after `onUpdate` resolves.
- `onUpdate` must not await client calls: the response would queue behind the handler.
- `onRequest` is called in order but not awaited; answer with `respond()` whenever the user decides.
- After a dropped connection, every open session resumes read-only from its cursor. A session that was writer is paused (`paused: true` in `onState`): its unanswered prompt and any prompt made while paused are held until `claimWriter`, `resume(id, { mode: "write" })` or a `claimed` update, then sent with their original `commandId`.
- Transcript reduction is left to the application.

See `docs/reference/acp.md` ("TypeScript client") for the full contract.

## Development

```sh
npm ci
npm run lint
npm run typecheck
npm test
npm run test:consumer
```

The PostgreSQL integration test runs from the repository root: `pytest packages/tantra/tests/test_acp_client_integration.py`.
