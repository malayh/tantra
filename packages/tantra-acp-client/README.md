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

See the [ACP server reference](https://malayh.github.io/tantra/docs/reference/acp/) ("TypeScript client") for the full contract.

## Transcript

A pure, framework-free reducer folds updates and requests into a `Transcript`: a `SessionView` per session (`entries`, `state`, `title`, `updatedAt`, `configOptions`, `queue`, `writerReason`), a subagent `roster` and the pending requests. Every function returns a new object, or the same one when nothing changed.

```ts
import {
  addRequest,
  apply,
  askOf,
  empty,
  removeRequest,
} from "tantra-acp-client";

let transcript = empty();
const client = new TantraAcpClient({
  url,
  onUpdate: (sessionId, update) => {
    transcript = apply(transcript, sessionId, update);
  },
  onRequest: (request) => {
    transcript = addRequest(transcript, request);
    request.signal.addEventListener("abort", () => {
      transcript = removeRequest(
        transcript,
        request.sessionId,
        askOf(request).askId,
      );
    });
  },
});
```

- Entries are `user`, `agent`, `thought`, `tool`, `message` (subagent traffic and `Runtime.deliver` events, with `sender`, `recipient`, `text`, `reply`, `finished`, and `source` from `_meta.tantra.source`, else `null`) and `status` (a cursored `idle` that was cancelled, interrupted or failed).
- `askOf(request)` gives `{kind: "approval" | "choice" | "free_text", askId, title, body, options, respond}`.
- `reset`, `descendants`, `treeRunning` and `pendingRequest` cover reopening a session, the subagent tree and the next ask.

## Development

```sh
npm ci
npm run lint
npm run typecheck
npm test
npm run test:consumer
```

The PostgreSQL integration test runs from the repository root: `pytest packages/tantra/tests/test_acp_client_integration.py`.
