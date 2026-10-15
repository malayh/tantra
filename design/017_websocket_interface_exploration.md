# 017 — Pluggable WebSocket interface

**Status:** Superseded for transport by 018. P0 Runtime additions retained (`submitted_by`, `lookup_ask`, migration 12).

## Goal

Provide a native ASGI WebSocket endpoint and framework-independent TypeScript client, then adopt them in Sarathi. Applications plug in authentication, permissions, input validation, event visibility, and notices.

## Scope

- **In:** private/team chats, editor snapshots, approvals, service clients, read-only observers, descendants, durable attribution, native ASGI transport, TypeScript client, Sarathi adoption.
- **Out:** Osuite adoption, HTTP session/history routes, application identity models, UI reducers, billing, quotas, execution permissions, paid inference.

## Decisions

- Wrap public Runtime streams and commands. Runtime owns ordering, replay, recovery, command identity, fencing, and commit-before-delivery.
- Applications own authentication and authorization. Writer ownership grants concurrency control, never application access.
- Use native ASGI with existing dependencies; add no FastAPI/Starlette runtime dependency. The client uses native WebSocket, ESM, and declarations, without React or a state-library dependency.
- Preserve Sarathi's full-history display. Replay is paged and memory-bounded; there is no default total-history cutoff. Applications may supply snapshot watermarks.
- Persist optional attribution separately from Runtime actor identity. Applications authorize every approval against its durable descriptor and reauthorize protected effects at execution.
- Deploy coordinated workers together before relying on attribution. Deploy Sarathi backend/client together; old wire clients require refresh.

## Application interface

```python
endpoint = SessionSocket(
    runtime,
    bind=bind_session,
    policy=access_policy,
    input_codec=InputCodec(MessageSchema, encode_input),
    views=open_event_view,
    notices=open_application_notices,
    allowed_origins=origins,
)
await endpoint(scope, receive, send)
```

- `bind(scope) -> SocketBinding` resolves the root and authenticated session through application middleware, cookies, tickets, or headers.
- Authentication supplies an opaque principal, optional stable `submitted_by`, credential expiry, revalidation callback, and optional revocation event. Revalidate before mutations and every 30 seconds; enforce expiry independently of client traffic.
- Policy authorizes subscribe, writer acquisition, send, answer, and cancel using server-loaded root/actor information, validated input, and durable ask details.
- The input codec validates application JSON and deterministically encodes Runtime input. Retries retain their original payload; encoding performs no business writes.
- A subscription-local event view starts at a cursor and maps committed events to JSON or null. Applications seed correlation state or redact conservatively.
- Optional application notices are outbound-only iterators for title/model updates. They have no journal cursor or durable delivery guarantee.
- Missing authentication/policy fails closed. Anonymous access requires explicit application configuration. Credential parsing, quotas, billing, and tool authorization stay application-owned.

## Durable attribution and approval details

- `Connection.send`, `prompt`, `answer`, and `cancel` gain optional keyword-only `submitted_by: str | None`. Non-null identities are exact, nonempty strings of at most 256 characters; no normalization.
- Persist attribution on `InputQueued`, `AskAnswered`, and `CancellationRequested`, and propagate input attribution to `TurnContext` and tool `Context`. Do not automatically include it in model prompts.
- Preserve `AskAnswered.answered_by` as the Runtime root actor. Changed attribution under the same command ID raises `InvalidCommandReuse`; legacy missing attribution means None and is never reassigned.
- Attribution is audit context, not a grant. Internal actor commands remain unattributed; applications control delegated execution scope.
- `Runtime.lookup_ask(root_id, ask_id) -> LocatedAsk | None` returns the original `AskRaised`, actor UUID, and sequence. Validate the root; lookup performs no recovery or provider work and says nothing about pending state.
- Duplicate matching asks are ambiguous and raise `ValueError`. Unknown ask IDs return None; missing roots raise `SessionNotFound`; live child IDs are rejected.
- PostgreSQL optionally implements indexed `lookup_ask`; other/custom stores use a full-read fallback. Keep required Store/Coordinator protocols unchanged.
- Permission approvals persist structured post-hook arguments in `Approval.extra['arguments']` alongside `permission`. Arguments must be JSON-serializable. Policies never parse display text or trust client descriptors.
- Runtime remains authoritative for answer expiry/idempotency. Policies requiring resource details deny legacy asks that lack them.

## Transport and client

- Subprotocol: `tantra.session.v1`. Strict JSON frames and canonical UUIDs.
- Client operations: subscribe/unsubscribe, send, typed answer, cancel. Each subscription specifies actor, cursor, read/write mode, and unique subscription ID.
- Server frames: projected event, replay-ready state, committed receipt, writer loss, sanitized error, application notice. Subscription frames echo the subscription ID.
- Each actor has an independent cursor. Hidden events carry `body: null` and advance it. Ignore late frames from replaced subscriptions.
- Root commands require a writable, caught-up subscription. Descendants are explicitly subscribed and read-only.
- Writer replacement downgrades to reading; reconnect starts read-only; reclaim is explicit.
- Receipts mean durable acceptance, not completion. Uncertain failures retry the same frozen command.
- Install no projection `on_event` hook and duplicate no coordinator observation machinery.
- Defaults: eight subscriptions, 256 KiB inbound, 1 MiB outbound, queue bounded to 256 frames/4 MiB. Await queue capacity with a ten-second timeout so healthy replay bursts survive while stalled clients disconnect.
- The TypeScript client owns independent cursors and a bounded FIFO frozen-command outbox. Retry at most three times, then require manual intervention; hold commands after writer loss.
- Advance cursors only after application event handling succeeds. Bound incoming queues; overflow reconnects from the last applied cursor. Persistence is opt-in and scoped to root plus authenticated identity.

**Concrete P1 contracts:** import `SessionSocket`, `SocketAuth`, `SocketBinding`, `SocketAccess`, `SocketViewContext`, `InputCodec`, and `SocketLimits` from Tantra. `SocketAuth` carries an async zero-argument boolean revalidator; the policy is an async `SocketAccess -> bool` callback. View factories return async event mappers and reseed at the queued committed cursor on writer transitions; optional mapper `aclose()` releases resources. Uncertain read revalidation pauses delivery and closes the connection. The wire uses `type`, canonical UUID subscription IDs, one subscription per actor, explicit `acquire_writer`, and `unsubscribed` acknowledgments. [Reference contracts](../docs/reference/websocket.md) specify every frame, error, plugin field, and bound.

## Considered & rejected

- **Framework-specific endpoint:** native ASGI works with application-selected frameworks without a runtime dependency.
- **Standardize identity, HTTP routes, history reducers, or UI:** applications have different ownership and visibility rules.
- **Use writer tokens as authorization:** concurrency ownership does not establish resource access.
- **Parse approval display text:** hook transformations and presentation make it unreliable authorization evidence.
- **Nonblocking queue overflow on every full queue:** healthy replay bursts can fill a bounded queue before the sender runs; timed capacity waits provide backpressure.
- **Migrate Osuite in the same spec:** its history/proposal contracts need separate adoption work.

## Implementation phases

### P0 — Durable command attribution and ask lookup · deps: none · ✅ DONE

**Deliver**
- Implement attribution, context propagation, duplicate comparisons, and durable approval descriptors.
- Add PostgreSQL migration 12: ask identity projection and partial lookup index. Backfill through the existing codec in keyset batches of at most 1,000 events; publish atomically with writers stopped.
- Preserve original journal bodies/sequences. Use compatible full-read lookup fallback outside PostgreSQL.

**Verify**
- Retries, changed attribution, original cancellation targets, lost replies, recovery, legacy events, transformed arguments, ambiguous asks, root isolation, and lookup without activation.
- Migration upgrade, repeat setup, both envelopes, NUL payloads, rollback/retry, and bounded PostgreSQL lookup on long journals.
- Focused checks, `just lint`, package/bench tests, PostgreSQL stress tests with zero skips, and one independent Ponytail review.

**Checklist**
- [x] Attribution and context
- [x] Durable descriptors and lookup
- [x] Migration and bounded reads
- [x] Correctness, project checks, documentation, and review

**Verified:** 976 package tests; 148 stress/bench tests with zero skips; 89 targeted checks after the review fix, including four additional parity/read-bound cases; `just lint` and diff checks. Owned Compose PostgreSQL used `fsync=on` and `synchronous_commit=on`; workers and volumes were removed afterward. Normal query plans used `journal_ask_idx` on 4,000/100,000-event journals and read one original body. Independent review found duplicate asks could read bodies before limiting; lookup now limits identity evidence first, and 1,000 duplicate asks read at most two bodies. Runtime additions are frozen for dependent phases.

### P1 — Native ASGI endpoint · deps: P0 · ✅ DONE

**Deliver**
- Implement protocol models, plugin interfaces, authentication lifecycle, ordered delivery, replay readiness, writer transitions, and bounded cleanup.
- Check Origin before accepting. Reject missing Origin unless explicitly enabled for authenticated non-browser clients.
- Revalidate read access periodically; pause protected delivery when access expires or becomes uncertain.

**Verify**
- Private, anonymous, team-role, editor, approval, observer, and descendant policies with deterministic fixtures.
- Reconnect, filtered replay, slow clients, deletion, shutdown, revocation, and two-worker ownership changes.

**Checklist**
- [x] Protocol and plugin contracts
- [x] Authentication and delivery lifecycle
- [x] Policy fixtures, transport checks, documentation, and review

**Verified:** 1,046 package tests; 148 stress/bench tests with zero skips; 70 focused transport/policy checks after final hardening, including four additional cases; `just lint`, strict documentation build, and diff checks. Deterministic ASGI fixtures covered private, anonymous, team, editor, approval, observer, and descendant access; expiry/revocation during stalled callbacks; paged/filtered replay; queue bounds; stale readiness; projection cancellation/reseeding; and cleanup. Two PostgreSQL Runtime instances verified writer replacement/reclaim, disconnect/reconnect, deletion with LISTEN unavailable, approval rollback, lost committed receipts, and exact retries. Independent Ponytail review reported no actionable findings. Owned Compose PostgreSQL used `fsync=on` and `synchronous_commit=on`; resources were removed after verification. Wire and plugin contracts are frozen for P2/P3. Transport scale and Sarathi browser checks remain in P3.

### P2 — TypeScript client · deps: P1 · DONE

**Goal:** ship a standalone client for the frozen P1 endpoint; Sarathi adoption remains P3.

**Deliver**
- Add `packages/tantra-client`, a local ESM package named `tantra-session-client`, with JavaScript exports and TypeScript declarations. Use native WebSocket, TypeScript tooling, and Node's test runner; no runtime dependency or root JavaScript workspace. Registry publication is separate.
- Export `SessionClient`, subscription handles, typed answers, receipts/errors/state, and JSON input/event generics. Provide connect/disconnect, subscribe/unsubscribe, explicit writer acquisition, send/answer/cancel, retry, and event/notice/receipt/error/state callbacks. Mutation promises resolve on committed receipts, never on turn completion.
- Fix each instance to one server scope, root UUID, and authenticated identity key. Resolve the URL asynchronously on every dial so applications can issue fresh tickets. Credentials, HTTP calls, schemas, and UI reducers stay application-owned; the identity key scopes state and is not authentication. Allow a socket-construction seam for tests/non-browser environments.
- Extend the WebSocket reference with client usage and failure/persistence contracts. Keep Sarathi dependencies, hooks, Docker files, and application behavior unchanged.

**Subscriptions and delivery**
- Offer and verify `tantra.session.v1`. Validate frame discriminants/fields, canonical UUIDs, finite JSON, and nonnegative safe-integer cursors. Reject non-JSON inputs before serialization silently alters them. Share serialized fixtures with Python tests without adding a generator framework.
- Retain desired actors across reconnects; use fresh subscription UUIDs and initially subscribe read-only from each actor's applied cursor. One subscription per actor, at most eight. Ignore retired IDs/old socket generations; wait for unsubscribe acknowledgment before replacing an actor on the same socket.
- Serialize async application handling across subscriptions. Advance cursors only after successful handling, including hidden `body: null` events. Ready becomes usable only after earlier events are applied; its cursor must match the applied replay boundary. Notices/receipts never advance cursors.
- Verify actor identity and contiguous ordering; ignore already-applied duplicates. Gaps reconnect from applied cursors. Handler failure pauses replay for explicit retry; reducers must tolerate replay after partially applied failures.
- Bound incoming state to 256 frames/4 MiB, including the executing handler, and 1 MiB per frame. Overflow reconnects from applied cursors after the old in-flight handler settles. Successful old handling still advances its actor cursor; replacement/replay cannot race that callback.
- Writer loss/closure immediately holds commands. Keep reading after writer loss, require explicit reclaim, and never restore write privilege automatically or persist it. Descendants stay read-only. Transmission requires a fresh writable Ready.

**Commands and reconnect**
- Freeze operation, command UUID, root/actor, input or ask/response at enqueue. Rebind only the subscription UUID after reconnect. Caller mutation cannot alter commands or callback snapshots; conflicting local UUID reuse and oversized/invalid frames fail before sending.
- Use a FIFO outbox bounded to 64 commands/1 MiB, including the in-flight command. Reject command frames above 256 KiB; bound native socket buffered bytes to 4 MiB with a ten-second capacity timeout. Allow one outstanding receipt; matching receipt, including `duplicate: true`, advances the queue. Journal events never acknowledge acceptance.
- Default receipt timeout: ten seconds. Allow the initial send plus three automatic retransmissions across reconnects, with one-, two-, and four-second retry delays. Lost receipts/uncertain retryable errors resend exact content. Exhaustion pauses the uncertain head for explicit retry with the same UUID; later mutations remain held. Explicit retry starts another bounded attempt cycle without changing content.
- Definitive rejection reports failure and releases the next command. `writer_required` holds for explicit reclaim despite its nonretryable wire flag. Validate receipt subscription, command, and operation; stale/unrelated receipts cannot remove an entry.
- Reconnect infrastructure/slow-client closures with jittered backoff from one second to 30 seconds, bounded to ten consecutive failed dials. Stop on authentication/policy denial, deletion, malformed frames/size violations, or handler failure. Expose manual recovery. Browser handshake rejection may be indistinguishable from infrastructure; report ambiguity rather than asserting an auth diagnosis.
- Disconnect releases sockets/listeners/timers/queues/subscriptions without cancelling accepted work. Retain unresolved commands and applied cursors for same-scope reconnect. Root/identity changes require a new client and never migrate its outbox. Root unsubscribe requires a settled outbox; disconnect retains unresolved commands.

**Optional persistence**
- Default to memory. Offer a minimal async load/save adapter for versioned, bounded state scoped to server/root/authenticated identity. Load before connecting; persist frozen commands and attempt counts before transmission. Storage failure holds transmission; restored in-flight commands remain uncertain.
- Never store credentials, ticket URLs, transport subscription IDs, writer grants, notices, or transcript. Reject invalid/mismatched saved state. Applications choose storage/security; concurrent sharing of one persisted outbox is unsupported.
- Starting cursors must match application-restored projections/snapshots. Persisted cursors alone never skip history for an empty reducer. The adapter cannot make UI state and cursor saves atomic; applications requiring durable resume must couple them. Cursor-save failure pauses processing and preserves the previous durable watermark.

**Verify**
- P1 fixture parity: every frame/answer, hidden events, strict fields, IDs, non-finite values, unsafe cursors, and size bounds.
- Deterministic socket/clock tests: lost receipts, exact retries, FIFO blocking, exhaustion, rejection, writer loss/reclaim, mutation isolation, stale generations, replacement, callbacks during reconnect, overflow, gaps, and cleanup.
- Persistence tests: wrong identity/root/server, malformed/oversized state, save failure, restart after uncertain send, retained attempt counts, and projection/cursor mismatch. A new identity never transmits another identity's command.
- Run the built native client against P1 ASGI and two Runtime instances using deterministic providers and owned durable Compose PostgreSQL. Cover root/descendant replay, typed approvals, writer replacement, lost committed receipts/retries, reconnect, and deletion. Authenticated non-browser fixtures explicitly enable missing Origin; production defaults stay unchanged.
- Inspect journals independently for one logical command per UUID, unchanged payload/attribution, ordered replay, and accepted work continuing after disconnect. Require bounded client queues and zero retained socket/subscription interests after closure.
- Run client lint/type/build/tests and an ESM/declaration consumer check; `just lint`, package/bench tests, PostgreSQL stress tests with zero skips, strict docs build, and one independent Ponytail review. Transport scale and Sarathi browser checks remain P3.

**Checklist**
- [x] ESM API, protocol parity, and bounded state
- [x] Retry, reconnect, cursor, and persistence contracts
- [x] Real endpoint/two-instance correctness and resource checks
- [x] Client/project checks, documentation, and independent review

**Verified:** 42 client tests; ESM/declaration consumer checks using ordinary TypeScript interfaces; 1,053 package tests; 148 stress/bench tests with zero skips; client lint/type/build, `just lint`, strict documentation build, and diff checks. The native client exercised both ASGI endpoints over two PostgreSQL Runtime instances: root/descendant replay, writer replacement/reclaim, typed approval, a lost committed answer receipt retried as a duplicate with one tool execution, accepted work finishing after disconnect, read-only reconnect, and deletion releasing all interests. Journal reads independently confirmed ordered evidence, one command per UUID, and unchanged payload/attribution. Independent Ponytail review found three issues, corrected and rechecked: duplicate JSON keys, ambiguous acquisition timeout, and idle subscription persistence. Owned durable Compose resources were removed. No paid inference or backend/wire changes were required in P2.

### P3 — Sarathi adoption · deps: P2 · DONE

**Goal:** ship Sarathi on the frozen endpoint/client without a second transport implementation. Deploy both backends and the UI together; old pages require refresh.

**Deliver**
- Replace `backend/src/sarathi/api/ws.py::SocketBridge` with a native `SessionSocket` mounted through `WebSocketRoute` at the existing `/api/ws/sessions/{session_id}` path. Create it from lifespan-managed resources; do not accept through FastAPI first. Retain HTTP session, upload, actor-status, memory, and model routes.
- Add `POST /api/sessions/{session_id}/socket-ticket`, authenticated by the existing bearer login. Check a persisted root owned by the user. Accept read/write capability selection, default write; return ticket, handshake expiry, original login expiry, and server-derived user identity. A read ticket never permits writer acquisition or mutation.
- Use existing PyJWT/HS256 and `SECRET_KEY`, with a dedicated audience/purpose, root UUID, user ID, capability, issued time, and signed original login expiry. Ticket expiry is `min(now + 30 seconds, login expiry)`; tickets are reusable during that window and accepted by either backend. Keep them in memory and use only `?ticket=...` in the socket URL. Reject login tokens as tickets and tickets as HTTP login credentials; retain query-free proxy/server logs and return `Cache-Control: no-store`.
- Bind a timezone-aware `SocketAuth` to the original login expiry and `submitted_by=str(user.id)`. Revalidation uses a fresh, short-lived database session to check the user; never hold a SQLAlchemy session/connection for the socket lifetime. Policy checks current server-loaded root/actor ownership, durable descendant membership, and the signed capability. Use configured `CORS_ORIGINS` explicitly for socket Origins; production rejects missing Origin.
- Add a strict application message schema for text/attachments and a deterministic encoder preserving existing attachment markers. Keep validation within protocol frame bounds; validate upload paths against the authenticated user's upload directory. Answer policy uses the original durable ask: require the root actor, compatible response kind, and recognized structured permission arguments for approval. Missing/unknown permission descriptors fail closed. Preserve execution-time per-user tool/memory scope; attribution does not grant access.
- Supply an explicit owner-only event view returning application `SessionEvent` JSON, preserving existing text, reasoning, tools, skills, attachments, and child history. It needs no correlation cache. Keep native actor/command/ask IDs canonical at the transport boundary; normalize envelope IDs back to Sarathi's existing hex keys without altering journal bodies or saved IDs.
- Move title scheduling from `SocketBridge` to a resource-owned lifecycle hook/task using the accepted root user turn and committed terminal evidence. Inherit the base `on_event` so P2 delta batching stays enabled. Reuse `generate_title`; skip synthetic child messages and titled sessions, bound task lifetime to application resources, and keep title failures separate from chat completion. Disconnecting either submitting socket must not stop title generation. Send initial/current title/model state and subsequent changes through application notices using existing header notifications; HTTP state remains authoritative on reconnect.

**Browser and build integration**
- Replace `ui/src/app/chat/hooks.ts` transport/retry logic with one `SessionClient` per API scope/root/authenticated identity. Fetch a fresh ticket in its async URL resolver for every dial. Keep persistence off: full reload starts from zero and rebuilds the full transcript; reconnect resumes only from cursors matching the live reducer. Retain application projection watermarks for cached child histories and seed matching `after`/`initialCursors` when replacing a subscription/client; the client's bounded retired-actor cache cannot stand in for those watermarks. Logout or identity/root replacement disconnects the old client and never transfers commands.
- Retain `state.ts` transcript reduction and actor polling, but remove its duplicate outbox/retry machinery and event-based acknowledgments. Shared-client receipts acknowledge acceptance; journal events update content only. Expose shared-client pending/manual/error state in the existing UI, with exact-ID Retry for uncertain outcomes. A lost receipt must not create another logical command.
- Separate replay readiness from write authority. Every transport starts read-only. Preserve initial editable-page behavior by calling `acquireWriter()` once after its first replay; read-only pages never acquire. After writer loss/reconnect, require explicit **Reclaim control** and never acquire automatically. Reading remains available. Gate Send, Stop, answers, and model-picker UI on caught-up write authority. Hold the first-message handoff until acquisition succeeds; never consume it during read-only replay.
- Keep child drawers lazy, with independent cursors and subscription handles; await unsubscribe/replacement acknowledgment. Retain structured ask descriptors in the reducer and emit typed Approval, Choice, or FreeText responses from the corresponding controls. Pending answers become answered only through durable events; expiry/rejection restores actionable UI.
- Distinguish ticket HTTP authentication failure, forbidden access, missing/deleted sessions, uncertain command acceptance, and infrastructure errors. Preserve browser handshake ambiguity instead of claiming every rejected dial is authentication failure. Offer retry/reconnect and preserve readable history when write authority is lost.
- Add the local `tantra-session-client` dependency and remove `react-use-websocket` after its final consumer is replaced. Regenerate OpenAPI/Orval for tickets and application input/event/notice schemas; remove obsolete Sarathi transport frame schemas. Keep shared wire types in the client package. Extend UI Docker/Compose build context to the repository root, build the client before Next.js, and configure standalone tracing/copy paths for the local package. Verify clean frozen-lock installs and the final image, not just the development symlink.

**Verify**
- API/policy tests: wrong user/root/child, tampered/expired/wrong-purpose tickets, shortened login lifetime, reuse on both backends, read-only escalation, disallowed/missing Origin, periodic user invalidation, attachment escape, typed/expired/legacy approvals, and attribution on retries. No long-lived login token appears in socket URLs or logs.
- Client/reducer tests: receipt-versus-event ordering, duplicate receipts, lost receipts, first-message handoff, read-ready versus writable-ready, writer loss/reclaim, same-identity reconnect, identity replacement, stale child subscriptions, full-history refresh, and all three ask kinds. Retain existing transcript/reduction checks.
- Use the isolated durable Sarathi E2E Compose stack with two backend processes, `fsync=on`, `synchronous_commit=on`, and its deterministic provider/gates. Preserve behavioral assertions while replacing old frame probes, event-only acknowledgment expectations, and 4009/reload takeover assertions. Cover root/child completion, remote answer/Stop, queued FIFO commands, committed reply loss, owner death/takeover, missed notifications, deletion, expiry, and accepted work/title completion after disconnect.
- Browser checks through Nginx and each backend: login/new chat, streaming/reasoning/tool output, upload and cross-backend document reading, lazy child history, approval/denial/choice/free text, cancellation then another send, model updates, titles, full refresh, read-only view, writer replacement/reclaim, reconnect, and error states. Extend deterministic fixtures only where those workflows need evidence; no paid inference or external search dependency.
- Add a separate real-WebSocket load harness under `stress/bench` using the shared client and two processes against owned durable PostgreSQL. First establish 1,000 idle connected subscriptions, then run 100 active synthetic turns with the observers connected. Preserve existing Runtime baseline/scale workload identities; direct Runtime tests do not substitute for this transport check.
- Record CLI/JSON/HTML evidence: correctness, queue/client-buffer maxima, SQL/auth-check counts, Runtime pool/dispatcher limits, memory/loop delay, delivery latency, and watcher/subscription cleanup. Verify zero per-delta authorization queries, unchanged idle journal/result reads, bounded queues, progress with slow readers, and zero retained interests after closure. Runtime's four data slots plus separate control/LISTEN connections remain unchanged; application auth connections are reported separately.
- Run `just lint`, package/bench tests, PostgreSQL stress with zero skips, Sarathi backend tests, client lint/type/build/tests, UI `yarn test`, `yarn lint`, `yarn tsc --noEmit`, `yarn build`, strict docs build, and clean backend/UI Docker builds. Run one independent Ponytail review after implementation; recheck only behavioral review fixes.
- Update Sarathi README, websocket reference adoption note, and E2E runbooks to the new ticket/receipt/control flow. Retain reports and remove only owned workers, Compose resources, and test volumes. Mark P3/spec complete after required checks pass, then stop.

**Checklist**
- [x] Backend/client adoption and tickets
- [x] Preserved application behavior and writer UI
- [x] Two-instance behavior and browser checks
- [x] Real-WebSocket scale/resource evidence
- [x] Project/build checks, documentation, and independent review

**Verified:** 98 Sarathi backend tests, 21 UI tests, 42 shared-client tests, 1,053 package tests, 148 existing PostgreSQL stress/bench tests, and nine new socket-bench tests; PostgreSQL cases had zero skips. Lint, types, builds, frozen installs, declaration consumer checks, strict docs, final backend/UI Docker builds, and independent Ponytail review passed. The two-backend native client, fault campaign, and browser covered tickets, all ask types, replay, children, uploads, titles/models, writer replacement/reclaim, reconnect, owner death, missed notifications, deletion, expiry, and continued work after failure. Browser/review fixes added regressions for stale inactive snapshots, hidden Reclaim actions, and the unstable pending-ask selector. See [application evidence](../apps/sarathi/e2e/reports/017-p3/verification.md).

The [real-WebSocket report](../stress/bench/artifacts/socket-p3-full/report.html) passed 1,000 observers/100 active turns with ordered delivery, all slow readers progressing, and journal oracle 100/100. Server queues peaked at six frames/1,705 bytes; client queues at four frames/1,107 bytes; pools/dispatchers stayed at four. Idle journal/result-body reads and retained interests after closure were zero. Completion latency p50/p95 was 3.312/4.486 seconds. Static fixture authentication uses no application SQL; Sarathi auth is verified separately. The injected oracle failed with exit 1 as required. Owned workers, browser, Compose projects, and volumes were removed; reports retained. No paid inference or frozen-contract changes.

**Boundaries:** no Osuite adoption, registry publication, Runtime migration, shared protocol/client redesign, durable title job system, application retention policy, or unrelated UI redesign. A defect requiring a frozen-contract change must be reported before crossing that boundary. A dual-wire compatibility bridge is rejected because backend/client deployment is already coordinated.

### Conventions

- One requested phase at a time; Ponytail full; no code comments.
- Use deterministic providers and owned durable Compose PostgreSQL; no paid inference.
- Each code phase runs focused checks, `just lint`, package/bench tests, PostgreSQL stress with zero skips, and one independent review. Clean up owned resources.
- **Contract freeze:** Runtime additions after P0; wire/plugin contracts after P1. Update this spec before changing frozen contracts.
- Stop writers for migrations; mixed-version writers are unsupported.

### Keeping this spec current

- Update status/checklists only after verification passes.
- Record material deviations and unresolved follow-ups; skip routine implementation history.
- Leave later phases untouched unless a boundary change is explicitly approved.

## Open Decisions

None blocking. Concrete wire/plugin types are finalized in P1 against the approved contracts. Osuite adoption remains separate.

## Risks

- Revoked or disconnected sockets do not undo accepted external effects; execution must reauthorize.
- Projections can expose tool arguments, skill bodies, and application context unless the application supplies a safe view.
- Durable attribution is caller-supplied audit context; trusted adapters must obtain it from authenticated identity.
- Legacy asks lack structured resource details; strict policies must deny them.
- Public wire contracts constrain independent deployments. Full replay remains proportional to history, with bounded memory rather than bounded total work.
