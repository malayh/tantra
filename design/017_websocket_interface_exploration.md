# 017 — Pluggable WebSocket interface

**Status:** P0 complete. P1–P3 not started.

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

### P1 — Native ASGI endpoint · deps: P0 · —

**Deliver**
- Implement protocol models, plugin interfaces, authentication lifecycle, ordered delivery, replay readiness, writer transitions, and bounded cleanup.
- Check Origin before accepting. Reject missing Origin unless explicitly enabled for authenticated non-browser clients.
- Revalidate read access periodically; pause protected delivery when access expires or becomes uncertain.

**Verify**
- Private, anonymous, team-role, editor, approval, observer, and descendant policies with deterministic fixtures.
- Reconnect, filtered replay, slow clients, deletion, shutdown, revocation, and two-worker ownership changes.

**Checklist**
- [ ] Protocol and plugin contracts
- [ ] Authentication and delivery lifecycle
- [ ] Policy fixtures, transport checks, documentation, and review

### P2 — TypeScript client · deps: P1 · —

**Deliver**
- Add a small ESM package with declarations and native WebSocket.
- Provide connect/disconnect, subscriptions, explicit writer acquisition, send, typed answer, cancel, state/event callbacks, bounded outbox, and cursor handling.
- Keep HTTP history and UI reducers application-owned.

**Verify**
- Protocol fixtures, mutation isolation, lost receipts, reconnect races, subscription replacement, identity changes, type/build/tests.

**Checklist**
- [ ] Client API and bounded state
- [ ] Retry, reconnect, cursor, and persistence contracts
- [ ] Client checks, documentation, and review

### P3 — Sarathi adoption · deps: P2 · —

**Deliver**
- Replace bridge/connection hook; preserve full history, attachments, children, typed asks, titles, and model updates.
- Add application-owned HTTP signed root-scoped 30-second handshake tickets. Reuse within the handshake window; socket lifetime follows original login expiry. Remove long-lived login tokens from WebSocket URLs.
- Supply codec, explicit view, policy, title/model notices. Schedule titles in application lifecycle code so disconnects do not lose them.
- Add explicit writer reclaim and distinguish authentication, deletion, and infrastructure failures. Update local client dependencies and Docker build context.

**Verify**
- Sarathi behavioral suite and browser workflows across both backend instances; lint/type/build/API checks.
- Transport scale: 1,000 idle connections and 100 active synthetic turns across two workers. Bound queues, preserve Runtime pool limits, avoid per-delta authorization queries, and release all subscriptions.

**Checklist**
- [ ] Backend/client adoption and tickets
- [ ] Preserved application behavior and writer UI
- [ ] Two-instance browser/scale checks, documentation, and review

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
