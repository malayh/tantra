# 011 — PostgreSQL Runtime Coordinator

## Goal

Let any Sarathi backend serve a root tree while exactly one Runtime process owns its execution. A browser can reconnect through another backend, replay durable actor journals, and send commands to the owner. Takeover after owner loss interrupts started turns and drains accepted inputs without replaying model or tool work.

## Scope

In: optional Coordinator interface, PostgreSQL implementation, fenced Store writes, remote commands and reader wake-ups, distributed writer ownership, whole-tree recovery, two Sarathi backends behind Nginx, and deterministic multi-process and Brave verification.

Out: Redis, Kafka, independent execution workers, sticky routing, automatic recovery scans, replay of interrupted turns, cross-process child execution, event-bus copies of journal payloads, and a persistent browser outbox.

## Decisions

- `Runtime(..., coordinator=None)` preserves existing behavior. Coordinated Runtime requires idempotent `await runtime.start()` during application lifespan and closes coordinator resources in `aclose()` without closing the application-owned Store or provider.
- `PostgresCoordinator(store, lease_ttl=60.0, request_timeout=10.0, catch_up_interval=2.0)` requires the identical `PostgresStore` object supplied to Runtime. Reject incompatible Stores and coordinator reuse at startup.
- Export `Coordinator`, `PostgresCoordinator`, `LeaseLost`, `CoordinatorUnavailable`, `CommandTimeout`, and `RemoteExecutionError`. A timed-out command may have been accepted; retry its UUID.
- A process instance gets a fresh UUID on startup. One root execution lease has an owner instance, monotonically increasing generation, and database-time expiry. Renew at `lease_ttl / 3` on an independent control connection throughout provider calls, tools, and root asks. An expired lease cannot be renewed.
- One separate writer token identifies a writable connection. The newest accepted claim wins across processes. Read-only subscriptions claim nothing. Writer replacement never moves or cancels execution. A writer token survives an idle execution-lease release.
- Keep ownership while tasks, asks, pending inputs, or command application exist. Release under the root coordination lock when the tree is quiescent. No browser connection count decides execution lifetime. The explicit exception is an owner-local programming or storage failure before `TurnStarted`: clear activity, relinquish when the tree is otherwise quiescent, keep the accepted input durable, fail remote waiters boundedly, and let the next writable claim recover the same command UUID. A failure after `TurnStarted` must gain a durable terminal or remain owned until takeover recovery interrupts it; never replay a started turn.
- Root ownership and execution writes use the same PostgreSQL transaction boundary. A check outside the write transaction is insufficient. All execution-owned event, input, child, status, usage, pending-ask, finish, lifecycle, cancellation, and interruption writes require an unexpired matching generation. Bare execution writes to an enrolled root fail even through a separate `PostgresStore` instance.
- Application title/metadata patches remain allowed. Model patches lock the root and reject changes while execution or accepted work exists. Whole-header replacement cannot bypass protection.
- Forward writer claims/releases, `send`, `answer`, and `cancel` to the owner. Authentication and tenant authorization happen in Sarathi first. Acceptance and its transport reply commit together. Only then do local tasks, ask futures, or cancellation signals change. `prompt()` awaits journal outcome separately.
- Preserve existing command UUID and per-actor journal FIFO. Concurrent commands are ordered by durable acceptance. A transport timeout means unknown acceptance; duplicate delivery must not duplicate the command. Persist cancellation targets before acknowledgment and replay incomplete cancellation against only those targets.
- Subscribers read Store pages by scalar actor cursor. PostgreSQL notifications contain identifiers and change kind only. Register the listener before final replay check; reread on reconnect and every two seconds while subscribed or waiting for a result. Slow readers never hold execution locks.
- `ActorStatus.active` reports a valid owner's active/scheduled actor tasks. Expired owner activity is false; database uncertainty raises `CoordinatorUnavailable`. Sarathi must stop inspecting `runtime.active` directly.
- Writable reconnect or mutation may claim an unowned/expired root. Read-only subscription and status polling never activate. Recover the full root tree behind a barrier: apply pending cancellation, interrupt abandoned started turns, expire asks, reconcile child lifecycle notices, then drain accepted unstarted inputs. Recovery is idempotent and can itself be retried after a crash.
- A stale owner invalidates local tasks and asks, never reacquires ownership from stale task objects, and cannot write or start fresh logical work. Already-started external side effects cannot be rolled back.

## Interface and PostgreSQL transport

Use backend-neutral `Ownership(root_id, instance_id, generation, expires_at)`, `WriterToken(root_id, connection_id)`, versioned JSON `CommandEnvelope(request_id, root_id, operation, writer_token, payload, deadline)`, `CommandReply`, and `ChangeNotice` types. Serialize explicit operation variants; never Python callables, provider objects, agents, or arbitrary exceptions.

The Coordinator contract covers `start(handler)`, `close()`, `locate/acquire/renew/release`, the narrowly scoped fenced `relinquish_failed_prestart`, `request`, an ownership-checked root transaction view, and change watching. It also exposes read-only writer-token validation so subscribers can detect replacement without claiming or releasing a writer, plus read-only recovery-generation state so result waiters distinguish takeover recovery from a recovered inactive actor. The transaction view supplies Store operations on the same connection, writer validation/claims, actor activity changes, and durable request replies. Runtime owns command semantics; backends own transport and transactional guarantees. A future adapter must pair with a Store capable of the same fencing.

Add versioned PostgreSQL tables for root ownership/writer/activity/recovery and bounded transport requests/replies. Index requests by destination/order. Retain completed and expired transport rows for 24 hours; prune in bounded ordinary maintenance. The actor journal remains the long-term command-deduplication source. Inserting a transport row does not acknowledge a human command.

Lock order: Runtime local root lock when needed, PostgreSQL root coordination row, then session rows by stable ID. Never hold the database root lock while waiting to acquire the local Runtime lock. Bound transaction, statement, and idle-in-transaction time so a frozen process cannot retain locks indefinitely. Use dedicated listener and ownership/control connections so journal reads cannot starve lease renewal. Do not add a pooling dependency.

Requests carry a transport UUID and deadline; human mutations retain their existing durable command UUID. A request to an obsolete generation returns ownership-changed; the originating Runtime resolves the owner and retries within the same deadline. Owners never recursively forward. Expired unapplied requests never run. Persist typed validation failures as replies; expose programming/storage failures as bounded infrastructure errors without leaving remote `prompt()` waiting forever.

Publish transactional notifications after journal, ownership, writer, or activity changes. Event bodies always come from Store. Coalesce local wake-ups, deduplicate by actor sequence, and use bounded catch-up reads to repair lost notifications. Keep the current independent child journals and lazy child subscriptions.

## Sarathi and deployment

Compose runs `db`, `migrate`, `backend_a`, `backend_b`, `nginx`, and `ui`. Both backend containers use the same image/configuration/database and uploads volume at the same mount path. Nginx takes the current `localhost:8001` API port; UI stays on `localhost:3001`. Browser and NextAuth internal traffic target Nginx. Use round-robin without affinity, WebSocket upgrade, streaming-friendly buffering/timeout settings, bounded upstream connection timeout, passive backend failure handling, Docker DNS re-resolution, and upstream logging without credentials. Pin an Nginx version with open-source dynamic resolution. Do not retry ambiguous non-idempotent HTTP requests at Nginx.

FastAPI lifespan starts coordinated Runtime and fails readiness if coordinator startup fails. The WebSocket bridge uses coordinated status and writer methods; old writers close with existing code `4009`. Add `?view=readonly` for an observing tab. Preserve root and opened-child cursors, lazy drawer subscription, and queued-message composer behavior. Unavailability must not be shown as actor inactivity or expired live asks.

Add stable error codes, optional command UUID, and retryability to command-error frames. Keep durable journal events as acceptance acknowledgments. Keep unsatisfied frames in page memory, replay the journal on reconnect, retry in submission order using unchanged UUIDs, stop on writer replacement or definitive validation failure, and offer explicit same-UUID retry after bounded automatic attempts. No refresh-persistent outbox. Header-change notices refresh title/model metadata. Regenerate OpenAPI clients.

## Considered and rejected

- Redis/Kafka: deferred; PostgreSQL already owns the journals and can atomically fence their writes.
- Sticky routing: does not provide requested reconnect and failure behavior across processes.
- Worker pool: adds deployment machinery without resolving root ownership.
- Out-of-transaction lease checks: stale process can write after takeover.
- Notification-only observation: a lost notification can leave a reader waiting despite durable events.
- Automatic startup scans: writable reconnect or mutation triggers recovery.
- Replaying started turns: external side effects may already have occurred.

## Implementation phases

### Phase 0 — Coordinator contract and PostgreSQL safety · deps: none · COMPLETE

Deliver Coordinator types, additive migrations, lease/fence operations, guarded Store transaction view, request transport, notifications, bounded cleanup, and standalone PostgreSQL tests. Do not integrate Runtime yet. Preserve non-coordinated Store behavior.

Verify concurrent claim winner; stale/expired renewal and every mutation rejected after takeover; no check/write race; migration preserves journals and concurrent startup; duplicate/expired/lost-reply requests; listener reconnect; blocked data connection does not starve renewals; raw enrolled-root writes fail. Run focused tests, `just lint`, `just test`, and `git diff --check` against real PostgreSQL.

Checklist:

- [x] Interface and types
- [x] Additive migration
- [x] Ownership and fenced Store view
- [x] Request transport and notifications
- [x] Real PostgreSQL failure tests

Verification: real PostgreSQL coordinator/store tests 31 passed; full pytest 577 passed; Ruff check and format check passed; independent safety review and `git diff --check` passed. Bounded transport cleanup is caller-driven; Phase 1 Runtime integration must invoke it during ordinary coordinator maintenance.

### Phase 1 — Distributed Runtime · deps: P0 · COMPLETE

Deliver optional Runtime coordinator lifecycle, remote writer/command handling, journal observation, distributed actor status, lease-bound TurnEngine/Runtime mutations, and whole-tree recovery. Use two independent OS processes and deterministic gated providers.

Verify B observes/controls A; ask/cancel work while model/tool blocked; root remains owned with active child and zero subscribers; idle release races safely with commands; stale writer and owner writes fail; child/grandchild takeover interrupts only started turns; cancellation retains target set; remote `prompt()` returns result or bounded infrastructure error; closing B does not cancel A; old single-process suites pass.

Checklist:

- [x] Runtime startup and lease lifetime
- [x] Distributed commands/writers
- [x] Remote observation/results
- [x] Whole-tree recovery
- [x] Multi-process tests and public docs

Verification: real PostgreSQL Runtime tests 21 passed; full package pytest 598 passed; stress pytest 84 passed; Ruff check and format check passed; strict MkDocs passed; independent concurrency review and `git diff --check` passed.

### Phase 2 — Sarathi and load-balanced Compose · deps: P1 · COMPLETE

Deliver lifespan wiring, two backends/Nginx/shared uploads, coordinated WebSocket status, in-memory pending-command retry, read-only view, generated client, docs, and a test-only Compose override for deterministic A/B routing and provider gates. No production backend selector or fault-injection route.

Verify HTTP/WebSocket both route through Nginx; B upload readable on A; readonly viewing never replaces writer; remote activity stays active; UUID retries and writer replacement behave; child journal opens only on demand; replaced backend DNS recovers; auth/account isolation works from both backends. Run backend/UI/Compose checks.

Checklist:

- [x] Lifespan, deployment, and shared uploads
- [x] Nginx routing
- [x] Retry/status/read-only UI
- [x] Generated types and tests
- [x] Deterministic E2E override

Verification: Sarathi backend pytest 89 passed; UI reducer tests 14 passed, lint and production build passed; Ruff check and format check passed; disposable two-backend/Nginx Compose passed HTTP/WebSocket routing, shared-upload, read-only/cross-backend writer takeover, account-isolation, and backend DNS-replacement checks; independent review and `git diff --check` passed.

### Phase 3 — Failure verification and documentation · deps: P2 · COMPLETE

Run test-only deterministic provider/tool gates through the real app, Runtime, Store, WebSocket, and UI. Use existing Brave with at least an A writer, a B writer, and a read-only viewer. Record actual backend, ownership generation, UUIDs, cursors, screenshots, and journal evidence in the ignored E2E report location.

Verify disconnect/reconnect through B while A works; distributed writer takeover; read-only viewers; active child while root idle; FIFO message while working; remote answer/Stop; dropped reply and notification; B death; A death before expiry and recovery after expiry; paused A resumes after B takeover; ask expiry; crash immediately before and after atomic cancellation acceptance; database outage; slow reader; graceful shutdown with immediate lease release; refreshed child history; and cross-backend upload. Use disposable Compose project/database for destructive tests, restore stopped/paused containers, and preserve the user's existing chats/uploads.

Then run real-provider Brave smoke for streaming, delegation, child communication, drawer, approval, cancellation, refresh, and reconnect. Run Ruff, package/stress/Store/Sarathi tests with real PostgreSQL, UI tests/lint/build, strict MkDocs, lock/package/Docker builds, and `git diff --check`. Missing required external services leaves `CODE DONE, VERIFICATION PENDING`.

Checklist:

- [x] Deterministic failure matrix, with the sustained slow-reader same-trial conjunction accepted as a release waiver
- [x] Brave multi-window evidence, with attachment upload accepted as a browser-gated release waiver
- [x] Real-provider smoke, with cross-backend mid-turn reconnect accepted as a release waiver after deterministic cross-backend coverage passed
- [x] Full automated checks
- [x] Deployment/failure documentation

Verification: real-PostgreSQL Tantra suite 610 passed, stress 66 passed with 18 optional skips, Sarathi backend 106 passed, UI reducer 14 passed; Ruff check/format, UI lint/build, strict MkDocs, lock check, package build, normal/E2E Compose config and disposable Docker build, and `git diff --check` passed. Independent review found and resolved provider-stream timeout and WebSocket token-log issues. Live deterministic checks passed cross-backend forwarding while the owner worked, recovery after owner death without model replay, graceful lease release, same-socket missed-notification catch-up, an 18-second non-reading observer catch-up, lazy child-journal subscription, and the cancellation commit/retry boundary. Brave real-provider smoke passed streaming, delegation with child-to-parent messaging, child drawer replay, approval, Stop and follow-up, refresh/replay, and same-backend mid-turn continuation. By user direction, three unproven checks are accepted as release waivers rather than claimed passes: Brave attachment upload was blocked by its security gate; sustained slow-reader queue pressure and catch-up were not proven together in one trial; and real-provider mid-turn reconnect did not switch backends, although deterministic A-owner/B-forwarder coverage passed. The ignored [local remaining-issues report](../apps/sarathi/e2e/reports/011-coordinator-2026-09-28/remaining-issues.md) contains supplementary evidence and reproduction details.

### Conventions (all phases)

- Work directly in `/home/malay/Code/tantra`; do not make implementation worktrees, tag, publish, or bump the version.
- One phase at a time; follow `AGENTS.md`, add no code comments, and independently review security/concurrency changes.
- Run focused tests plus `just lint`, `just test`, and phase-specific checks before marking a phase done.
- **Contract freeze:** transaction/fencing and transport types freeze in P0; Runtime ownership/recovery semantics freeze in P1. Update this spec before changing them.

### Keeping this spec current

- Update phase marker and checklist only after required verification passes.
- Record deviations only when behavior, scope, contract, or phase boundary changes.
- Keep unresolved problems in Open Decisions or a follow-up note; do not silently cross phase boundaries.
- Ask the user to inspect, compact, and commit each completed phase.

## Open Decisions

None.

## Risks

- A valid lease does not prove its owner responds; takeover waits for expiry.
- PostgreSQL primary is the availability and ordering authority. Database uncertainty is not `active=False`.
- Fencing cannot undo external tool side effects. Idempotent external operations remain necessary.
- Existing independent-journal partial-delivery limitations remain; do not promise general cross-journal transactions.
- UI retries survive socket reconnects in a live page, not refresh.
