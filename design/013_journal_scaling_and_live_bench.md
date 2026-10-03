# 013 — Journal scaling and live test bench

## Goal

Make command acceptance, recovery, and observation scale independently of historical streaming volume. Verify behavior and performance through automated Runtime tests with PostgreSQL and a real LLM.

## Scope

- In: an isolated Compose bench, command indexes, operational checkpoints, optional compacted history, database concurrency, notification routing, and before/after evidence.
- Out: Osuite P4, Sarathi UI changes, journal deletion, codec replacement, streaming microbatching, publication, and version bumps.

## Decisions

- Work directly in this repository, one requested phase at a time. Start with the baseline bench.
- Keep the append-only journal, event ordering, `connect(after=...)`, writer control, fencing, and cancellation semantics.
- PostgreSQL optimizations are optional capabilities; unsupported custom stores keep their existing full-read behavior.
- `Runtime(history_mode="full" | "compacted")` defaults to `"full"`. Existing hooks and custom compactors continue to see full history unless explicitly opted in.
- Use an isolated Docker Compose PostgreSQL project, ephemeral localhost port, and dedicated volume. Require normal durability. Missing prerequisites fail explicitly.
- Use two Runtime worker processes. Target 10,000 stored chats, 1,000 observers, and up to 100 active synthetic or recorded turns. Fresh inference has at most four concurrent requests, further bounded by the campaign budget.
- Provide CLI and standalone HTML reports. No bench web service or Sarathi dependency.
- Use OpenRouter `z-ai/glm-5.3-flash` and the environment API key for explicit fresh-model runs. No automatic paid fallback from replay.
- Limit the entire campaign, including reruns, to 5,000,000 prompt plus completion tokens. Cached input and reasoning count; retries and compaction requests count.
- Store a transactional SQLite budget ledger and exact-request recordings outside Docker volumes. Reserve verified context-window capacity plus bounded completion capacity before paid requests. Unknown usage retains its reservation.
- Agent defaults are six steps and 4,096 output tokens. Provider SDK retries are disabled; Runtime retries remain accounted requests.
- Keep default campaign identity stable across reruns. Database resets and report deletion never reset the ledger.
- Use journal invariants, typed results, and database evidence; do not spend tokens on an LLM judge.
- Backfill projections with writers stopped. Mixed-version writers are unsupported during migration.

## Considered and rejected

- UI-driven validation alone cannot reproduce concurrency or measure library overhead.
- Fresh LLM calls for load tests waste tokens and mix provider latency with runtime performance. Use deterministic and recorded streams for scale.
- Truncating event history would break replay and command identity. Compaction changes model context only.
- Making compacted history the default silently changes the hooks contract.
- Character-based token estimates cannot enforce a hard campaign ceiling. Reserve verified provider limits instead.
- Redis, Kafka, a benchmark server, custom connection pooling, and buffered stream durability are unnecessary for this work.

## Bench contract

- `just bench baseline|live|replay|scale|compare` runs `python -m stress.bench`.
- Baseline and scale use synthetic inference and real PostgreSQL. Live bypasses response replay and records real provider streams. Replay requires exact compatible recordings and performs no inference requests. Compare requires compatible workloads and environments.
- Runtime scenarios use public APIs. Fixture creation uses Store APIs before coordinator enrollment. Measurements do not alter production library code.
- Record raw observations plus latency distributions, SQL executions/fetched rows, CPU, RSS, event-loop delay, notifications, watcher counts, and expired transport backlog. Include commit, Python/PostgreSQL versions, image identity, durability settings, and coordinator settings.
- Separate fixture setup, command acceptance, context assembly, result completion, playback, and recovery measurements. Clearly label inclusive background SQL counts and process lifetime peak RSS.
- Baseline defaults are 10,000 sessions, 4,000/100,000-event journals, and five repetitions. Smaller explicit fixtures support quick local verification. Scale defaults are 1,000 observers and 100 active turns.
- Failures produce reports and a nonzero exit. Preserve recordings and budget reservations on failure. Always stop owned workers and tear down only the current Compose project and its volume.
- Compare identical workload/provider/settings on the same host; report provider behavior separately from runtime failures. Never label replay as fresh-model verification.
- P0 establishes infrastructure and a basic real-provider database-tool smoke. P4 adds the full fresh-model and failure campaign.

## Implementation phases

### P0 — Automated bench and baseline · deps: none · ✅ DONE

Deliver Compose lifecycle, two-process Runtime runner, synthetic/real/recorded providers, campaign ledger, baseline/scale scenarios, comparison, CLI/HTML reports, and focused bench tests.

Verify fixture and report reproducibility; missing Docker fails; normal PostgreSQL durability; long-journal claim/release/send/duplicate/context/playback/recovery; cross-process writer replacement; injected oracle failure detection; exact replay misses; persistent/concurrent budget enforcement and unknown reservations. Capture the full baseline and run the existing stress suite against the Compose database without PostgreSQL skips.

Checklist:

- [x] Spec, Compose, and CLI
- [x] Budget and exact-request recordings
- [x] Two workers, fixtures, and measurements
- [x] Correctness oracles and reports
- [x] Focused tests and repository checks
- [x] Real Compose baseline and PostgreSQL stress matrix
- [x] One independent review

Verified on 2026-10-02:

- [Final baseline report](../stress/bench/artifacts/p0-baseline-final/report.html): 10,000 chats, 4,000/100,000-event journals, five repetitions, 166 observations, zero errors. PostgreSQL 17.10 ran with `fsync=on` and `synchronous_commit=on`; workers and owned Compose resources were cleaned up.
- Median claim/send latency was 106/177 ms at 4,000 events and 3,256/4,726 ms at 100,000 events. Long-journal claim/send fetched approximately 300,000/400,000 rows, including background activity; recovery took 3,149 ms after lease expiry.
- 622 package/bench tests passed against PostgreSQL; the existing stress suite passed 94 tests with zero skips. Ruff lint/format and `git diff --check` passed. One independent review found no material issues.
- Cross-process scale smoke passed with eight observers and four active turns. Compare-mode compatibility and report generation passed; this is a baseline, not an optimization result. The 1,000-observer campaign remains P4.
- Budget, strict replay, fresh recording, and fragmented usage accounting passed through the actual provider SDK with mocked HTTP streams. No paid requests ran: `OPENROUTER_API_KEY` was unavailable in the execution environment. Fresh-model acceptance remains P4.
- Idle polling continued after observers disconnected. This measured runtime issue remains assigned to P3.

### P1 — Indexed commands and selective reads · deps: P0 · ✅ DONE

Deliver an atomic root/command index pointing to original actor events, selective command dispatch, indexed enqueue deduplication, and compatible backfill.

- Keep a small `journal_index` projection of commands, turn starts/terminals, and agent finishes. Root lookup follows existing actor relationships and traversal precedence; original typed events remain authoritative.
- Store projected command/turn keys as UTF-8 bytes so generic Store events with NUL identifiers remain valid. Select the projection pointer before the exact event fetch to prevent planner-selected journal scans.
- Maintain the projection in the same append/enqueue transaction. Backfill at most 1,000 events per page during versioned setup with writers stopped; corruption or interruption rolls back migration without rewriting event bodies or sequences.
- Optional command/finish lookup capabilities leave the required Store interface unchanged. Recovery, cancellation reduction, context loading, and result reconstruction retain full reads until later phases.

Verify input/answer/cancel retries, payload conflicts, cross-actor reuse, concurrent acceptance, lost replies, old envelopes, NUL content, and bounded duplicate reads for long journals. Claim/release may still pay recovery costs until P2.

- [x] Index and backfill
- [x] Runtime and enqueue cutover
- [x] Focused checks, comparison, and review

Verified on 2026-10-02:

- [Baseline](../stress/bench/artifacts/p1-baseline-final/report.html) and [P0/P1 comparison](../stress/bench/artifacts/p1-comparison/report.html): matching 10,000-chat, 4,000/100,000-event workloads and environment, five repetitions, 166 observations, zero errors. PostgreSQL 17.10 retained `fsync=on` and `synchronous_commit=on`.
- At 100,000 events, median claim/send improved from 3,256/4,726 ms to 2,446/2,312 ms; duplicate send from 2,481 to 27 ms; writer replacement from 922 to 13 ms; remote duplicate from 1,148 to 13 ms. Cold claim/send still fetch approximately 200,000 rows including background activity; recovery and context remain P2 work.
- Comparisons report regressions as well: 4,000-event provider-ready/completion p95 rose by 50/11 ms, recovery verification by 52 ms; 100,000-event playback median rose by 13 ms and recovery verification by 135 ms. These operations still include full reads and scheduling effects; five samples establish a local comparison, not a broad latency guarantee.
- 635 package/bench tests and 95 stress tests passed against durable Compose PostgreSQL with zero skips. Long-journal tests assert zero journal reads for warm writer control, bounded event decoding, and indexed original-event query plans. Migration, NUL keys, conflicts, cancellation transaction reads, retries, and rollback checks passed.
- Ruff lint/format and `git diff --check` passed. One independent review completed after its fixes. No paid inference ran. Owned workers and Compose resources were removed.

### P2 — Bounded recovery and optional compacted history · deps: P1 · ✅ DONE

Separate operational state from model history. Replace repeated full reductions in recovery, queue draining, cancellation, and result waits. Keep full history as the default. Use deterministic providers and the owned durable Compose database; no paid inference.

Deliverables, in order:

1. **Transactional operational checkpoints — `stores/postgres.py`, `stores/base.py`, `coordinator.py`.**
   - Add migration 6 with an operational format version and covered sequence on each session. Advance a current watermark in the existing header/sequence update; preserve stale/invalid markers until catch-up or repair. Streaming-only appends add no separate checkpoint write or round trip.
   - Extend `journal_index` with a live-work flag and partial actor/type/sequence index. Live input/start pointers represent pending inputs and unmatched starts; the existing latest-finish index supplies journal-backed finish evidence. Fetch original typed events only for live work and finish. Validate live pointer identity and absence of closing lifecycle evidence before use; contradictory live flags require fenced repair.
   - Match `reduce_journal`: preserve input sequence order and generic append duplicates; any start/terminal excludes matching inputs, and any terminal excludes matching starts. Keep all unmatched starts and expose the latest, including terminal-before-input/start cases. Use P1's lifetime existence indexes and byte keys.
   - Add normalized unresolved cancellation-target pointers. Terminal commits resolve matching projected targets; original cancellation events/targets stay immutable. Only root-journal intent participates in tree recovery. This avoids checkpoint blobs and cancellation lists growing with history.
   - Share maintenance across ordinary/coordinated append and enqueue on their transaction connection. Backfill at most 1,000 events per keyset page through the existing codec, with writers stopped. Preserve both envelopes, NUL payloads, original bodies/sequences, and atomic schema-version publication; failure rolls back and remains retryable.

2. **Bounded state reads and recovery — `runtime.py`, Store and coordinated view.**
   - Add optional `read_operational(actor_id)` returning pending/start/finish evidence, unresolved targets, version, and watermark. Required Store/Coordinator protocols remain unchanged; unsupported implementations retain full reduction.
   - Read a consistent snapshot at a journal high-water mark. Catch up a valid stale checkpoint from indexed lifecycle evidence, excluding streaming deltas. Missing, unsupported-version, invalid-watermark, or structurally invalid state requires authoritative reduction and fenced repair before activation. Corrupt journal evidence and database failures remain explicit errors.
   - Route `_recover_locked`, `_drain` selection/rechecks/error handling, `_interrupt_if_incomplete`, pending cancellation on finish, explicit cancellation, and shutdown interruption through operational state. Mutation decisions/repair use the fenced connection; preserve lock order, task generations, and effects after commit. Result waiters synchronize with shutdown completion before treating a missing terminal as closed; shutdown still rejects new work immediately.
   - Preserve the recovery barrier: apply stored cancellation targets, interrupt abandoned starts, expire their asks, reconcile child lifecycle notices idempotently, then drain accepted unstarted inputs. Do not replay samples/tools or recreate historical ask futures. Repeated recovery must not duplicate terminals or parent notifications.

3. **Targeted results — `runtime.py::_wait_result`, PostgreSQL Store.**
   - Add optional `read_turn(actor_id, turn_id)`. Resolve the first matching start/terminal with P1's index. Until a terminal exists, read no turn body; prestart terminals require only their own row.
   - Page the inclusive start-to-terminal interval at a frozen upper bound and reuse `_turn_result`. Preserve completed-sample text, usage, output, and every terminal outcome. Unsupported stores keep full reconstruction; notification/watcher behavior stays in P3.

4. **Optional compacted history — `runtime.py`, `loop.py`, Store, reference docs.**
   - Add validated `Runtime(history_mode="full" | "compacted")`, default `"full"`. Full mode retains full history for hooks, callable prompts, and custom compactors; load it only when preparing a turn, independently of operational state.
   - Index `CompactionApplied` pointers. Optional `read_compacted(actor_id)` selects the latest original marker and resolves its floor to the first matching `TurnStarted`, including floors before the marker. Read the retained window with the marker in original order at a fixed high-water mark. Absent/unresolved floors use existing marker-plus-suffix behavior; no marker means full history.
   - Opted-in callbacks see that retained window. Unsupported stores read full history and derive the same window in memory. Reuse `compaction_window`/`build_messages`; preserve request assembly, retained tool pairs/skill bodies, and current pruning policy.
   - Give `TurnEngine` the loaded high-water mark for subsequent absorption. Shrink its in-memory window after new compaction commits and existing event callbacks. Public Store reads, Runtime events, and `connect(after=...)` still replay the complete unchanged journal.

Verify:

- Differential state tests against full reduction: generated event sequences, generic duplicates/unusual ordering, multiple unmatched starts, finish, cancellation, rollback, lost replies, concurrent acceptance, and header-independent finish evidence. Cover stale/invalid checkpoint repair and interrupted migration/retry without changing original events.
- Extend existing two-process gates for takeover, failures before/after start, cancellation commit boundaries, expired asks, child/grandchild delivery, queued recovery, repeated recovery, and shutdown. Accepted unstarted inputs execute once; abandoned started turns never execute again.
- At 4,000/100,000 historical events, enforce read bounds for operational recovery/cancel/shutdown, active/completed result waits, and compacted loading. Inspect SQL plans as well as fetched/decoded rows. Current checkpoints read live evidence; stale checkpoints read uncovered lifecycle rows. Repair and default full context are excluded from these bounds.
- The bench accepts `--history-mode compacted`, with separate fixture/workload identity. The default full-mode workload remains identical to P0/P1; compare never mixes these modes.
- Compare full/compacted request assembly for repeated markers, earlier/missing floors, pruning overrides, tools, skills, cancellation context, and actor isolation. Verify default hooks, custom Store/Coordinator fallbacks, compaction during a turn, and unchanged replay digests/cursors.
- Rerun the unchanged P0 baseline workload; compare with `p1-baseline-final` and publish CLI/HTML results, including regressions. Keep full context/playback measurements comparable; add separate deterministic compacted Runtime evidence. Disclose background SQL, projection write costs, repair costs, and full context reads when queued work begins.
- Run focused PostgreSQL checks, `just lint`, package/bench tests, the PostgreSQL stress suite with zero skips, one independent review, and `git diff --check`. Update P2 only after verification; stop before P3. Pooling, observer cleanup, and the fresh-model campaign remain later phases.

- [x] Checkpoints and backfill
- [x] Recovery and targeted results
- [x] Compacted context and public option
- [x] Focused checks, comparison, and review

Verified on 2026-10-03:

- [Full baseline](../stress/bench/artifacts/p2-baseline-final/report.html) and [P1/P2 comparison](../stress/bench/artifacts/p2-comparison/report.html): unchanged 10,000-chat, 4,000/100,000-event workload, five repetitions, 166 observations, zero errors. Host, Python, PostgreSQL 17.10, image, and coordinator settings match P1; `fsync=on` and `synchronous_commit=on`. Reports identify the final source hash; owned workers and Compose resources were removed.
- At 100,000 events, median claim/send fell from 2,446/2,312 ms to 55/44 ms; completion from 1,014 to 118 ms; recovery after lease expiry from 2,240 to 65 ms. Claim/send/recovery fetched 55/56/61 rows, including background activity, versus approximately 200,000 previously. Claim CPU fell from 2,322 to 13 ms. Actual SQL-plan tests enforce indexed event reads at both journal sizes.
- Regressions remain visible: 100,000-event p95 duplicate/context/playback rose from 36/870/848 to 98/1,675/1,446 ms; full-journal oracle verification from 1,937 to 3,047 ms. At 4,000 events, playback/verification p95 rose by 56/54 ms. Default context, public replay, and bench oracles still decode historical events; five samples on a shared host do not establish causal attribution or a broad latency guarantee. No regression was hidden or used to change the matched workload.
- [Separate compacted report](../stress/bench/artifacts/p2-compacted-final/report.html): two chats with 4,000/100,000-event histories, five repetitions, 166 observations, zero errors. Both loaded 15–71 retained events; median context reads were 1.9/2.1 ms with four SQL calls and 45 median fetched rows. This is a distinct fixture, not a matched P1 comparison. Public playback and journal invariants still cover every original event.
- Projection maintenance adds two indexed live-flag updates per distinct lifecycle key and one cancellation-target delete per terminal key. Compaction markers and unresolved targets add their index/pointer inserts. Streaming-only appends add no projection statements or separate checkpoint write; the existing header update advances current coverage. Current 100,000-event claim/send still issue 111/122 median SQL calls including background work; pooling, observer polling, and watcher cleanup remain P3. Stale catch-up scales with uncovered lifecycle evidence; invalid-state repair and maintenance backfill require full authoritative reads. Default context still fetches approximately 100,000 rows when a turn begins.
- 696 package/bench tests and 96 stress tests passed against durable Compose PostgreSQL with zero skips. Coverage includes projection/reduction parity, rollback, both envelopes/NUL content, interrupted migration, stale/invalid repair, finish evidence, takeover/cancellation/asks/descendants, bounded active/completed result reads, compaction/request parity, custom-store fallback, callbacks, and unchanged replay cursors. Stress-discovered grandchild fallback and shutdown races have deterministic regressions, including all three coordinator liveness-query stages and genuine outage propagation.
- Ruff lint/format and `git diff --check` passed. One independent review completed, with material fixes re-reviewed and no remaining findings. [Verification output](../stress/bench/artifacts/p2-checks/package-bench.txt) and the earlier [failed stress report](../stress/bench/artifacts/p2-baseline-stress-failure/report.html) are retained. No paid inference ran. Stop after P2.

### P3 — Concurrent database access and efficient observation · deps: P2 · ✅ DONE

Remove cross-root database serialization and idle polling proportional to observers. Preserve P2's journal, checkpoints, history modes, fencing, and result semantics. Use deterministic providers and the isolated durable Compose database; no paid inference. Implement P3 only.

Deliverables, in order:

1. **Pooled access — `stores/postgres.py`, `coordinator.py`, dependency manifests/lockfile.**
   - Add the official pool to the PostgreSQL extra and workspace test dependencies. Use `psycopg_pool.AsyncConnectionPool(open=False, min_size=1, max_size=4)` with autocommit connections; open during async setup. Keep `PostgresStore(dsn, schema)` unchanged. Follow [Psycopg's pool lifecycle](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) and [async API](https://www.psycopg.org/psycopg3/docs/api/pool.html).
   - Replace Store's shared `_conn`/data `_lock` with scoped checkouts. Keep a setup lock and existing migration advisory lock. `_TransactionContext` holds one checkout through commit/rollback and invalidates its view before return. Preserve transaction-local fence settings, timeouts, and repeatable-read snapshots; reuse the borrowed connection instead of nesting checkouts.
   - Return connections before yielding events, awaiting providers, or running post-commit callbacks. Cancellation, rollback, broken connections, and close release capacity. Store owns its pool; Runtime retains its existing Store ownership contract and Store retains lazy connection behavior outside setup.
   - Keep dedicated lease/control and LISTEN connections outside the pool: at most six runtime database connections per worker, excluding bench SQL tools. Move request transport, observation, cleanup, and ordinary reads to pooled access so they cannot monopolize renewal. No custom pool or public sizing option.
   - Normalize enrolled-root lock order before enabling concurrency: root advisory lock, coordinator root row, then actor/request rows. Cover coordinated writes, transport, ownership, and metadata/header publication. Verify a reused connection cannot inherit another root's fence.

2. **Bounded dispatch — `coordinator.py::_listen`, `_process_requests`, `_next_request`.**
   - Separate LISTEN consumption from handlers. Dispatch at most four roots concurrently, one handler per root. Select the oldest eligible committed request per available root using existing `(created_at, request_id)` ordering; exclude busy roots before limiting candidates.
   - Recheck destination, generation, expiry, and unanswered status in the fenced transaction. Preserve Runtime root locks, idempotency, reply-after-commit, rerouting, and cancellation. Add no global ordering guarantee for racing submissions.
   - Bound task creation. Close stops admission and cancels/awaits dispatchers before closing connections; interrupted transactions roll back and unanswered requests remain recoverable.

3. **Shared observation — `coordinator.py::watch`, `request`, `_listen`; `runtime.py::_watch`, `_stream`, `_wait_result`.**
   - Parse committed notification payloads and route wake-ups by root. Treat payloads as hints: page authoritative changes in order, advance only through rows read, and coalesce hints without queuing journal bodies. Unrelated roots stay asleep.
   - Run one worker-level batched catch-up statement per `catch_up_interval` for interested roots/actors and outstanding transport replies. Read root change high-water marks, writer identity, owner generation/validity/recovery, relevant actor sequence/activity, and reply availability. Drain indicated change pages separately. Remove consumer timers issuing periodic `_changes`, journal, result, or writer queries.
   - Register interest before initial reads; use generations to close read/wait races. Streams reread when their actor advances. Result waits share liveness evidence and retain P2's inactive-owner and shutdown behavior. Count inactive-owner evidence across distinct catch-up samples, including unchanged samples; observe expiry without a notification.
   - Compare authoritative actor sequence/root state as well as transport cursors so missed notices remain recoverable after 24-hour transport retention. Never use one global `bigserial` cursor as a commit watermark. Reconnect catches up; malformed/duplicate hints cannot skip durable events. Preserve explicit database errors and bounded reconnect behavior.
   - Keep required protocols and public `watch`/`connect(after=...)` unchanged. Use an optional concrete PostgreSQL observation capability; custom coordinators retain their existing fallback. Slow consumers keep independent durable cursors and pull pages; they cannot block LISTEN or grow an unbounded event queue.

4. **Watcher lifetime — `runtime.py::_watch_root`, `events`, `_claim`, `_release_connection`, `_wait_result`.**
   - Reference-count entered connections, event generators, and result waits independently. Release in `finally`, including failed claims, cancellation, generator close, and remote release errors. Keep one Runtime watcher per interested root.
   - Last release cancels/awaits that watcher and removes its coordinator subscription. Compare task identity during cleanup so an old task cannot remove a concurrent replacement. A result waiter retains interest after its connection closes.
   - Unsubscription never stops accepted work or lease renewal. Reconnect starts a fresh subscription at the caller's durable cursor. Shutdown releases subscriptions and wakes waiters.

5. **Indexes and cleanup — migration 7, `coordinator.py::cleanup`, `runtime.py::_maintain`.**
   - Add an expression root index matching `COALESCE(NULLIF(header->>'root_id', ''), id)`, session ordering on `(created_at DESC, id COLLATE "C" DESC)`, parent/session ordering, and a root/request-order partial index for unanswered requests. Reuse the existing change/root index; align transport cleanup indexes with expiry selection. Confirm plans at 10,000 chats; avoid redundant variants.
   - Run the index-only migration with writers stopped and atomic version publication. No journal/projection rewrite or public format change.
   - Preserve `cleanup(limit)`'s total deletion bound, 24-hour retention, and `SKIP LOCKED`. Alternate transport tables so both backlogs progress. Maintenance drains batches of 100 with cooperative yields; stop starting batches after one second per pass. Retry remaining backlog after 100 ms; retain the existing 60-second interval when idle and on database failure.
   - Keep cleanup off renewal/control. Never delete journals, lifecycle projections, operational pointers, or lifetime command identity. Transport expiry cannot enable a second execution of an accepted command.

6. **Measurements/docs — `stress/bench/worker.py`, focused tests, reference docs.**
   - Preserve the full P0/P2 baseline workload identity. Ensure pooled connections use measured connection/cursor classes: the current direct-connect monkeypatch must not silently miss pool SQL.
   - Report pool capacity/waits, dispatcher concurrency, catch-up statements, routed wake-ups, watchers/subscriptions, and cleanup backlog alongside existing measurements. Use pool statistics and bench instrumentation, without a new production telemetry API.
   - Document the six-connection bound, lifecycle, optional fallback, and migration requirements. Require post-disconnect watcher cleanup in bench assertions.

Verify:

- Transaction gates prove cross-root progress with one blocked data operation, four-connection/dispatcher bounds, and renewal with all data slots occupied. Cover root ordering, mixed commands, retries, takeover, lock-order races, pooled fence isolation, cancelled checkout, rollback, broken connections, and shutdown.
- With 1, 64, and 1,000 idle synthetic observers, require one shared observation check per healthy scheduled interval per worker after subscription settles, and zero journal/result reads while unchanged. Wake one root; unrelated consumers neither wake nor query. Fixed request/renewal/cleanup activity is measured separately. P4 retains the integrated 1,000-observer/100-active-turn failure campaign.
- Cover registration/read races, lost/duplicate/malformed notices, listener reconnect, silent expiry, transport cleanup during disconnection, writer replacement, genuine database failure, and P2 shutdown regressions. Verify unchanged replay digests/cursors and commit-before-delivery; slow readers cannot block others.
- Mix connections, streams, and waits on one root; close/cancel in different orders and race last release against resubscription. Watchers/subscriptions return to zero after the final user, accepted work continues, and later idle intervals issue no observer-related SQL.
- Inspect `EXPLAIN (ANALYZE, BUFFERS)` for root lookup, session/child pagination, request selection, catch-up, and cleanup. Drain both transport backlogs beyond one batch within per-call limits; retries after cleanup still identify the original command.
- Rerun the unchanged 10,000-chat, 4,000/100,000-event baseline; compare with `p2-baseline-final`. Capture a P2 reference for a separate concurrent-root/observer workload before edits. Publish CLI/HTML comparisons with throughput, waits, idle SQL, cleanup progress, regressions, and write amplification. Keep full-context/replay CPU costs visible and workload identities separate.
- Run focused PostgreSQL/concurrency checks, repository lint, package/bench tests, PostgreSQL stress tests with zero skips, `git diff --check`, and one independent review. Mark P3 complete only after verification; stop before P4. No paid inference, Sarathi/Osuite changes, codec replacement, or streaming microbatching.

- [x] Pool, lock order, and bounded dispatch
- [x] Shared catch-up, notification routing, and watcher release
- [x] Index migration and draining cleanup
- [x] Focused checks, comparison reports, docs, and review

Verified on 2026-10-03:

- [Full baseline](../stress/bench/artifacts/p3-baseline-final/report.html) and [P2/P3 comparison](../stress/bench/artifacts/p3-comparison/report.html): unchanged 10,000-chat, 4,000/100,000-event workload, five repetitions, 166 observations, zero errors. Host, Python, PostgreSQL 17.10, image, and coordinator settings match P2; durability settings remain on. Both final workloads match source hash `e4de3ca37cb6355fa9e85a50bd242a397d9ba2bad5a673e9b4dbac582d3e505c`.
- [Concurrent workload](../stress/bench/artifacts/p3-scale-final/report.html) and [recorded P2 comparison](../stress/bench/artifacts/p3-scale-comparison/report.html): 64 observers, 16 synthetic turns, identical fixture/environment. Aggregate completion fell from 2.08 to 1.51 seconds, approximately 7.69 to 10.57 turns/second. This workload runs one concurrent drive per worker, not five latency repetitions. Settled three-second SQL counts fell from 1,530/1,575 to 37/30 per worker; initial idle includes startup work and fell from 1,420/1,392 to 110/122. After disconnect, watchers/subscriptions and observation checks are zero; the remaining 15 SQL calls are fixed dispatcher polling. Healthy scheduled catch-up contributes one shared statement per interval; active hints add targeted checks.
- Pool and dispatcher peaks both reached four. The active workload queued about 840 checkouts per worker, with roughly 12.75 seconds of accumulated concurrent wait time, about 15 ms per queued checkout. Active work caused 131/136 observation checks, including seven periodic samples per worker. These costs remain visible in raw pool/counter deltas. Dedicated control/LISTEN connections preserve the six-connection bound; saturation, renewal, fence reuse, cancellation, rollback, and broken-connection tests pass.
- At 100,000 events, median claim/duplicate/recovery fell from 55/36/65 to 41/25/47 ms. Regressions remain: 4,000-event median Send/Complete rose from 30/77 to 45/95 ms, context p95 from 26 to 85 ms, and 100,000-event recovery verification p95 from 1,777 to 2,082 ms. Claim/Send median SQL counts rose from 111/122 to 126/144 at 100,000 events, including background work. Five samples on a shared host do not establish causal attribution. Full context and playback still read about 100,000 historical rows and consume roughly 803/820 ms median Python CPU. P3 adds transport/session indexes and lock statements; it adds no journal/projection writes or streaming buffering. Commit-before-delivery remains unchanged.
- [721 package/bench tests](../stress/bench/artifacts/p3-checks/package-bench.txt) and [96 stress tests](../stress/bench/artifacts/p3-checks/stress.txt) passed with zero PostgreSQL skips. Focused checks cover 1/64/1,000 shared subscriptions, unchanged journal/result read bounds, routing/reconnect/expiry, writer replacement, watcher teardown, SQL plans at 10,000 chats, and bounded alternating cleanup preserving command identity. The memory stress fixture now issues dependent write/recall calls in successive provider steps; parallel tools remain unchanged. Failed gate logs are retained in `p3-checks`.
- Ruff lint/format and `git diff --check` passed. One independent review completed, with material fixes re-reviewed and no remaining findings. Malformed notification kinds cannot kill LISTEN; claim watermarks reject stale writer snapshots; result waits ignore cached inactivity and reset on active/recovering hints while preserving bounded recovered-owner failure. The release test requires eventual ownership release before recovery because error delivery and relinquishment commit separately. Owned workers, Compose containers, and volumes were removed. No paid inference ran; full history and custom-coordinator fallbacks remain unchanged. Stop after P3.

### P4 — Live campaign and final comparison · deps: P3 · —

Deliver fresh SQL read/write, skills, approvals, structured output, child completion, and compacted recall workflows; 1,000 observers/100 active turns; and automated writer replacement, reconnect, process death, cancellation boundaries, lost replies/notifications, slow readers, and database interruption.

Verify machine-checkable outcomes and database effects; strict campaign ceiling; before/after report; and no provider replay presented as a live pass.

- [ ] Fresh-model workflows
- [ ] Failure and scale campaign
- [ ] Final comparison, checks, and review

### Conventions and keeping this spec current

- Follow `AGENTS.md`, use Ponytail, add no code comments, and preserve the unrelated investigation report.
- Run focused verification, repository lint/tests, and `git diff --check` for each code phase. Use one independent review; re-review only materially changed review fixes.
- Mark `CODE DONE, VERIFICATION PENDING` when required external verification cannot run. Tick only proven checks.
- Update phase status, checklists, and material decisions in place. Record only deviations that affect scope, behavior, contracts, or phase boundaries.
- Public replay, writer, fencing, and recovery contracts remain frozen. Update this spec before any proposed contract change.
- Stop at each completed requested phase so the user can inspect and commit it.

## Open decisions

None.

## Risks

- Existing stress tests are useful but PostgreSQL cases can skip; required bench checks must not.
- Fresh provider behavior is nondeterministic. Record model behavior failures separately from runtime correctness failures.
- Missing usage after cancellation or process death conservatively consumes a reservation until authoritative usage is available.
- Runtime background polling contributes to measured SQL. Reports must retain raw samples and disclose measurement boundaries.
