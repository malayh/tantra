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

### P2 — Bounded recovery and optional compacted history · deps: P1 · —

Deliver transactional versioned operational checkpoints with watermarks, pending inputs, unfinished turns, finish state, and unresolved cancellation intent; suffix recovery; targeted result reads; and opt-in compacted context loading.

Verify equivalence with full journal reduction, corrupt/stale checkpoint recovery, crash boundaries, asks, child delivery, cancellation, queued work, retained floors preceding compaction markers, tool pairing, skill content, and default hook compatibility. Started abandoned turns remain interrupted and are never replayed.

- [ ] Checkpoints and backfill
- [ ] Recovery and targeted results
- [ ] Compacted context and public option
- [ ] Focused checks, comparison, and review

### P3 — Concurrent database access and efficient observation · deps: P2 · —

Deliver official Psycopg pooling capped at four data connections per worker, independent control/LISTEN connections, bounded concurrent dispatch across roots, root-directed notifications, batched catch-up, watcher lifecycle, useful root/order indexes, and bounded draining cleanup.

Verify per-root FIFO and fencing, cross-root progress, missed notifications, observer lifecycle, query plans, cleanup throughput, and no per-observer idle polling. Preserve stream commit-before-delivery.

- [ ] Pool and dispatch
- [ ] Notification routing and watcher release
- [ ] Indexes and cleanup
- [ ] Focused checks, comparison, and review

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
