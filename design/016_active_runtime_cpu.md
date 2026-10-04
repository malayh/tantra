# 016 — Reduce active-runtime CPU

## Summary

Reduce Python and PostgreSQL CPU during streaming, observation, and reader fan-out. Applications receive the improvements by upgrading Tantra. Preserve [investigation 015](015_runtime_cpu_investigation.md) and its reports as supporting evidence.

**In:** Tantra runtime, PostgreSQL optimizations, existing automated bench, correctness tests.
**Out:** Osuite/Sarathi changes, paid inference, codec replacement, journal pruning, changing the default full-history mode.

## Decisions and contracts

- Preserve individual event bodies, sequence numbers, command identity, replay cursors, fencing, synchronous durability, and delivery after commit.
- Keep public Runtime APIs and required Store/Coordinator protocols unchanged. Optional internal capabilities retain compatible fallbacks.
- Automatically batch PostgreSQL provider deltas with bounds of **25 ms, 32 events, or 64 KiB serialized payload**, whichever comes first. The timer starts with the first buffered delta.
- Custom `on_event` overrides retain immediate commits, preserving their ordering and failure behavior. SQLite, memory, and custom stores retain the immediate path.
- Cancellation, deletion, shutdown, or ownership loss discard uncommitted buffered deltas. A batch already committed remains part of the durable prefix.
- Preserve default full-history loading and callback semantics. Compacted history remains an application opt-in.
- Reject disabling guards, asynchronous durability, larger connection pools, or moving event decoding wholesale into PostgreSQL: they do not safely address the measured amplification.

## Implementation phases

### P0 — Reproducible CPU benchmark · deps: none · DONE

**Deliver**
- Add `just bench cpu` using the existing isolated durable Compose database and two Runtime worker processes.
- Cover fixed-output streams with 64, 256, and 1,024 fragments; add an 8,192-fragment stress fixture. Include burst and paced delivery, text/reasoning/tool arguments, and tool progress.
- Exercise 0/1/8 extra readers, slow readers, 10,000 unrelated sessions, roots with 100 children, and 4,000/100,000-event histories in full and compacted modes.
- Use distinguishable ordered fragments and independent database evidence. Normalize only runtime-generated identities and timestamps for cross-run digest comparison.
- Capture a fresh baseline before runtime changes. Preserve existing baseline and scale workload identities.

**Verify**
- Measure Python CPU separately for both workers and PostgreSQL container CPU, alongside wall time, SQL calls/rows, WAL, memory, loop delay, notifications, and delivery latency.
- Use at least five warmed, unprofiled trials for primary comparisons. Keep database image, instrumentation, fixtures, and configuration identical; vary execution order.
- Deliberately corrupt ordering, duplicate/drop events, and simulate premature delivery. Each oracle must fail with a nonzero exit.
- Avoid custom event hooks in timing instrumentation, which would disable automatic batching.

**Checklist**
- [x] CPU runner and failure oracles
- [x] Baseline JSON/HTML reports and environment identity

**Verification — 2026-10-04**
- [Fresh baseline](../stress/bench/artifacts/016-p0-baseline-final/report.html): 10,000 unrelated sessions, two workers, 21 warmed cases, and 57 measured runs. Nine primary cases have five trials each; extended cases have one exploratory trial each.
- JSON retains individual samples, independent journal/replay evidence, worker CPU, PostgreSQL CPU/SQL/WAL, delivery timing, resource cleanup, commit/source identities, image identity, and actual database settings. Runtime source remains unchanged at `88dd512`.
- Ordering, duplicate, dropped-event, and premature-delivery injections each produced a failed report and exit code 1. The live reader and premature-delivery injection share the same commit oracle. Reports are retained under `016-p0-oracle-*`.
- `just lint` passed; package, bench, and durable PostgreSQL stress tests passed: **992 passed, zero skips**. One independent Ponytail review passed after fixes to the commit oracle and Compose/configuration identity.
- The CLI comparator self-check passed with identical input reports; this verifies reporting, not an optimization gain. Failed/superseded runs remain retained; `016-p0-baseline-final` is the authoritative baseline.
- Owned workers, Compose container, and volume were removed; watcher/subscription counts and expired transport backlog returned to zero. No paid inference ran. Stop after P0; matched optimization comparisons begin in P1.

### P1 — Reduce observation and result amplification · deps: P0 · DONE

**Deliver**
- Debounce journal-only notification hints by 25 ms using a fixed first-dirty deadline. Keep writer, ownership, activity, recovery, deletion, request, and reply handling immediate.
- Retain periodic authoritative catch-up and distinct samples for inactive-owner detection.
- Add migration 10 with a partial index for the latest terminal event. Extend optional observation metadata with journal-authoritative terminal sequences; retain existing actor tuple shapes.
- Check results initially and when terminal evidence advances, rather than after every delta. A later completed turn must still wake a waiter for an older command. Custom coordinators retain existing fallback behavior.
- Use the existing observation scheduler for fixed per-root journal deadlines; control hints preempt their own root without delaying unrelated control work. Remove deadlines when interest ends or the coordinator closes.
- Optional `terminal_sequences` metadata defaults to unavailable; actor tuples remain `(sequence, active)`. PostgreSQL obtains terminal evidence in the shared observation statement without reading event bodies.
- Capture a fresh unchanged before campaign, then run the identical after campaign and compare with both it and the retained P0 baseline. Preserve bench identities and instrumentation.

**Verify**
- Exercise missed notifications, successive terminals before wake-up, prestart cancellation, owner expiry, deletion, and shutdown.
- Unchanged periodic samples cause no journal or result-body reads. Terminal lookup uses the new index.
- Run matching before/after CPU benchmarks and report observation and result-read reductions.

**Checklist**
- [x] Debounce and terminal-aware waits
- [x] Migration, correctness, and comparison evidence

**Verification — 2026-10-04**
- [Fresh before](../stress/bench/artifacts/016-p1-before/report.html), [after](../stress/bench/artifacts/016-p1-after/report.html), [matched comparison](../stress/bench/artifacts/016-p1-comparison/report.html), and [retained P0 comparison](../stress/bench/artifacts/016-p1-vs-p0/report.html) passed. Each CPU campaign retained 21 warmed scenarios and 57 measured trials; benchmark source, instrumentation, database image/settings, and workload identities match. All matching replay digests and independent commit probes passed.
- On five-trial 1,024-fragment cases with 0/1/8 extra readers, median total Python CPU fell **22.2%/41.0%/51.2%**, PostgreSQL CPU fell **37.7%/52.1%/59.4%**, and observation queries fell **77.4–77.7%**. Turn-boundary queries fell from **1,029 to 2**; median wall time fell **11.9–14.0%**.
- Remote-reader first delivery increased from roughly **8–10 ms to 21–23 ms**; delivery P95 increased from **12–17 ms to 29–34 ms**. The 64-fragment/no-extra-reader wall P95 rose **45.04 ms** despite a lower median; cause is not isolated. Retained-P0 4,000-event full/compacted cases rose **4.37/25.68 ms**, each one exploratory trial. Primary nested write-statement counts are unchanged; WAL rose **0.04–1.76%**. Individual commits, reader duplication, full-context costs, and the previously deferred P3 latency findings remain for later work.
- **37 focused checks** and **1,010 package/bench/durable PostgreSQL stress tests passed, zero skips**; `just lint` and one independent Ponytail review passed. Migration rollback/retry preserves journal bodies and sequences. Actual shared-observation plans use `journal_terminal_idx` with one index-only result at 4,000 and 100,000 events, without event-body reads. Plans and review evidence are retained under `016-p1-checks`.
- Owned workers, Compose databases, and volumes were removed; watcher/subscription counts and expired transport backlog returned to zero. No paid inference or application changes. Stop after P1; P2 remains unstarted.

### P2 — Bounded durable delta batching · deps: P1 · DONE

**Deliver**
- Batch only provider `TextDelta`, `ReasoningDelta`, and `ToolCallDelta`. Preserve each original event and its order.
- Enable the private batching path only for the built-in PostgreSQL append path without custom coordination or `on_event` overrides. Other stores and custom event hooks retain immediate commits.
- Flush at 25 ms, 32 events, or 64 KiB. Count the complete UTF-8 delta JSON, including extra fields and escaping; flush before exceeding the byte bound and commit oversized events alone.
- Use bounded buffering with at most one pending provider read; flush on timeout even when the provider stalls. Commit an oversized event alone.
- Flush before stream completion, tool execution boundaries, and ordinary provider error/retry handling while ownership remains valid.
- Treat received buffered output as partial output for context-overflow recovery decisions.
- Run batched provider construction, sequential reads, and closure in one private context so tracing and other `ContextVar` scopes reset correctly. Immediate paths retain the caller context.
- Preserve generation checks, root ordering, hooks after commit, and existing unknown-commit handling. Never retry a possibly committed batch blindly or append after a cancellation terminal.
- Capture the unchanged fresh before campaign before runtime edits; retain benchmark identities and compare the matching after campaign with both it and retained P1 evidence. Keep append failures outside provider retry handling.

**Verify**
- Prove count, byte, and timer bounds; commit-before-delivery; exact replay identity; and immediate behavior for fallible custom event hooks.
- Inject cancellation, process death, lease loss, database failure, and lost commit acknowledgments around flush boundaries.
- Compare CPU, transactions, WAL, first-event latency, and inter-event latency against P1.

**Checklist**
- [x] Automatic batching and compatible fallback
- [x] Flush-boundary faults and before/after reports

**Verification — 2026-10-04**
- [Fresh before](../stress/bench/artifacts/016-p2-before/report.html), [final after](../stress/bench/artifacts/016-p2-after/report.html), [matched comparison](../stress/bench/artifacts/016-p2-comparison/report.html), and [retained P1 comparison](../stress/bench/artifacts/016-p2-vs-p1/report.html) passed. Each campaign retained 21 warmed scenarios and 57 measured trials, identical workload/instrumentation/database identities, and matching replay digests and independent commit probes.
- Five-trial 1,024-fragment cases with 0/1/8 extra readers reduced median Python CPU **88.5%/88.8%/85.4%**, PostgreSQL CPU **89.1%/89.3%/83.9%**, and wall time **92.8%/92.8%/92.1%**. Commits fell **1,042 → 49** and WAL fell roughly **79%**. Event bodies and individual sequences remain unchanged; header/guard work, encoding, full-history loading, and duplicate reader work remain for P3/P4.
- Remote delivery P95 increased **33.31 → 39.03 ms** with one extra reader and **29.25 → 45.19 ms** with eight. The one-trial paced case increased first delivery **17.95 → 60.60 ms** and delivery P95 **33.91 → 66.58 ms**; buffering and observation both contribute alongside database/scheduler delay. The unbatched tool-progress exploratory case regressed **115.22 ms (+6.8%)** in wall time, **14.4%** in Python CPU, and **7.2%** in PostgreSQL CPU; cause is not isolated. Reports retain these findings and maximum inter-event gaps, since burst-local P95 can hide gaps between batches. Previously deferred latency findings remain separate.
- **70 focused checks** and **1,050 package/bench/durable PostgreSQL stress tests passed, zero skips**; `just lint` and one independent Ponytail review passed. A provider `ContextVar` compatibility defect discovered after the initial review required a narrow fix and re-review: construction, sequential reads, and closure now share one context on the batched path. The superseded after run remains incomplete under `016-p2-after-superseded`; only the corrected final campaign is authoritative. Fault tests prove exact committed prefixes across cancellation, deletion, shutdown, takeover, rollback, lost acknowledgments, and process death before/during/after flush.
- Owned before/after/focused workers, Compose containers, and volumes were removed; watcher/subscription counts and expired transport backlog returned to zero. No paid inference, migration, or application changes. Stop after P2; P3 remains unstarted.

### P3 — Reduce transaction and header overhead · deps: P2 · DONE

**Deliver**
- Combine the three timeout settings and the three transaction-local authorization settings into two parameterized statements; authorize only after ownership validation. Use a materialized locking CTE so initial expiry is checked after the root row lock.
- Preserve final fencing, transaction-local authorization, and root → coordinator row → actor lock order.
- Share a connection-level PostgreSQL fast path for nonempty batches containing only `TextDelta`, `ReasoningDelta`, and `ToolCallDelta`. Read root/sequence evidence, reuse event encoding/insertion, and update native/JSON sequence, timestamp, and the conditional operational watermark atomically without Python header hydration, copying, or serialization. Mixed/empty appends retain the reducer.
- Add migration 11 with stored generated `root_key = COALESCE(NULLIF(header->>'root_id', ''), id)` and replace `sessions_root_idx` with its native-column index. Table-backed lookups use the key; session BEFORE guards still derive roots from header values. This allows testing HOT eligibility without indexing the changing header. [PostgreSQL HOT requirements](https://www.postgresql.org/docs/17/storage-hot.html).
- Preserve legacy parent traversal, deletion guards, cleanup age/revisions, extra header fields, and lifecycle projections. Keep fillfactor and database settings unchanged.
- Capture a fresh unchanged before CPU campaign and separate matched HOT/WAL probes before runtime edits. Compare the identical after campaign with fresh before and retained P2 reports; retain instrumentation/workload identities.

**Verify**
- Test pooled authorization isolation, stale writers, rollback, extra header fields, operational watermarks, and cleanup races.
- Inspect query plans and actual HOT/WAL statistics; do not assume a HOT improvement.
- Compare transaction statements and CPU against P2.

**Checklist**
- [x] Transaction/header path and migration
- [x] Fencing, cleanup parity, and measured write costs

**Verification — 2026-10-04**
- [Fresh before](../stress/bench/artifacts/016-p3-before/report.html), [verified after](../stress/bench/artifacts/016-p3-after-verified/report.html), [matched comparison](../stress/bench/artifacts/016-p3-comparison/report.html), and [retained P2 comparison](../stress/bench/artifacts/016-p3-vs-p2/report.html) preserve 21 warmed scenarios, 57 matching trials, identical benchmark/database identities, and all replay digests/commit probes. Runtime source hashes identify unchanged P2 before and measured P3 after; no timing instrumentation changed.
- Five-trial 1,024-fragment cases with 0/1/8 extra readers reduced median Python CPU **17.3%/9.2%/5.2%**, wall time **11.5%/4.5%/5.8%**, client SQL **12.3%/12.5%/9.2%**, and WAL **8.6%/8.2%/7.8%**. PostgreSQL CPU fell **5.6%/6.8%** with 0/1 readers but rose **1.8%** with eight. Commits remain **49**, nested SQL remains **8,514**, and five client setting/expiry executions were removed per coordinated transaction.
- Delivery P95 with eight readers increased **42.58 → 47.52 ms**. The 256-fragment 0/8-reader wall P95 rose **24.20/10.84 ms** despite lower medians. Other primary PostgreSQL CPU increases range **0.6–2.6%**. The one-trial unbatched tool-progress case rose **41.49 ms (+2.4%)** in wall time while Python/PostgreSQL CPU fell **12.1%/4.7%**. Causes are not isolated; all distributions and remaining reader/full-history costs are retained. Previously deferred latency findings remain separate.
- Separate [before](../stress/bench/artifacts/016-p3-hot-before/hot.json)/[after](../stress/bench/artifacts/016-p3-hot-after/hot.json) five-trial write probes recorded **0 → 1,024** HOT updates for single-event appends and **0 → 32** for 32-event batches. Median WAL fell **32.2%/4.2%** respectively, without fillfactor or database tuning. These single-session probes do not promise a production HOT rate. Actual shared-observation plans use the native `sessions_root_idx` with 10,000 unrelated sessions and 100 children; the expiry plan has a materialized locking CTE.
- **1,064 package/bench/durable PostgreSQL stress tests passed, zero skips**; focused fast-path/fencing/checkpoint/migration/cleanup checks, `just lint`, and one independent Ponytail review passed. Migration rollback/retry preserves journal bodies and header fields. The original timing campaign's full gate had one obsolete root-expression plan fixture (**1,063 passed, one failed**); the fixture now uses production `root_key` predicates. The final full suite was rerun on identical durable database settings. `016-p3-after` retains the original failure; `016-p3-after-verified` preserves its timing samples unchanged and cites the successful recheck. Verification logs and probes are retained under `016-p3-checks`.
- Owned workers, Compose containers, and volumes were removed; watcher/subscription counts and expired transport backlogs returned to zero. No paid inference or application changes. Stop after P3; P4 remains unstarted.

### P4 — Share committed reads and publish final comparison · deps: P3 · —

**Deliver**
- Share recent committed event pages within each Runtime, with independent reader cursors and single-flight page reads.
- Bound the cache to 256 events/1 MiB per actor and 64 actors/16 MiB globally. Oversized events bypass it; gaps and slow readers fall back to SQL.
- Populate only after successful commits or authoritative reads. Purge on deletion, final reader departure, and shutdown.
- Reduce duplicate empty-page/header race checks using registered interest, observed high-water marks, and signal generations. Keep conservative fallbacks.
- Filter internal Runtime actor observations to registered interests; preserve existing root-wide observer behavior.
- Reuse the assembled request for unchanged compaction projections. Preserve custom compactors, hooks, skill bodies, tool pairing, and full-history semantics.
- Publish final comparisons against P0 and each preceding phase.

**Verify**
- Exercise reader mutation isolation, reconnect cursors, missed hints, slow readers, deletion, cache eviction, and subscription cleanup.
- Verify unchanged provider requests, compaction outcomes, public replay, and retained context.
- Rerun existing baseline, 64-observer/16-turn, and 1,000-observer/100-turn workloads alongside the CPU suite.

**Checklist**
- [ ] Bounded read sharing and request reuse
- [ ] Final correctness, resource, and CPU reports

## Verification and upkeep

- **Every optimization phase requires matching before/after benchmarks and correctness checks.** Retain raw samples, commit identities, workload identities, and classified failures in CLI/JSON/HTML reports.
- Final completion requires reproducibly lower Python and PostgreSQL CPU on primary tiny-fragment workloads. Lower wall time alone is insufficient; diagnostic batching percentages are not promised production gains.
- Report latency regressions and remaining full-context/replay costs explicitly. Preserve the previously deferred P3 latency findings separately.
- Run focused tests, `just lint`, package/bench tests, PostgreSQL stress tests with zero skips, and one independent Ponytail review per code phase.
- Execute one requested phase at a time. Apply Ponytail full; add no code comments.
- Stop writers for migrations; publish migrations atomically. Mixed-version writers remain unsupported.
- Update status markers and checklists only after verification passes. Record material deviations and unresolved follow-ups.

## Risks and open decisions

- The 25 ms batching bound excludes database and scheduler delay; journal observation can add another 25 ms. Measure both.
- Process death can lose received but uncommitted buffered output; committed replay remains intact.
- The generated-column migration rewrites session rows and requires a maintenance window. It does not rewrite journals.
- Caches introduce race and memory risks; correctness and resource bounds gate completion.
- Full-history decoding remains proportional to retained history by default.

**Open decisions:** none blocking.
