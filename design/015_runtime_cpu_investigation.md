# 015 — Runtime CPU investigation

**Date:** 2026-10-04 · **Status:** investigated; implementation pending.

## Finding

Active streaming does excessive work per provider fragment. Command and recovery reads are bounded after spec 013, but streaming still commits each delta separately, rewrites the session header, publishes a change, and immediately refreshes observations. Readers and result waits amplify this work. This explains Python and PostgreSQL CPU rising together.

The scaling bench missed the magnitude: its active synthetic answer is `60`, with large historical journals seeded in batches. It measures history, recovery, concurrency, and idle observation well, but does not model thousands of tiny live fragments. Add this workload before claiming active-stream CPU improvement.

The retained live campaign reinforces that gap: sixty recorded GLM streams had a median of 26.5 fragments and a maximum of 162; the largest output was 2,335 characters. Their median average fragment length was 12.5 characters, so the investigation's 16-character fragments are representative, while its 16 KiB response exercises a larger output. [Recording statistics](../stress/bench/artifacts/cpu-investigation/existing-live-fragments.json) contain no prompt bodies. Recorded provider waits exclude consumer database work and cannot establish original end-to-end throughput.

## Evidence

[Machine-readable results](../stress/bench/artifacts/cpu-investigation/report.json), [HTML report](../stress/bench/artifacts/cpu-investigation/report.html), profiles, SQL statements, and reproduction scripts are retained together. Production runtime and application code were unchanged. No paid inference ran.

The test used commit `88dd512`, Python 3.13, Psycopg's binary implementation, and isolated PostgreSQL 17.10 through the bench's durable Compose setup, with `fsync=on` and `synchronous_commit=on`. Forty-three database/runtime cases and nineteen warmed SDK measurements completed. Two Runtime instances ran in one profiling process; Python CPU includes both when a remote reader is enabled. This is an attribution experiment, not the existing two-process throughput benchmark.

All main comparisons emit the same 16,384 characters. Rows below are medians of three unprofiled runs. CPU is process/container CPU time, not wall time. Container CPU includes PostgreSQL background processes. Shared-host noise and instrumentation prevent treating these as universal latency guarantees.

| Workload | Delta events | Extra readers | Wall time | Python CPU | PostgreSQL CPU |
|---|---:|---:|---:|---:|---:|
| Larger fragments | 64 | 0 | 0.54 s | 0.181 s | 0.180 s |
| Tiny fragments | 1,024 | 0 | 6.95 s | 2.420 s | 1.813 s |
| Tiny fragments | 1,024 | 1 | 7.89 s | 3.258 s | 3.089 s |
| Tiny fragments | 1,024 | 8 | 10.81 s | 6.297 s | 4.482 s |

The writable connection itself observes the root, even with zero extra readers. At 1,024 deltas, this produced about 1,049 observation checks and 1,029 unfinished-result boundary lookups. One extra reader produced approximately 2,078 observation queries across both runtimes, 3,087 journal page queries, and 2,067 plain header queries. A follow-up run counted **26,005 client SQL executions plus 15,458 nested PostgreSQL statements**. Earlier raw SQL counts include both; they are not all network round trips.

## Where the CPU goes

### 1. One durable transaction per fragment

[`TurnEngine._sample`](../packages/tantra/src/tantra/loop.py) awaits `_append([event])` for every text, reasoning, and tool-call delta. [`Runtime._engine_append`](../packages/tantra/src/tantra/runtime.py) obtains the root lock and opens a new fenced transaction.

[`_TransactionContext` and `CoordinatedStore.append`](../packages/tantra/src/tantra/coordinator.py) perform approximately seventeen client statements per append including transaction boundaries: three timeout settings, advisory/root locking, an expiry query, three authorization settings, session locking, event insertion, header update, another fence check, change insertion, and notification. The whole header is validated, deep-copied, serialized, and rewritten even for a header-neutral delta.

Database triggers independently enforce ownership and permanent deletion markers on the event insert and header update. These protections matter; reducing repeated work must retain stale-writer and resurrection rejection.

### 2. Notifications trigger additional SQL immediately

[`PostgresCoordinator._listen/_observe/_catch_up`](../packages/tantra/src/tantra/coordinator.py) route journal hints correctly, but immediately query current state for every dirty root. The catch-up interval bounds idle polling; it does not throttle active hints. The query fetches **every actor in that root**, even when only one actor is being watched.

[`Runtime._wait_observed_result`](../packages/tantra/src/tantra/runtime.py) looks for completion whenever the observed journal sequence changes. [`PostgresStore.read_turn`](../packages/tantra/src/tantra/stores/postgres.py) correctly avoids reading an unfinished turn's body, but still performs its indexed boundary lookup approximately once per delta.

Each independent event generator reads the same committed rows again. The empty/race-check path in `Runtime._stream` adds repeated page and header reads before blocking. Eight readers substantially increased CPU even though coordinator observation was shared.

### 3. Full-history decoding causes additional Python spikes

Osuite and Sarathi construct Runtime without `history_mode="compacted"`. Each turn therefore decodes the full journal before building messages. In the profiled 100,000-event case, `_parse` ran about 100,072 times; typed event validation, JSON/base64 decoding, and message assembly consumed substantial CPU.

With 64 fresh deltas, the unprofiled 100,000-event full-history turn used **1.112 s Python CPU and 1.534 s wall time**. A matching fixture with a committed summary and compacted loading used **0.205 s CPU and 0.613 s wall time**. This is one sample per mode with a minimal retained window; no marker means compacted mode still loads full history. Public replay remains complete.

`PruneThenSummarize` can rebuild and serialize a request already assembled by the turn engine. This is a source-confirmed extra cost, not separately quantified here. Static Osuite prompts and its current turn hook do not inspect historical events; opting that application into compacted loading is plausible after callback and recall verification.

### 4. Header/index maintenance amplifies writes

The root index uses an expression over `header`. Every delta changes that JSON value's timestamp. Before the diagnostic index removal, thousands of session updates produced **zero HOT updates**. Removing only that index in the owned test database made 3,075 subsequent session updates HOT; the index was restored afterward.

The raw append comparison reduced SQL-attributed WAL from approximately **1.877 MB to 1.270 MB**, with an observed PostgreSQL CPU median about **6% lower**, and no clear Python or wall-time improvement. Index-off runs were later, not interleaved controls; this CPU difference is directional. An indexed stored/native root key could retain indexed root lookup while permitting HOT updates. Dropping root lookup indexing in production would harm observation and is not the proposed fix.

The newer deletion/timestamp triggers add measurable work but are secondary: a diagnostic run without them used about 1.166 s PostgreSQL CPU versus 1.271 s with them. Two diagnostic samples are insufficient to claim a release regression. Trigger guards were restored; disabling them is unsafe for production.

### 5. Provider SDK work is smaller in these cases

An offline HTTP fixture exercised the actual `OpenAICompatible` and OpenAI SDK SSE path, including text, reasoning, and tool arguments. After warm-up, 1,024 fragments used about **94/79/108 ms Python CPU**, respectively, without storage. At 8,192 fragments, text/tool arguments used about **782/881 ms**. These costs grow with fragment count but were much smaller than the durable path at the same count. Socket/network waits and application WebSocket serialization were excluded.

Profiles show high Psycopg and event-loop call volume around query preparation, waiting, result conversion, and pool checkout. cProfile measures elapsed time here; async/cumulative times cannot assign CPU shares. In particular, `epoll.poll` time is waiting. Actual CPU comes from `process_time` and the container counter; the batch/read comparisons establish the cost of repeated operations. Profiled timings are kept separate because profiling roughly doubled Python overhead.

## Controlled optimization experiments

These were temporary diagnostic variants, not production changes.

| Experiment | Python CPU before → after | PostgreSQL CPU before → after | Interpretation |
|---|---:|---:|---|
| Raw append: commit batches of 16, retain all 1,024 individual events | 1.841 → 0.175 s | 1.271 → 0.171 s | About 91%/87% less CPU; wall time 6.255 → 0.472 s. No provider, cancellation, or consumer timing claim. |
| Debounce non-control observation hints by 25 ms, one reader | 3.258 → 2.388 s | 3.089 → 1.776 s | About 27%/43% less CPU; both runtimes' observation checks fell about 76%. Event rows/commits were unchanged. |
| Combine three timeout settings in one SQL statement | 2.420 → 2.227 s | 1.813 → 1.566 s | Observed medians about 8%/14% lower; PostgreSQL ranges overlap, so this is directional. |

Raw append checks cover delta count and reconstructed output. The generated fragments are identical, so those checks cannot prove identity/order or delivery timing. Reader cases separately checked contiguous sequences and reconstructed output. The append experiment submits individual delta rows in batches; it proves CPU potential, while exact identity/order, fault boundaries, and a production buffering policy remain implementation verification work.

Adding 10,000 unrelated stored sessions left statement counts essentially unchanged, and the real catch-up query used `sessions_root_idx`; its sampled execution took 0.065 ms. This rules out a global session scan in that tested observation path. A root with 100 children increased Python CPU from 0.823 to 1.195 s for 256 deltas with one reader, consistent with fetching all root actors on each hint.

A five-second provider delay with only one eventual fragment used 0.097 s Python CPU across both runtimes. That workload does not show a CPU busy loop while awaiting inference. Multiple active chats, fine-grained output, long history, and reader fan-out are the measured multipliers.

## Recommended changes

1. **Reduce observation/result amplification first.** Debounce journal-only hints with a short bound; preserve prompt wake-up for control changes, deletion, ownership, and replies. Include a terminal watermark/evidence in shared observation so a result waiter queries when a terminal advances, rather than for every delta. Keep distinct periodic samples for inactive-owner detection and authoritative catch-up after missed notifications. A waiter must still find an older command if newer turns finish before it wakes.
2. **Introduce bounded durable delta batches.** Retain individual rows and sequences, commit before hooks or public delivery, and cap count, bytes, and delay. Flush before stream completion, provider errors/retry decisions, tool boundaries, and cancellation/control transitions. Preserve root ordering, generation checks, fencing, partial-output behavior, and unknown-commit handling. Measure the full Runtime path and fault suite before selecting a default; sixteen-event raw batches are evidence, not a recommended fixed setting.
3. **Amortize transaction/header work.** Combine settings and consistent fence reads. For header-neutral events, avoid Python header hydration/deep-copy/dump; maintain sequence, timestamp, and operational watermark atomically. Consider a stored/native root key to preserve indexed observation while allowing HOT updates. Verify legacy root relationships, cleanup revisions/age, rollback, and late-writer rejection.
4. **Reduce reader and history duplication.** Share bounded committed event pages locally, with independent cursors and SQL fallback for slow/reconnected readers. Bound active actor reads. Reuse the assembled request in compaction. Adopt compacted loading in compatible applications after hook/recall checks.

Increasing the pool or moving all event decoding into PostgreSQL does not address the measured cause. Python primarily pays for thousands of driver/scheduler operations; PostgreSQL pays for tiny transactions, guards, JSON/index work, and WAL. Fewer operations help both sides. Preserve synchronous durability, event identity, and complete replay.

## Verification needed for implementation

- Add fixed-output fragment matrices: 64/256/1,024/8,192 deltas; paced and burst output; text, reasoning, tool arguments, and tool progress; short/full/compacted histories; multiple readers and root widths.
- Report CPU seconds per 1,000 deltas and per output byte, client versus nested SQL, WAL, observation/result reads, and provider wait separately. Extend the existing bench rather than create another framework.
- Gate unchanged event/replay digests, tool/skill pairing, bounded memory, delivery-after-commit, cancellation/error flushes, writer takeover, missed hints, disconnect, slow readers, and database failure.
- Compare repeated matched workloads with profiling disabled. Retain the earlier P3 latency regressions independently; this investigation does not explain every previous latency change.

## Reproduction and limits

The artifact directory contains `tantra_cpu_probe.py`, `tantra_cpu_extras.py`, `tantra_cpu_followup.py`, `run_sdk.py`, and `setup_database.py`. Run from the repository with its virtual environment, Docker daemon access, and `PYTHONPATH` including the repository and artifact directory. Setup requires a dedicated Compose project and verifies durability. Stop that project after testing; never point the diagnostic trigger/index experiments at an application database. The owned project and volume were removed after collection; trigger guards and root indexing were restored before shutdown.

`pg_stat_statements.track=all` exposes nested trigger statements. Planning instrumentation was enabled during the third initial repetition and remained enabled for follow-ups; SQL counts stayed stable, but fine latency differences between diagnostic variants are approximate. The first history attempt lacked a coordinated seeding fence and the initial tool SSE fixture supplied null continuation fields; both harness mistakes were corrected. Those failed logs are retained and excluded from results. No current production process was sampled, so application tool CPU, real network behavior, telemetry exporters, and PostgreSQL configuration under the user's observed workload remain unmeasured.
