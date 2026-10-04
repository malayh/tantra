# Stress suite

Run with `uv run pytest stress` or `just stress`.

| File | Coverage |
|---|---|
| `driver.py` | Deterministic synthetic provider, policies, blobs, embeddings, and retry injection |
| `invariants.py` | Provider tool-pair integrity, context-window bounds, and actor-journal sequence/header consistency |
| `test_kitchen.py` | Hooks, parallel tools, permissions, live root asks, memory, skills, output schemas, cancellation, malformed calls, retries |
| `test_tree.py` | Deep actors, explicit finish delivery, root-only asks, independent journals, parallel siblings, interruption, depth permissions |
| `test_marathon.py` | Long histories, pruning, summarization, tail preservation, live ask after compaction, writer replacement |
| `test_scale.py` | 10k-event replay, session paging, memory scale, same-Runtime ask answers, writer takeover, independent roots |
| `live_raw.py` | Human-driven OpenAI-compatible Runtime smoke |
| `live_telemetry.py` | Runtime and child-actor OTLP export smoke |
| `live_fetch_proxy.py` | Direct and proxied web fetch comparison |

The store matrix uses memory, filesystem, SQLite, and PostgreSQL. PostgreSQL cases skip when Docker is unavailable.

## Automated Runtime bench

`just bench` owns an isolated Docker Compose PostgreSQL database and starts two Runtime worker processes. It requires Docker Compose and fails if PostgreSQL cannot run. Normal durability stays enabled. It tears down only its own project and volume.

```sh
just bench baseline --stress
just bench baseline --sessions 12 --histories 40 400 --samples 1
just bench baseline --history-mode compacted --output stress/bench/artifacts/compacted
just bench scale
just bench cpu --stress
just bench cpu --scenario text-1024-readers-8 --sessions 12 --samples 1
just bench live
just bench live --scenario sql_read
just bench live --suite smoke
just bench replay
just bench replay --scenario compacted_recall --history-mode compacted
just bench scale --faults
just bench compare --before path/to/before/report.json --after path/to/after/report.json
```

Baseline defaults to 10,000 stored sessions, 4,000/100,000-event histories, and five repetitions. It measures claim/release, send, duplicates, writer replacement, context assembly, playback, and owner-death recovery. Scale defaults to 1,000 observers and 100 synthetic active turns; its idle and post-disconnect measurements expose shared catch-up cost and require watchers and coordinator subscriptions to return to zero. The preserved P2 concurrent reference uses the same 10,000-session full fixture with 64 observers and 16 active turns at `stress/bench/artifacts/p3-p2-scale-reference/report.json`.

`--history-mode compacted` seeds a summary after each historical journal and runs Runtime with retained context. Its context measurements read that retained window; public playback still reads every event. This has a distinct fixture identity and cannot be compared directly with full-mode reports. The default full workload remains unchanged.

Reports are JSON and standalone HTML under ignored `stress/bench/artifacts/`. Raw samples include CPU, current/lifetime peak RSS, SQL executions and fetched rows, notifications, event-loop delay, pool capacity and wait deltas, dispatcher concurrency, observation checks, routed wake-ups, Runtime watchers, coordinator subscriptions, and expired transport backlog. SQL and metric deltas include background work during the operation. Setup, verification, and provider readiness have separate labels. These are direct Runtime measurements, not WebSocket/server throughput measurements.

Only explicit `live` makes paid inference requests, using `OPENROUTER_API_KEY` and `z-ai/glm-5.3-flash`. Live/replay default to seven behavioral scenarios: SQL reads, approved and denied audited writes, skills, typed output, child completion, and fresh compaction followed by recall after restart. `--suite smoke` retains the original total-tool check; repeat `--scenario` to select cases. Background turn controls keep approval and fault commands responsive. Oracles use journal invariants, typed results, and independent SQL evidence; no LLM judge.

Live bypasses response recordings and disables OpenRouter response caching; provider prompt caching remains allowed. Replay requires exact request and fixture identities, preserves provider wait times without duplicating database-consumer delay, and fails closed on a miss. A full-mode replay and a compacted-mode recall replay check equivalent retained context. Fixture writes are explicitly idempotent; arbitrary external tools do not gain an exactly-once guarantee.

`scale --faults` runs the separate deterministic two-process fault suite: writer replacement/cursor reconnect, owner death, cancellation commit boundaries, lost notifications/expired transport, slow readers, and interruption of the owned database. `--inject-oracle-failure` deliberately fails that run to check reporting. Default scale and baseline workload identities remain unchanged. Reports retain expected fault evidence separately from classified failures; failed required scenarios produce a nonzero exit.

The stable `journal-scaling` campaign has a 5,000,000-token ceiling across reruns, enforced by `stress/bench/artifacts/campaign.sqlite3`. Input including cache reads/writes, output including reasoning, compaction, and every retry count. Cache/reasoning detail fields are subsets of prompt/completion totals and are not counted twice. Each HTTP attempt reserves the verified model context capacity plus the 4,096-token output limit before dispatch. Completed usage settles that reservation; unknown usage remains reserved. Startup reconciliation queries at most 20 saved generation IDs and releases reservations only for complete, matching native-token usage. The ledger upgrades in place and retains its prior requests. Fresh workflows run serially by default; a shared four-slot gate covers both workers, child calls, and compaction until streams close. Recordings link exact request keys to ledger usage. The campaign targets less than 250,000 reported tokens, without weakening the five-million hard ceiling. The ledger and recordings survive database teardown. Do not delete the ledger to reset tests within the same campaign. `--campaign` explicitly starts or resumes a separately authorized campaign.

The approved [scaling spec](../design/013_journal_scaling_and_live_bench.md) defines phase gates. Current stress tests remain useful deterministic regression coverage; `baseline --stress` runs them against the bench database and rejects skips.

P3 serial latency regressions remain a documented follow-up. P4 gates behavioral correctness and bounded database/resource use; it does not require latency tuning. Reports separate provider waits, worker CPU/SQL, committed tool and post-provider timing boundaries, and model behavior failures. Behavioral gate measurements include oracle reads; they are not isolated runtime overhead. Performance regressions remain visible.

## Active-stream CPU bench

`just bench cpu` records a pre-optimization baseline for [spec 016](../design/016_active_runtime_cpu.md). It uses two Runtime worker processes, 10,000 unrelated stored sessions, normal PostgreSQL durability, and CPU-only Compose instrumentation. Existing baseline/scale fixtures and database configuration remain unchanged. No event hooks or inference requests run.

Nine primary cases emit the same 16 KiB output in 64/256/1,024 unique ordered fragments, with 0/1/8 extra readers. Each primary case gets a warm-up and five measured trials by default; execution order rotates and reverses. Extended cases get a warm-up and one measured trial: paced output, an 8,192-fragment 128 KiB stream, reasoning, tool arguments, tool progress, a slow reader, 100 children, 4,000/100,000-event full/compacted histories, and an every-delta commit audit. These single-trial costs are exploratory, not improvement claims. Repeat `--scenario` to filter the fixed cases; `--samples` sets primary repetitions. A filtered/small run is not marked baseline-eligible.

Each report distinguishes owner/reader Python process CPU and database cgroup CPU, top-level/nested SQL, WAL, memory, event-loop delay, observation/notification activity, and emission-to-reader delivery delay. Process windows include control IPC and commit probes; PostgreSQL CPU includes background work and counter probes. SQL statements include nested guards with planning instrumentation consistently disabled. Fixture seeding, warm-ups, full database audits, and cleanup are outside measured windows.

Reader sequence/content digests are compared against independently decoded SQL journal rows. First/middle/last delta probes check committed watermarks; the dedicated commit-audit case checks every delta. `--cpu-failure order|duplicate|drop|premature` deliberately fails the relevant oracle, writes a FAILED report, and exits nonzero. CPU comparison rejects incompatible instrumentation, fixture/trial identities, failed evidence, and changed replay digests.

`cpu --stress` runs package, bench, and PostgreSQL stress tests against its owned durable database and rejects skipped cases. Reports and records stay under ignored `stress/bench/artifacts/`; the owned workers, Compose project, and volume are removed afterward. P0 changes benchmark code only. Runtime CPU reductions and matched before/after comparisons belong to the following phases.
