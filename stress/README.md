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
just bench scale
just bench live
just bench replay
just bench compare --before path/to/before/report.json --after path/to/after/report.json
```

Baseline defaults to 10,000 stored sessions, 4,000/100,000-event histories, and five repetitions. It measures claim/release, send, duplicates, writer replacement, context assembly, playback, and owner-death recovery. Scale defaults to 1,000 observers and 100 synthetic active turns; its idle and post-disconnect measurements expose polling and watcher retention.

Reports are JSON and standalone HTML under ignored `stress/bench/artifacts/`. Raw samples include CPU, current/lifetime peak RSS, SQL executions and fetched rows, notifications, event-loop delay, and expired transport backlog. SQL measurements include background work during the operation. Setup, verification, and provider readiness have separate labels. These are direct Runtime measurements, not WebSocket/server throughput measurements.

Only explicit `live` makes paid inference requests, using `OPENROUTER_API_KEY` and `z-ai/glm-5.3-flash`. P0's smoke calls a real database tool and checks the returned total; the full behavioral campaign belongs to P4. Live always bypasses recorded responses. Replay requires exact completed recordings, preserves provider wait times without duplicating database-consumer delay, and fails closed on a miss.

The stable `journal-scaling` campaign has a 5,000,000-token ceiling across reruns, enforced by `stress/bench/artifacts/campaign.sqlite3`. Input including cache reads, output including reasoning, and every retry count. Each HTTP attempt reserves the verified model context capacity plus the 4,096-token output limit before dispatch. Completed usage settles that reservation; unknown usage remains reserved. The ledger and recordings survive database teardown. Do not delete the ledger to reset tests within the same campaign. `--campaign` explicitly starts or resumes a separately authorized campaign.

The approved [scaling spec](../design/013_journal_scaling_and_live_bench.md) defines phase gates. Current stress tests remain useful deterministic regression coverage; `baseline --stress` runs them against the bench database and rejects skips.
