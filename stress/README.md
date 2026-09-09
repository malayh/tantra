# Stress suite

Run with `uv run pytest stress` or `just stress`.

| File | Coverage |
|---|---|
| `driver.py` | Deterministic synthetic provider, policies, blobs, embeddings, and retry injection |
| `invariants.py` | Provider tool-pair integrity, context-window bounds, and actor-journal sequence/header consistency |
| `test_kitchen.py` | Hooks, parallel tools, permissions, live asks, memory, skills, output schemas, cancellation, malformed calls, retries |
| `test_tree.py` | Deep actors, explicit finish delivery, live descendant asks, independent journals, parallel siblings, interruption, depth permissions |
| `test_marathon.py` | Long histories, pruning, summarization, tail preservation, live ask after compaction, writer replacement |
| `test_scale.py` | 10k-event replay, session paging, memory scale, same-Runtime ask answers, writer takeover, independent roots |
| `live_raw.py` | Human-driven OpenAI-compatible Runtime smoke |
| `live_telemetry.py` | Runtime and child-actor OTLP export smoke |
| `live_fetch_proxy.py` | Direct and proxied web fetch comparison |

The store matrix uses memory, filesystem, SQLite, and PostgreSQL. PostgreSQL cases skip when Docker is unavailable.
