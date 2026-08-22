# stress/

Capability stress harness for tantra — synthetic long-horizon scenarios that push every feature past what the unit tests cover. Committed as a reusable regression harness. Not collected by `just test` (root `testpaths` excludes it).

## Run

- `just stress` — full suite, ~2 min (needs `uv sync` dev deps only)
- `just stress stress/test_marathon.py` — one module
- `just stress -k '[postgres]'` — one backend; every test is parametrized over `memory` / `fs` / `sqlite` / `postgres`
- `just stress -s stress/test_scale.py` — show the timing prints
- Run from the repo root.

## Postgres

- Auto-spins `pgvector/pgvector:pg17` via the docker CLI (session-scoped container, stopped on teardown). Without docker or psycopg the postgres params skip cleanly.
- `TANTRA_POSTGRES_DSN=postgres://…` uses an external server instead; per-test schemas are dropped afterwards.

## Layout

| File | What it covers |
|---|---|
| `driver.py` | `SyntheticProvider` — policy-driven programmatic `Provider` (deterministic, small `ModelLimits` so compaction fires often), policy helpers (`worker_policy`, `by_model`, `flaky`), `SyntheticEmbedder` |
| `invariants.py` | `check_pairs` (no orphaned tool_call/result in any assembled request), `check_window` (requests stay inside the usable window), `check_log` (seq contiguity, header/lease consistency), `pairs_intact` (log-level); all raise on vacuous input and return counts callers can pin |
| `conftest.py` | `store` fixture over the four backends |
| `test_marathon.py` | long-horizon compaction: repeated prune + summarize, marker-fact survival, suspend-after-compaction, process swap mid-marathon |
| `test_tree.py` | subagent trees at max depth, bubbled asks resumed from fresh harnesses, `fan_out` mixed outcomes, abandon-mid-spawn |
| `test_kitchen.py` | raw-library adopter app: six hooks, permission matrix, `ctx.ask` re-execution, memory incl. backfill + hybrid recall, skills, `output_schema`, cross-harness cancel, `max_steps`, retries |
| `test_scale.py` | stores at scale: 10k events, 200-session paging, 1k memory rows, cross-instance handoff, lease contention |
| `live_raw.py` | manual live smoke, never collected by pytest |
| `live_fetch_proxy.py` | manual live smoke of `web_fetch(proxy=...)`, never collected by pytest |
| `live_telemetry.py` | manual live smoke of `Telemetry` against a real OTLP collector, never collected by pytest |

## Live smoke (manual, network)

- Needs `OPENAI_API_KEY`, `OPENAI_ENDPOINT`, `AGNI_MODELS` exported; refuses to run without them.
- `uv run python stress/live_raw.py` — one real turn: subagent delegation, one interactive approval on stdin, structured output printed.
- `uv run python stress/live_raw.py check` — builds the harness and exits, no network.
- `TANTRA_TEST_PROXY=http://user:pass@host:port uv run python stress/live_fetch_proxy.py` — fetches an IP echo directly and through the proxy, prints both IPs, non-zero if the proxied fetch fails or matches the direct IP. Refuses to run without `TANTRA_TEST_PROXY`.
- `OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.example/otel OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic\ <b64> uv run python stress/live_telemetry.py` — one scripted turn (tool call, subagent spawn, final answer) exported over OTLP with `capture_content=True`; prints the span tree and the trace id. The model is faked, so the collector is the only network hop. `OTEL_SERVICE_NAME` is optional and defaults to `tantra-smoke`. Refuses to run without `OTEL_EXPORTER_OTLP_ENDPOINT`, and exits non-zero if the collector rejects the batch. Then check the backend: an agent observation with input and output, a generation per model call, a tool observation with arguments and result, and a nested agent under the spawning tool.

## Rules

- Import only public `tantra` names. One sanctioned exception: provider event types (`ToolCall`, stream events) from `tantra.providers.base` — required to implement any `Provider`, not yet exported (recorded in the spec's P13 landed notes).
- No network in pytest scenarios; everything is seeded and deterministic.
- New scenarios should run their captured requests/logs through `invariants.py` — the checkers are mutation-tested; keep them honest.
