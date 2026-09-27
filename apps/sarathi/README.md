# Sarathi

A deep-search chat system in the style of Perplexity — ask anything, watch it think, search the web, read your PDFs, and remember you. Built as the demo app for [tantra](../../README.md) (`tantra-harness`): the actor runtime owns streaming, subagents, approvals, cancellation, compaction, and durability.

## Demo

[![Sarathi demo](https://img.youtube.com/vi/yAnC1LHKQZk/maxresdefault.jpg)](https://youtu.be/yAnC1LHKQZk)

## What it demonstrates

- **Streaming turns with visible thinking** — reasoning deltas render live, collapse when done.
- **Skill-based research** — Sarathi researches inline by default and can delegate independent work to a named general-purpose `subagent`; both load the same `research` skill with `shallow`, `normal`, or `deep` effort.
- **Lazy subagent inspection** — lightweight actor status is polled while child journals stream only when their drawers are open.
- **Adaptive compaction** — each actor keeps an independent durable summary and bounded recent tail using configured, discovered, or conservative model limits.
- **Stop that works** — one click sends a durable cancellation command for the whole actor tree, and the thread stays usable.
- **Human-in-the-loop** — root memory writes suspend behind an approval card; subagents have no approval-gated tools or human asks.
- **Durability** — each actor has an independent journal, including streaming deltas; reconnect through either backend replays after that actor's browser cursor while PostgreSQL coordinates the single execution owner.
- **Per-user memory** — `memory_tools(scope=...)` stamps every row with the tenant; recall and the memory panel never cross users.
- **Document grounding** — upload a PDF, the agent reads it with `read_doc` and answers from it.
- **Model switching** — per-session model updates are atomic and apply to the next turn.

## Stack

- `backend/` — two FastAPI + tantra processes, a PostgreSQL coordinator and event-sourced store, and WebSockets served through Nginx.
- `ui/` — Next.js 15, React 19, Tailwind v4, shadcn/ui; API client generated from OpenAPI (Orval).

## Run it

Requires Docker.

```bash
cd apps/sarathi
cp .env.example .env
```

Set in `.env`:

- `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `SARATHI_MODELS` — any OpenAI-compatible endpoint; two models make the picker interesting.
- `SARATHI_CONTEXT_WINDOW` — optional explicit context window for every configured chat model. When unset, the provider discovers per-model limits lazily and falls back to 128k context and 4k output limits if discovery fails. Explicit Sarathi limits use an 8,192-token output reserve.
- `SECRET_KEY`, `NEXTAUTH_SECRET` — any random strings.
- `BRAVE_API_KEY` — optional; without it web search is disabled and deep-search prompts degrade.
- `EMBEDDING_MODEL` — optional; enables vector memory recall.
- `WEB_PROXY` — optional; routes `web_fetch` through an HTTP/SOCKS proxy.
- `OTEL_EXPORTER_OTLP_ENDPOINT` — optional; setting it turns on OpenTelemetry tracing of every turn. Unset disables telemetry entirely.
- `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME` (default `sarathi`), `OTEL_RESOURCE_ATTRIBUTES` — optional; the standard OTel variables, parsed by the SDK. Values set here are copied into the environment before the SDK starts, and a real environment variable always wins.
- `TELEMETRY_CAPTURE_CONTENT` — optional; `true` puts prompts, completions and tool results on the spans. Off by default.

```bash
docker compose up --build -d
```

Open http://localhost:3001, sign up, chat. Nginx serves the API and WebSockets on http://localhost:8001 and round-robins both backends without affinity.

Both backends use the same image, configuration, database, and uploads volume. PostgreSQL leases one Runtime as the execution owner for each root while writer claims, commands, journal readers, and recovery work across either backend. Add `?view=readonly` to a chat URL for an observing tab that never claims the writer. Storage created by Tantra 1.0 upgrades to 1.1 without a destructive migration. Pre-1.0 storage still requires a fresh database. Legacy `researcher` history remains readable but cannot resume; new work uses `subagent`.

After an owner loss, the next writable reconnect or mutation recovers the whole root after lease expiry, interrupts abandoned started turns, expires asks, and drains accepted unstarted inputs. `docker compose restart` does not re-read `.env` — after editing it, recreate with `docker compose up -d backend_a backend_b ui`.

For deterministic coordinator tests, start the isolated E2E project with `docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml up --build -d`. Its separate volumes and network use disposable Nginx/API and UI ports `127.0.0.1:18000` and `127.0.0.1:13001`; backend A uses `127.0.0.1:18001`, backend B uses `127.0.0.1:18002`, and the loopback-only provider/tool gate uses `127.0.0.1:18090`. The override defaults `E2E_OPENAI_BASE_URL` to that gate and shortens only the disposable stack's lease. Each port and the lease have environment variable overrides, so this stack can run beside the default deployment.

## Development

Backend (from `backend/`): `uv sync --dev`, then `just runserver` / `just test` / `just lint` / `just migrate`.

UI (from `ui/`): `yarn install`, then `just runserver`; `yarn typegen` regenerates the API client after backend schema changes (never edit `ui/src/generated/`).

## E2E

`e2e/runbook.md` — scenarios driven live through a browser agent, reports land in `e2e/reports/`.

`e2e/coordinator.md` — disposable two-backend failure matrix, deterministic gate controls, evidence queries, and recovery notes.
