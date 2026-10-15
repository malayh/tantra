# Sarathi

A deep-search chat system in the style of Perplexity — ask anything, watch it think, search the web, read your PDFs, and remember you. Built as the demo app for [tantra](../../README.md) (`tantra-harness`): the actor runtime owns streaming, subagents, approvals, cancellation, compaction, and durability.

## Demo

[![Sarathi demo](https://img.youtube.com/vi/yAnC1LHKQZk/maxresdefault.jpg)](https://youtu.be/yAnC1LHKQZk)

## What it demonstrates

- **Streaming turns with visible thinking** — reasoning deltas render live, collapse when done.
- **Skill-based research** — Sarathi researches inline by default and can delegate independent work to a named general-purpose `subagent`; both load the same `research` skill with `shallow`, `normal`, or `deep` effort.
- **Lazy subagent inspection** — child status arrives as ACP `subagent_update` while child transcripts stream only when their drawers are open.
- **Adaptive compaction** — each actor keeps an independent durable summary and bounded recent tail using configured, discovered, or conservative model limits.
- **Stop that works** — one click sends a durable cancellation command for the whole actor tree, and the thread stays usable.
- **Human-in-the-loop** — root memory writes suspend behind an approval card; subagents have no approval-gated tools or human asks.
- **Durability** — each actor has an independent journal, including streaming deltas; reconnect through either backend replays after that actor's browser cursor while PostgreSQL coordinates the single execution owner.
- **Per-user memory** — `memory_tools(scope=...)` stamps every row with the tenant; recall and the memory panel never cross users.
- **Document grounding** — upload a PDF, the agent reads it with `read_doc` and answers from it.
- **Model switching** — per-session model updates are atomic and apply to the next turn.

## Session transport

Sarathi serves tantra's ACP interface (`tantra.acp` with the `tantra` profile) at WebSocket `/api/acp`; the UI uses the local `tantra-acp-client` package and its transcript reducer. Authentication, scoping, the access policy, and attachment encoding remain application-owned: prompts carry text plus `resource_link` blocks whose `file://` URIs must stay under the user's upload directory.

The browser requests a user-scoped ticket through `POST /api/acp/ticket` (`{"mode": "write"}`) before each dial. Tickets are reusable for at most 30 seconds and never outlive the login; the socket closes at login expiry. WebSocket URLs carry the ticket rather than the login token. Both backends must share `SECRET_KEY`, and `CORS_ORIGINS` must include the actual browser origin. `read` tickets serve observers such as the E2E client.

Each tab holds one ACP connection. Opening a chat resumes it from the start and leaving closes it. Editable pages resume as writer. Writer replacement or a dropped connection leaves history readable and requires **Reclaim control** before Send, Stop, answers, or model changes; prompts in flight during a drop are held and resent with their original command IDs after reclaim. `?view=readonly` resumes read-only and never claims control. Ask details reach only the writer, so readers see "Awaiting input". Prompts resolve on durable acceptance and show in the queue strip until they start.

Deploy both backends and the UI together; pages using the previous wire protocol require refresh. Local UI development builds `packages/tantra-acp-client` before consuming its file dependency. The UI Compose build context is the repository root so its standalone image includes that package.

## Stack

- `backend/` — two FastAPI + tantra processes, a PostgreSQL coordinator and event-sourced store, and the ACP WebSocket served through Nginx.
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

Open http://localhost:3001, sign up, chat. Nginx serves the API and the ACP WebSocket on http://localhost:8001 and round-robins both backends without affinity.

Both backends use the same image, configuration, database, and uploads volume. PostgreSQL leases one Runtime as the execution owner for each root while writer claims, commands, journal readers, and recovery work across either backend. Add `?view=readonly` to a chat URL for an observing tab that never claims the writer. Storage created by Tantra 1.0 or 1.1 upgrades additively to 1.2. Pre-1.0 storage still requires a fresh database. Legacy `researcher` history remains readable but cannot resume; new work uses `subagent`.

After an owner loss, the next writable reconnect or mutation recovers the whole root after lease expiry, interrupts abandoned started turns, expires asks, and drains accepted unstarted inputs. `docker compose restart` does not re-read `.env` — after editing it, recreate with `docker compose up -d backend_a backend_b ui`.

For deterministic coordinator tests, start the isolated E2E project with `docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml up --build -d`. Its separate volumes and network use disposable Nginx/API and UI ports `127.0.0.1:18000` and `127.0.0.1:13001`; backend A uses `127.0.0.1:18001`, backend B uses `127.0.0.1:18002`, and the loopback-only provider/tool gate uses `127.0.0.1:18090`. The override defaults `E2E_OPENAI_BASE_URL` to that gate and shortens only the disposable stack's lease. Each port and the lease have environment variable overrides, so this stack can run beside the default deployment.

## Development

Backend (from `backend/`): `uv sync --dev`, then `just runserver` / `just test` / `just lint` / `just migrate`.

UI: run `npm ci && npm run build` in `packages/tantra-acp-client`, then run `yarn install` and `yarn dev` from `apps/sarathi/ui` (yarn copies the file dependency, so rerun `yarn install` after rebuilding the client); `yarn typegen` regenerates the API client after backend schema changes (never edit `ui/src/generated/`).

## E2E

`e2e/runbook.md` — scenarios driven live through a browser agent, reports land in `e2e/reports/`.

`e2e/coordinator.md` — disposable two-backend failure matrix, deterministic gate controls, evidence queries, and recovery notes.

The automated native-client workflow is `node apps/sarathi/e2e/acp-client.mjs` from the repository root after installing the UI dependencies and building `packages/tantra-acp-client`. It targets the disposable two-backend stack and writes `e2e/reports/018-p5/client-behavior.json`. Set `SARATHI_E2E_API`, `SARATHI_E2E_A`, `SARATHI_E2E_B`, `SARATHI_E2E_GATE`, and `SARATHI_E2E_ORIGIN` when ports differ. Its owner-death case kills (SIGKILL) and restarts `backend_a` through `SARATHI_E2E_COMPOSE` (default `docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml`, run from `apps/sarathi`); `SARATHI_E2E_MODEL` (default `e2e-other`) picks the second model. It uses only the deterministic provider.
