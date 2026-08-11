# Sarathi

A deep-search chat system in the style of Perplexity — ask anything, watch it think, search the web, read your PDFs, and remember you. Built as the demo app for [tantra](../../README.md) (`tantra-harness`): every hard part — the turn loop, streaming, subagents, approvals, cancellation, durability — is the library, not the app.

## Demo

[![Sarathi demo](https://img.youtube.com/vi/yAnC1LHKQZk/maxresdefault.jpg)](https://youtu.be/yAnC1LHKQZk)

## What it demonstrates

- **Streaming turns with visible thinking** — reasoning deltas render live, collapse when done.
- **Deep search** — a `researcher` subagent runs `web_search` / `web_fetch` loops; nested activity streams inside the chat; answers cite sources.
- **Stop that works** — one click cancels the whole session tree (`harness.cancel(sid, recursive=True)`), and the thread stays usable.
- **Human-in-the-loop** — memory writes suspend the turn behind an approval card; the ask survives a backend restart and resumes on a fresh socket.
- **Durability** — reload mid-turn and the transcript replays from the event log while the turn keeps running; reconnect resumes it.
- **Per-user memory** — `memory_tools(scope=...)` stamps every row with the tenant; recall and the memory panel never cross users.
- **Document grounding** — upload a PDF, the agent reads it with `read_doc` and answers from it.
- **Model switching** — per-session model picker, switchable mid-turn.

## Stack

- `backend/` — FastAPI + tantra, Postgres (pgvector) event-sourced store, WebSocket per session.
- `ui/` — Next.js 15, React 19, Tailwind v4, shadcn/ui; API client generated from OpenAPI (Orval).

## Run it

Requires Docker.

```bash
cd apps/sarathi
cp .env.example .env
```

Set in `.env`:

- `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `SARATHI_MODELS` — any OpenAI-compatible endpoint; two models make the picker interesting.
- `SECRET_KEY`, `NEXTAUTH_SECRET` — any random strings.
- `BRAVE_API_KEY` — optional; without it web search is disabled and deep-search prompts degrade.
- `EMBEDDING_MODEL` — optional; enables vector memory recall.

```bash
docker compose up --build -d
```

Open http://localhost:3000, sign up, chat.

Note: `docker compose restart` does not re-read `.env` — after editing it, recreate with `docker compose up -d backend`.

## Development

Backend (from `backend/`): `uv sync --dev`, then `just runserver` / `just test` / `just lint` / `just migrate`.

UI (from `ui/`): `yarn install`, then `just runserver`; `yarn typegen` regenerates the API client after backend schema changes (never edit `ui/src/generated/`).

## E2E

`e2e/runbook.md` — 10 scenarios driven live through a browser agent, reports land in `e2e/reports/`.
