# Sarathi — Tantra demo webapp — Spec

## Goal
- `apps/sarathi`: an AI chat webapp (FastAPI+WS backend, Next.js frontend) that demos tantra's capabilities end-to-end: streaming text + collapsed thoughts, websearch, PDF reading, subagents, durable HITL approval, user-scoped memory, sessions — all on `PostgresStore`, started with one `docker compose up`.
- This is the deferred v1 P9 adapter work, built as an app. Nothing in the library provides the WS layer, reconnect handling, or sweeps — they are deliberately app-side (001 spec, P9 deferral).

## Scope
- **In:** email+password auth (JWT), chat UI with streaming/thoughts/tool-chips/subagent nesting, approval card for asks, PDF attach in composer, memory panel, session sidebar with auto titles, stop button, per-session model picker, docker compose stack, e2e runbook driven by Claude in a browser.
- **Out:** refresh tokens / token rotation, session delete, multi-worker backend, background sweep daemon, message timestamps (events carry none — accepted), mobile layout, i18n, rate limiting, CI pipeline for e2e.

## Decisions
- **Auth:** email+password, pbkdf2_sha256 (passlib), pyjwt HS256, single 24h access token. No refresh tokens — demo posture. NextAuth (credentials provider, JWT strategy) on the frontend for middleware route-guarding and `getSession()`-fed token injection, mirroring observability_ui.
- **Provider:** any OpenAI-compatible endpoint via env (`OPENAI_BASE_URL`, `OPENAI_API_KEY`, `SARATHI_MODELS`). Chosen over my OpenRouter recommendation — user wants endpoint-agnostic. Consequence: thoughts streaming works only if the endpoint emits reasoning deltas. Mitigation: `sarathi.provider.ReasoningCompat(OpenAICompatible)` reads both `reasoning` and `reasoning_content` delta fields (tantra's `openai_compat.py:129` reads only `reasoning`).
- **HITL trigger:** no bash tool. `memory_write` gets `permission="ask"` via `Agent.permissions` — "remember that I…" reliably produces an approval card, exercising durable suspend/resume without contrived tools.
- **Memory UI:** read+delete panel (dialog off the sidebar). Not full CRUD — demos the capability without building an editor.
- **User scoping is app-enforced** (tantra enforces none): root sessions created with `metadata={"user": <uid>, "kind": "root"}`; every list filters `metadata={"user": uid, "kind": "root"}`. Memory scoping via custom tools that pass `metadata={"user": uid}` — the shipped tantra memory tools always write `metadata={}` and are not used.
- **Store:** `PostgresStore(dsn, schema="tantra")`. It holds one connection per instance behind a lock (`postgres.py:308`) — so one store+harness instance **per WS connection**, and per-request instances for REST. `store.setup()` called once in FastAPI lifespan.
- **App DB:** same Postgres, `public` schema, SQLAlchemy async with the **psycopg3 driver** (`postgresql+psycopg://`) — one DB driver total, avoiding observability_ui's asyncpg+psycopg split. Alembic for app tables.
- **Backend concurrency posture:** single uvicorn worker. Tantra has no cross-process live tailing (001 open decision); one worker makes live streams + in-process state correct by construction. Recorded as a demo constraint, not a framework one.
- **Wire format:** `Emitted.model_dump_json()` passed through verbatim; app frames are distinct (see WS protocol). No translation layer — the demo *is* the tantra event stream.
- **Orval input is a committed `openapi.json`** exported by a Justfile target — not a live URL (observability_ui's typegen requires a running backend; a wart, not copied). Clean operationIds via FastAPI `generate_unique_id_function=lambda r: r.name` → hooks like `useListSessions`. `mock: false`.
- **Frontend transcript state is server-truth:** full `replay` on every WS connect, client rebuilds. No IndexedDB transcript persistence — replay is the capability being demoed.
- **E2E:** a `runbook.md` the user references in Claude Code; Claude drives a browser (whatever browser tooling the session has), executes scenarios, writes a markdown report. Not a CI runner script — chosen over `claude -p`+Playwright-MCP harness.
- **Test LLM:** real env-configured endpoint with a cheap model for the runbook; behavioral assertions only. Backend pytest suite uses tantra's `FakeProvider` + `MemoryStore` + SQLite (aiosqlite) app DB — no network, no docker.
- **Agent shape:** `Sarathi` (web_search, web_fetch, read_doc, memory_write, memory_recall) + `Researcher` subagent (web_search, web_fetch) for deep-research fan-out.
- **PDFs:** paperclip in composer → `POST /uploads` → file under `UPLOAD_DIR/{user_id}/` → attachment marker appended to the message text → agent calls `read_doc(path)`.

## Architecture

```
apps/sarathi/
├── backend/            uv workspace member "sarathi", src layout (src/sarathi/)
│   ├── pyproject.toml  deps: tantra-harness[postgres,web,doc], fastapi, uvicorn, sqlalchemy,
│   │                   psycopg[binary], alembic, pydantic-settings, pyjwt, passlib,
│   │                   python-multipart; dev: pytest, pytest-asyncio, httpx, aiosqlite
│   ├── alembic.ini, migrations/
│   ├── justfile        runserver, test, lint, migrate, makemigrations, export-openapi
│   ├── src/sarathi/
│   │   ├── main.py     app factory, lifespan (store.setup, engine, upload dir), routers
│   │   ├── config.py   pydantic-settings: DATABASE_URL, SECRET_KEY, OPENAI_BASE_URL,
│   │   │               OPENAI_API_KEY, SARATHI_MODELS (csv, first=default),
│   │   │               BRAVE_API_KEY, EMBEDDING_MODEL (optional), UPLOAD_DIR,
│   │   │               CORS_ORIGINS (added in P0)
│   │   ├── db.py       async engine (postgresql+psycopg), Base, get_db, DbDep alias
│   │   ├── models.py   User(id, email, password_hash, created_at)
│   │   ├── schemas.py  request/response + WS frame models
│   │   ├── auth.py     hash/verify, JWT encode/decode, CurrentUser (http),
│   │   │               CurrentUserWS (token query param)
│   │   ├── provider.py ReasoningCompat(OpenAICompatible)
│   │   ├── agent.py    Sarathi, Researcher, memory tools, make_store(),
│   │   │               make_harness(model), deps_factory
│   │   ├── titles.py   one-shot title generation (agni repl.py:131 pattern)
│   │   └── api/        auth.py, sessions.py, memory.py, uploads.py, ws.py, meta.py
│   └── tests/
├── ui/                 Next.js 15, App Router, TS, Tailwind v4, ~~shadcn (new-york)~~ shadcn (radix-nova) **Changed in P0.** CLI v4.16 dropped named styles; radix-nova preset (Radix + Lucide + Geist) is the successor
│   ├── orval.config.js input: ./openapi.json (committed)
│   ├── src/app/{login,signup}/, src/app/chat/, src/lib/apiClient.ts, src/generated/
│   └── Justfile, Dockerfile (standalone, next-runtime-env)
├── e2e/                runbook.md, fixtures/ (sample.pdf), reports/
├── docker-compose.yaml db (pgvector/pgvector:pg17), migrate, backend, ui
└── .env.example        every env var, no secrets committed
```

- Root `pyproject.toml`: add `"apps/sarathi/backend"` to `[tool.uv.workspace] members`; ~~`sarathi` dev group not needed (its extras already in root dev group)~~ **Changed in P0.** sarathi carries its own dev group (aiosqlite/httpx are not in the root dev group).
- Backend Dockerfile builds from **repo root context** (needs the workspace `tantra` package): `uv sync --frozen --package sarathi`, CMD `uvicorn sarathi.main:app --host 0.0.0.0 --port 8000` (one worker).

## Harness wiring
- `make_harness(model)` → `Harness(provider=ReasoningCompat(base_url, key), store=make_store(), agents=[Sarathi], default_model=model, deps_factory=deps_factory, memory=BuiltinMemory(store, embedder), compactor=PruneThenSummarize(), skills=[])`.
- `deps_factory(header)` → `{"user_id": header.metadata.get("user")}` — `.get`, because child-session headers may not carry user metadata; only the root agent has memory tools.
- Embedder: `OpenAICompatibleEmbedder` iff `EMBEDDING_MODEL` set; else keyword-only recall (`MemoryHit.mode == "keyword"`), which is fine for the demo.
- ~~Pass real `limits={model: ModelLimits(...)}` where known~~ **Changed in P2.** `Harness` has no `limits` param — limits live on the provider: `ReasoningCompat(..., limits={m: ModelLimits(...)})`, built iff `SARATHI_CONTEXT_WINDOW` is set (`max_output=8192`); otherwise the provider's 128k fallback applies.
- Memory tools (in `agent.py`, docstrings are the model-facing descriptions per tantra convention):
  - `memory_write(ctx, content, kind)` → `ctx.memory.write(MemoryWrite(..., metadata={"user": ctx.deps["user_id"]}))`
  - `memory_recall(ctx, query)` → `ctx.memory.recall(query, metadata={"user": ctx.deps["user_id"]})`
  - `Sarathi.permissions = {"memory_write": "ask"}`
- `Researcher` docstring doubles as its delegate-tool description; `Sarathi.subagents = [Researcher]`.

## REST API (all `/api` prefix; tags drive orval tags-split filenames)
| Method/path | Notes |
|---|---|
| `POST /auth/signup`, `POST /auth/login` | → `{access_token}`; `GET /auth/me` |
| `GET /sessions` | `store.list(metadata={"user": uid, "kind": "root"})` → id, title, status, model, updated_at |
| `POST /sessions` | body `{model?}`; `create_session(Sarathi, metadata={"user", "kind": "root", "model", "title": None})` |
| `PATCH /sessions/{id}` | `{model}` — model picker; validated against `SARATHI_MODELS`; ownership check on metadata |
| `GET /memory` | `store.memory_all()` filtered to `metadata.user == uid` |
| `DELETE /memory/{id}` | ownership check, then `memory.delete(id)` |
| `POST /uploads` | multipart; ~~pdf/docx/txt~~ pdf/docx (**Changed in P3.** `read_doc` reads only `.pdf`/`.docx`; a `.txt` attachment would produce a model-facing error), ≤20 MB; saved `UPLOAD_DIR/{uid}/{uuid}_{name}`; → `{path, name}` |
| `GET /models` | from `SARATHI_MODELS` |
| `POST /meta/ws-types` | schema-only dummy (observability_ui `agents.py:19` trick): request = client-frame union, response = server-frame union incl. tantra `SessionEvent` + delta models, so orval generates TS types for the whole WS vocabulary |
- `FastAPI(separate_input_output_schemas=False, generate_unique_id_function=...)`.
- Every session/memory endpoint verifies ownership via metadata before acting — tantra's `list()` with a wrong filter returns everything (001 sharp edge).

## WS protocol — `GET /api/ws/sessions/{session_id}?token=`
- Auth: JWT query param (browsers can't set WS headers). ~~Ownership check on the session header before accepting.~~ **Changed in P2.** Accept first, then check auth+ownership and close 1008 on failure — a pre-accept rejection can't carry a close code to the client.
- **Server→client:** tantra `Emitted` JSON verbatim (`{session_id, depth, seq, event}` — distinguishable by the `event` key), plus app frames: `{"type": "replay_done"}`, `{"type": "busy", "retry_in": s}`, `{"type": "title_updated", "title"}`, `{"type": "server_error", "message"}`.
- **Client→server:** `{"type": "user_message", "text", "attachments": [{"path", "name"}]}`, `{"type": "ask_response", "ask_id", "response"}`, `{"type": "cancel"}`.
- **On connect:** replay root via `harness.replay(sid)`; `replay()` does NOT reproduce child events (001 sharp edge) — when a `child_session_spawned` event passes, depth-first replay that child from `store.list(parent_id=...)` before continuing. Then `replay_done`. Then: header `awaiting_input` → re-send the pending `AskRaised` so the approval card renders; turn incomplete (re-scan log, agni `repl.py:179` pattern — status alone is unreliable) → bare `resume(sid)` and pump. This reconnect-driven resume is the sweep; no background daemon.
- **Turn flow:** `user_message` → append attachment markers (`[attachment: {name} path={path}]`, one per line) to text → `run(sid, text)` and pump every yield to the socket. `ask_response` → `resume(sid, ask_id, response)` and pump; if the ask came from a child (frame's `session_id` ≠ root), resolve child sid then bare `resume(root)` — the two-call dance (`stress/live_raw.py` `settle()`). `cancel` → `await harness.cancel(sid)` (persisted flag; takes effect at boundaries).
- Two asyncio tasks per connection: reader (inbound frames → queue/cancel) and pump (consumes the running generator). The loop only advances while the generator is consumed (001 sharp edge #1); client disconnect mid-turn pauses the turn — durable, resumed on reconnect.
- `SessionBusy` → `busy` frame (60s lease TTL after a crash); `TantraError`/provider failure → `server_error`; errors surface on first iteration of the generator, so wrap the `async for`, not the call.
- After first `TurnCompleted` on an untitled session: fire title generation (one-shot `provider.stream`, `put_header`), emit `title_updated`.
- Model resolution: before each `run`/`resume`, set `harness.default_model` from session `metadata.model` (fresh-read per turn, agni `/model` pattern) — picker changes apply next turn.

## Frontend
- Stack: Next.js 15 (standalone output), React 19, Tailwind v4 (CSS-first, `globals.css` only), shadcn new-york, Geist fonts, TanStack Query v5 + orval axios mutator (**non-async** — observability_ui's `async customInstance` silently breaks cancellation), Zustand, `react-use-websocket`, `react-markdown` + `remark-gfm`, `next-runtime-env` (`NEXT_PUBLIC_API_URL` read at runtime → one image).
- NextAuth credentials provider: `authorize()` posts to `/api/auth/login`, access token stored in the NextAuth JWT; `middleware.ts` `withAuth` matcher `["/chat/:path*"]`; axios request interceptor + module TTL token cache (`apiClient.ts` pattern); same `getToken()` feeds the WS URL.
- Routes: `/login`, `/signup`, `/` → redirect `/chat`, `/chat` (new-session landing), `/chat/[sessionId]`. Colocation convention: `app/chat/components|hooks|state.ts` own everything chat-specific; only true cross-route pieces in `src/components`.
- Layout: left sidebar (new chat, session list with titles + `updated_at`, memory button → dialog, user menu) · chat pane (header: title + model picker dropdown + connection dot · transcript · composer: textarea, paperclip, send/stop toggle). Dark default with a working light/dark toggle (no `forcedTheme`).
- **Transcript model** (Zustand store factory per session, `createChatStore(sid)` + `useMemo`): ordered turns; each turn = user message (attachment markers regex-stripped into chips) + ordered items: `thinking` (collapsed by default, shimmer + streams open while active), `text` (markdown, streaming cursor), `tool` chip (name + args summary; spinner between `tool_call_started`/`completed`; result JSON collapsed behind click; `tool_progress` lines inside), `subagent` block (all frames with `session_id` ≠ root grouped by child sid under its `child_session_spawned`, collapsed, badge = agent name + live spinner; `depth` available for nesting), `ask` card (request text + Approve/Deny → `ask_response`), `turn_failed` banner.
- **Reducer rules from the gotcha list:** reset delta buffers on every `sample_started` (~~retried samples re-yield deltas — 001 known bug, no reset marker~~ **Corrected in P2.** retries happen inside one `SampleStarted` with no second event, so the reset never fires for them — the `*_part` REPLACE rule is what actually cleans up re-yielded deltas; the reset stays as defense for multi-sample turns); on `text_part`/`reasoning_part` replace the accumulated delta text for that sample (persisted part is authoritative); duplicate `ToolProgress` after ask-resume is expected (pre-ask side effects re-run) — dedupe by `(call_id, message)`; history comes from persisted parts only.
- States: composer disabled while turn running or ask pending; "thinking…" shimmer between `sample_started` and first delta; stop button visible while running; `busy` frame → toast with retry; WS reconnect (`react-use-websocket` auto) → clear transcript, full replay.
- Login/logout, session switch = navigate; opening a session opens its WS.

## Docker compose (`apps/sarathi/docker-compose.yaml`)
- `db`: `pgvector/pgvector:pg17`, volume, healthcheck.
- `migrate`: backend image, `alembic upgrade head`, one-shot, depends_on db healthy.
- `backend`: depends_on migrate complete; env from `.env`; uploads volume; single worker; healthcheck `/api/health`.
- `ui`: standalone runner; `NEXT_PUBLIC_API_URL` via runtime env; port 3000.
- All secrets from `.env` (gitignored); `.env.example` committed. No inline secrets (observability_ui committed real ones — not copied).

## E2E runbook (`apps/sarathi/e2e/runbook.md`)
- Preamble: prerequisites (`docker compose up -d`, healthchecks green, `.env` with a real cheap model), how to reset state (`docker compose down -v`), instruction to Claude: drive the browser with available browser tools, judge behaviorally (never exact-text), write `e2e/reports/<YYYY-MM-DD>.md` from the embedded report template (per-scenario PASS/FAIL + evidence + notes).
- Scenarios (each: steps + expected observations):
  1. Signup → login → lands on `/chat`; direct `/chat` visit while logged out redirects to `/login`.
  2. Basic chat: message → thinking shimmer → streamed answer; thoughts block present and collapsed (SKIP with note if endpoint emits no reasoning).
  3. Websearch: current-events question → `web_search`/`web_fetch` tool chips with spinners → cited answer.
  4. PDF: attach `fixtures/sample.pdf` → chip on message → `read_doc` chip → content-grounded answer.
  5. Subagent: "research X in depth" → collapsed Researcher block, nested tool activity inside, synthesis after.
  6. Memory HITL: "remember I prefer Y" → approval card → Approve → completes; memory panel shows the row; **new session** recalls Y; second user does NOT see it.
  7. Deny path: trigger memory_write → Deny → turn completes without writing; panel unchanged.
  8. Cancel: long research prompt → Stop → turn ends `cancelled` within a few seconds.
  9. Model picker: switch model → next turn runs on it (visible in stream/behavior).
  10. Reconnect: reload mid-turn → transcript replays, turn resumes and completes.

## Considered & rejected
- **SSE instead of WS** — turn input, ask responses, and cancel are client→server mid-stream; WS is bidirectional, SSE needs side-channel POSTs.
- **Translating tantra events into a custom chat protocol** — the event stream *is* the demo; translation hides it and doubles schema surface.
- **IndexedDB transcript persistence (observability_ui pattern)** — replay from the store is a tantra capability; caching client-side would mask it.
- **Global singleton Harness/store** — `PostgresStore`'s single locked connection serializes all sessions; per-connection instances are the documented mitigation.
- **asyncpg for the app DB** — second driver against the same Postgres (observability_ui does this; called out as a wart there).
- **Background sweep daemon** — reconnect-driven resume covers the demo; a daemon adds lifecycle complexity for no visible capability. Parked in Open Decisions.

## Implementation phases

Linear: P0 → P1 → P2 → P3 → P4 → P5 → P6 → P7 → P8. P3/P4/P5 look independent on paper but all touch `api/ws.py`, `agent.py`, and the transcript reducer — sequential in practice; do not parallelize them.

### Conventions (all phases)
- Backend: ruff line-length 120, py313, `select = ["E","F","I","UP","B"]`, no `noqa`; **no comments**; docstrings only on tools (they are model-facing descriptions, opencode-style) and public protocols. pytest `asyncio_mode = "auto"`; no network in tests (`FakeProvider`/cassettes); `MemoryStore` + aiosqlite app DB in tests.
- Frontend: Prettier `printWidth: 120`, `.prettierignore` → `src/generated/`; ESLint ignores `src/generated/**`; never hand-edit `src/generated/`; semantic Tailwind tokens only; lucide icons only.
- Codegen loop: backend `just export-openapi` (dumps `app.openapi()` → `apps/sarathi/ui/openapi.json`, committed) → ui `yarn typegen`. Regenerate in any phase that touches routes or schemas.
- Run backend `just lint` + `just test` and ui `yarn lint` before marking a phase done. Root `just test` must stay green (tantra suite untouched).
- **Contract freeze (set in P0):** WS frame vocabulary (client + app-server frames; tantra `Emitted` passthrough), REST paths + operationIds, `.env` variable names, `apps/sarathi` layout. Changing any means updating this spec first.

### Keeping this spec current
- Update the status marker on the heading and tick the checklist as you go.
- When the build deviates from the plan, **strike the original line and say why** — `~~original~~ **Changed in P3.** <reason>`. Never silently rewrite.
- After a phase lands, add only detail that would surprise the next reader — a load-bearing constant, a behavior that isn't what the name suggests, an ordering that matters.
- Problems found but not fixed go to Open Decisions or a Follow-up note with enough detail to act on later.
- Suspected tantra core-library bugs/warts hit during app work go to `docs/issues.md` (what/where/impact/workaround) — user triages later.

### Phase 0 — Scaffolding + frozen contracts · deps: none · blocks all · ✅ done 2026-08-10
- Workspace member `apps/sarathi/backend` (src layout, deps, justfile targets incl. `export-openapi`); root `pyproject.toml` members updated.
- `config.py`, `db.py`, empty routers mounted, `/api/health`, `POST /api/meta/ws-types` dummy carrying full WS frame + tantra event union schemas.
- All WS frame models in `schemas.py` — this freezes the wire contract.
- ui scaffold: create-next-app, Tailwind v4 + shadcn init, orval wired to committed `openapi.json`, `apiClient.ts` (non-async mutator), `next-runtime-env`, standalone output, Dockerfile.
- Compose skeleton: db + migrate (no-op) + backend + ui boot and pass healthchecks. `.env.example`.
- **Verify:** `docker compose up` → ui at :3000, `/api/health` ok; `just export-openapi && yarn typegen` produces TS types for every WS frame and tantra event type; root `just test` green.
- Checklist:
  - [x] workspace member + justfile
  - [x] WS frame schemas + dummy endpoint
  - [x] ui scaffold + typegen loop
  - [x] compose skeleton + .env.example
- P0 notes:
  - `CORS_ORIGINS` env var (csv, default `http://localhost:3000`) added — ui on :3000 calls backend on :8000 cross-origin.
  - `SECRET_KEY`/`OPENAI_BASE_URL`/`OPENAI_API_KEY`/`SARATHI_MODELS` are required (no defaults); `just export-openapi` and `tests/conftest.py` inject dummies.
  - Sarathi tests are NOT in root pytest `testpaths` — backend `just test` is the gate; root `just test` proves only tantra/agni.
  - `ClientFrame` uses `pydantic.Discriminator("type")` (a `Field(discriminator=...)` in `Annotated` makes FastAPI infer a query param).

### Phase 1 — Auth · deps: P0 · ✅ done 2026-08-10
- `models.User` + alembic migration; signup/login/me endpoints; `CurrentUser`/`CurrentUserWS` deps.
- ui: NextAuth credentials, `/login` + `/signup` pages, `middleware.ts` guard, axios interceptor + token cache.
- **Verify:** in-browser signup → login → `/chat` shell; logged-out `/chat` redirects; pytest covers wrong-password, duplicate-email, expired-token via ASGI transport.
- Checklist:
  - [x] users table + migration
  - [x] auth endpoints + JWT deps
  - [x] login/signup UI + middleware
  - [x] auth tests
- P1 notes:
  - Env vars added: `NEXTAUTH_SECRET` (required, compose `:?` guard), `NEXTAUTH_URL`, `API_URL_INTERNAL` (NextAuth `authorize()` runs server-side in the ui container where `localhost:8000` is unreachable; compose sets `http://backend:8000`).
  - `CurrentUser` uses `HTTPBearer` (not `OAuth2PasswordBearer`) — login is a JSON body, not an OAuth2 form.
  - Duplicate signup handled by `IntegrityError → 409` on the unique index (no SELECT pre-check; concurrent-safe).
  - Bare-metal `yarn dev` needs `NEXTAUTH_SECRET` in `ui/.env.local` (Next doesn't read `apps/sarathi/.env`); without it `withAuth` redirects to a Configuration error page, not `/login`. Compose path unaffected.
  - No logout UI yet — P2 sidebar user menu owns it; `clearToken()` is exported and already called on login/signup account switch.

### Phase 2 — Core chat loop · deps: P1 · ✅ done 2026-08-10
- `provider.py` `ReasoningCompat`; `agent.py` `Sarathi` (no tools yet), `make_store`/`make_harness`, `deps_factory`; `store.setup()` in lifespan.
- Sessions REST (list/create/patch); WS endpoint: connect→replay(+children walk)→replay_done→pending-ask/incomplete-turn handling; user_message→run; cancel; busy/server_error frames; reader+pump task pair.
- ui chat: sidebar (sessions via react-query), `createChatStore` reducer (delta buffers, sample_started reset, part-overwrites), transcript with markdown text + collapsed thinking + shimmer + streaming cursor, composer, stop button, reconnect-replays.
- Confirm the permission-ask response contract (`"allow"`/`"deny"` vs other) from tantra's tests before freezing the `ask_response` payload semantics — the frame shape is frozen, the response string values are not.
- **Verify:** against a real endpoint — send message, watch text stream token-by-token, thinking collapses; reload mid-turn → replay + auto-resume completes the turn; stop ends turn `cancelled`; pytest drives the WS via ASGI with `FakeProvider` asserting frame order (replay_done, deltas, parts, turn_completed) and busy-on-concurrent-run.
- Checklist:
  - [x] provider subclass + harness factory
  - [x] sessions REST + ownership checks
  - [x] WS handler (replay/run/resume/cancel)
  - [x] transcript reducer + streaming UI
  - [x] WS pytest suite with FakeProvider
- P2 notes:
  - `ask_response` semantics frozen: `resume` takes a typed `AskResponse`; the frame's `response` string maps by the pending ask's `request.kind` — approval: `"allow"` → `ApprovalResponse(allow=True)`, anything else → deny; choice → `ChoiceResponse(selected=response)`; free_text → `FreeTextResponse(text=response)`.
  - Stop→`cancelled` is unverifiable until P3: with no tools a turn is one sample and `CancelRequested` only takes effect at store boundaries — a mid-sample cancel still ends `stop_reason="completed"`. Cancel wiring is in and tested for the no-op path; the behavioral check moves to P3's verify.
  - `list_sessions` post-filters `parent_id is None` — children inherit parent metadata verbatim (incl. `kind: "root"`) and `list()` matches metadata as a subset, so the filter alone would leak subagent sessions into the sidebar.
  - Per-connection/request teardown is `close_harness()` = `store.close()` + `provider.aclose()` (the provider holds an eager httpx client that otherwise leaks).
  - WS auth/ownership failures accept-then-close 1008 (can't set a status before the handshake completes in FastAPI); reconnect stops on 1008 client-side, and a null cached token redirects to `/login` instead of connecting.
  - `httpx-ws` added to backend dev deps (ASGI WS transport for pytest); disconnect-mid-turn test gates FakeProvider on an asyncio.Event to park the server mid-sample deterministically.
  - Dark theme made the default now (`<html className="dark">`); the toggle stays in P5.
  - Real-endpoint streaming verify deferred — no `OPENAI_*` creds available; compose smoke (signup → session → WS replay/turn/reconnect/1008) + FakeProvider frame-order suite ran instead. Run the real-endpoint pass alongside P3 verify.

### Phase 3 — Tools, subagent, attachments · deps: P2 · ✅ done 2026-08-10
- Wire `web_search(BRAVE_API_KEY)`, `web_fetch()`, `read_doc()` onto `Sarathi`; add `Researcher` subagent.
- `POST /uploads` + validation; composer paperclip; attachment markers appended to input, regex-stripped to chips on render.
- Transcript: tool chips (spinner/result/progress), subagent collapsed blocks keyed by child `session_id`; reconnect walk renders finished child blocks from replay.
- **Verify:** research question shows search+fetch chips then a cited answer; "deep research" shows a nested Researcher block; uploaded `fixtures/sample.pdf` gets summarized via a visible `read_doc` chip; oversized/wrong-type upload rejected with a toast; pytest covers upload validation and the attachment-marker round-trip.
- Checklist:
  - [x] tools + Researcher on Sarathi
  - [x] uploads endpoint + paperclip
  - [x] tool chip + subagent block UI
  - [x] child replay on reconnect
- P3 notes:
  - Tools wired lazily: `@cache _wire_tools()` called from `make_harness` sets `Sarathi.tools`/`Researcher.tools` (module-level `get_settings()` would break conftest import order). Empty `BRAVE_API_KEY` drops `web_search` silently instead of crashing at construction.
  - Attachment path guard added in `run_turn` (unspecced): every attachment path must resolve under `UPLOAD_DIR/{uid}/` else `server_error` and no turn — client controls the frame, and unguarded it's arbitrary-PDF read + cross-user access. Verified prefix/`..`/symlink bypasses fail (`Path.resolve` + `is_relative_to` on both sides).
  - Tool-chip spinner keys on `tool_call_requested`→`tool_call_completed`, never `tool_call_started` — Started is skipped on every error path (unknown tool, bad args, deny). Live order is Requested → SampleCompleted → Started → Completed.
  - `ToolCallStarted` precedes `ChildSessionSpawned` live; replay splices child frames right after `child_session_spawned`. Replayed child frames carry `depth: 0` (each session replays as its own root) — subagent blocks group by child `session_id` only, never depth.
  - A subagent turn emits two `turn_completed`s (child then parent); everything waiting on turn end filters by `session_id`.
  - The delegate's tool chip is replaced in place by the subagent block on `child_session_spawned` (same `call_id`; appending would double-render one call); delegate `result` renders only when `is_error` (success duplicates the child's own text).
  - Shipped tools emit no `tool_progress` (none take `ctx`) — chips show spinner-only between requested/completed; progress rendering is built anyway with `(call_id, message)` dedupe returning identical state on duplicates (no re-render).
  - Stop→`cancelled` behavioral test landed (P2 deferral resolved): gate-parked tool sample + WS cancel frame → `turn_completed(stop_reason="cancelled")`, no tool started, no child spawned.
  - Compose smoke ran live: upload 201 (`{uid}/{uuid}_{name}` on disk)/415/413/401, guard `server_error` on foreign path, marker round-trip in `turn_started.input`. Real-endpoint behavioral pass (cited answer, Researcher block, PDF summarized, live stop) still needs `OPENAI_*` (+`BRAVE_API_KEY`) creds — deferred to the P7 runbook alongside the P2 streaming deferral.

### Phase 4 — Memory + approval flow · deps: P3 · ✅ done 2026-08-10
- User-scoped `memory_write`/`memory_recall` tools; `permissions={"memory_write": "ask"}`; optional embedder from `EMBEDDING_MODEL`.
- Approval card on `AskRaised` → `ask_response` → resume; child-ask two-call dance handled generically in the WS handler; composer locked while ask pending; pending ask re-sent on reconnect.
- Memory REST (list/delete with ownership) + memory dialog in sidebar.
- **Verify:** "remember I prefer X" → card → approve → row in panel → new session recalls X → second account sees nothing; deny path writes nothing; kill the backend container mid-ask, restart, reconnect → card re-renders and approve still lands (durable suspend across processes); pytest covers scoping (user A cannot list/delete B's memories) and the ask→resume flow with FakeProvider.
- Checklist:
  - [x] scoped memory tools + ask permission
  - [x] approval card + resume wiring
  - [x] memory panel + REST
  - [x] durable-ask restart test
- P4 notes:
  - Approval card needed no new WS code — P2's `track`/`answer_ask`/`_typed_response` + the `incomplete()`→bare-`resume` reconnect path already implement the whole ask flow; P4 added the tools, tests, and UI only.
  - Reconnect delivers the pending `AskRaised` TWICE (replay + idempotent auto-resume, same original seq) — the reducer upserts asks by `ask_id`, returning identical state on the duplicate. Ask items are created `final: true` so the post-resume `sample_started` wipe spares the card.
  - `memory_write(content, kind)` maps to `MemoryWrite(title=content[:80], body=content)` — tantra's model has no `content` field (`extra="forbid"`).
  - Memory list is `store.memory_all()` + app-side filter (user ∧ not deleted ∧ not superseded) — no filtered query exists and `recall("")` returns `[]`; delete pre-checks via `memory_get` because `BuiltinMemory.delete` has no ownership concept and raises on unknown ids.
  - Test factory now calls `_wire_tools()` — it builds Harness directly, so without it `Sarathi.tools` was empty and `memory_write` resolved as an unknown tool (no ask ever raised).
  - Durable-ask restart proven in pytest: fresh Harness per socket over the shared store (store-level equivalent of a process kill); replay re-renders the card, approve on the new socket completes and writes. Literal kill-the-container check needs real creds → P7 runbook.
  - Child-ask two-call dance is generic but app-untested (Researcher has no asking tools); traced sound by review, covered by tantra's test_subagents.py.
  - AskCard clears its local sent-latch when a busy/server_error banner arrives (two-tab lease contention otherwise left the card stuck disabled).
  - Compose smoke ran live (REST level): two users, `GET /api/memory` 200 `[]` each, DELETE bogus id → 404, no auth → 401. Ask-flow behavioral pass (card → approve → panel row → fresh-session recall → cross-account isolation → deny → container-kill) needs real `OPENAI_*` creds → P7 runbook.
  - `docs/issues.md` started (user request): tantra warts log — seeded with the metadata `None` fail-open filter, unfiltered `memory_all`, cross-tenant shipped tools, ownerless delete, AskRaised re-emit, depth-0 replay, skipped `ToolCallStarted`, `reasoning`-only provider field.

### Phase 5 — Titles, model picker, polish · deps: P4 · ✅ done 2026-08-10
- `titles.py` one-shot generation after first turn; `title_updated` frame; sidebar live-updates.
- `GET /models`, header dropdown, `PATCH /sessions/{id}`, fresh-read `default_model` per turn.
- Polish: empty states, error toasts, connection dot, light/dark toggle, `turn_failed` banner, busy toast.
- **Verify:** first exchange names the session in the sidebar without reload; switching model changes the model on the next turn (assert via `sample_started.model` in the stream); theme toggle persists.
- Checklist:
  - [x] title generation + frame
  - [x] model picker end-to-end
  - [x] states/toasts/theme
- P5 notes:
  - Title source is the last `TurnStarted.input` from the log (attachment-marker lines stripped), not the in-flight frame — one code path covers run, ask-resume, and reconnect-resume; an attachment-only or blank first message gets titled from a later turn instead of never.
  - `maybe_title` fires only after `pump()` returns (generator drained ⇒ `_settle` done) and re-reads the header right before `put_header` — writing mid-turn would be clobbered by the TurnLoop's own per-sample header writes (see `docs/issues.md`: last-writer-wins `put_header`).
  - Per-connection title latch: at most one generation attempt per WS connection; a failed attempt retries on the next connect (fresh `Connection`). Ask-suspended turns don't consume the latch — the title lands after approve on the same socket.
  - Title generation runs synchronously in the pump loop: a message sent while the title call is in flight waits for it (bounded by provider timeout). Accepted latency — a background task would reopen the header-write race.
  - Model picker is disabled while a turn runs: a PATCH landing mid-turn is silently reverted by the running turn's header writes (same wart).
  - `TitleUpdatedFrame` carries no `session_id`; the client keys it to its own socket and just invalidates the sessions list (sidebar + header both read that query).
  - Busy surfaces as a toast per spec, but the store still sets the busy banner state — AskCard's stuck-button recovery keys on `banner !== null`; only the busy strip rendering was dropped (error strip stays).
  - `GET /api/models` lives on a second `APIRouter(prefix="/api")` in `meta.py` — the meta router's `/api/meta` prefix would have broken the frozen `/api/models` path.
  - Cancelled turns do get titled (`turn_completed(stop_reason="cancelled")` passes the incomplete check) — input is real, harmless.
  - FakeProvider budget: every completed turn on an untitled session consumes one extra scripted sample for the title attempt (exhaustion is swallowed). Existing tests needed no edits; new title tests pin exact `provider.requests` counts.
  - Backend 69 tests (14 new: titles unit, title flow/no-retitle/silent-failure/ask-defers/latch-retry, model-switch via `sample_started.model`, `/api/models` auth); compose smoke: `GET /api/models` 200 `["gpt-4o-mini","gpt-4o"]` authed, 401 unauthed. Live behavioral pass (real title text, theme persistence, picker) → P7 runbook.

### Phase 6 — Compose hardening + docs · deps: P5 · —
- Final compose pass: depends_on/healthcheck ordering, uploads volume, restart policies; backend Dockerfile from repo-root context, `uv sync --frozen --package sarathi`.
- `apps/sarathi/README.md`: quickstart (`cp .env.example .env` → fill 3 required vars → `docker compose up`), env reference, dev-mode instructions (bare-metal `just runserver` + `yarn dev`).
- **Verify:** on a clean checkout with only `.env` populated, `docker compose up` → full flow (signup→chat→stream) works first try; `docker compose down -v && up` re-initializes cleanly (migrations + `store.setup()` idempotent).
- Checklist:
  - [ ] compose final + Dockerfiles
  - [ ] README + .env.example accurate
  - [ ] clean-checkout boot test

### Phase 7 — E2E runbook · deps: P6 · ✅ done 2026-08-10 (P6 skipped by user)
- `e2e/runbook.md` (preamble, 10 scenarios, report template), `e2e/fixtures/sample.pdf` (multi-page, distinctive content to assert grounding), `e2e/reports/` gitignored except `.gitkeep`.
- **Verify:** execute the runbook once via Claude Code driving a browser against the compose stack with a real cheap model; produce the first report; every scenario PASS or a spec deviation is recorded.
- Checklist:
  - [x] runbook + fixtures + template
  - [x] first full run + report committed observations
- Status notes:
  - Runbook folds in the four deferred checks (S2 title-no-reload, S6 container-kill durable ask, S8 tree cancel, S10 theme-across-reload); fixture is a hand-written 3-page raw PDF (no PDF lib available) verified through `tantra.extratools.doc._read` — 12/12 distinctive strings extract.
  - First run (models `z-ai/glm-5.2` + `deepseek/deepseek-v4-flash-0731`): **9 PASS · 1 FAIL · 0 SKIP** after the S3 re-run — full report in `e2e/reports/2026-08-10.md` (gitignored; observations here).
  - S3: first attempt SKIP (`BRAVE_API_KEY` empty → `web_search` unregistered; researcher improvised constructed google/bing URLs in a non-converging loop). User added the key → backend **recreated** (`up -d backend`; `restart` doesn't re-read env_file) → re-run PASS: 3 `web_search` chips with real Brave JSON, `web_fetch` only on result URLs (fetch-discipline prompt works once the tool exists), cited answer.
  - S8 FAIL (both attempts, DB-verified): stop cancels only the root — `harness.cancel`'s `expect_seq` retry (5×, no backoff) always loses against the actively-appending child log, `SeqConflict` swallowed per-target in `api/ws.py`; child runs to `completed`, root ends `cancelled` only when the spawn tool returns. New `docs/issues.md` entry; fix belongs in tantra core (blind append for `CancelRequested`).
  - Other PASS highlights: thoughts stream + collapse on glm-5.2; PDF grounding recovered all page-2/3 facts + page-1 from history without a second `read_doc`; durable ask survived `docker compose restart backend` with exactly one card; user-B isolation clean (3 empty recalls); model switch proven via `sample_started.model`; mid-turn reload replayed completed items only and re-sampled to a coherent finish.
  - Minor deviations logged in the report: sidebar `New chat` click intermittently doesn't navigate from a session page; theme verified via DOM/localStorage because the tester's Chrome runs Dark Reader (repaints the app dark regardless of app theme).

### Phase 8 — tantra fixes + release · deps: P7 · ⏳ code done 2026-08-11, publish pending
- Status: all fixes landed in three commits (`cc92722` cancel cluster, `67c9dd6` memory cluster, `913fecf` patch_header + provider + release). Triage with user chose **fix everything**: cancel cluster + all 4 memory items + resume seq + ToolCallStarted pairing + put_header patch op + reasoning_content. Replay-depth issue proven stale (child headers persist depth) — regression test added, entry annotated.
- Suites: tantra 628, stress 82, sarathi backend 68, UI lint+tsc — all green. Each chunk went through opus implementer → opus reviewer → fix round (reviewers caught: cancel-absorption single-retry wedge, submit-output cancel drop, MemoryStore Usage aliasing, a vacuous livelock test — all fixed with empirical negatives).
- Live S8 re-run **PASS** (report `e2e/reports/2026-08-11.md`): stop-to-idle ~6s (was 50–75s), child log shows `cancel_requested` → `turn_completed cancelled` (P7: zero cancel events, child ran to completion), follow-up message in the same thread works. Spot-checks S2/S5/S6 PASS incl. scoped `memory_tools` stamping `{"user": ...}` live.
- issues.md now: 11 fixed, 1 stale, 1 open by design (boundary-only cancel), 2 new (session `list()` None-wildcard; unscoped `memory_search` vector pass).
The `packages/tantra` freeze is lifted for this phase only. Fix the `docs/issues.md` backlog in tantra core, ship `tantra-harness` 0.2.0, prove the cancel fixes with the runbook. Publishing to PyPI is manual — pause and ask the user when the release commit is ready.

Cancel cluster (caused the S8 FAIL; runbook-verifiable):
1. `cancel` seq-race: append `CancelRequested` **without** `expect_seq` — it's a flag, needs no seq consistency (`harness.py:441-459`). Kills the livelock against busy logs.
2. Tree cancel: `cancel(sid, recursive=True)` walking `ChildSessionSpawned`; sarathi `api/ws.py` drops its hand-rolled walk for one call.
3. Final-sample drop: re-check `state.cancelled` before `_terminal("completed")` in the no-tool-calls branch (`loop.py:773-782`).
4. Boundary-only observation stays (documented behavior, not fixed) — but with 1–3 landed, stop latency = current step only.

Remaining `docs/issues.md` entries (unit-test-verifiable only; triage with user in plan mode — fix, defer, or wontfix each):
- `_Filter` `None`-wildcard fail-open; `memory_*` absent from Store protocol + unfiltered `memory_all`; cross-tenant shipped memory tools; `delete`/`supersede` ownership+idempotency; bare `resume()` re-emitting `AskRaised` with original seq; replay child `depth: 0`; `ToolCallStarted` skipped on error paths; `put_header` last-writer-wins (needs CAS/`patch_metadata`); `reasoning_content` delta field (fold sarathi's `ReasoningCompat` into the provider).

Deliverables: fixes + unit tests in `packages/tantra`; `docs/issues.md` entries struck/annotated; sarathi updated to drop app-side workarounds that the lib now covers (recursive cancel; possibly `ReasoningCompat`); version bump to 0.2.0 + changelog; compose image rebuild (stack builds from workspace source — runbook re-test does NOT wait on PyPI).
- **Verify:** tantra + sarathi test suites green; S8 re-run PASSes live (child log shows `cancel_requested`, child `stop_reason="cancelled"`, stop-to-idle bounded by the in-flight step); no regressions in a spot-check of S2/S5/S6; user confirms publish done.
- Checklist:
  - [x] cancel cluster fixed + unit tests
  - [x] remaining issues triaged with user; agreed fixes landed
  - [x] sarathi workarounds removed where superseded
  - [ ] 0.2.0 bump + changelog; user published (manual) — bump+changelog done, publish pending
  - [x] compose rebuild + S8 runbook re-run PASS

## Open Decisions
- **Background sweep daemon** — reconnect-driven resume suffices for the demo; revisit if abandoned turns holding `running` status confuse the sidebar. Resolve by observing runbook runs.
- **Session delete** — Store protocol has no delete; would mean SQL against `tantra.sessions` directly or a metadata tombstone. Deferred (user descoped).
- **Message timestamps** — tantra events carry none (001 open decision). If wanted later: `Emitted` is `extra="allow"`, the WS handler could stamp frames at forward time (display-only, not persisted).
- **Multi-worker** — needs cross-process tailing (PG LISTEN/NOTIFY or a Bus protocol in tantra). Out until the library grows it.

## Risks
- **Endpoint emits no reasoning deltas** → thoughts UI is empty, a headline feature silently missing. Mitigation: `ReasoningCompat` covers both known field names; runbook scenario 2 SKIPs loudly with the endpoint named; README recommends a known-good endpoint/model pair.
- ~~**Tantra v2 tools not actually landed** (spec assumes v2 complete; today `web_search`/`web_fetch`/`read_doc` are `NotImplementedError` stubs) — P3 hard-blocks on tantra v2 P2–P4. Signatures are frozen, so P0–P2 proceed regardless.~~ **Resolved in P3.** All three landed fully implemented in `tantra.extratools`.
- **PostgresStore single connection per instance** — per-connection/request instances mitigate; if connection count becomes a problem at demo scale it won't, but note `close()` exists outside the frozen protocol for cleanup.
- **Permission-ask response format assumed** ("allow"/"deny") — verified in P2 before the semantics freeze; frame shape unaffected either way.
- **Replay/children asymmetry regressions** — the depth-first child walk is hand-rolled app code against a documented library gap; the P2 FakeProvider WS test pins frame order to catch drift.
- **E2E nondeterminism** (real LLM) — behavioral assertions + cheap model; a flaky scenario re-runs once before FAIL per the runbook rules.

## Success criteria
- Clean checkout + `.env` → `docker compose up` → signup → chat with streamed text and collapsed streamed thoughts, visible websearch/fetch tool activity, a PDF attached and read, a Researcher subagent block, an approval card that survives a backend restart, user-scoped memory recalled in a fresh session and invisible to another user, cancel/stop, auto titles, model switching, and mid-turn reload that replays and completes.
- The e2e runbook executed by Claude produces a report with all scenarios passing against that stack.
