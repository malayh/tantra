# Tantra v2 — Core Tools — Spec

## Goal
- Ship an installable toolset with tantra: `pip install tantra-harness[web,doc]` → `from tantra.extratools.web import web_search, web_fetch` → drop into `Agent.tools`.
- Tools: `web_search` (Brave), `web_fetch` (hardened page fetch), `bash` + `ShellGuard` hook, `read_doc` (pdf/docx).
- Publish `tantra` to PyPI for the first time (dist name `tantra-harness` — `tantra` is taken).
- Public docs site at `malayh.github.io/tantra`: designed landing page + full library documentation, live before the PyPI release.

## Scope
- **In:** `tantra.extratools.web` (search + fetch), `tantra.extratools.shell` (bash + guard + hook-escalation core change), `tantra.extratools.doc` (pdf/docx), extras wiring, docs site (landing page + whole-library docs + deploy workflow), PyPI release (version, README, py.typed, publish workflow).
- **Out:** agni adopting these tools (follow-up; PyInstaller implications noted in Open Decisions). Playwright/JS-rendering fallback. Search backends beyond Brave. robots.txt / politeness / response caching. Sandboxing — the guard is a guardrail, not a sandbox. http_request and filesystem packs (rejected in interrogation — agni's fs tools stay app-side for now).

## Decisions
- **Packaging — extras on the tantra distribution**, not sibling packages. `tantra-harness[web]`, `[doc]`; combine freely with `[postgres]`. One version, one release. Rejected: one-distribution-per-tool (N release pipelines for a project that has published nothing) and a separate `tantra-tools` dist (user wants the `tantra[...]` install shape).
- **Dist name `tantra-harness`, import name stays `tantra`.** PyPI `tantra` is owned by another agent framework (tantra.run, v0.1.0). Accepted risk: if a user installs both, the import names clobber — small, documented in README. Rejected: repo-wide import rename (churn in every file for a young conflict).
- **Module layout — shipped tools live under `tantra.extratools.*`:** new package `tantra/extratools/` with `web/` (`search.py`, `fetch.py`), `shell.py`, `doc.py`. One namespace grouping the shipped packs, separate from the core; `tantra/tools.py` (the `@tool` abstraction) is untouched. Rejected: converting `tools.py` into a `tantra/tools/` package to host the packs (restructures a core module purely for packaging aesthetics); packs as top-level packages directly under `tantra` (mixes them into the core namespace).
- **Shell ships in the base install** — stdlib only, no extra needed. `[web]` gates curl_cffi + trafilatura; `[doc]` gates pypdf + python-docx. `web_search` itself only needs httpx (already core) but lives behind `[web]` anyway — one extra, one import path, no per-function dep matrix.
- **Config via factories returning `Tool`**: `web_search(api_key=...)` called in the user's agent module. Matches the v1 "explicit construction" posture; per-agent keys work; fails at construction not runtime. Rejected: `ctx.deps` (couples tool pack to the user's Deps shape) and env vars (v1 spec forbids the library reading the environment).
- **Brave-only search.** A second provider later is another factory (`searxng_search(...)`) returning the same-shaped `Tool`. Rejected: `SearchBackend` protocol now — kalki built one and never added a second impl.
- **Fetch stack: curl_cffi (Chrome impersonation) + trafilatura**, hardened over kalki's version (see web_fetch section). Rejected: plain httpx (403'd by Cloudflare-class walls — defeats the resilience brief); playwright fallback (300MB browser dep, park as future `[web-js]` extra).
- **ShellGuard default action is deny-with-reason**; `on_trip="ask"` escalates to the human approval flow for interactive apps. Deny works headless — a turn must never suspend forever in a server deployment because no asker is wired.
- **Guard hardened against agni's known bypasses** (`sh -c`, `xargs`, `find -delete`, `env` prefix, `python -c`) via recursive shlex parsing. Live evidence (v1 spec P12) shows a model routing around the unhardened guard. Rejected: allow-list mode (painful UX, could be a later mode); port-as-is (weak centerpiece for a safety feature).
- **`bash` tool default `permission="ask"`** (decorator-level default; agent rulesets override with one glob rule). `timeout` is a factory arg, not a tool param — deliberate divergence from agni, where the model can extend its own 120s cap (`apps/agni/src/agni/tools.py:191`).
- **Hook contract grows an `Escalation` return** for `before_tool` — routed through the existing ask/suspend flow, same as a `"ask"` permission decision. Today hooks can only pass/transform/deny (`loop.py:627-637`); without this, `on_trip="ask"` is unimplementable.
- **PDF/docx live in `[doc]`, and `web_fetch` dispatches to it**: fetch hits a PDF → if `tantra.extratools.doc` importable, extract; else error text tells the model the URL is a PDF and the user to `pip install tantra-harness[doc]`.
- **Spec lives at `design/002_v2_core_tools.md`** — matches `design/001_v1_spec.md`; no new `spec/` dir.
- **Docs: hand-rolled landing page at the site root + MkDocs Material under `/docs/`.** One CI job builds both (`mkdocs build --site-dir out/docs`, copy `landing/` into `out/`). The landing gets full design freedom without fighting mkdocs theming; the docs get markdown authoring with zero friction. Rejected: Material custom-homepage template (couples landing design to theme overrides); hand-rolled everything (doc pages rot when writing them has friction).
- **Docs cover the whole library**, not just the v2 tools — it's tantra's first public face; tools-only docs would document accessories to an undocumented core.
- **API reference is hand-written**, FastAPI-style curated pages. The codebase convention is docstrings on tools/protocols only, so autodoc would render mostly empty. Drift risk is bounded by the contract freeze. Rejected: mkdocstrings (requires a convention change to backfill docstrings everywhere).
- **Docs deploy on every push to `main`** → `malayh.github.io/tantra` via gh-pages. No versioning machinery (mike) at one unreleased version.

## Public API (contract — frozen in P0)

```python
from tantra.extratools.web import web_search, web_fetch
def web_search(api_key: str, *, http_client: httpx.AsyncClient | None = None) -> Tool
def web_fetch(*, max_chars: int = 64_000, timeout: float = 20.0, ssrf_guard: bool = True) -> Tool

from tantra.extratools.shell import bash, ShellGuard
def bash(*, timeout: float = 120.0) -> Tool
class ShellGuard(Hook):
    def __init__(self, *, on_trip: Literal["deny", "ask"] = "deny", deny_extra: list[str] | None = None)

from tantra.extratools.doc import read_doc
def read_doc() -> Tool
```

Model-facing tool params (factory args never appear in schemas):

| Tool | Params | Returns |
|---|---|---|
| `web_search` | `query: str`, `count: int = 5` (clamped 1–20) | `list[dict]` — `title`, `url`, `snippet` |
| `web_fetch` | `url: str` | `str` — `title`, final URL, extracted text |
| `bash` | `command: str` | `str` — merged stdout+stderr, capped |
| `read_doc` | `path: str` | `str` — extracted text, capped |

- Tool names = factory names. `web_*` glob gives users one permission rule for the pack.
- Docstrings on the inner functions are the model-facing descriptions — write them opencode-style like agni's (15–25 lines). `web_search`'s must carry kalki's README guidance: provider ranking ≠ truth, never fetch every result. `web_fetch`'s error strings coach the model ("may be JS-rendered or paywalled — try another source").

## web_search (Brave)

- Port `kalki/backend/kalki/search/web.py` mechanics: GET `https://api.search.brave.com/res/v1/web/search`, `X-Subscription-Token`, `params={"q": query, "count": min(count, 20)}`, read only `data["web"]["results"]`, skip hits without `url`.
- Retry: attempts=6 on `{429, 500, 502, 503, 504}` **plus `httpx.TransportError`** (kalki gap), full-jitter backoff capped 30s, honor integer `Retry-After` clamped 30s. Brave free tier is ~1 req/s — backoff is the rate limiter; no separate limiter.
- Snippet cleaning: strip tags → unescape → strip tags again. Kalki unescapes after stripping, so `&lt;b&gt;` becomes a literal `<b>` in model-facing text.
- `http_client=None` → construct per-call with 15s timeout; the kwarg exists for `httpx.MockTransport` in tests (same seam as `OpenAICompatible(http_client=...)`).
- Clamp `count` to ≥1 — kalki lets 0/negative through to a non-retried Brave 4xx.

## web_fetch

Kalki's `page.py` is the template; every listed weakness gets fixed:

- **Client:** `curl_cffi.AsyncSession(impersonate="chrome")`, kalki's browser header set (no hand-set User-Agent — it would desync the TLS fingerprint).
- **Redirects: manual.** `allow_redirects=False`, follow up to 5 hops, validate each hop against the SSRF guard, record the final URL. Kalki delegates to libcurl: uncapped, unvalidated, final URL lost.
- **SSRF guard (default on):** http/https schemes only; resolve host, reject loopback/private/link-local ranges; applied per redirect hop. `ssrf_guard=False` for local/trusted use.
- **Download cap:** stream the body, abort past 5,000,000 bytes with a self-describing error. Kalki buffers unboundedly.
- **Content-type dispatch** (header, with magic-bytes fallback for PDF since servers lie):
  - HTML → `trafilatura.extract` on **bytes** (kalki passes decoded `str`, bypassing trafilatura's charset sniffing → mojibake), `favor_recall=True`, run in `asyncio.to_thread` (it's blocking lxml; kalki stalls the event loop).
  - `text/*`, `application/json`, `application/xml` → decoded passthrough, capped.
  - PDF / docx → `tantra.extratools.doc` extractors if importable, else error naming the type and the `[doc]` extra.
  - Anything else → error naming the content type.
- **Retry:** 3 attempts on `{403, 429, 500, 502, 503, 504}` and transport errors, honor `Retry-After`. Kalki retries only 403/429 and drops connection resets on the floor.
- **Output:** title line, final-URL line, blank line, text; truncate at `max_chars` with an explicit `[truncated at N chars]` marker.
- **Empty extraction** → error string coaching the model, verbatim spirit of kalki's: "no extractable text (may be JS-rendered, a login wall, or genuinely empty) — try another source".
- Test seam: fetch pipeline splits at `_get(url) -> (final_url, content_type, body_bytes)`; tests monkeypatch `_get`. curl_cffi has no MockTransport equivalent — this module-function seam is the contract, don't inline it.

## shell: bash + ShellGuard

- `bash` tool: port agni's (`apps/agni/src/agni/tools.py:190`) — subprocess with process-group kill, merged output, 64k char cap, `ctx.emit(f"$ {command}")`. Changes: timeout fixed at factory, decorator `permission="ask"`.
- `ShellGuard(Hook)` implements `before_tool`; acts only when `call.name == "bash"`; other tools pass through.
- **Parsing, not regex:** shlex-split the command line, walk the top-level command and each `&&`/`||`/`;`/`|` segment. Recurse into:
  - `sh|bash|zsh -c "<string>"` → parse the string as a new command line
  - `xargs [flags] <cmd>` → check `<cmd>`
  - `env [VAR=x ...] <cmd>`, `nohup|nice|timeout <cmd>` → check `<cmd>`
  - `find … -delete` / `-exec <cmd>` → treat as delete/`<cmd>`
  - `python|node|perl|ruby -c/-e "<code>"` → scan the code string against the deny rules (best-effort substring scan; can't parse arbitrary languages)
- **Default deny rules** (destructive-irreversible only, kept deliberately short): recursive `rm` targeting `/`, `~`, or a path outside CWD; `dd` writing to `/dev/*`; `mkfs*`; `shutdown|reboot|halt|poweroff`; fork bomb; `sudo`/`doas`; recursive `chmod`/`chown` on `/`. `deny_extra` appends user patterns.
- On trip: `on_trip="deny"` → return `Denial(reason=...)` → model sees `denied by hook: <reason>` and adapts. `on_trip="ask"` → return `Escalation(reason=...)`.
- **Core change (this spec's only edit to existing tantra code):** add `Escalation` to `hooks.py`; `loop.py` `before_tool` handling treats it as a `"ask"` permission decision — same `AskRequested`/suspend/resume machinery, no new event types. Documented loudly as guardrail-not-sandbox: a determined model or a novel wrapper still gets through.

## doc

- `tantra/extratools/doc.py`: `read_doc()` tool (dispatch on suffix: `.pdf` → pypdf, `.docx` → python-docx; anything else → error listing supported types) plus bytes-level extractors `extract_pdf(data: bytes) -> str`, `extract_docx(data: bytes) -> str` — public, because `web_fetch` calls them on fetched bytes.
- Output capped at 64k chars with truncation marker.
- Missing deps at import → `ImportError` naming `tantra-harness[doc]` (same for `tantra.extratools.web` / `[web]`).

## Sharp edges
- **Only `str(exc)` reaches the model** (`loop.py:511`) and `is_error` is dropped on the wire (`openai_compat.py:36`) — the error string is the entire error contract. Every raise in these tools must be self-describing and actionable by the model.
- **`@tool` output is uncallable** — tests exercise tools via `await tool.invoke({...}, ctx)`, never direct calls.
- **`ctx` injection is by annotation** (`tools.py:94`): `ctx: Context` exactly; `ctx: Context | None` leaks into the model-facing schema as a broken param.
- **Import-cycle landmine:** `tools.py` imports `Store`/`Memory` under `TYPE_CHECKING` only. `extratools` modules must not runtime-import from `tantra.stores`/`tantra.memory` — none of these need to.
- **Reserved tool names** `skill` and `submit_output` — ours don't collide; keep it that way.
- **Extras are all installed in dev** (psycopg precedent: also in the dev group), so the missing-extra path never runs in `just test` by accident — it needs the explicit ImportError-message tests in each phase.
- **Compaction will stub large `web_fetch` outputs** — intended; no exemption mechanism exists and none is wanted.
- **uv workspace sources key by dist name**: after the rename, agni needs `tantra-harness = { workspace = true }` and root `[tool.uv.sources]` updated, or resolution silently reaches for PyPI.

## Implementation phases

```
P0 (contract) ──┬── P1 shell ──────────┐
                ├── P2 web_search ─────┼── P5 docs site ── P6 release
                └── P3 web_fetch ── P4 doc ──┘
```

P5's concept/core-library pages depend only on P0 (they document v1 surface); its tool guides need P1–P4 landed. On paper it could start alongside P1–P4 — in practice write it after the tool APIs stop moving, or the guides get written twice.

### Conventions (all phases)
- uv workspace; Python 3.13; ruff line-length 120; `just lint`, `just test`, `just sync`.
- Tests in `packages/tantra/tests/` (already in `testpaths`). pytest `asyncio_mode = "auto"`.
- No comments. Docstrings only on tools (model-facing) and public protocols.
- No network in tests — `httpx.MockTransport` for search, monkeypatched `_get` for fetch, tmp files for doc, real subprocesses fine for shell.
- Missing-extra tests use the skip-with-hint pattern (`packages/tantra/tests/conftest.py:45`).
- Run `just lint` + `just test` before marking a phase done.
- **Contract freeze (after P0):** the Public API block above — factory signatures, tool names, tool param schemas, `Escalation` shape. Changing them means updating this spec first, then telling dependent phases.

### Keeping this spec current
- Update the status marker on the heading and tick the checklist as you go.
- When the build deviates from the plan, **strike the original line and say why** — `~~original~~ **Cut in P3.** <reason>`. Never silently rewrite.
- After a phase lands, add only detail that would surprise the next reader — a load-bearing constant, a behavior that isn't what the name suggests. Skip anything the code says plainly.
- Problems found but not fixed go to Open Decisions or a Follow-up note. Never fixed inline unrecorded.

### Phase 0 — Contract + scaffolding · deps: none · blocks all · ✅ DONE
- `packages/tantra/pyproject.toml`: add `[project.optional-dependencies]` `web = ["curl-cffi>=0.15", "trafilatura>=2.0"]`, `doc = ["pypdf>=6", "python-docx>=1.1"]`; add the same to the root dev group.
- Create `tantra/extratools/__init__.py` (empty), `web/__init__.py` + `search.py` + `fetch.py`, `shell.py`, `doc.py` with frozen factory signatures raising `NotImplementedError`, and the ImportError-with-extra-hint guards for missing deps.
- **Verify:** `uv sync --all-packages` succeeds; `from tantra.extratools.web import web_search, web_fetch`, `from tantra.extratools.shell import bash, ShellGuard`, `from tantra.extratools.doc import read_doc` all import; a test proves the ImportError message names the right extra when deps are absent.
- Checklist:
  - [x] extras + dev deps declared
  - [x] module skeletons with frozen signatures
  - [x] import-guard tests
- Landed notes: guards use `importlib.util.find_spec` (try-import is F401-red and the repo has no noqa pragmas); P2–P4 replace them with real imports. `search.py` has no guard of its own — gated transitively via `web/__init__.py` importing `fetch`; keep that ordering.

### Phase 1 — shell: bash + ShellGuard + Escalation · deps: P0 · ∥ P2, P3 · —
- `tantra/extratools/shell.py`: `bash` factory (agni port, fixed timeout, `permission="ask"`), `ShellGuard` with recursive shlex parsing and the default deny rules.
- `hooks.py`: `Escalation` dataclass; `loop.py`: `before_tool` returning `Escalation` routes into the ask flow.
- Tests: each default deny rule trips; each documented agni bypass (`sh -c`, `xargs rm`, `find -delete`, `env` prefix, `python -c`) trips; benign commands (`ls`, `git status`, pipes, `&&` chains) pass; `deny_extra` works; `on_trip="deny"` yields `denied by hook:` result via FakeProvider turn; `on_trip="ask"` suspends and resumes through the existing ask machinery on a fresh harness.
- **Verify:** a FakeProvider turn calling `bash` with `sh -c "rm -rf /"` completes with an error result containing the guard's reason, and the same command with `on_trip="ask"` produces an `AskRequested` that a resumed harness can answer.
- Checklist:
  - [ ] bash tool + tests
  - [ ] ShellGuard parser + deny rules + bypass tests
  - [ ] Escalation in hooks.py + loop routing + suspend/resume test

### Phase 2 — web_search · deps: P0 · ∥ P1, P3 · —
- `tantra/extratools/web/search.py`: Brave call, retry/backoff, snippet cleaning, count clamping, `http_client` seam.
- Tests via `MockTransport`: result shape; skip-no-url; 429-with-Retry-After retries then succeeds; transport error retries; attempts exhausted → self-describing error string; snippet with escaped tags comes out clean; `count=0` clamped.
- **Verify:** with a MockTransport returning a canned Brave payload, `await web_search(api_key="k", http_client=client).invoke({"query": "x"}, ctx)` returns `[{"title": ..., "url": ..., "snippet": ...}]`; no test opens a socket.
- Checklist:
  - [ ] search impl
  - [ ] retry/backoff tests
  - [ ] cleaning + clamping tests

### Phase 3 — web_fetch · deps: P0 · ∥ P1, P2 · —
- `tantra/extratools/web/fetch.py`: `_get` (curl_cffi, manual redirects, SSRF checks, streaming cap), content-type dispatch, trafilatura-on-bytes in `to_thread`, output formatting + truncation, doc-extra dispatch stub (try-import `tantra.extratools.doc`, else the install-hint error).
- Tests monkeypatch `_get`: HTML → extracted text with title + final URL; JSON/text passthrough; oversize body → cap error; redirect chain resolves final URL; redirect to `169.254.x.x`/`127.0.0.1` denied when guard on, allowed when off; non-http scheme denied; empty extraction → coaching error; PDF bytes without `[doc]` → install-hint error; truncation marker at `max_chars`.
- **Verify:** with `_get` monkeypatched to serve a real saved HTML file's bytes, the tool returns readable article text under the cap; with it serving `%PDF-` bytes and `tantra.extratools.doc` unimportable, the result names `tantra-harness[doc]`.
- Checklist:
  - [ ] _get: redirects + SSRF + streaming cap
  - [ ] dispatch + extraction + formatting
  - [ ] full error-path test matrix

### Phase 4 — doc · deps: P3 · —
- `tantra/extratools/doc.py`: `extract_pdf`, `extract_docx`, `read_doc` tool; wire `web_fetch`'s dispatch to the real extractors.
- Tests: tiny checked-in `.pdf`/`.docx` fixtures under `packages/tantra/tests/fixtures/`; suffix dispatch; unsupported suffix error; cap; `web_fetch` + monkeypatched `_get` serving PDF bytes now returns extracted text.
- **Verify:** `read_doc` on the fixture PDF returns its known sentence; the P3 install-hint test flips to an extraction test when `[doc]` is installed.
- Checklist:
  - [ ] extractors + read_doc
  - [ ] fetch dispatch integration test
  - [ ] fixtures committed

### Phase 5 — docs site · deps: P1–P4 · —
- Repo layout: `docs/` (markdown + `mkdocs.yml`), `landing/` (`index.html` + assets, self-contained, no build step). `mkdocs-material` added to the root dev group; `just docs` recipe runs `mkdocs serve`.
- `mkdocs.yml`: Material theme, `site_url: https://malayh.github.io/tantra/docs/`, `--strict` builds (broken internal links fail CI).
- Landing page: what tantra is (harness framework — the turn loop as a library), the pitch (durable/re-entrant turns, one engine many drivers, FastAPI-style posture), install command, a real code sample (Agent + tools + Harness), links to docs and GitHub. Designed, not a theme default — this is the "look nice" requirement.
- Docs nav (each page hand-written, sourced from `design/001_v1_spec.md` and this spec — the spec's landed notes are the only existing documentation):
  - Getting started: install (extras matrix), quickstart (agent + FakeProvider→real provider in ~30 lines)
  - Concepts: the turn loop (turns/samples/events), Agent vs Session vs Harness, durability & resume
  - Guides: defining tools (`@tool`, Context, factories), the tool pack (web_search, web_fetch, shell + ShellGuard, read_doc — one page each), permissions & hooks (incl. Escalation), skills, memory, subagents & fan-out, storage backends, providers, compaction
  - Reference: hand-written per-module pages for the public API surface
  - Sharp edges: port the v1 spec's section — it's the highest-value content for real users
- `.github/workflows/docs.yml`: on push to `main` — `mkdocs build --strict --site-dir out/docs`, copy `landing/` into `out/`, deploy `out/` to GitHub Pages.
- **Verify:** `mkdocs build --strict` passes; deployed site serves the landing at `malayh.github.io/tantra/` and docs at `/tantra/docs/`; every code sample in the docs is lifted from a working test or example, not written freehand.
- Checklist:
  - [ ] mkdocs scaffold + `just docs` + dev dep
  - [ ] landing page designed and self-contained
  - [ ] getting started + concepts pages
  - [ ] guides (core + one per new tool)
  - [ ] reference pages
  - [ ] sharp edges page
  - [ ] deploy workflow live, site reachable

### Phase 6 — release: tantra-harness on PyPI · deps: P1–P5 · —
- `packages/tantra/pyproject.toml`: `name = "tantra-harness"`, `version = "0.1.0"`; hatch wheel target still `src/tantra`; minimal `README.md` (what it is, install matrix incl. extras, the import-name-collision note vs PyPI `tantra`, link to the docs site); `src/tantra/py.typed` + hatch inclusion.
- `apps/agni/pyproject.toml`: depend on `tantra-harness>=0.1`; update `[tool.uv.sources]` here and at root to key `tantra-harness`.
- `.github/workflows/publish.yml`: on tag `v*` (or manual), `uv build` sdist+wheel, publish via PyPI trusted publishing (`pypa/gh-action-pypi-publish`). Account + trusted-publisher setup is manual — user does it.
- **Verify:** `uv build` produces `tantra_harness-0.1.0` artifacts; in a scratch venv, `pip install dist/tantra_harness-*.whl[web,doc]` then importing all five factories works; after the real upload, `pip install tantra-harness[web]` from PyPI succeeds.
- Checklist:
  - [ ] rename + version + README + py.typed
  - [ ] agni dep + uv sources fixed
  - [ ] publish workflow
  - [ ] trusted publisher configured (manual, with user)
  - [ ] first release tagged and live

## Open Decisions
- **agni adoption** — agni's `bash` duplicates `tantra.extratools.shell.bash`; migrating also means adding `web_*` to agni and teaching `release.yml` PyInstaller about optional submodules (`--collect-submodules`/`--hidden-import`, else they're silently absent from the binary). Own spec.
- **More search providers** — `searxng_search`/`tavily_search` factories when someone wants them; introduce a shared protocol only at the second impl.
- **`[web-js]` extra** — playwright fallback for JS-rendered pages, if empty-extraction errors prove frequent in practice.
- **Cross-process Brave rate limiting** — backoff-only is fine at 1 rps free tier; a real limiter needs shared state and isn't worth it until a paid tier / multi-worker deployment exists.

## Risks
- **Guard bypass** — a model routed around agni's guard live (v1 spec P12). Hardening closes known routes, not the category. Mitigation: `permission="ask"` decorator default, loud guardrail-not-sandbox docs, `deny_extra` for site-specific rules.
- **Import-name collision** — PyPI `tantra` (tantra.run) likely also installs `import tantra`; co-installation clobbers silently. Mitigation: README warning; revisit rename only if their project gains real adoption.
- **curl_cffi platform coverage** — native wheels; a platform without prebuilt wheels makes `[web]` a compile-from-source install. Mitigation: it's an extra; base install unaffected.
- **trafilatura 2.x API drift** — extraction options changed across 1.x→2.x. Mitigation: floor at `>=2.0`, the `_get`-seam tests exercise real extraction against checked-in HTML.
- **Brave API contract drift** — response shape changes break parsing silently (tests use canned payloads). Mitigation: skip-guarded live smoke in `stress/live_raw.py` style, run manually.
- **Docs drift** — hand-written reference pages go stale when signatures change. Mitigation: contract freeze covers the documented surface; docs update is part of any freeze-breaking change; samples are lifted from tests.

## Success criteria
- `pip install tantra-harness[web,doc]` from PyPI, then `Agent.tools = [web_search(api_key=KEY), web_fetch(), bash(), read_doc()]` + `Harness(hooks=[ShellGuard()])` runs a search→fetch→summarize turn against a live provider.
- `web_fetch` returns readable text for HTML, JSON, plain-text, and PDF URLs, and self-describing errors (naming the reason and next step) for everything else.
- Every documented agni guard bypass is denied by `ShellGuard` in tests; `on_trip="ask"` suspends and resumes across harness instances.
- `just lint` + `just test` green with zero network access; base install (`tantra-harness` alone) imports `tantra` and `tantra.extratools.shell` with no optional deps present.
- `malayh.github.io/tantra` serves the designed landing page; `/tantra/docs/` answers "how do I build an agent with tantra" end-to-end without reading the source, and every code sample in it runs.
