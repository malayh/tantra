# web_fetch — proxy support + tenacity retry — Spec

## Goal
- `web_fetch(proxy="http://user:pass@gw.dataimpulse.com:823")` routes every hop through one proxy; the retry loop in `fetch.py` is restructured on tenacity with identical behaviour; sarathi exposes it as `WEB_PROXY`.

## Scope
- **In:** `packages/tantra/src/tantra/extratools/web/fetch.py` (retry port, `proxy` kwarg, proxy-failure messages), `[web]` extra gains tenacity, `stress/live_fetch_proxy.py` live smoke, sarathi `WEB_PROXY` setting + wiring, docs pages, CHANGELOG, version 0.3.0.
- **Out:** proxy for `web_search` (Brave is key-authenticated; a residential proxy buys nothing and may flag the key). Porting `web_search`'s retry to tenacity (user chose fetch only). Client-side proxy rotation / lists. Custom CA / `verify=` for `https://` proxies. Retry policy changes or `attempts`/`backoff` kwargs. Actual PyPI tag + publish (manual, user).

## Decisions
- **One proxy URL, `proxy: str | None = None`, passed to `AsyncSession(proxy=...)`.** DataImpulse-style gateways rotate the exit IP server-side per request, so every retry already lands on a new IP; a client-side list is dead weight. Rejected: `str | list[str]` with round-robin — only useful for a static pool of fixed IPs nobody has.
- **Config is a factory kwarg only.** Matches 002's "factories return `Tool`, the library never reads env" rule. Sarathi reads `WEB_PROXY` and passes it in.
- **No fallback to direct when the proxy fails.** A paid gateway blip must not silently leak the real IP or hide the outage. Rejected: fail-immediately (loses resilience against transient gateway errors, which rotating providers do have).
- **Proxy failures retry inside the existing 3-attempt budget, then raise a proxy-specific message.** `cffi_exc.ProxyError` → "could not reach the configured proxy … tell the user; another URL will not help". Because a refused connection *to the proxy* surfaces as plain `ConnectionError` (curl code 7), not `ProxyError`, connection/timeout failures also get a trailing hint when a proxy is configured: "a proxy is configured — if every URL fails the same way, the proxy is the problem; tell the user". Without the hint the model would burn its turn trying other sources through a dead proxy.
- **Invalid proxy URL fails at `web_fetch(...)` construction** with `ValueError` naming the accepted schemes (`http`, `https`, `socks4`, `socks4a`, `socks5`, `socks5h`) and requiring a host. `proxy=""` is treated as `None` so apps pass `settings.WEB_PROXY` straight through. Same posture as `web_search`'s empty-key check. The message names the scheme it received, never the URL — the URL carries credentials. ~~Always echo `urlparse(proxy).scheme`~~ **Amended in P2.** `urlparse("user:pass@gw:823").scheme == "user"`, so the scheme is echoed only when the value contains `://`; otherwise the message says `got scheme ''`.
- **No message, ours or curl's, is allowed to include the proxy URL.** libcurl's own error text names at most the proxy hostname ("Could not resolve proxy: gw…"); accepted. Our strings say "the configured proxy".
- **SSRF guard unchanged: resolve locally, reject private targets; the proxy is trusted egress.** The guard does not inspect the proxy's own address (a local `127.0.0.1:8080` egress proxy stays usable). Known consequence: `socks5h://`/remote-DNS setups whose target hosts do not resolve locally get the existing "could not resolve the host" error. Rejected: skipping address checks when proxied (a proxy inside the LAN reopens SSRF); guarding the proxy address (blocks the common local egress proxy).
- **Env-var proxying stays as it is today when `proxy=None`.** curl_cffi's `trust_env` is dead code (declared, never read); libcurl itself honours lowercase `http_proxy`, `HTTPS_PROXY`/`https_proxy`, `ALL_PROXY`, `NO_PROXY` whenever `CURLOPT_PROXY` is unset. Not disableable from Python, so it is documented as a sharp edge rather than fought.
- **Retry ported to tenacity, behaviour-preserving.** User's call, for structure/readability. Same 3 attempts per hop, same retry set, same `Retry-After` handling (cap 30 s, `0`/negative → backoff), same `uniform(0.5, 1.5) * attempt` backoff, same one-budget-across-failure-kinds, same "size-cap error never burns a retry". The existing test file is the oracle: it must pass with only the seam signatures adjusted. Rejected: curl_cffi's built-in `retry=RetryStrategy` (transport-only; no status/`Retry-After`; would still need our loop on top); keeping the hand-rolled loop (user overruled — port wanted).
- **tenacity lives in the `[web]` extra** (`tenacity>=9`), plus the root dev group. `fetch.py` is its only importer; the top-of-file `try/except ImportError` naming `tantra-harness[web]` covers it.
- **Version 0.3.0.** Additive kwarg + new extra dependency + a dependency-visible refactor → minor bump. Sarathi pins `tantra-harness[postgres,web,doc]>=0.3` (workspace source, so no publish needed for local/Docker builds).

## Retry loop on tenacity (P1)

Shape — names are suggestions, semantics are not:

```python
class _Retry(Exception):            # "this attempt failed retryably"
    failure: str                    # the sentence used in the give-up message
    delay: float | None             # Retry-After seconds, already capped; None → backoff

async def _attempt(session, url, timeout, proxy) -> tuple[str, bytes, str | None]:
    retrying = AsyncRetrying(
        stop=stop_after_attempt(MAX_ATTEMPTS),
        retry=retry_if_exception_type(_Retry),
        wait=_wait,                 # retry_state.outcome.exception().delay or _backoff(retry_state.attempt_number - 1)
        sleep=asyncio.sleep,        # resolved at call time — see test seam below
    )
    try:
        async for attempt in retrying:
            with attempt:
                return await _once(session, url, timeout, proxy)
    except RetryError as exc:
        raise RuntimeError(f"web_fetch gave up on {url} after {MAX_ATTEMPTS} attempts: it {exc.last_attempt.exception().failure}") from None
```

- `_once` is one attempt: `session.get(..., stream=True)` → status dispatch → redirect → `_body`. It raises `_Retry` for every currently-retried case (timeout, `RequestException`, retryable status with `delay=_retry_after(headers)`, mid-download drop) and lets `RuntimeError` (terminal status, size cap) propagate — tenacity does not retry it because it is not `_Retry`.
- `_wait` returns `exc.delay or _backoff(n - 1)` — the `or` reproduces today's `_retry_after(...) or _backoff(attempt)`, so `Retry-After: 0` and `-5` fall through to backoff exactly as before. `attempt_number` is 1-based; today's `_backoff(attempt)` is 0-based — hence `n - 1`.
- tenacity checks `stop` before sleeping, so 3 attempts → 2 sleeps, matching today (`test_a_retryable_status_is_retried_until_it_succeeds` asserts one sleep for two attempts).
- Response `aclose()` on every path — keep the `finally` in `_body` and the explicit closes on status/redirect branches.
- **Test seam:** `record_sleeps` monkeypatches `fetch.asyncio.sleep`, i.e. the `asyncio` module attribute. Build `AsyncRetrying` *inside* `_attempt` and pass `sleep=asyncio.sleep` there so the patched function is picked up. A module-level `AsyncRetrying` or a default-arg binding would freeze the real sleep at import and every retry test would actually sleep.
- `_session(timeout)` and `_get(url, timeout, ssrf_guard)` signatures do not change in P1.

## Proxy (P2)

- `web_fetch(*, max_chars=64_000, timeout=20.0, ssrf_guard=True, proxy: str | None = None) -> Tool`.
- Construction: `proxy = proxy or None`; if set, `scheme = urlparse(proxy).scheme if "://" in proxy else ""` must be in `PROXY_SCHEMES` and `urlparse(proxy).hostname` truthy, else `ValueError("web_fetch(proxy=...) must be a URL with scheme http, https, socks4, socks4a, socks5 or socks5h and a host; got scheme {scheme!r}")`. ~~`urlparse(proxy)` must have `scheme in PROXY_SCHEMES`~~ **Amended in P2:** a scheme-less paste parses its username as the scheme; the `://` guard keeps credentials out of the message.
- Seams grow a trailing param: `_get(url, timeout, ssrf_guard, proxy)`, `_session(timeout, proxy)` → `cffi.AsyncSession(impersonate="chrome", timeout=timeout, allow_redirects=False, proxy=proxy)`. `proxy=None` → curl_cffi sets no `CURLOPT_PROXY` → libcurl env behaviour, unchanged from today. Every test fake of `_get`/`_session` updates its signature.
- Session-level `proxy` covers all redirect hops (one session per `_get`).
- Failure text (all inside `_once`, all still `_Retry` so they share the budget):
  - `cffi_exc.ProxyError` (caught before `RequestException`) → `"could not reach the configured proxy ({exc}) — this is a proxy or network problem, not a problem with the page; tell the user and do not try other URLs"`.
  - Timeout / other `RequestException` when `proxy` is set → existing sentence + `" (a proxy is configured — if every URL fails the same way, the proxy is the problem; tell the user)"`.
- Model-facing docstring of the tool: unchanged. The model does not need to know a proxy exists until an error says so.
- `stress/live_fetch_proxy.py` — manual, `uv run python stress/live_fetch_proxy.py`, docstring like `live_raw.py`. Reads `TANTRA_TEST_PROXY`; exits with a message if unset. Fetches `https://api.ipify.org?format=json` once with `web_fetch()` and once with `web_fetch(proxy=...)` via `tool.invoke({"url": ...}, ctx)` (build a `Context` the way `tests/test_extratools_fetch.py::make_ctx` does), prints both IPs, exits non-zero if the proxied fetch fails or returns the same IP as direct. `stress/README.md` table gains the row.

## Sarathi wiring (P3)

- `apps/sarathi/backend/src/sarathi/config.py`: `WEB_PROXY: str = ""`.
- `apps/sarathi/backend/src/sarathi/agent.py:62-63`: both `web_fetch()` calls become `web_fetch(proxy=settings.WEB_PROXY)` (empty → `None` in the factory).
- `apps/sarathi/.env.example`: `# WEB_PROXY=http://user:pass@host:port` under the optional block; `apps/sarathi/README.md` env list gains one line ("optional; routes web_fetch through an HTTP/SOCKS proxy").
- `apps/sarathi/backend/pyproject.toml`: `tantra-harness[postgres,web,doc]>=0.3`.
- Test in `apps/sarathi/backend/tests/test_agent.py`: monkeypatch `sarathi.agent.web_fetch` with a recorder, set `WEB_PROXY`, assert both agents were wired with `proxy=` equal to the setting; and with it unset, `proxy=""`. Uses the existing `unwired` fixture + `get_settings.cache_clear()` pattern.

## Sharp edges
- **`ProxyError` is a sibling of `ConnectionError`, not a subclass** (`curl_cffi/requests/exceptions.py`). `except ConnectionError` misses it; the current `except RequestException` catches it — the new specific branch must sit above that.
- A CONNECT-tunnel failure becomes `ProxyError` only via a `"CONNECT"` substring sniff on `RECV_ERROR` in `code2error` — brittle; that is why the generic hint exists.
- libcurl reads `NO_PROXY`/`no_proxy` from the env even when `CURLOPT_PROXY` is set explicitly (`CURLOPT_NOPROXY` is never set by curl_cffi). A `NO_PROXY=*` in the environment silently bypasses an explicit `proxy=`. Not verified live in this repo — the smoke script proves the positive path only. Documented, not fixed.
- Uppercase `HTTP_PROXY` is ignored by libcurl (CGI safety); `HTTPS_PROXY` either case works. Easy to misdiagnose when relying on env.
- `https://` proxy URLs against https targets emit a `CurlCffiWarning` suggesting `http://`; the bundled certifi CA is used for the proxy TLS too, so a corporate-CA https proxy fails without `verify=` — out of scope, see Open Decisions.
- curl_cffi's `proxy` docstrings show `http://user@pass:host` — transposed. Correct form is `http://user:pass@host:port`.
- Extras are all installed in dev, so the missing-tenacity path never runs in `just test`; the import-guard test in `tests/test_extratools_imports.py` is what covers it — extend it if it enumerates the `[web]` modules.
- Sarathi tests are **not** in the root `testpaths`; run `just test` from `apps/sarathi/backend/`.

## Implementation phases

```
P1 tenacity port ── P2 proxy ── P3 sarathi + docs + release
```
Strictly sequential: P2 writes the proxy branches against the tenacity structure; P3 documents the P2 signature.

### Conventions (all phases)
- uv workspace; Python 3.13; ruff line-length 120; `just lint`, `just test`, `just sync` from the repo root; sarathi's own `just lint`/`just test` from `apps/sarathi/backend/`.
- No comments. Docstrings only on tools (model-facing) and public protocols.
- No network in tests — fake `_session`/`_get` seams as in `packages/tantra/tests/test_extratools_fetch.py`; sleeps recorded via `fetch.asyncio.sleep`.
- Only `str(exc)` reaches the model — every raise is self-describing and actionable.
- Run `just lint` + `just test` before marking a phase done.
- **Contract freeze:** `web_fetch(*, max_chars, timeout, ssrf_guard, proxy)` signature and the `PROXY_SCHEMES` set from P2 on; the tool's model-facing param schema stays `{url}`. Changing them means updating this spec first, then telling dependent phases.

### Keeping this spec current
- Update the status marker on the heading and tick the checklist as you go.
- When the build deviates from the plan, **strike the original line and say why it changed** — `~~original~~ **Cut in P2.** <reason>`. Never silently rewrite; the reason a plan changed is worth more than the plan.
- After a phase lands, add only detail that would surprise the next reader — a constant whose value is load-bearing, a behavior that isn't what the name suggests, an ordering that matters. Skip anything the code already says plainly.
- Problems found but not fixed go to Open Decisions or a Follow-up note, with enough detail to act on later. Don't fix them inline and don't leave them unrecorded.

### Phase 1 — tenacity port, behaviour-preserving · deps: none · ✅ DONE
- `packages/tantra/pyproject.toml` `[web]` += `tenacity>=9`; root `pyproject.toml` dev group += `tenacity>=9`; `uv lock`/`just sync`.
- `fetch.py`: `_Retry`, `_once`, `_wait`; `_attempt` becomes the `AsyncRetrying` shell above; `_backoff`, `_retry_after`, `_hint`, `_body` untouched. Import tenacity inside the existing `try/except ImportError` block.
- Tests: `tests/test_extratools_fetch.py` unchanged in intent; if any test needs editing beyond a seam signature, that is a behaviour change — stop and reconcile.
- **Verify:** `just test packages/tantra/tests/test_extratools_fetch.py` passes with the retry tests unmodified: 503→ok takes 2 requests + 1 recorded sleep; persistent 503 → 3 requests + "after 3 attempts"; `Retry-After: 600` → sleep `30.0`; size-cap error after 1 request; 403 exhausts 3 attempts, 404 raises on the first. `just test` overall green; no test's wall time grows (sleeps are still intercepted).
- Checklist:
  - [x] tenacity in `[web]` + dev group, lock updated (9.1.4)
  - [x] `_attempt` on `AsyncRetrying`, `sleep=asyncio.sleep` at call time
  - [x] existing retry tests pass unmodified
- Landed notes: tenacity runs `wait` before `stop`, so `_wait` is also evaluated on the final failed attempt (delay discarded, no extra sleep) — keep `_wait` side-effect free. `test_extratools_imports.py` parametrize gained `tenacity`; the guard's ImportError text is now "web extras not installed".

### Phase 2 — proxy kwarg · deps: P1 · ✅ DONE (live gateway smoke pending — needs a real `TANTRA_TEST_PROXY`)
- `web_fetch(..., proxy=None)`, `PROXY_SCHEMES`, construction-time validation, `_session(timeout, proxy)`, `_get(..., proxy)`, `ProxyError` branch + proxied hint in `_once`.
- Tests (same file): `web_fetch(proxy="gw:823")` → `ValueError` naming schemes, message excludes the URL; `web_fetch(proxy="")` constructs; `_session` receives `proxy=` (fake it and assert); `ExplodingSession(cffi_exc.ProxyError(...))` → 3 requests, message contains "configured proxy" and "do not try other URLs"; `ConnectionError` with proxy set → message contains "a proxy is configured"; without proxy → unchanged message; a redirect chain with proxy set makes every hop on the one session.
- `stress/live_fetch_proxy.py` + `stress/README.md` row.
- **Verify:** unit tests above green. Live: `TANTRA_TEST_PROXY=<your DataImpulse URL> uv run python stress/live_fetch_proxy.py` prints two different IPs and exits 0; with a bogus host in the URL it exits non-zero within ~3 attempts and the message says "configured proxy" and does not contain the URL.
- Checklist:
  - [x] kwarg + validation + seams
  - [x] ProxyError branch + proxied hint
  - [x] test matrix (12 tests added; 641 total)
  - [ ] live smoke script, run once against a real gateway — script landed; failure path verified live (bogus host → exit 1 after 3 attempts, "configured proxy", no URL); positive path awaits the user's gateway URL
- Landed notes: `web_fetch(proxy="http://[::1")` raises urlparse's own `ValueError("Invalid IPv6 URL")` before our check — still construction-time, still URL-free, but it does not name the accepted schemes.

### Phase 3 — sarathi wiring, docs, release prep · deps: P2 · ✅ DONE (tag/publish manual, pending)
- Sarathi: `WEB_PROXY` setting, `agent.py` wiring, `.env.example`, README line, `>=0.3` pin, `test_agent.py` case.
- Docs: `docs/guides/web-fetch.md` (signature, a `## Proxy` section: single URL, creds in URL, schemes, no fallback, SSRF note, env-var behaviour when unset, `NO_PROXY` caveat), `docs/reference/extratools.md` (signature + one bullet), `docs/sharp-edges.md` (env-proxy edge under Writing tools or a new web bullet), ~~`docs/getting-started/install.md` if it lists `[web]` contents~~ **Skipped in P3.** It lists the tools an extra provides, not the packages; nothing there to update.
- `packages/tantra/pyproject.toml` version `0.3.0`; `CHANGELOG.md` `## 0.3.0` — Added: `web_fetch(proxy=...)`; Changed: `[web]` extra pulls tenacity, fetch retry restructured with unchanged behaviour.
- **Verify:** `cd apps/sarathi/backend && just lint && just test` green; `WEB_PROXY=http://x:y@h:1 uv run python -c "from sarathi.agent import _wire_tools; _wire_tools()"`-style construction succeeds and `WEB_PROXY=nonsense` raises `ValueError` at wiring; `uv run mkdocs build --strict` clean; `grep -rn "ssrf_guard=True)" docs/` shows the new signature everywhere it is quoted.
- Checklist:
  - [x] sarathi setting + wiring + test (2 tests added; sarathi 70 total)
  - [x] docs pages (`## Proxy` in the guide, reference bullet, sharp-edges bullet; `mkdocs build --strict` clean)
  - [x] version + CHANGELOG (`uv.lock` re-locked to 0.3.0)
  - [ ] (manual, user) tag `tantra-v0.3.0` → publish
- Landed: the docs distinguish the two dead-proxy paths — curl_cffi `ProxyError` gets the stop-and-tell-the-user message, a connection refused *by* the proxy (curl code 7) gets the plain connection error plus the "a proxy is configured" hint. Both `test_agent.py` proxy tests `setenv` rather than `delenv`, because `Settings` reads `.env` and a developer's local file would otherwise win.

## Open Decisions
- **`verify=`/custom CA for `https://` proxies** — needed only for TLS-terminating corporate proxies; add a `verify: str | bool` kwarg when someone hits it.
- **Default `timeout=20.0` under residential proxies** — exit nodes add latency; if the live smoke or sarathi use shows routine timeouts, raise the default or set `timeout=` in sarathi. Decide on evidence.
- **`NO_PROXY` bypass of an explicit proxy** — if it bites, set `CURLOPT_NOPROXY=""` via `curl_options` on the session; needs a live check first.
- **`web_search` on tenacity** — user declined for now; revisit if the two styles cause confusion.

## Risks
- **Behaviour drift during the port** — tenacity's attempt/sleep ordering differs subtly from a hand loop. Mitigation: the existing test file is the oracle and must pass unmodified in P1.
- **tenacity's async sleep vs the test seam** — a frozen `sleep` reference makes retry tests really sleep (seconds each) and hides it as slowness, not failure. Mitigation: `sleep=asyncio.sleep` resolved per call; P1 Verify checks wall time.
- **Proxy-down misdiagnosed by the model** — `ConnectionError`-class proxy failures are indistinguishable from target failures. Mitigation: the proxied hint on every connection/timeout message.
- **Credential leakage** — the proxy URL carries a password. Mitigation: no message of ours includes the URL; construction error names the scheme only; smoke script prints IPs, never the URL.

## Success criteria
- `web_fetch(proxy=GATEWAY)` fetches a public page through the gateway; `stress/live_fetch_proxy.py` shows a different exit IP than a direct fetch.
- A dead proxy yields, after 3 attempts, an error telling the model to stop fetching and inform the user, with no proxy URL in the text.
- Every pre-existing `test_extratools_fetch.py` retry test passes unmodified against the tenacity implementation.
- Sarathi with `WEB_PROXY` set routes both `Sarathi` and `Researcher` fetches through it; unset, behaviour is identical to 0.2.0.
