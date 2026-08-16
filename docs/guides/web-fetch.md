# Web fetch

```python
from tantra.extratools.web import web_fetch
```

Needs `pip install "tantra-harness[web]"` — `curl_cffi` for the transport, `trafilatura` for article extraction, `tenacity` for the retry loop.

## `web_fetch(*, max_chars=64_000, timeout=20.0, ssrf_guard=True, proxy=None)`

A factory returning a `Tool` named `web_fetch` whose only model-facing parameter is `url: str`.

```python
class Researcher(Agent):
    tools = [web_search(api_key=BRAVE_API_KEY), web_fetch(max_chars=32_000)]
    permissions = {"web_*": "allow"}
```

## The request path

- **Chrome impersonation.** `curl_cffi.AsyncSession(impersonate="chrome")` with a browser header set. No hand-set `User-Agent` — it would desync the TLS fingerprint and defeat the point.
- **Manual redirects.** `allow_redirects=False`; up to 5 hops are followed by hand, each hop re-checked against the SSRF guard, and the URL that finally served the content is reported in the output. A longer chain raises "redirect loop or login bounce".
- **Streaming cap.** The body is streamed and aborted past 5 000 000 bytes. That error does not burn a retry.
- **Retries.** 3 attempts per hop on `403`, `429`, `500`, `502`, `503`, `504`, connection failures and mid-download drops, honouring `Retry-After` (capped 30 s). `403`/`429` are treated as retryable because they are usually a bot wall rather than a verdict; `401` gets an authentication hint and is not retried. Proxy failures share the same budget.

## Content dispatch

| Content type | Result |
|---|---|
| `text/html`, `application/xhtml+xml` | `trafilatura.extract` on the raw **bytes** (so its charset sniffing works), `favor_recall=True`, run in a thread — navigation, ads and boilerplate stripped |
| `text/*`, `application/json`, `application/xml` | decoded verbatim |
| `application/pdf`, or a body starting `%PDF-` | extracted via `tantra.extratools.doc` |
| `.docx` content type | extracted via `tantra.extratools.doc` |
| anything else | an error naming the content type |

The `%PDF-` magic bytes beat a lying `Content-Type` header. Without the `[doc]` extra installed, a PDF or Word document produces an error telling the model the type and telling the user to `pip install "tantra-harness[doc]"` — see [reading documents](read-doc.md).

## Output

```
<title>
<final url>

<text>
```

Truncated at `max_chars` with an explicit `[truncated at N chars]` marker. An extraction that comes back empty raises rather than returning a blank page: *"no extractable text (page may be JS-rendered, a login wall, or genuinely empty) — try another source"*. There is no JavaScript rendering; a JS-built page is a dead end here.

## SSRF guard

On by default. It rejects non-`http(s)` schemes, resolves the host, and refuses loopback, link-local and private addresses — on the initial URL and again on every redirect hop. `ssrf_guard=False` disables the address checks for local or trusted targets; **the scheme check still runs**.

!!! warning "Best-effort, not a security boundary"
    Two gaps are known and accepted. The guard resolves the host, then `curl` resolves it again — a DNS-rebinding attacker can win that race (fixing it needs `CURLOPT_RESOLVE` pinning). CGNAT (`100.64.0.0/10`) and NAT64 (`64:ff9b::/96`) are not blocked, because Python's `is_private` says they are not. If the URL comes from an untrusted party, put a real egress policy in front of the process.

## Proxy

One URL, applied to every hop and every retry: `web_fetch(proxy="http://user:pass@host:port")`. Credentials go in the URL — there is no separate auth argument. Accepted schemes are `http`, `https`, `socks4`, `socks4a`, `socks5` and `socks5h`; anything else raises `ValueError` at construction, naming the scheme it received and never the URL, because the URL carries a password. `proxy=""` is treated as `None`, so an application can pass a setting straight through.

There is no fallback to a direct connection — a dead gateway must not silently leak the real IP. Proxy failures share the 3-attempt budget with everything else. What the model is then told depends on how the failure surfaced: a curl_cffi `ProxyError` raises a message telling it to stop fetching and inform the user, while a connection refused *by* the proxy (curl code 7) is indistinguishable from a dead target and raises the usual connection or timeout message plus a trailing "a proxy is configured — if every URL fails the same way, the proxy is the problem; tell the user" hint.

The SSRF guard is unchanged: it still resolves the target locally and rejects private addresses. The proxy's own address is not inspected, so a local egress proxy on `127.0.0.1` works. The consequence is that a `socks5h://` setup whose targets only resolve on the remote side fails the guard with "could not resolve the host".

!!! warning "libcurl reads proxy env vars on its own"
    With no `proxy=`, libcurl honours lowercase `http_proxy`, `HTTPS_PROXY`/`https_proxy` and `ALL_PROXY` — uppercase `HTTP_PROXY` is ignored, for CGI safety. `NO_PROXY`/`no_proxy` applies either way and bypasses even an explicit `proxy=`. None of this is disableable from Python.

An `https://` proxy URL against an https target emits a `CurlCffiWarning` and uses the bundled certifi CA for the proxy's own TLS; a proxy with a corporate CA is not supported.

## Next

- [Web search](web-search.md) — finding the URL to fetch.
- [Reading documents](read-doc.md) — the `[doc]` extractors this dispatches to.
