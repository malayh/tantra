# Web fetch

```python
from tantra.extratools.web import web_fetch
```

Needs `pip install "tantra-harness[web]"` — `curl_cffi` for the transport, `trafilatura` for article extraction.

## `web_fetch(*, max_chars=64_000, timeout=20.0, ssrf_guard=True)`

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
- **Retries.** 3 attempts per hop on `403`, `429`, `500`, `502`, `503`, `504`, connection failures and mid-download drops, honouring `Retry-After` (capped 30 s). `403`/`429` are treated as retryable because they are usually a bot wall rather than a verdict; `401` gets an authentication hint and is not retried.

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

## Next

- [Web search](web-search.md) — finding the URL to fetch.
- [Reading documents](read-doc.md) — the `[doc]` extractors this dispatches to.
