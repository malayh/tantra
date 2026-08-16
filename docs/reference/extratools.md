# Extra tools

The shipped tool pack. Every entry is a **factory** returning a `Tool`: call it in your own agent module, so keys and limits are per-agent and a misconfiguration fails at construction rather than mid-turn. Factory arguments never appear in the model-facing schema.

```python
from tantra.extratools.shell import ShellGuard, bash
from tantra.extratools.web import web_fetch, web_search
from tantra.extratools.doc import read_doc

class Researcher(Agent):
    tools = [bash(), web_search(api_key=BRAVE_API_KEY), web_fetch(), read_doc()]
```

| Import path | Factory | Extra required |
|---|---|---|
| `tantra.extratools.shell` | `bash`, `ShellGuard` | none — stdlib |
| `tantra.extratools.web` | `web_search`, `web_fetch` | `tantra-harness[web]` |
| `tantra.extratools.doc` | `read_doc` (+ `extract_pdf`, `extract_docx`) | `tantra-harness[doc]` |

A missing extra raises `ImportError` naming the install command at import time. `tantra.extratools.web` imports both submodules, so `web_search` needs `[web]` even though its own dependency (`httpx`) is core.

## Model-facing parameters

| Tool | Parameters | Returns |
|---|---|---|
| `bash` | `command: str` | `str` — merged stdout+stderr, capped at 64,000 chars, `[exit status N]` appended on failure |
| `web_search` | `query: str`, `count: int = 5` | `list[dict]` — `title`, `url`, `snippet` |
| `web_fetch` | `url: str` | `str` — title line, final URL line, blank line, extracted text |
| `read_doc` | `path: str` | `str` — extracted text, capped at 64,000 chars |

Tool names are the factory names, so `"web_*"` is one permission rule for the web pack.

## `bash(*, timeout: float = 120.0) -> Tool`

Runs `command` through a shell in the process's working directory, in its own process group, and returns stdout and stderr interleaved.

- Declares `permission="ask"`. A headless deployment must override it: `permissions = {"bash": "allow"}`.
- `timeout` is a factory argument on purpose — the model cannot extend its own cap. On expiry the whole process group is killed and the tool raises `TimeoutError` naming the timeout and the command.
- Emits `$ <command>` through `ctx.emit`, so it lands in the log as `ToolProgress`.
- Output over 64,000 chars is cut with `[truncated: N chars omitted]`.

## `ShellGuard(*, on_trip="deny", deny_extra=None)`

```python
class ShellGuard(Hook):
    def __init__(self, *, on_trip: Literal["deny", "ask"] = "deny", deny_extra: list[str] | None = None) -> None
```

A [`Hook`](hooks.md) implementing `before_tool`. It acts only on calls named `bash`; everything else passes through untouched. Pass it to the harness: `Harness(..., hooks=[ShellGuard()])`.

- **`on_trip="deny"`** (default) returns `Denial(reason)`; the model reads `denied by hook: <reason>` and adapts. Works headless — nothing can suspend forever waiting for an approver that was never wired.
- **`on_trip="ask"`** returns `Escalation(reason)`, routing the call into the normal approval suspend/resume flow with the reason shown to the human.
- **`deny_extra`** appends your own patterns, matched as substrings against the raw command and against inline interpreter code.
- **`guard.inspect(command) -> str | None`** is the decision on its own — the reason it tripped, or `None`. Useful in tests.

Default deny rules cover the destructive-irreversible set only: recursive `rm` outside the working directory, `dd` to a device, `mkfs*`, halt/reboot, fork bombs, `sudo`/`doas`/`su`, and recursive `chmod`/`chown`/`chgrp` on `/` or `~`. The parser is a real shell tokenizer and recurses into `sh -c`, `xargs`, `env`/`nohup`/`nice`/`timeout` prefixes, `find -delete`/`-exec`, `eval`, and `python -c`-style inline code.

!!! danger "A guardrail, not a sandbox"
    It fails closed (unparseable commands, runtime-built delete targets and shell-fed stdin all trip) and it closes every bypass known at the time it was written. A determined model or a novel wrapper still gets through. Verdicts are also CWD-dependent — `Path.cwd()` at check time decides what "outside the working directory" means.

## `web_search(api_key, *, http_client=None) -> Tool`

Brave Search. Raises `ValueError` at construction for an empty key.

- `count` is clamped to 1–20; the model's default is 5.
- Retries up to 6 attempts on 429/500/502/503/504 and on transport errors, with full-jitter backoff capped at 30s, honouring an integer `Retry-After` (also capped at 30s). Brave's free tier is roughly 1 request/second — the backoff *is* the rate limiter.
- Non-retryable statuses raise coaching errors: 401/403 says the key was rejected and rewording will not help, 400/422 says to shorten the query.
- Snippets are stripped of tags, unescaped, then stripped again, so `&lt;b&gt;` never reaches the model as markup.
- `http_client` is the test seam (`httpx.MockTransport`). Left `None`, a client is built per call with a 15s timeout.

## `web_fetch(*, max_chars=64_000, timeout=20.0, ssrf_guard=True, proxy=None) -> Tool`

One hardened page fetch, returning readable text.

- Uses curl_cffi with Chrome impersonation, so Cloudflare-class walls answer. Redirects are followed **manually**, up to 5 hops, and the final URL is reported to the model.
- **SSRF guard** (default on): http/https only, then every hop's resolved address is checked against loopback, private and link-local ranges. `ssrf_guard=False` skips the address checks only — the scheme check always runs. Known gaps: CGNAT and NAT64 ranges are not blocked, and DNS rebinding between the check and the fetch is not defended.
- Body download aborts past 5,000,000 bytes with a self-describing error.
- Content-type dispatch: HTML through trafilatura (on bytes, off the event loop), `text/*` / JSON / XML verbatim, PDF and docx through the `[doc]` extractors — `%PDF-` magic bytes beat a lying header. Anything else is an error naming the type.
- 3 attempts on 403/429/500/502/503/504 and transport failures, including mid-stream drops; the size-cap error never burns a retry.
- Output is capped at `max_chars` with a `[truncated at N chars]` marker. Empty extraction raises the "may be JS-rendered, a login wall, or genuinely empty — try another source" error.
- Without `[doc]` installed, a PDF or Word response returns an error naming `pip install tantra-harness[doc]`.
- `proxy` is one URL used for every hop and every retry — `http`, `https`, `socks4`, `socks4a`, `socks5` or `socks5h`, credentials in the URL. An invalid value raises `ValueError` at construction naming only the scheme, never the URL; `""` means no proxy. There is no fallback to a direct connection. A `ProxyError` raises an error telling the model to stop fetching and tell the user; other connection and timeout failures raise the unproxied message plus a trailing "a proxy is configured" hint, because a connection refused by the proxy is indistinguishable from a dead target.

## `read_doc() -> Tool`

Reads a local `.pdf` or `.docx` and returns its text. Dispatch is by file extension; anything else is an error listing the supported types. Stat, read and extract all happen in one worker thread. Output capped at 64,000 chars with `[truncated at N chars]`.

The module also exports the bytes-level extractors, which is how `web_fetch` handles downloaded documents:

```python
from tantra.extratools.doc import extract_docx, extract_pdf

text = extract_pdf(data)   # raises RuntimeError on corrupt or text-free input
```

## Error strings are the contract

Every failure in this pack raises with a message that names what went wrong **and** the next move, because only `str(exc)` reaches the model. Do not wrap these tools in handlers that flatten the message.

## See also

- Guides: [shell](../guides/shell.md) · [web search](../guides/web-search.md) · [web fetch](../guides/web-fetch.md) · [reading documents](../guides/read-doc.md)
- [Hooks](hooks.md) · [Permissions](permissions.md) · [Install](../getting-started/install.md)
