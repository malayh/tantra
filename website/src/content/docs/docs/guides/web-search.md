# Web search

```python
from tantra.extratools.web import web_search
```

Needs `pip install "tantra-harness[web]"`. The search code itself only uses `httpx` (a core dependency), but importing `tantra.extratools.web` pulls `web_fetch` alongside it, so the extra is required for this import path.

## `web_search(api_key, *, http_client=None)`

A factory returning a `Tool` named `web_search`, backed by the [Brave Search API](https://brave.com/search/api/). An empty `api_key` raises at construction, not mid-turn.

```python
class Researcher(Agent):
    tools = [web_search(api_key=BRAVE_API_KEY)]
    permissions = {"web_*": "allow"}
```

`http_client` is a test seam — pass an `httpx.AsyncClient` over a `MockTransport` to exercise the tool without a socket. Left `None`, one client is constructed per call with a 15 s timeout.

| Model-facing param | Notes |
|---|---|
| `query: str` | required |
| `count: int = 5` | clamped to 1–20 |

Returns a list of `{"title", "url", "snippet"}` dicts. Hits without a `url` are skipped; non-dict hits and non-string titles are tolerated rather than raised, so a change in Brave's payload never reaches the model as a parser traceback. Snippets are stripped of tags, HTML-unescaped, then stripped again — otherwise `&lt;b&gt;` arrives as a literal `<b>` in model-facing text.

## Retry behaviour

Six attempts. Retried: `429`, `500`, `502`, `503`, `504`, and any `httpx.TransportError`. Backoff is full-jitter exponential capped at 30 s; an integer `Retry-After` header is honoured, also capped at 30 s. Brave's free tier is about one request per second, and the backoff *is* the rate limiter — there is no separate limiter and no cross-process coordination.

Everything else raises immediately with a self-describing message the model can act on:

- `401` / `403` — the key was rejected; rewording will not help.
- `400` / `422` — the query was rejected; shorten it, drop operators.
- any other 4xx — not retryable, report and continue without search.
- a non-JSON 200 body — "unparseable response (HTTP N)", i.e. an error page or a proxy body.
- attempts exhausted — names the last failure and tells the model to stop calling `web_search` this turn.

## What the model is told

The docstring is long on purpose and carries the guidance that matters most: ranking is the provider's opinion, not truth; snippets are for ranking, never for quoting; fetch the one or two most promising URLs with [`web_fetch`](web-fetch.md), never all of them.

## Next

- [Web fetch](web-fetch.md) — reading the page you picked.
- [Extra tools reference](../reference/extratools.md).
