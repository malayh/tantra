from __future__ import annotations

import asyncio
import html
import random
import re
from typing import Any

import httpx

from tantra.tools import Tool, tool

ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
MAX_COUNT = 20
MAX_ATTEMPTS = 6
BACKOFF_CAP = 30.0
TIMEOUT = 15.0
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
TAG = re.compile(r"<[^>]+>")


def _clean(text: Any) -> str:
    if not text:
        return ""
    return TAG.sub("", html.unescape(TAG.sub("", str(text)))).strip()


def _rejected(status: int) -> str:
    if status in (401, 403):
        return (
            f"web_search got HTTP {status} from the Brave Search API: the API key was rejected. Rewording the query "
            f"will not help; report this to the user and continue without search results."
        )
    if status in (400, 422):
        return (
            f"web_search got HTTP {status} from the Brave Search API: the query itself was rejected. Shorten or "
            f"simplify it — drop operators and long quoted strings — then try once more."
        )
    return (
        f"web_search got HTTP {status} from the Brave Search API, which is not a retryable status. Report this to "
        f"the user and continue without search results."
    )


def _delay(attempt: int, response: httpx.Response | None) -> float:
    if response is not None:
        after = (response.headers.get("Retry-After") or "").strip()
        if after.isdigit():
            return min(float(after), BACKOFF_CAP)
    return random.uniform(0, min(2**attempt, BACKOFF_CAP))


async def _request(client: httpx.AsyncClient, params: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    last = "no attempt completed"
    for attempt in range(MAX_ATTEMPTS):
        response: httpx.Response | None = None
        try:
            response = await client.get(ENDPOINT, params=params, headers=headers)
        except httpx.TransportError as exc:
            last = f"a transport failure ({type(exc).__name__}: {exc})"
        else:
            if response.status_code not in RETRY_STATUSES:
                if response.status_code >= 400:
                    raise RuntimeError(_rejected(response.status_code))
                try:
                    return response.json()
                except ValueError as exc:
                    raise RuntimeError(
                        f"web_search got an unparseable response (HTTP {response.status_code}) from the Brave "
                        f"Search API — an error page or proxy body rather than search results. Try again later, "
                        f"or answer from what you already know and tell the user that search is unavailable."
                    ) from exc
            last = f"HTTP {response.status_code}"
        if attempt < MAX_ATTEMPTS - 1:
            await asyncio.sleep(_delay(attempt, response))
    raise RuntimeError(
        f"web_search gave up after {MAX_ATTEMPTS} attempts against the Brave Search API; the last failure was "
        f"{last}. The provider is down or rate-limiting this key — do not call web_search again for this turn; "
        f"answer from what you already know or tell the user that search is unavailable."
    )


def web_search(api_key: str, *, http_client: httpx.AsyncClient | None = None) -> Tool:
    if not api_key:
        raise ValueError("web_search(api_key=...) needs a Brave Search API key, got an empty string")

    @tool
    async def web_search(query: str, count: int = 5) -> list[dict[str, str]]:
        """Search the web with Brave and return ranked hits as `title`, `url`, `snippet`.

        Usage:
        - This tool finds where an answer lives; it does not deliver the answer. Search, judge the
          hits, then read the promising page with `web_fetch`.
        - Ranking is the provider's opinion, not a measure of truth. The top hit is routinely SEO
          filler, an outdated post, or a mirror of the real source. Weigh each hit by its domain and
          title before trusting it, and prefer primary sources — official docs, the project's own
          repository, a standards document — over sites that restate them.
        - Snippets are keyword-matched fragments selected to look relevant. Use them to rank, never
          as facts to quote or reason from: they are frequently stale, cut mid-claim, or lifted from
          a part of the page that has nothing to do with the query.
        - Pick the one or two most promising URLs and fetch those. Never fetch every result — it
          spends the turn's context on near-duplicates and buries the answer you already had.
        - Write queries the way a search engine reads them, not as a question to a person:
          distinctive keywords, an exact error string in quotes, a version number, `site:` when you
          already know which source should have it.
        - When the results are all off-target, rewrite the query with different terms rather than
          asking for more of them — the same ranking, extended, rarely helps.
        - `count` is clamped to 1-20; the default of 5 answers almost every query.
        - The index reflects what was crawled, not what is true today. For anything fast-moving,
          confirm the date on the page you fetch instead of trusting the snippet.
        """
        params = {"q": query, "count": max(1, min(count, MAX_COUNT))}
        headers = {"Accept": "application/json", "X-Subscription-Token": api_key}
        if http_client is not None:
            data = await _request(http_client, params, headers)
        else:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                data = await _request(client, params, headers)
        results = (data.get("web") or {}).get("results") or []
        return [
            {"title": _clean(hit.get("title")), "url": hit["url"], "snippet": _clean(hit.get("description"))}
            for hit in results
            if isinstance(hit, dict) and hit.get("url")
        ]

    return web_search
