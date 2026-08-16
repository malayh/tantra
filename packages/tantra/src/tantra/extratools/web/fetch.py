from __future__ import annotations

import asyncio
import ipaddress
import json
import random
import socket
from typing import Any
from urllib.parse import urljoin, urlparse

from tantra.tools import Tool, tool

try:
    import trafilatura
    from curl_cffi import requests as cffi
    from curl_cffi.requests import exceptions as cffi_exc
    from tenacity import AsyncRetrying, RetryCallState, RetryError, retry_if_exception_type, stop_after_attempt
except ImportError as exc:
    raise ImportError("web extras not installed: install tantra-harness[web]") from exc

MAX_BYTES = 5_000_000
MAX_HOPS = 5
MAX_ATTEMPTS = 3
RETRY_AFTER_CAP = 30.0
RETRY_STATUSES = frozenset({403, 429, 500, 502, 503, 504})
PROXY_SCHEMES = frozenset({"http", "https", "socks4", "socks4a", "socks5", "socks5h"})
BLOCKED_STATUSES = frozenset({403, 429})
AUTH_STATUS = 401
HTML_TYPES = frozenset({"text/html", "application/xhtml+xml"})
PLAIN_TYPES = frozenset({"application/json", "application/xml"})
PDF_TYPE = "application/pdf"
DOCX_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
UNTITLED = "(untitled)"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

Address = ipaddress.IPv4Address | ipaddress.IPv6Address


def web_fetch(
    *, max_chars: int = 64_000, timeout: float = 20.0, ssrf_guard: bool = True, proxy: str | None = None
) -> Tool:
    proxy = proxy or None
    if proxy is not None:
        parsed = urlparse(proxy)
        scheme = parsed.scheme if "://" in proxy else ""
        if scheme not in PROXY_SCHEMES or not parsed.hostname:
            raise ValueError(
                "web_fetch(proxy=...) must be a URL with scheme http, https, socks4, socks4a, socks5 or socks5h "
                f"and a host; got scheme {scheme!r}"
            )

    @tool
    async def web_fetch(url: str) -> str:
        """Fetch one web page and return its readable text.

        Usage:
        - Reach for this after `web_search`, on the one or two results whose title and snippet look
          most likely to answer the question. Fetching every result burns the context you need for
          the answer and rarely changes it.
        - Pass a single absolute URL including the scheme (`https://...`). A bare domain or a
          relative path will not resolve.
        - HTML comes back as article text with navigation, ads and boilerplate stripped. Quote what
          you actually received rather than assuming the page said more.
        - Plain text, JSON and XML come back verbatim. PDFs and Word documents are extracted to text
          when the application has the document extras installed; the error says so when it does not.
        - The first two lines are the page title and the URL that finally served the content after
          redirects. Cite that final URL, not the one you asked for.
        - Long pages are cut with a `[truncated at N chars]` marker. That is all you get — refetching
          the same URL returns the same truncated text.
        - Every failure raises a message that names what went wrong. Read it: a bot wall, a timeout,
          a blocked private address and an unreadable content type each need a different next move.
        - An error is never a reason to retry the same URL. Fetch a different source, or tell the
          user plainly what you could not reach.
        - Pages that build their content with JavaScript come back with no extractable text. Look for
          an article, documentation, print or plain-text version of the same material instead.
        """
        final_url, content_type, body = await _get(url, timeout, ssrf_guard, proxy)
        title, text = await _extract(final_url, content_type, body)
        return _cap(f"{title}\n{final_url}\n\n{text}", max_chars)

    return web_fetch


def _session(timeout: float, proxy: str | None) -> cffi.AsyncSession:
    return cffi.AsyncSession(impersonate="chrome", timeout=timeout, allow_redirects=False, proxy=proxy)


async def _get(url: str, timeout: float, ssrf_guard: bool, proxy: str | None) -> tuple[str, str, bytes]:
    current = url
    async with _session(timeout, proxy) as session:
        for _ in range(MAX_HOPS + 1):
            await _ssrf_check(current, ssrf_guard)
            content_type, body, location = await _attempt(session, current, timeout, proxy)
            if location is None:
                return current, content_type, body
            current = urljoin(current, location)
    raise RuntimeError(
        f"web_fetch stopped following {url} after {MAX_HOPS} redirects, and it kept redirecting — "
        "the URL is a redirect loop or a login bounce; try another source"
    )


class _Retry(Exception):
    def __init__(self, failure: str, delay: float | None = None) -> None:
        super().__init__(failure)
        self.failure = failure
        self.delay = delay


async def _attempt(session: Any, url: str, timeout: float, proxy: str | None) -> tuple[str, bytes, str | None]:
    retrying = AsyncRetrying(
        stop=stop_after_attempt(MAX_ATTEMPTS),
        retry=retry_if_exception_type(_Retry),
        wait=_wait,
        sleep=asyncio.sleep,
    )
    try:
        async for attempt in retrying:
            with attempt:
                return await _once(session, url, timeout, proxy)
    except RetryError as exc:
        raise RuntimeError(
            f"web_fetch gave up on {url} after {MAX_ATTEMPTS} attempts: it {exc.last_attempt.exception().failure}"
        ) from None


def _wait(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception()
    return exc.delay or _backoff(retry_state.attempt_number - 1)


async def _once(session: Any, url: str, timeout: float, proxy: str | None) -> tuple[str, bytes, str | None]:
    try:
        response = await session.get(url, headers=HEADERS, stream=True)
    except cffi_exc.ProxyError as exc:
        raise _Retry(
            f"could not reach the configured proxy ({exc}) — this is a proxy or network problem, "
            "not a problem with the page; tell the user and do not try other URLs"
        ) from exc
    except cffi_exc.Timeout as exc:
        raise _Retry(
            f"timed out after {timeout}s — the site is slow or unreachable; try another source{_proxied(proxy)}"
        ) from exc
    except cffi_exc.RequestException as exc:
        raise _Retry(f"could not be reached ({exc}) — try another source{_proxied(proxy)}") from exc
    status = response.status_code
    if status >= 400:
        await response.aclose()
        if status not in RETRY_STATUSES:
            raise RuntimeError(f"web_fetch got HTTP {status} for {url}{_hint(status)}")
        raise _Retry(f"returned HTTP {status}{_hint(status)}", _retry_after(response.headers))
    location = response.headers.get("Location")
    if 300 <= status < 400 and location:
        await response.aclose()
        return "", b"", location
    try:
        body = await _body(response, url)
    except cffi_exc.RequestException as exc:
        raise _Retry(
            f"dropped the connection mid-download ({exc}) — try again or use another source{_proxied(proxy)}"
        ) from exc
    return response.headers.get("Content-Type", ""), body, None


def _proxied(proxy: str | None) -> str:
    if proxy is None:
        return ""
    return " (a proxy is configured — if every URL fails the same way, the proxy is the problem; tell the user)"


async def _body(response: Any, url: str) -> bytes:
    chunks: list[bytes] = []
    total = 0
    try:
        async for chunk in response.aiter_content():
            total += len(chunk)
            if total > MAX_BYTES:
                break
            chunks.append(chunk)
    finally:
        await response.aclose()
    if total > MAX_BYTES:
        raise RuntimeError(
            f"web_fetch stopped downloading {url}: the response is larger than the {MAX_BYTES} byte limit — "
            "it is too big to read here; try a specific page on the site instead of a bulk file"
        )
    return b"".join(chunks)


def _hint(status: int) -> str:
    if status == AUTH_STATUS:
        return " — the URL requires authentication; try another source that is readable without signing in"
    if status in BLOCKED_STATUSES:
        return " — blocked/forbidden — likely bot protection; try another source"
    return ""


def _retry_after(headers: Any) -> float | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, min(float(int(value)), RETRY_AFTER_CAP))
    except ValueError:
        return None


def _backoff(attempt: int) -> float:
    return random.uniform(0.5, 1.5) * (attempt + 1)


async def _ssrf_check(url: str, ssrf_guard: bool) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise RuntimeError(
            f"web_fetch refused {url}: only http and https URLs can be fetched, not {parsed.scheme or 'a bare path'!r}"
            " — pass a full https:// URL"
        )
    if not ssrf_guard:
        return
    host = parsed.hostname
    if not host:
        raise RuntimeError(f"web_fetch refused {url}: the URL names no host — pass a full https:// URL")
    for address in await _addresses(host, url):
        kind = _blocked(address)
        if kind is not None:
            raise RuntimeError(
                f"web_fetch refused {url}: it resolves to {address}, a {kind} address on the machine's own "
                "network — only public internet addresses may be fetched; try a public source"
            )


async def _addresses(host: str, url: str) -> list[Address]:
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise RuntimeError(
            f"web_fetch could not resolve the host {host!r} of {url} ({exc}) — check the spelling or try another source"
        ) from exc
    return [ipaddress.ip_address(str(info[4][0]).split("%")[0]) for info in infos]


def _blocked(address: Address) -> str | None:
    if address.is_loopback:
        return "loopback"
    if address.is_link_local:
        return "link-local"
    if address.is_private:
        return "private"
    return None


async def _extract(final_url: str, content_type: str, body: bytes) -> tuple[str, str]:
    mime = content_type.split(";")[0].strip().lower()
    if mime == PDF_TYPE or body.startswith(b"%PDF-"):
        return UNTITLED, await _pdf(final_url, body)
    if mime == DOCX_TYPE:
        return UNTITLED, await _docx(final_url, body)
    if mime in HTML_TYPES:
        return await _html(final_url, body)
    if mime.startswith("text/") or mime in PLAIN_TYPES:
        return UNTITLED, _nonempty(final_url, body.decode("utf-8", errors="replace"))
    described = f"of type {content_type!r}" if mime else "with no content type"
    raise RuntimeError(
        f"web_fetch reached {final_url} but cannot read content {described} — it is not a web page, "
        "plain text, JSON, XML, PDF or Word document; try another source"
    )


async def _html(final_url: str, body: bytes) -> tuple[str, str]:
    raw = await asyncio.to_thread(
        trafilatura.extract,
        body,
        url=final_url,
        output_format="json",
        with_metadata=True,
        favor_recall=True,
    )
    if not raw:
        raise RuntimeError(_empty(final_url))
    parsed = json.loads(raw)
    return parsed.get("title") or UNTITLED, _nonempty(final_url, parsed.get("text") or "")


async def _pdf(final_url: str, body: bytes) -> str:
    try:
        from tantra.extratools.doc import extract_pdf
    except ImportError as exc:
        raise RuntimeError(_missing_doc_extra(final_url, "PDF")) from exc
    try:
        return await asyncio.to_thread(extract_pdf, body)
    except RuntimeError as exc:
        raise RuntimeError(_doc_failure(final_url, "PDF", exc)) from exc


async def _docx(final_url: str, body: bytes) -> str:
    try:
        from tantra.extratools.doc import extract_docx
    except ImportError as exc:
        raise RuntimeError(_missing_doc_extra(final_url, "Word document")) from exc
    try:
        return await asyncio.to_thread(extract_docx, body)
    except RuntimeError as exc:
        raise RuntimeError(_doc_failure(final_url, "Word document", exc)) from exc


def _missing_doc_extra(final_url: str, kind: str) -> str:
    return (
        f"web_fetch downloaded {final_url}, which is a {kind}, but the {kind} extractors are not installed — "
        "tell the user to run `pip install tantra-harness[doc]` and try again, or find an HTML source instead"
    )


def _doc_failure(final_url: str, kind: str, exc: Exception) -> str:
    return f"web_fetch downloaded {final_url} as a {kind} but could not read it: {exc} — try another source"


def _empty(final_url: str) -> str:
    return (
        f"web_fetch reached {final_url} but found no extractable text (page may be JS-rendered, a login wall, "
        "or genuinely empty) — try another source"
    )


def _nonempty(final_url: str, text: str) -> str:
    if not text.strip():
        raise RuntimeError(_empty(final_url))
    return text


def _cap(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n[truncated at {max_chars} chars]"
