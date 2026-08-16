from __future__ import annotations

import importlib
import sys
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import pytest
from curl_cffi.requests import exceptions as cffi_exc

from tantra.extratools.web import fetch
from tantra.extratools.web.fetch import web_fetch
from tantra.tools import Context

FIXTURES = Path(__file__).parent / "fixtures"
ARTICLE = FIXTURES / "article.html"
SAMPLE_PDF = FIXTURES / "sample.pdf"
SAMPLE_DOCX = FIXTURES / "sample.docx"
PDF_SENTENCE = "The lighthouse keeper counted forty-one gulls before breakfast."
DOCX_SENTENCE = "Marginalia in the ledger mentions a missing brass telescope."
PAGE = "https://example.test/transport/night-train-return"
PUBLIC = "https://93.184.216.34/start"
PROXY = "http://u:p@gw:823"


def make_ctx() -> Context:
    async def emit(message: str) -> None:
        return None

    return Context(
        session_id="s1",
        turn_id="t1",
        call_id="c1",
        depth=0,
        deps=None,
        store=None,
        emit=emit,
    )


def serve(monkeypatch: pytest.MonkeyPatch, content_type: str, body: bytes, final_url: str = PAGE) -> None:
    async def _get(url: str, timeout: float, ssrf_guard: bool, proxy: str | None) -> tuple[str, str, bytes]:
        return final_url, content_type, body

    monkeypatch.setattr(fetch, "_get", _get)


class FakeResponse:
    def __init__(self, status_code: int, headers: dict[str, str], chunks: list[bytes]) -> None:
        self.status_code = status_code
        self.headers = headers
        self.chunks = chunks
        self.closed = False

    async def aiter_content(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class DroppingResponse(FakeResponse):
    def __init__(self, chunks: list[bytes], error: Exception) -> None:
        super().__init__(200, {"Content-Type": "text/plain"}, chunks)
        self.error = error

    async def aiter_content(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk
        raise self.error


class FakeSession:
    def __init__(self, handler: Callable[[str], FakeResponse]) -> None:
        self.handler = handler
        self.requested: list[str] = []

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def get(self, url: str, *, headers: dict[str, str] | None = None, stream: bool = False) -> FakeResponse:
        self.requested.append(url)
        return self.handler(url)


class ExplodingSession(FakeSession):
    def __init__(self, error: Exception) -> None:
        super().__init__(lambda url: FakeResponse(200, {}, []))
        self.error = error

    async def get(self, url: str, *, headers: dict[str, str] | None = None, stream: bool = False) -> FakeResponse:
        self.requested.append(url)
        raise self.error


def install(monkeypatch: pytest.MonkeyPatch, session: FakeSession) -> FakeSession:
    monkeypatch.setattr(fetch, "_session", lambda timeout, proxy: session)
    return session


def install_recording(monkeypatch: pytest.MonkeyPatch, session: FakeSession) -> list[str | None]:
    seen: list[str | None] = []

    def _session(timeout: float, proxy: str | None) -> FakeSession:
        seen.append(proxy)
        return session

    monkeypatch.setattr(fetch, "_session", _session)
    return seen


def record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []

    async def sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr(fetch.asyncio, "sleep", sleep)
    return slept


def ok(body: bytes = b"body", content_type: str = "text/plain") -> FakeResponse:
    return FakeResponse(200, {"Content-Type": content_type}, [body])


def redirect(location: str) -> FakeResponse:
    return FakeResponse(302, {"Location": location}, [])


def test_the_tool_offers_only_the_url_to_the_model() -> None:
    tool = web_fetch()

    assert tool.name == "web_fetch"
    assert set(tool.schema.parameters["properties"]) == {"url"}


async def test_html_is_extracted_to_article_text_under_the_title_and_final_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serve(monkeypatch, "text/html; charset=utf-8", ARTICLE.read_bytes())

    result = await web_fetch().invoke({"url": "https://example.test/t"}, make_ctx())

    lines = result.splitlines()
    assert lines[0] == "The Quiet Return of the Night Train"
    assert lines[1] == PAGE
    assert lines[2] == ""
    assert "The sleeper carriage was supposed to be finished." in result
    assert "occupancy above ninety percent" in result
    assert "Privacy policy" not in result
    assert len(result) < 64_000


async def test_json_passes_through_undisturbed(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(monkeypatch, "application/json", b'{"answer": 42}')

    result = await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert result.endswith('{"answer": 42}')
    assert PAGE in result


async def test_plain_text_passes_through_undisturbed(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(monkeypatch, "text/plain; charset=utf-8", b"line one\nline two\n")

    result = await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert result.endswith("line one\nline two\n")


async def test_pdf_magic_bytes_beat_a_lying_content_type_and_the_text_is_extracted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serve(monkeypatch, "application/octet-stream", SAMPLE_PDF.read_bytes())

    result = await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert result.splitlines()[1] == PAGE
    assert PDF_SENTENCE in result


async def test_a_docx_is_extracted_to_its_paragraph_text(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(
        monkeypatch,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        SAMPLE_DOCX.read_bytes(),
    )

    result = await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert result.splitlines()[1] == PAGE
    assert DOCX_SENTENCE in result


async def test_a_pdf_with_the_doc_extra_missing_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    importlib.import_module("tantra.extratools.doc")
    monkeypatch.delitem(sys.modules, "tantra.extratools.doc")
    monkeypatch.setitem(sys.modules, "pypdf", None)
    serve(monkeypatch, "application/pdf", b"%PDF-1.7\n%stuff")

    with pytest.raises(RuntimeError) as info:
        await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert "PDF" in str(info.value)
    assert "tantra-harness[doc]" in str(info.value)


async def test_a_docx_with_the_doc_extra_missing_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    importlib.import_module("tantra.extratools.doc")
    monkeypatch.delitem(sys.modules, "tantra.extratools.doc")
    monkeypatch.setitem(sys.modules, "docx", None)
    serve(
        monkeypatch,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        b"PK\x03\x04 not really a docx",
    )

    with pytest.raises(RuntimeError) as info:
        await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert "Word document" in str(info.value)
    assert "tantra-harness[doc]" in str(info.value)


async def test_a_corrupt_downloaded_pdf_names_the_url_and_not_just_the_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serve(monkeypatch, "application/pdf", b"%PDF-1.7 garbage")

    with pytest.raises(RuntimeError) as info:
        await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert f"web_fetch downloaded {PAGE}" in str(info.value)
    assert "PDF" in str(info.value)
    assert str(info.value).endswith("try another source")


async def test_an_unreadable_content_type_is_named_in_the_error(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(monkeypatch, "image/png", b"\x89PNG\r\n\x1a\n")

    with pytest.raises(RuntimeError) as info:
        await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert "image/png" in str(info.value)
    assert "try another source" in str(info.value)


async def test_a_response_with_no_content_type_says_so_rather_than_naming_an_empty_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serve(monkeypatch, "", b"\x00\x01\x02")

    with pytest.raises(RuntimeError) as info:
        await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert "no content type" in str(info.value)
    assert "try another source" in str(info.value)


async def test_a_naked_pdf_body_is_still_extracted_without_a_content_type(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(monkeypatch, "", SAMPLE_PDF.read_bytes())

    result = await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert result.splitlines()[1] == PAGE
    assert PDF_SENTENCE in result


async def test_a_page_with_no_extractable_text_coaches_the_model(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(monkeypatch, "text/html", b"<html><head><title>App</title></head><body><div id='root'></div></body></html>")

    with pytest.raises(RuntimeError) as info:
        await web_fetch().invoke({"url": PAGE}, make_ctx())

    assert "no extractable text" in str(info.value)
    assert "JS-rendered" in str(info.value)
    assert "try another source" in str(info.value)


async def test_a_long_page_is_truncated_with_an_explicit_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    serve(monkeypatch, "text/plain", b"z" * 5_000)

    untruncated = await web_fetch().invoke({"url": PAGE}, make_ctx())
    result = await web_fetch(max_chars=1_000).invoke({"url": PAGE}, make_ctx())

    assert len(untruncated) == 5_000 + len(f"(untitled)\n{PAGE}\n\n")
    assert result == untruncated[:1_000] + "\n[truncated at 1000 chars]"


async def test_a_redirect_chain_reports_the_last_url_and_its_body(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = {
        "https://a.test/1": redirect("https://b.test/2"),
        "https://b.test/2": redirect("/3"),
        "https://b.test/3": ok(b"the end"),
    }
    session = install(monkeypatch, FakeSession(pages.__getitem__))

    final_url, content_type, body = await fetch._get("https://a.test/1", 5.0, False, None)

    assert final_url == "https://b.test/3"
    assert content_type == "text/plain"
    assert body == b"the end"
    assert session.requested == ["https://a.test/1", "https://b.test/2", "https://b.test/3"]


async def test_five_redirects_are_followed_but_a_sixth_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    def bounce(url: str) -> FakeResponse:
        hop = int(url.rsplit("/", 1)[1])
        return ok(b"landed") if hop == 5 else redirect(f"https://loop.test/{hop + 1}")

    install(monkeypatch, FakeSession(bounce))
    _, _, body = await fetch._get("https://loop.test/0", 5.0, False, None)
    assert body == b"landed"

    install(monkeypatch, FakeSession(lambda url: redirect(f"{url}0")))
    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://loop.test/x", 5.0, False, None)

    assert "after 5 redirects" in str(info.value)
    assert "redirect loop" in str(info.value)


@pytest.mark.parametrize("target", ["http://127.0.0.1/x", "http://169.254.169.254/latest/meta-data"])
async def test_a_redirect_into_the_private_network_is_refused_by_the_guard(
    monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    install(monkeypatch, FakeSession({PUBLIC: redirect(target), target: ok(b"secrets")}.__getitem__))

    with pytest.raises(RuntimeError) as info:
        await fetch._get(PUBLIC, 5.0, True, None)

    assert target in str(info.value)
    assert "only public internet addresses" in str(info.value)


@pytest.mark.parametrize("target", ["http://127.0.0.1/x", "http://169.254.169.254/latest/meta-data"])
async def test_the_same_redirect_is_allowed_with_the_guard_off(monkeypatch: pytest.MonkeyPatch, target: str) -> None:
    install(monkeypatch, FakeSession({PUBLIC: redirect(target), target: ok(b"secrets")}.__getitem__))

    final_url, _, body = await fetch._get(PUBLIC, 5.0, False, None)

    assert final_url == target
    assert body == b"secrets"


@pytest.mark.parametrize("guard", [True, False])
async def test_a_non_http_scheme_is_refused_whatever_the_guard_setting(
    monkeypatch: pytest.MonkeyPatch, guard: bool
) -> None:
    install(monkeypatch, FakeSession(lambda url: ok()))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("ftp://files.test/pub/x.txt", 5.0, guard, None)

    assert "only http and https" in str(info.value)


async def test_a_body_past_the_download_cap_aborts_and_names_the_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    huge = FakeResponse(200, {"Content-Type": "text/plain"}, [b"x" * 1_000_000] * 6)
    session = install(monkeypatch, FakeSession(lambda url: huge))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://big.test/dump", 5.0, False, None)

    assert "5000000 byte limit" in str(info.value)
    assert huge.closed
    assert len(session.requested) == 1


async def test_a_retryable_status_is_retried_until_it_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = record_sleeps(monkeypatch)
    queue = [FakeResponse(503, {}, []), ok(b"recovered")]
    session = install(monkeypatch, FakeSession(lambda url: queue.pop(0)))

    _, _, body = await fetch._get("https://flaky.test/a", 5.0, False, None)

    assert body == b"recovered"
    assert len(session.requested) == 2
    assert len(slept) == 1


async def test_a_persistent_retryable_status_exhausts_the_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    session = install(monkeypatch, FakeSession(lambda url: FakeResponse(503, {}, [])))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://down.test/a", 5.0, False, None)

    assert len(session.requested) == 3
    assert "after 3 attempts" in str(info.value)
    assert "HTTP 503" in str(info.value)


async def test_a_terminal_status_names_it_and_hints_at_a_bot_wall(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    install(monkeypatch, FakeSession(lambda url: FakeResponse(403, {}, [])))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://walled.test/a", 5.0, False, None)

    assert "HTTP 403" in str(info.value)
    assert "likely bot protection" in str(info.value)

    install(monkeypatch, FakeSession(lambda url: FakeResponse(404, {}, [])))
    with pytest.raises(RuntimeError) as missing:
        await fetch._get("https://walled.test/b", 5.0, False, None)

    assert "HTTP 404 for https://walled.test/b" in str(missing.value)


async def test_a_401_asks_for_another_source_instead_of_blaming_a_bot_wall(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    install(monkeypatch, FakeSession(lambda url: FakeResponse(401, {}, [])))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://members.test/a", 5.0, False, None)

    assert "HTTP 401" in str(info.value)
    assert "requires authentication" in str(info.value)
    assert "bot protection" not in str(info.value)


async def test_an_integer_retry_after_sets_the_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = record_sleeps(monkeypatch)
    queue = [FakeResponse(429, {"Retry-After": "2"}, []), ok(b"finally")]
    install(monkeypatch, FakeSession(lambda url: queue.pop(0)))

    _, _, body = await fetch._get("https://slow.test/a", 5.0, False, None)

    assert body == b"finally"
    assert slept == [2.0]


async def test_a_retry_after_beyond_the_cap_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = record_sleeps(monkeypatch)
    queue = [FakeResponse(429, {"Retry-After": "600"}, []), ok(b"finally")]
    install(monkeypatch, FakeSession(lambda url: queue.pop(0)))

    await fetch._get("https://slow.test/a", 5.0, False, None)

    assert slept == [30.0]


def test_a_negative_retry_after_never_becomes_a_negative_sleep() -> None:
    assert fetch._retry_after({"Retry-After": "-5"}) == 0.0


async def test_a_timeout_names_the_timeout_and_the_url(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    session = install(monkeypatch, ExplodingSession(cffi_exc.Timeout("read timed out")))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://stalled.test/a", 7.5, False, None)

    assert len(session.requested) == 3
    assert "timed out after 7.5s" in str(info.value)
    assert "https://stalled.test/a" in str(info.value)


async def test_a_transport_error_is_retried_then_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    session = install(monkeypatch, ExplodingSession(cffi_exc.ConnectionError("connection reset")))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://reset.test/a", 5.0, False, None)

    assert len(session.requested) == 3
    assert "connection reset" in str(info.value)
    assert "try another source" in str(info.value)


async def test_a_connection_dropped_mid_download_is_retried_and_the_response_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_sleeps(monkeypatch)
    dropped = DroppingResponse([b"half a "], cffi_exc.RequestException("incomplete read"))
    queue: list[FakeResponse] = [dropped, ok(b"whole page")]
    session = install(monkeypatch, FakeSession(lambda url: queue.pop(0)))

    _, _, body = await fetch._get("https://dropped.test/a", 5.0, False, None)

    assert body == b"whole page"
    assert len(session.requested) == 2
    assert dropped.closed


async def test_a_download_that_keeps_dropping_says_so_and_closes_every_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_sleeps(monkeypatch)
    served: list[DroppingResponse] = []

    def drop(url: str) -> DroppingResponse:
        served.append(DroppingResponse([b"half a "], cffi_exc.RequestException("incomplete read")))
        return served[-1]

    install(monkeypatch, FakeSession(drop))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://dropped.test/b", 5.0, False, None)

    assert len(served) == 3
    assert all(response.closed for response in served)
    assert "web_fetch gave up on https://dropped.test/b" in str(info.value)
    assert "dropped the connection mid-download" in str(info.value)
    assert "try again or use another source" in str(info.value)


async def test_a_status_failure_and_mid_download_drops_share_one_attempt_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_sleeps(monkeypatch)
    queue: list[FakeResponse] = [
        FakeResponse(503, {}, []),
        DroppingResponse([b"half a "], cffi_exc.RequestException("incomplete read")),
        DroppingResponse([b"half a "], cffi_exc.RequestException("incomplete read")),
    ]
    session = install(monkeypatch, FakeSession(lambda url: queue.pop(0)))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://mixed.test/a", 5.0, False, None)

    assert len(session.requested) == 3
    assert queue == []
    assert "after 3 attempts" in str(info.value)
    assert "dropped the connection mid-download" in str(info.value)


async def test_the_ssrf_check_lets_a_public_address_through() -> None:
    await fetch._ssrf_check("https://93.184.216.34/", True)


@pytest.mark.parametrize("url", ["http://10.0.0.5/", "http://[::1]/", "http://127.0.0.1/x", "http://169.254.1.1/"])
async def test_the_ssrf_check_rejects_a_private_address(url: str) -> None:
    with pytest.raises(RuntimeError) as info:
        await fetch._ssrf_check(url, True)

    assert url in str(info.value)


@pytest.mark.parametrize("url", ["ftp://files.test/x", "file:///etc/passwd"])
async def test_the_ssrf_check_rejects_a_non_http_scheme(url: str) -> None:
    for guard in (True, False):
        with pytest.raises(RuntimeError) as info:
            await fetch._ssrf_check(url, guard)

        assert "only http and https" in str(info.value)


@pytest.mark.parametrize("value", ["gw:823", "user:pass@gw:823"])
def test_a_proxy_without_a_scheme_is_refused_without_echoing_any_of_it(value: str) -> None:
    with pytest.raises(ValueError) as info:
        web_fetch(proxy=value)

    assert "socks5h" in str(info.value)
    assert "got scheme ''" in str(info.value)
    assert "user" not in str(info.value)
    assert "pass" not in str(info.value)
    assert "gw:823" not in str(info.value)


def test_an_unsupported_proxy_scheme_is_named_and_the_url_is_not() -> None:
    with pytest.raises(ValueError) as info:
        web_fetch(proxy="ftp://gw:823")

    assert "'ftp'" in str(info.value)
    assert "gw:823" not in str(info.value)


def test_a_proxy_url_with_no_host_is_refused() -> None:
    with pytest.raises(ValueError) as info:
        web_fetch(proxy="http://")

    assert "a host" in str(info.value)


async def test_an_empty_proxy_reaches_the_session_as_no_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = install_recording(monkeypatch, FakeSession(lambda url: ok(b"direct")))

    result = await web_fetch(proxy="").invoke({"url": PUBLIC}, make_ctx())

    assert result.endswith("direct")
    assert seen == [None]


async def test_a_credentialed_proxy_is_handed_to_the_session_verbatim(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = install_recording(monkeypatch, FakeSession(lambda url: ok(b"proxied")))

    result = await web_fetch(proxy=PROXY).invoke({"url": PUBLIC}, make_ctx())

    assert result.endswith("proxied")
    assert seen == [PROXY]


async def test_an_unreachable_proxy_is_retried_then_named_without_its_url(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = record_sleeps(monkeypatch)
    session = install(monkeypatch, ExplodingSession(cffi_exc.ProxyError("Could not resolve proxy: gw.example")))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://target.test/a", 5.0, False, PROXY)

    assert len(session.requested) == 3
    assert len(slept) == 2
    assert "configured proxy" in str(info.value)
    assert "do not try other URLs" in str(info.value)
    assert "u:p@" not in str(info.value)


async def test_a_transport_error_with_a_proxy_configured_points_at_the_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    install(monkeypatch, ExplodingSession(cffi_exc.ConnectionError("Failed to connect")))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://target.test/a", 5.0, False, PROXY)

    assert "could not be reached" in str(info.value)
    assert "a proxy is configured" in str(info.value)


async def test_the_same_transport_error_without_a_proxy_mentions_none(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    install(monkeypatch, ExplodingSession(cffi_exc.ConnectionError("Failed to connect")))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://target.test/a", 5.0, False, None)

    assert "could not be reached" in str(info.value)
    assert "a proxy is configured" not in str(info.value)


async def test_a_timeout_with_a_proxy_configured_points_at_the_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    record_sleeps(monkeypatch)
    install(monkeypatch, ExplodingSession(cffi_exc.Timeout("read timed out")))

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://stalled.test/b", 5.0, False, PROXY)

    assert "timed out after 5.0s" in str(info.value)
    assert "a proxy is configured" in str(info.value)


async def test_a_mid_download_drop_with_a_proxy_configured_points_at_the_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_sleeps(monkeypatch)
    install(
        monkeypatch,
        FakeSession(lambda url: DroppingResponse([b"half a "], cffi_exc.RequestException("incomplete read"))),
    )

    with pytest.raises(RuntimeError) as info:
        await fetch._get("https://dropped.test/c", 5.0, False, PROXY)

    assert "dropped the connection mid-download" in str(info.value)
    assert "a proxy is configured" in str(info.value)


async def test_every_redirect_hop_rides_the_one_proxied_session(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = {
        "https://a.test/1": redirect("https://b.test/2"),
        "https://b.test/2": redirect("/3"),
        "https://b.test/3": ok(b"the end"),
    }
    session = FakeSession(pages.__getitem__)
    seen = install_recording(monkeypatch, session)

    final_url, _, body = await fetch._get("https://a.test/1", 5.0, False, PROXY)

    assert final_url == "https://b.test/3"
    assert body == b"the end"
    assert session.requested == list(pages)
    assert seen == [PROXY]
