from __future__ import annotations

from typing import Any

import httpx
import pytest

from tantra.extratools.web import search as search_module
from tantra.extratools.web import web_search
from tantra.tools import Context

PAYLOAD: dict[str, Any] = {
    "web": {
        "results": [
            {
                "title": "Brave <strong>Search</strong>",
                "url": "https://example.com/a",
                "description": "a <strong>ranked</strong> snippet",
            }
        ]
    }
}


def make_ctx() -> Context:
    async def emit(message: str) -> None:
        return None

    return Context(session_id="s1", turn_id="t1", call_id="c1", depth=0, deps=None, store=None, emit=emit)


def client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def serving(payload: dict[str, Any], seen: list[httpx.QueryParams] | None = None) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request.url.params)
        return httpx.Response(200, json=payload)

    return handler


def sequence(responses: list[Any]) -> Any:
    remaining = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        step = remaining.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    return handler


def patch_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr(search_module.asyncio, "sleep", fake_sleep)
    return slept


async def test_a_canned_brave_payload_becomes_title_url_snippet_dicts() -> None:
    tool = web_search("k", http_client=client(serving(PAYLOAD)))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results == [{"title": "Brave Search", "url": "https://example.com/a", "snippet": "a ranked snippet"}]


async def test_a_hit_without_a_url_is_skipped() -> None:
    payload = {"web": {"results": [{"title": "no link", "description": "x"}, PAYLOAD["web"]["results"][0]]}}
    tool = web_search("k", http_client=client(serving(payload)))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert [hit["url"] for hit in results] == ["https://example.com/a"]


async def test_junk_entries_beside_a_real_hit_are_skipped_not_raised() -> None:
    payload = {"web": {"results": ["a bare string", None, PAYLOAD["web"]["results"][0]]}}
    tool = web_search("k", http_client=client(serving(payload)))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results == [{"title": "Brave Search", "url": "https://example.com/a", "snippet": "a ranked snippet"}]


async def test_a_non_string_title_is_coerced_instead_of_blowing_up() -> None:
    payload = {"web": {"results": [{"title": 42, "url": "https://example.com/a", "description": None}]}}
    tool = web_search("k", http_client=client(serving(payload)))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results == [{"title": "42", "url": "https://example.com/a", "snippet": ""}]


async def test_a_429_with_retry_after_is_honored_and_the_retry_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = patch_sleep(monkeypatch)
    handler = sequence([httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(200, json=PAYLOAD)])
    tool = web_search("k", http_client=client(handler))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results[0]["url"] == "https://example.com/a"
    assert slept == [2.0]


async def test_a_retry_after_beyond_the_cap_is_clamped_to_the_backoff_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = patch_sleep(monkeypatch)
    handler = sequence([httpx.Response(429, headers={"Retry-After": "86400"}), httpx.Response(200, json=PAYLOAD)])
    tool = web_search("k", http_client=client(handler))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results[0]["url"] == "https://example.com/a"
    assert slept == [30.0]


@pytest.mark.parametrize("after", ["2.5", "Wed, 21 Oct 2015 07:28:00 GMT"])
async def test_a_non_integer_retry_after_falls_back_to_jittered_backoff(
    after: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    slept = patch_sleep(monkeypatch)
    handler = sequence([httpx.Response(429, headers={"Retry-After": after}), httpx.Response(200, json=PAYLOAD)])
    tool = web_search("k", http_client=client(handler))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results[0]["url"] == "https://example.com/a"
    assert len(slept) == 1
    assert 0.0 <= slept[0] <= 1.0


async def test_without_a_supplied_client_one_is_built_with_the_timeout_and_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = httpx.AsyncClient
    built: list[httpx.AsyncClient] = []
    kwargs: list[dict[str, Any]] = []

    def fake_async_client(**kw: Any) -> httpx.AsyncClient:
        kwargs.append(kw)
        made = real(transport=httpx.MockTransport(serving(PAYLOAD)))
        built.append(made)
        return made

    monkeypatch.setattr(search_module.httpx, "AsyncClient", fake_async_client)
    tool = web_search("k")

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results == [{"title": "Brave Search", "url": "https://example.com/a", "snippet": "a ranked snippet"}]
    assert kwargs == [{"timeout": 15.0}]
    assert built[0].is_closed


async def test_a_transport_failure_is_retried_instead_of_failing_hard(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = patch_sleep(monkeypatch)
    handler = sequence([httpx.ConnectError("connection reset"), httpx.Response(200, json=PAYLOAD)])
    tool = web_search("k", http_client=client(handler))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results[0]["title"] == "Brave Search"
    assert len(slept) == 1


async def test_a_persistent_500_exhausts_the_attempts_and_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    slept = patch_sleep(monkeypatch)
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500)

    tool = web_search("k", http_client=client(handler))

    with pytest.raises(RuntimeError) as info:
        await tool.invoke({"query": "brave"}, make_ctx())

    message = str(info.value)
    assert len(calls) == 6
    assert len(slept) == 5
    assert "6 attempts" in message
    assert "HTTP 500" in message
    assert "Brave Search API" in message


async def test_a_non_retryable_status_names_the_code_without_retrying() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(401)

    tool = web_search("k", http_client=client(handler))

    with pytest.raises(RuntimeError) as info:
        await tool.invoke({"query": "brave"}, make_ctx())

    assert len(calls) == 1
    assert "HTTP 401" in str(info.value)


@pytest.mark.parametrize(
    ("status", "phrase"),
    [
        (400, "Shorten or simplify it"),
        (422, "Shorten or simplify it"),
        (401, "the API key was rejected"),
        (403, "the API key was rejected"),
        (418, "not a retryable status"),
    ],
)
async def test_a_rejected_request_coaches_the_model_by_status_class(status: int, phrase: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    tool = web_search("k", http_client=client(handler))

    with pytest.raises(RuntimeError) as info:
        await tool.invoke({"query": "brave"}, make_ctx())

    message = str(info.value)
    assert f"HTTP {status}" in message
    assert phrase in message
    assert ("Rewording the query will not help" in message) == (status in (401, 403))


@pytest.mark.parametrize(("status", "body"), [(200, "<html>502 Bad Gateway</html>"), (302, "")])
async def test_a_body_that_is_not_json_is_named_as_unparseable(status: int, body: str) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(status, text=body)

    tool = web_search("k", http_client=client(handler))

    with pytest.raises(RuntimeError) as info:
        await tool.invoke({"query": "brave"}, make_ctx())

    message = str(info.value)
    assert len(calls) == 1
    assert "unparseable response" in message
    assert f"HTTP {status}" in message


async def test_escaped_markup_in_a_snippet_is_stripped_not_shown_to_the_model() -> None:
    payload = {
        "web": {"results": [{"title": "t", "url": "https://example.com/a", "description": "&lt;b&gt;bold&lt;/b&gt;"}]}
    }
    tool = web_search("k", http_client=client(serving(payload)))

    results = await tool.invoke({"query": "brave"}, make_ctx())

    assert results[0]["snippet"] == "bold"


@pytest.mark.parametrize(("requested", "sent"), [(0, "1"), (-3, "1"), (50, "20"), (7, "7")])
async def test_count_is_clamped_before_it_reaches_brave(requested: int, sent: str) -> None:
    seen: list[httpx.QueryParams] = []
    tool = web_search("k", http_client=client(serving(PAYLOAD, seen)))

    await tool.invoke({"query": "brave", "count": requested}, make_ctx())

    assert seen[0]["count"] == sent
    assert seen[0]["q"] == "brave"


def test_an_empty_api_key_fails_at_construction() -> None:
    with pytest.raises(ValueError, match="Brave Search API key"):
        web_search("")


async def test_a_supplied_client_is_left_open_for_its_owner() -> None:
    supplied = client(serving(PAYLOAD))
    tool = web_search("k", http_client=supplied)

    await tool.invoke({"query": "brave"}, make_ctx())

    assert not supplied.is_closed


def test_the_model_facing_schema_carries_only_query_and_count() -> None:
    tool = web_search("k")

    assert tool.name == "web_search"
    assert tool.permission is None
    assert set(tool.schema.parameters["properties"]) == {"query", "count"}
    assert tool.schema.parameters.get("required") == ["query"]
