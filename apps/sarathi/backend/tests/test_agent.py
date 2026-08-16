from collections.abc import Iterator
from typing import Any

import pytest

from sarathi.agent import Researcher, Sarathi, _wire_tools
from sarathi.config import get_settings
from tantra.extratools.web import web_fetch as real_web_fetch
from tantra.tools import Tool


@pytest.fixture
def unwired() -> Iterator[None]:
    yield
    Sarathi.tools = []
    Researcher.tools = []
    _wire_tools.cache_clear()
    get_settings.cache_clear()


def _names(agent: type[Sarathi] | type[Researcher]) -> list[str]:
    return [tool.schema.name for tool in agent.tools]


def test_tools_wire_without_a_brave_key(monkeypatch: pytest.MonkeyPatch, unwired: None) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "")
    get_settings.cache_clear()
    _wire_tools.cache_clear()

    _wire_tools()

    assert _names(Sarathi) == ["web_fetch", "read_doc", "memory_write", "memory_recall"]
    assert _names(Researcher) == ["web_fetch"]


def test_tools_include_web_search_when_a_brave_key_is_set(monkeypatch: pytest.MonkeyPatch, unwired: None) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "brave-key")
    get_settings.cache_clear()
    _wire_tools.cache_clear()

    _wire_tools()

    assert _names(Sarathi) == ["web_search", "web_fetch", "read_doc", "memory_write", "memory_recall"]
    assert _names(Researcher) == ["web_search", "web_fetch"]


def _record_web_fetch(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def recorder(**kwargs: Any) -> Tool:
        calls.append(kwargs)
        return real_web_fetch(**kwargs)

    monkeypatch.setattr("sarathi.agent.web_fetch", recorder)
    return calls


def test_web_fetch_is_wired_with_the_configured_proxy(monkeypatch: pytest.MonkeyPatch, unwired: None) -> None:
    calls = _record_web_fetch(monkeypatch)
    monkeypatch.setenv("WEB_PROXY", "http://u:p@gw:823")
    get_settings.cache_clear()
    _wire_tools.cache_clear()

    _wire_tools()

    assert calls == [{"proxy": "http://u:p@gw:823"}, {"proxy": "http://u:p@gw:823"}]
    assert "web_fetch" in _names(Researcher)


def test_web_fetch_is_wired_without_a_proxy_when_unset(monkeypatch: pytest.MonkeyPatch, unwired: None) -> None:
    calls = _record_web_fetch(monkeypatch)
    monkeypatch.setenv("WEB_PROXY", "")
    get_settings.cache_clear()
    _wire_tools.cache_clear()

    _wire_tools()

    assert calls == [{"proxy": ""}, {"proxy": ""}]
    assert "web_fetch" in _names(Researcher)


def test_memory_write_asks_before_it_runs() -> None:
    assert Sarathi.permissions == {"memory_write": "ask"}


def test_the_researcher_is_a_sarathi_subagent_with_a_delegate_description() -> None:
    assert Sarathi.subagents == [Researcher]
    assert Researcher.__doc__ is not None
    assert Researcher.__doc__.strip().startswith("Delegate a focused research task")
