from __future__ import annotations

from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.errors import TantraError
from tantra.events import (
    AskRaised,
    SampleStarted,
    ToolCallCompleted,
    ToolCallStarted,
    TurnCompleted,
)
from tantra.harness import Harness
from tantra.permissions import decide
from tantra.providers.base import ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import tool


@tool
async def read_metrics(query: str) -> str:
    """Read some metrics."""
    return f"read {query}"


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def test_the_longest_matching_glob_wins() -> None:
    rules = {"*": "deny", "write_*": "ask", "write_dashboard": "allow"}

    assert decide("write_dashboard", rules, None, "deny") == "allow"
    assert decide("write_panel", rules, None, "deny") == "ask"
    assert decide("read_metrics", rules, None, "allow") == "deny"


def test_equal_length_patterns_resolve_to_the_most_restrictive() -> None:
    assert decide("write_x", {"write_?": "allow", "*rite_x": "deny"}, None, "allow") == "deny"
    assert decide("write_x", {"write_?": "ask", "*rite_x": "allow"}, None, "allow") == "ask"


def test_a_tool_declared_permission_applies_when_no_rule_matches() -> None:
    assert decide("bash", {"read_*": "allow"}, "ask", "allow") == "ask"


def test_the_default_applies_when_nothing_else_does() -> None:
    assert decide("bash", {}, None, "ask") == "ask"


def test_an_agent_rule_overrides_the_tool_declaration() -> None:
    assert decide("bash", {"bash": "allow"}, "deny", "ask") == "allow"


def test_an_invalid_default_permission_is_rejected_at_construction() -> None:
    class Reader(Agent):
        tools = [read_metrics]

    with pytest.raises(TantraError, match="invalid permission"):
        Harness(FakeProvider([]), MemoryStore(), [Reader], default_permission="maybe")


def test_an_invalid_rule_value_is_rejected_at_construction() -> None:
    class Broken(Agent):
        tools = [read_metrics]
        permissions = {"read_*": "sometimes"}

    with pytest.raises(TantraError, match="invalid permission"):
        Harness(FakeProvider([]), MemoryStore(), [Broken])


def test_an_invalid_tool_permission_is_rejected_at_construction() -> None:
    @tool(permission="whenever")
    async def odd(query: str) -> str:
        """Odd."""
        return query

    class Odd(Agent):
        tools = [odd]

    with pytest.raises(TantraError, match="invalid permission"):
        Harness(FakeProvider([]), MemoryStore(), [Odd])


async def test_a_deny_rule_errors_the_call_without_invoking_the_tool() -> None:
    invoked: list[str] = []

    @tool
    async def write_dashboard(path: str) -> str:
        """Write a dashboard."""
        invoked.append(path)
        return "wrote"

    class Editor(Agent):
        tools = [write_dashboard, read_metrics]
        permissions = {"write_*": "deny"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("write_dashboard", '{"path": "p99.json"}')]),
                Sample(text="I cannot write that."),
            ]
        ),
        store,
        [Editor],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Editor)).id

    events = await collect(harness.run(sid, "write it"))

    assert not invoked
    completed = picks(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result == "denied by permissions: write_dashboard"
    assert [event.call_id for event in picks(events, ToolCallStarted)] == [completed.call_id]
    assert len(picks(events, SampleStarted)) == 2
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_the_harness_default_permission_denies_unmatched_tools() -> None:
    invoked: list[str] = []

    @tool
    async def anything(query: str) -> str:
        """Do anything."""
        invoked.append(query)
        return "done"

    class Loose(Agent):
        tools = [anything]

    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("anything", '{"query": "x"}')]), Sample(text="ok")]),
        MemoryStore(),
        [Loose],
        default_model="fake/model",
        default_permission="deny",
    )
    sid = (await harness.create_session(Loose)).id

    events = await collect(harness.run(sid, "go"))

    assert not invoked
    assert picks(events, ToolCallCompleted)[0].is_error


async def test_a_tool_declared_ask_suspends_when_no_rule_matches() -> None:
    @tool(permission="ask")
    async def dangerous(query: str) -> str:
        """Dangerous."""
        return "done"

    class Careful(Agent):
        tools = [dangerous]

    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("dangerous", '{"query": "x"}')])]),
        MemoryStore(),
        [Careful],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Careful)).id

    events = await collect(harness.run(sid, "go"))

    assert picks(events, AskRaised)[0].request.extra == {"permission": "dangerous"}
    assert not picks(events, TurnCompleted)
