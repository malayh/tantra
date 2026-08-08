from __future__ import annotations

import pytest

from tantra.agent import Agent, agent_name, build_name_table
from tantra.errors import TantraError
from tantra.harness import Harness
from tantra.providers.fake import FakeProvider
from tantra.stores.memory import MemoryStore
from tantra.tools import tool


class PromQLWriter(Agent): ...


class Explore(Agent): ...


class DashboardEditor(Agent):
    subagents = [PromQLWriter]


class Build(Agent):
    subagents = [DashboardEditor, Explore]


def test_name_is_snake_case_of_the_class_name() -> None:
    assert agent_name(DashboardEditor) == "dashboard_editor"
    assert agent_name(PromQLWriter) == "prom_ql_writer"
    assert agent_name(Build) == "build"


def test_explicit_name_overrides_and_is_not_inherited() -> None:
    class Named(Agent):
        name = "custom"

    class Child(Named): ...

    assert agent_name(Named) == "custom"
    assert agent_name(Child) == "child"


def test_name_table_walks_subagents_transitively() -> None:
    assert set(build_name_table([Build])) == {"build", "dashboard_editor", "explore", "prom_ql_writer"}


def test_a_repeated_class_is_not_a_collision() -> None:
    assert set(build_name_table([Build, DashboardEditor, PromQLWriter])) == {
        "build",
        "dashboard_editor",
        "explore",
        "prom_ql_writer",
    }


def test_duplicate_names_raise_at_harness_construction() -> None:
    class One(Agent):
        name = "twin"

    class Two(Agent):
        name = "twin"

    with pytest.raises(TantraError, match="duplicate agent name"):
        Harness(FakeProvider([]), MemoryStore(), [One, Two], default_model="fake/model")


def test_a_duplicate_reached_through_subagents_also_raises() -> None:
    class Leaf(Agent):
        name = "twin"

    class Other(Agent):
        name = "twin"

    class Parent(Agent):
        subagents = [Leaf]

    with pytest.raises(TantraError, match="duplicate agent name"):
        Harness(FakeProvider([]), MemoryStore(), [Parent, Other], default_model="fake/model")


def test_undecorated_tools_raise_at_harness_construction() -> None:
    def not_a_tool(query: str) -> str:
        return query

    class Broken(Agent):
        tools = [not_a_tool]

    with pytest.raises(TantraError, match="not decorated with @tool"):
        Harness(FakeProvider([]), MemoryStore(), [Broken], default_model="fake/model")


def test_duplicate_tool_names_raise_at_harness_construction() -> None:
    @tool
    def search(query: str) -> str:
        """Search."""
        return query

    @tool(name="search")
    def search_again(query: str) -> str:
        """Search again."""
        return query

    class Broken(Agent):
        tools = [search, search_again]

    with pytest.raises(TantraError, match="duplicate tool name"):
        Harness(FakeProvider([]), MemoryStore(), [Broken], default_model="fake/model")


def test_max_steps_below_one_raises_at_harness_construction() -> None:
    class Zero(Agent):
        max_steps = 0

    with pytest.raises(TantraError, match="max_steps must be at least 1"):
        Harness(FakeProvider([]), MemoryStore(), [Zero], default_model="fake/model")


def test_an_unannotated_ctx_parameter_raises_at_harness_construction() -> None:
    @tool
    def leaky(query: str, ctx) -> str:
        """Leaks ctx into the model-facing schema."""
        return query

    class Broken(Agent):
        tools = [leaky]

    with pytest.raises(TantraError, match="unannotated 'ctx'"):
        Harness(FakeProvider([]), MemoryStore(), [Broken], default_model="fake/model")


def test_a_required_parameter_with_no_inferable_type_raises() -> None:
    @tool
    def untyped(payload) -> str:
        """Takes an untyped argument."""
        return str(payload)

    class Broken(Agent):
        tools = [untyped]

    with pytest.raises(TantraError, match="no inferable JSON type"):
        Harness(FakeProvider([]), MemoryStore(), [Broken], default_model="fake/model")


def test_every_tool_schema_is_an_object_schema() -> None:
    @tool
    def fine(query: str) -> str:
        """Fine."""
        return query

    class Ok(Agent):
        tools = [fine]

    harness = Harness(FakeProvider([]), MemoryStore(), [Ok], default_model="fake/model")
    assert harness.tools["ok"]["fine"].schema.parameters["type"] == "object"
