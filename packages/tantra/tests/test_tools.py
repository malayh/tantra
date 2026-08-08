from __future__ import annotations

import pytest
from pydantic import ValidationError

from tantra.tools import Context, Tool, tool


@tool
def search_metrics(query: str, limit: int = 5) -> list[str]:
    """Search for relevant metrics based on query."""
    return [query] * limit


def test_schema_infers_types_defaults_and_docstring() -> None:
    @tool
    def demo(a: str, b: int, c: float, d: bool, e: list[str], f: dict, g: str | None = None, h: int = 3) -> None:
        """Does a demo thing."""

    schema = demo.schema
    params = schema.parameters
    props = params["properties"]

    assert schema.name == "demo"
    assert schema.description == "Does a demo thing."
    assert params["type"] == "object"
    assert set(params["required"]) == {"a", "b", "c", "d", "e", "f"}
    assert props["a"]["type"] == "string"
    assert props["b"]["type"] == "integer"
    assert props["c"]["type"] == "number"
    assert props["d"]["type"] == "boolean"
    assert props["e"]["type"] == "array"
    assert props["e"]["items"] == {"type": "string"}
    assert props["f"]["type"] == "object"
    assert props["g"]["anyOf"] == [{"type": "string"}, {"type": "null"}]
    assert props["h"]["default"] == 3


def test_ctx_parameter_is_stripped_from_the_model_facing_schema() -> None:
    @tool
    async def probe(query: str, ctx: Context) -> str:
        """Probe something."""
        return query

    assert probe.takes_ctx
    assert probe.ctx_param == "ctx"
    assert list(probe.schema.parameters["properties"]) == ["query"]
    assert "ctx" not in probe.schema.parameters.get("required", [])


def test_module_level_tool_keeps_its_name_and_description() -> None:
    assert isinstance(search_metrics, Tool)
    assert search_metrics.name == "search_metrics"
    assert search_metrics.description == "Search for relevant metrics based on query."
    assert not search_metrics.takes_ctx


def test_decorator_accepts_arguments() -> None:
    @tool(permission="ask")
    def write_dashboard(path: str) -> str:
        """Write a dashboard."""
        return path

    assert isinstance(write_dashboard, Tool)
    assert write_dashboard.permission == "ask"
    assert write_dashboard.name == "write_dashboard"


async def test_arguments_are_coerced_through_the_inferred_model() -> None:
    @tool
    def add(a: int, b: int = 1) -> int:
        """Add two numbers."""
        return a + b

    assert await add.invoke({"a": "41"}, None) == 42


async def test_bad_arguments_raise_a_validation_error_for_the_loop_to_report() -> None:
    @tool
    def add(a: int) -> int:
        """Add."""
        return a

    with pytest.raises(ValidationError):
        await add.invoke({"a": "not a number"}, None)


async def test_sync_and_async_tools_both_invoke() -> None:
    @tool
    def sync_tool(value: str) -> str:
        """Sync."""
        return f"sync:{value}"

    @tool
    async def async_tool(value: str) -> str:
        """Async."""
        return f"async:{value}"

    assert await sync_tool.invoke({"value": "x"}, None) == "sync:x"
    assert await async_tool.invoke({"value": "x"}, None) == "async:x"


async def test_ctx_is_injected_by_annotation() -> None:
    @tool
    async def uses_ctx(value: str, ctx: Context) -> str:
        """Uses ctx."""
        return f"{ctx.session_id}:{value}"

    ctx = Context(
        session_id="s1",
        turn_id="t1",
        call_id="c1",
        depth=0,
        deps=None,
        store=None,
        emit=None,
    )
    assert await uses_ctx.invoke({"value": "x"}, ctx) == "s1:x"
