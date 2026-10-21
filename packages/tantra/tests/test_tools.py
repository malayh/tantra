from __future__ import annotations

import json
from enum import Enum
from typing import Annotated, Any, Literal

import pytest
from pydantic import BaseModel, Field, ValidationError

from tantra.loop import submit_output_schema
from tantra.tools import Context, Tool, portable_schema, tool


class Window(BaseModel):
    """Query window."""

    start: str
    end: str


class PromqlSource(BaseModel):
    type: Literal["promql"]
    query: str


class LogSource(BaseModel):
    type: Literal["logs"]
    expr: str


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Report(BaseModel):
    window: Window
    source: Annotated[PromqlSource | LogSource, Field(discriminator="type")]


class Windowed(BaseModel):
    window: Window = Field(description="time window")


class Outcome(BaseModel):
    error: str | None
    window: Window | None


class Tagged(BaseModel):
    discriminator: str


class Node(BaseModel):
    value: int
    children: list[Node] = []


class Step(BaseModel):
    text: str
    next: Step | None = None
    detail: Step


class Left(BaseModel):
    right: Right | None = None


class Right(BaseModel):
    left: Left | None = None


class CycleA(BaseModel):
    b: CycleB | None = None
    c: CycleC | None = None


class CycleB(BaseModel):
    b: CycleB | None = None
    c: CycleC | None = None


class CycleC(BaseModel):
    a: CycleA | None = None


class Mesh0(BaseModel):
    children: list[Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5] = []


class Mesh1(BaseModel):
    children: list[Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5] = []


class Mesh2(BaseModel):
    children: list[Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5] = []


class Mesh3(BaseModel):
    children: list[Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5] = []


class Mesh4(BaseModel):
    children: list[Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5] = []


class Mesh5(BaseModel):
    children: list[Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5] = []


def _refs(node: Any) -> list[str]:
    if isinstance(node, dict):
        own = [node["$ref"]] if "$ref" in node else []
        return own + [ref for value in node.values() for ref in _refs(value)]
    if isinstance(node, list):
        return [ref for item in node for ref in _refs(item)]
    return []


def _assert_refs_resolve(schema: dict[str, Any]) -> None:
    refs = _refs(schema)
    assert all(ref.startswith("#/$defs/") for ref in refs)
    assert {ref.removeprefix("#/$defs/") for ref in refs} == set(schema.get("$defs", {}))


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
    assert props["g"]["type"] == "string"
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


def test_nested_model_params_are_inlined_with_an_object_type() -> None:
    @tool
    def query(
        window: Window,
        source: PromqlSource | LogSource,
        windows: list[Window],
        fallback: Window | None = None,
    ) -> None:
        """Query."""

    params = query.schema.parameters
    props = params["properties"]

    assert _refs(params) == []
    assert "$defs" not in params
    assert props["window"]["type"] == "object"
    assert props["window"]["properties"]["start"] == {"title": "Start", "type": "string"}
    assert props["source"]["type"] == "object"
    assert [branch["title"] for branch in props["source"]["anyOf"]] == ["PromqlSource", "LogSource"]
    assert props["windows"]["type"] == "array"
    assert props["windows"]["items"]["type"] == "object"
    assert props["fallback"]["type"] == "object"
    assert props["fallback"]["anyOf"][1] == {"type": "null"}
    assert props["fallback"]["default"] is None


def test_enum_param_is_inlined() -> None:
    @tool
    def paint(color: Color) -> None:
        """Paint."""

    params = paint.schema.parameters

    assert "$defs" not in params
    assert params["properties"]["color"] == {"enum": ["red", "blue"], "title": "Color", "type": "string"}


def test_output_schema_inlines_models_and_drops_the_discriminator() -> None:
    params = submit_output_schema(Report).parameters
    source = params["properties"]["source"]

    assert _refs(params) == []
    assert "$defs" not in params
    assert params["properties"]["window"]["type"] == "object"
    assert source["type"] == "object"
    assert "discriminator" not in source
    assert [branch["title"] for branch in source["oneOf"]] == ["PromqlSource", "LogSource"]


def test_ref_sibling_keys_win_over_the_inlined_model() -> None:
    window = submit_output_schema(Windowed).parameters["properties"]["window"]

    assert window["description"] == "time window"
    assert window["type"] == "object"


def test_required_nullable_fields_get_no_hoisted_type() -> None:
    params = submit_output_schema(Outcome).parameters
    props = params["properties"]

    assert "type" not in props["error"]
    assert props["error"]["anyOf"] == [{"type": "string"}, {"type": "null"}]
    assert "type" not in props["window"]
    assert props["window"]["anyOf"][0]["type"] == "object"
    assert props["window"]["anyOf"][1] == {"type": "null"}
    assert set(params["required"]) == {"error", "window"}


def test_a_field_named_discriminator_is_kept() -> None:
    assert portable_schema(Tagged)["properties"]["discriminator"] == {"title": "Discriminator", "type": "string"}


def test_self_referencing_model_param_keeps_a_ref_to_itself() -> None:
    @tool
    def plant(tree: Node) -> None:
        """Plant."""

    params = plant.schema.parameters
    tree = params["properties"]["tree"]

    assert tree["type"] == "object"
    assert tree["properties"]["children"]["items"] == {"$ref": "#/$defs/Node"}
    assert set(params["$defs"]) == {"Node"}
    _assert_refs_resolve(params)


def test_recursive_output_schema_root_is_an_object() -> None:
    params = submit_output_schema(Node).parameters

    assert params["type"] == "object"
    assert "$ref" not in params
    assert set(params["$defs"]) == {"Node"}
    _assert_refs_resolve(params)


def test_recursive_output_schema_inlines_its_direct_fields() -> None:
    params = submit_output_schema(Step).parameters
    props = params["properties"]

    assert props["detail"]["type"] == "object"
    assert props["next"]["type"] == "object"
    assert set(params["$defs"]) == {"Step"}
    _assert_refs_resolve(params)


@pytest.mark.parametrize("model", [Left, Right, CycleA, CycleB, CycleC])
def test_mutually_recursive_models_produce_resolvable_schemas(model: type[BaseModel]) -> None:
    schema = portable_schema(model)

    assert schema["type"] == "object"
    _assert_refs_resolve(schema)


def test_mutually_recursive_union_stays_compact() -> None:
    @tool
    def visit(node: Mesh0 | Mesh1 | Mesh2 | Mesh3 | Mesh4 | Mesh5) -> None:
        """Visit."""

    params = visit.schema.parameters

    assert len(json.dumps(params)) <= 3 * len(json.dumps(visit.args_model.model_json_schema()))
    assert params["properties"]["node"]["type"] == "object"
    assert set(params["$defs"]) == {f"Mesh{index}" for index in range(6)}
    _assert_refs_resolve(params)


async def test_model_params_still_validate_dicts_and_reject_json_strings() -> None:
    @tool
    def query(window: Window) -> str:
        """Query."""
        return window.start

    assert await query.invoke({"window": {"start": "now-1h", "end": "now"}}, None) == "now-1h"
    with pytest.raises(ValidationError):
        await query.invoke({"window": '{"start": "now-1h", "end": "now"}'}, None)
