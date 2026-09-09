from __future__ import annotations

from uuid import uuid4

import pytest

from tantra import Agent, FakeProvider, MemoryStore, Runtime, Sample, TantraError, tool
from tantra.events import ToolCallCompleted
from tantra.permissions import decide
from tantra.providers.base import ToolCall


@tool(description="Read metrics.")
async def read_metrics(query: str) -> str:
    return f"read {query}"


@pytest.mark.parametrize(
    ("name", "rules", "tool_permission", "default", "expected"),
    [
        ("write_dashboard", {"*": "deny", "write_*": "ask", "write_dashboard": "allow"}, None, "deny", "allow"),
        ("write_panel", {"*": "deny", "write_*": "ask"}, None, "deny", "ask"),
        ("write_x", {"write_?": "allow", "*rite_x": "deny"}, None, "allow", "deny"),
        ("write_x", {"write_?": "ask", "*rite_x": "allow"}, None, "allow", "ask"),
        ("bash", {"read_*": "allow"}, "ask", "allow", "ask"),
        ("bash", {}, None, "ask", "ask"),
        ("bash", {"bash": "allow"}, "deny", "ask", "allow"),
    ],
)
def test_permission_precedence_and_defaults(
    name: str,
    rules: dict[str, str],
    tool_permission: str | None,
    default: str,
    expected: str,
) -> None:
    assert decide(name, rules, tool_permission, default) == expected


def test_runtime_rejects_invalid_permissions() -> None:
    class Reader(Agent):
        tools = [read_metrics]

    class Broken(Agent):
        tools = [read_metrics]
        permissions = {"read_*": "sometimes"}

    @tool(description="Odd.", permission="whenever")
    async def odd(query: str) -> str:
        return query

    class Odd(Agent):
        tools = [odd]

    with pytest.raises(TantraError, match="invalid permission"):
        Runtime(FakeProvider([]), MemoryStore(), [Reader], default_model="m", default_permission="maybe")
    with pytest.raises(TantraError, match="invalid permission"):
        Runtime(FakeProvider([]), MemoryStore(), [Broken], default_model="m")
    with pytest.raises(TantraError, match="invalid permission"):
        Runtime(FakeProvider([]), MemoryStore(), [Odd], default_model="m")


async def test_runtime_default_denies_and_agent_rule_overrides() -> None:
    invoked: list[str] = []

    @tool(description="Run an action.")
    async def blocked(value: str) -> str:
        invoked.append(f"blocked:{value}")
        return value

    @tool(description="Run an allowed action.")
    async def allowed(value: str) -> str:
        invoked.append(f"allowed:{value}")
        return value

    class Guarded(Agent):
        tools = [blocked, allowed]
        permissions = {"allowed": "allow"}

    provider = FakeProvider(
        [
            Sample(
                tool_calls=[
                    ToolCall(id="blocked", name="blocked", args='{"value":"no"}'),
                    ToolCall(id="allowed", name="allowed", args='{"value":"yes"}'),
                ]
            ),
            Sample(text="done"),
        ]
    )
    store = MemoryStore()
    runtime = Runtime(provider, store, [Guarded], default_model="m", default_permission="deny")
    sid = await runtime.create(Guarded)
    try:
        async with runtime.connect(sid, writable=True) as connection:
            result = await connection.prompt("go", command_id=uuid4())
        completed = [item.event for item in await store.read_page(sid.hex) if isinstance(item.event, ToolCallCompleted)]
        assert result.outcome == "completed"
        assert invoked == ["allowed:yes"]
        assert {item.call_id: (item.result, item.is_error) for item in completed} == {
            "blocked": ("denied by permissions: blocked", True),
            "allowed": ("yes", False),
        }
    finally:
        await runtime.aclose()
