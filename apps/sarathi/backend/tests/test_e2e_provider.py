import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

PROVIDER_PATH = Path(__file__).resolve().parents[2] / "e2e" / "provider.py"
SPEC = importlib.util.spec_from_file_location("sarathi_e2e_provider", PROVIDER_PATH)
assert SPEC is not None and SPEC.loader is not None
PROVIDER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PROVIDER
SPEC.loader.exec_module(PROVIDER)
GateState = PROVIDER.GateState
Plan = PROVIDER.Plan
_plan = PROVIDER._plan
_text = PROVIDER._text
_tool = PROVIDER._tool


def payload(text: str, *, last_tool: str | None = None) -> dict:
    messages = [{"role": "user", "content": text}]
    if last_tool is not None:
        messages.extend(
            [
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {"id": "call", "type": "function", "function": {"name": last_tool, "arguments": "{}"}}
                    ],
                },
                {"role": "tool", "tool_call_id": "call", "content": "done"},
            ]
        )
    return {"model": "e2e-model", "messages": messages}


@pytest.mark.parametrize(
    ("text", "action", "name", "wait_for", "drop"),
    [
        ("[gate:provider=model]", "text", "model", "model", False),
        ("[gate:ask=approval]", "ask", "approval", None, False),
        ("[gate:tool=io]", "tool", "io", None, False),
        ("[gate:child=worker]", "child", "worker", None, False),
        ("[gate:child-tool=worker]", "child-tool", "worker", None, False),
        ("[gate:drop=reply]", "text", "reply", "reply", True),
    ],
)
def test_directives_select_a_deterministic_plan(text, action, name, wait_for, drop) -> None:
    assert _plan(payload(text)) == Plan(action=action, name=name, wait_for=wait_for, drop=drop)


def test_tool_results_do_not_repeat_root_tools() -> None:
    plan = _plan(payload("[gate:ask=approval]", last_tool="memory_write"))

    assert plan == Plan(name="memory_write")
    assert _tool(plan) is None
    assert _text(plan, {}) == "memory_write completed deterministically."


def test_child_finishes_after_its_blocked_tool() -> None:
    plan = _plan(payload("[gate:tool=child] [gate:finish=child]", last_tool="e2e_gate"))

    assert plan == Plan(action="finish", name="child")
    assert _tool(plan) == ("finish", {"result": "Child child finished"})


def test_child_plan_carries_a_provider_gate_and_finish_directive() -> None:
    assert _tool(Plan(action="child", name="worker")) == (
        "spawn",
        {
            "agent_name": "subagent",
            "input": "[gate:provider=worker] [gate:finish=worker]",
            "name": "Gate worker",
        },
    )


async def test_gate_waits_until_open_and_records_the_pass() -> None:
    gates = GateState()
    await gates.set_open("tool", False)
    waiting = asyncio.create_task(gates.wait("tool"))
    await asyncio.sleep(0)

    assert (await gates.snapshot())["gates"]["tool"] == {"open": False, "waiters": 1, "passes": 0}

    await gates.set_open("tool", True)
    await waiting

    assert (await gates.snapshot())["gates"]["tool"] == {"open": True, "waiters": 0, "passes": 1}


async def test_reset_releases_waiters_and_clears_evidence() -> None:
    gates = GateState()
    await gates.set_open("provider", False)
    waiting = asyncio.create_task(gates.wait("provider"))
    await asyncio.sleep(0)
    request_id = await gates.start_request(source="172.18.0.4", model="e2e-model", plan=Plan())

    await gates.reset()
    await waiting

    assert request_id == 1
    assert await gates.snapshot() == {
        "gates": {},
        "requests": [],
        "command_reply": {"armed": False, "drops": []},
    }


async def test_command_reply_drop_is_consumed_once_and_records_both_ids() -> None:
    gates = GateState()
    evidence = {"request_id": "transport", "command_id": "command", "operation": "send"}

    await gates.arm_send_reply_drop()

    assert await gates.consume_send_reply_drop(evidence)
    assert not await gates.consume_send_reply_drop(evidence)
    assert (await gates.snapshot())["command_reply"] == {"armed": False, "drops": [evidence]}
