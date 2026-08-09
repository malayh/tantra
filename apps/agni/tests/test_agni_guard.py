from __future__ import annotations

from pathlib import Path

import pytest
from support import call, history, make_repl, only

from agni.guard import BashGuard, destructive
from tantra import Denial, TurnContext
from tantra.events import AskRaised, ToolCallCompleted, ToolCallRequested, TurnCompleted
from tantra.providers.fake import Sample

DESTRUCTIVE = [
    "rm -rf /",
    "rm -fr build",
    "rm -r -f node_modules",
    "rm -r build",
    "rm -R build",
    "cd /tmp && rm --recursive work",
    "cd /tmp && rm --recursive --force work",
    "sudo systemctl stop nginx",
    "dd if=/dev/zero of=/dev/sda bs=1M",
    "mkfs.ext4 /dev/sdb1",
    "chmod -R 777 /srv",
    "git push --force origin main",
    "git push -f",
    "echo boom > /dev/sda",
]

HARMLESS = [
    "ls -la",
    "rm -f stale.log",
    "rm stale.log",
    "git push origin main",
    "git commit -am 'wip' && git push",
    "chmod 644 notes.md",
    "chmod -R u+w src",
    "echo quiet > /dev/null",
    "grep -rf patterns.txt src",
    "pytest -q apps/agni/tests",
]


@pytest.mark.parametrize("command", DESTRUCTIVE)
def test_the_guard_names_a_reason_for_a_destructive_command(command: str) -> None:
    assert destructive(command) is not None


@pytest.mark.parametrize("command", HARMLESS)
def test_the_guard_lets_an_ordinary_command_through(command: str) -> None:
    assert destructive(command) is None


def _requested(name: str, args: dict[str, str]) -> ToolCallRequested:
    return ToolCallRequested(sample_id="s1", call_id="c1", name=name, args=args)


async def test_the_before_tool_hook_denies_only_destructive_bash_calls() -> None:
    guard = BashGuard()
    turn = TurnContext(session_id="s1", turn_id="t1", agent="build", depth=0, input="clean up")

    denied = await guard.before_tool(_requested("bash", {"command": "rm -rf /"}), turn)
    allowed = await guard.before_tool(_requested("bash", {"command": "ls"}), turn)
    other = await guard.before_tool(_requested("write", {"path": "a", "content": "b"}), turn)

    assert isinstance(denied, Denial)
    assert "rm -r" in denied.reason
    assert allowed is None
    assert other is None


async def test_auto_mode_denies_rm_rf_and_the_model_recovers_with_no_human_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "stale.o").write_text("junk")
    samples = [
        Sample(tool_calls=[call("bash", {"command": "rm -rf build"}, cid="c1")]),
        Sample(tool_calls=[call("bash", {"command": "ls build"}, cid="c2")]),
        Sample(text="I cannot delete it; build still holds stale.o"),
    ]
    repl, io = await make_repl(tmp_path, samples, auto=True)

    await repl.handle("delete the build directory")

    events = await history(repl.harness.store, repl.session_id)
    denied, executed = only(events, ToolCallCompleted)
    assert denied.call_id == "c1"
    assert denied.is_error
    assert denied.result.startswith("denied by hook")
    assert executed.call_id == "c2"
    assert not executed.is_error
    assert "stale.o" in executed.result
    assert (tmp_path / "build" / "stale.o").exists()
    assert only(events, TurnCompleted)[0].stop_reason == "completed"
    assert not only(events, AskRaised)
    assert io.prompts == []


async def test_interactive_mode_asks_before_running_bash_at_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    samples = [Sample(tool_calls=[call("bash", {"command": "echo hello"})]), Sample(text="done")]
    repl, io = await make_repl(tmp_path, samples, replies=["y"])

    await repl.handle("say hello from the shell")

    events = await history(repl.harness.store, repl.session_id)
    assert only(events, AskRaised)[0].request.extra == {"permission": "bash"}
    assert io.prompts == ["  allow? [y/N] "]
    assert "hello" in only(events, ToolCallCompleted)[0].result
