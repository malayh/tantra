from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from support import tool_context

from agni.tools import MAX_CHARS, bash, edit, glob, grep, read, write
from tantra import MemoryStore


@pytest.fixture
def progress() -> list[str]:
    return []


@pytest.fixture
def ctx(progress: list[str]) -> object:
    return tool_context(MemoryStore(), progress)


async def test_read_returns_the_file_text(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "notes.md").write_text("first\nsecond\n")

    assert await read.invoke({"path": "notes.md"}, ctx) == "first\nsecond\n"


async def test_read_names_the_path_it_could_not_find(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileNotFoundError, match="no such file: missing.md"):
        await read.invoke({"path": "missing.md"}, ctx)


async def test_write_creates_missing_parent_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)

    result = await write.invoke({"path": "src/pkg/mod.py", "content": "x = 1\n"}, ctx)

    assert (tmp_path / "src" / "pkg" / "mod.py").read_text() == "x = 1\n"
    assert "src/pkg/mod.py" in result


async def test_edit_replaces_the_one_matching_occurrence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "app.py"
    target.write_text("def one():\n    return 1\n")

    await edit.invoke({"path": "app.py", "old": "return 1", "new": "return 2"}, ctx)

    assert target.read_text() == "def one():\n    return 2\n"


async def test_edit_refuses_an_absent_match(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app.py").write_text("value = 1\n")

    with pytest.raises(ValueError, match="does not appear"):
        await edit.invoke({"path": "app.py", "old": "value = 9", "new": "value = 2"}, ctx)


async def test_edit_refuses_an_ambiguous_match_without_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "app.py"
    target.write_text("x = 1\nx = 1\n")

    with pytest.raises(ValueError, match="appears 2 times"):
        await edit.invoke({"path": "app.py", "old": "x = 1", "new": "x = 2"}, ctx)

    assert target.read_text() == "x = 1\nx = 1\n"


async def test_glob_lists_matches_and_skips_build_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("a")
    (tmp_path / "src" / "b.txt").write_text("b")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "hooks.py").write_text("nope")

    assert await glob.invoke({"pattern": "**/*.py"}, ctx) == ["src/a.py"]


async def test_grep_reports_the_file_line_and_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app.py").write_text("import os\n\ndef handler():\n    return os.getcwd()\n")
    (tmp_path / "notes.md").write_text("handler is defined in app.py\n")
    (tmp_path / "blob.bin").write_bytes(b"\xff\xfe\x00handler")

    matches = await grep.invoke({"pattern": r"def handler", "include": "*.py"}, ctx)

    assert matches == ["app.py:3: def handler():"]


async def test_grep_rejects_a_broken_regular_expression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="invalid regular expression"):
        await grep.invoke({"pattern": "("}, ctx)


async def test_bash_returns_combined_output_and_reports_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object, progress: list[str]
) -> None:
    monkeypatch.chdir(tmp_path)

    output = await bash.invoke({"command": "echo out; echo err 1>&2"}, ctx)

    assert "out" in output
    assert "err" in output
    assert progress == ["$ echo out; echo err 1>&2"]


async def test_bash_appends_a_non_zero_exit_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)

    output = await bash.invoke({"command": "echo nope; exit 3"}, ctx)

    assert "[exit status 3]" in output


async def test_bash_kills_a_command_that_outruns_its_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(TimeoutError, match="timed out after"):
        await bash.invoke({"command": "sleep 5", "timeout": 0.2}, ctx)


async def test_a_timeout_kills_the_children_the_command_spawned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(TimeoutError):
        await bash.invoke({"command": "(sleep 0.5; touch orphan.txt) & wait", "timeout": 0.1}, ctx)
    await asyncio.sleep(1.0)

    assert not (tmp_path / "orphan.txt").exists()


async def test_read_truncates_a_file_that_would_flood_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "huge.log").write_text("x" * (MAX_CHARS + 5_000))

    content = await read.invoke({"path": "huge.log"}, ctx)

    assert len(content) < MAX_CHARS + 200
    assert content.endswith("[truncated: 5000 chars omitted]")


async def test_bash_truncates_output_that_would_flood_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ctx: object
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "huge.log").write_text("y" * (MAX_CHARS + 1_000))

    output = await bash.invoke({"command": "cat huge.log"}, ctx)

    assert len(output) < MAX_CHARS + 200
    assert output.endswith("[truncated: 1000 chars omitted]")
