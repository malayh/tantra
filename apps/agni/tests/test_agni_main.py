from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from support import ScriptedIo

from agni.config import load_config
from agni.main import serve


def _write_skill(root: Path, name: str, description: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / name).mkdir()
    (root / name / "SKILL.md").write_text(f"---\nname: {name}\ndescription: {description}\n---\n\nbody\n")


@pytest.fixture
def sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "user"
    project = tmp_path / "project"
    project.mkdir()
    _write_skill(home / ".agents" / "skills", "review", "the shared review checklist")
    _write_skill(project / "skills", "release", "how this project cuts a release")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    monkeypatch.chdir(project)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("AGNI_MODELS", "fast/model,slow/model")
    monkeypatch.setenv("AGNI_HOME", str(tmp_path / "agni"))
    return tmp_path


async def test_serve_opens_the_store_the_memory_root_and_both_skill_roots(sandbox: Path) -> None:
    config = load_config()
    io = ScriptedIo(["/skills", "/exit"])

    assert await serve(config, auto=False, resume=None, io=io.io) == 0

    assert config.database.exists()
    assert config.memory_root.is_dir()
    assert "the shared review checklist" in io.text
    assert "how this project cuts a release" in io.text


async def test_serve_warns_on_stderr_about_a_skill_it_cannot_parse(
    sandbox: Path, capsys: pytest.CaptureFixture
) -> None:
    broken = sandbox / "project" / "skills" / "half-written"
    broken.mkdir()
    (broken / "SKILL.md").write_text("---\nname: half-written\n---\n\nno description\n")
    config = load_config()
    io = ScriptedIo(["/exit"])

    assert await serve(config, auto=False, resume=None, io=io.io) == 0

    warned = capsys.readouterr().err
    assert f"agni: skipping skill {broken / 'SKILL.md'}" in warned
    assert "no 'description'" in warned


def test_the_install_script_is_valid_shell_and_names_both_published_assets() -> None:
    script = Path(__file__).resolve().parents[3] / "install.sh"

    checked = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)

    assert checked.returncode == 0, checked.stderr
    body = script.read_text()
    assert "agni-linux-x86_64" in body
    assert "agni-darwin-arm64" in body
    assert body.startswith("#!/usr/bin/env bash")


async def test_serve_refuses_to_resume_a_session_it_cannot_find(sandbox: Path) -> None:
    config = load_config()
    io = ScriptedIo()

    assert await serve(config, auto=False, resume="nosuchsession", io=io.io) == 1
    assert io.prompts == []
