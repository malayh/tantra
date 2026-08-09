from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

import pytest
from support import BRIEF, MODELS, TinyProvider, chat_turn, history, make_repl, only, open_store

from agni.main import build_harness
from agni.prompts import COMPACTION_INSTRUCTION, MAX_STATUS, TITLE_PROMPT, environment_block
from tantra.events import TurnCompleted
from tantra.providers.fake import FakeProvider, Sample

LIMITS = {"read": "64000", "glob": "500", "grep": "200", "bash": "120"}


GIT_IDENTITY = ["-c", "user.email=agni@test", "-c", "user.name=agni", "-c", "commit.gpgsign=false"]


def git_repo(root: Path, branch: str = "trunk") -> Path:
    subprocess.run(["git", "init", "-b", branch, str(root)], check=True, capture_output=True)
    return root


def git_commit(root: Path, path: str, content: str) -> None:
    (root / path).write_text(content)
    subprocess.run(["git", "add", path], cwd=root, check=True, capture_output=True)
    subprocess.run([*["git", *GIT_IDENTITY], "commit", "-m", "seed"], cwd=root, check=True, capture_output=True)


async def test_the_build_system_prompt_carries_the_environment_block_and_the_working_sections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = git_repo(tmp_path / "project")
    (project / "notes.md").write_text("hello\n")
    monkeypatch.chdir(project)
    repl, _ = await make_repl(tmp_path, [Sample(text="ok"), Sample(text="Project overview")])

    await repl.handle("what is here?")

    prompt = repl.harness.provider.requests[0].system[0].text
    assert "<env>" in prompt and "</env>" in prompt
    assert f"Working directory: {project}" in prompt
    assert date.today().isoformat() in prompt
    assert "Git branch: trunk" in prompt
    assert "?? notes.md" in prompt
    assert "## Workflow" in prompt
    assert "## Safety" in prompt
    assert "## Output" in prompt
    assert "## Code" in prompt


async def test_the_explore_agent_gets_the_same_environment_block_baked_into_its_prompt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = git_repo(tmp_path / "project")
    monkeypatch.chdir(project)
    harness = build_harness(FakeProvider([]), await open_store(tmp_path), model=MODELS[0])

    prompt = harness.agent_for("explore").prompt

    assert "READ ONLY" in prompt
    assert f"Working directory: {project}" in prompt
    assert "Git branch: trunk" in prompt


async def test_unattended_mode_adds_the_no_operator_note_and_normal_mode_does_not(tmp_path: Path) -> None:
    store = await open_store(tmp_path)

    attended = build_harness(FakeProvider([]), store, model=MODELS[0]).agent_for("build").prompt
    unattended = build_harness(FakeProvider([]), store, model=MODELS[0], auto=True).agent_for("build").prompt

    assert "## Unattended session" in unattended
    assert "no human operator" in unattended
    assert "## Unattended session" not in attended


async def test_every_tool_description_says_when_not_to_use_it_and_states_its_real_limits(tmp_path: Path) -> None:
    harness = build_harness(FakeProvider([]), await open_store(tmp_path), model=MODELS[0])

    tools = harness.tools["build"]

    for name in ("read", "write", "edit", "glob", "grep", "bash"):
        description = tools[name].schema.description
        assert "do not use" in description.lower(), name
        assert description.splitlines()[1] == "", name
        assert "Usage:" in description, name
    for name, limit in LIMITS.items():
        assert limit in tools[name].schema.description, name


async def test_the_explore_delegate_tool_says_what_it_returns_and_when_not_to_reach_for_it(tmp_path: Path) -> None:
    harness = build_harness(FakeProvider([]), await open_store(tmp_path), model=MODELS[0])

    description = harness.tools["build"]["explore"].schema.description

    assert "Do not use it when you already know the file" in description
    assert "cannot write, edit or run commands" in description
    assert "paths, line numbers and short excerpts" in description


async def test_the_first_turn_titles_the_session_and_a_later_turn_leaves_the_title_alone(tmp_path: Path) -> None:
    repl, _ = await make_repl(
        tmp_path,
        [
            Sample(text="the parser lives in src/parse.py"),
            Sample(text="Parser location"),
            Sample(text="it hand-rolls the lexer"),
            Sample(text="Lexer implementation"),
        ],
    )
    provider = repl.harness.provider

    await repl.handle("where is the parser?")
    titled = await repl.harness.store.header(repl.session_id)
    await repl.handle("and the lexer?")

    assert titled.title == "Parser location"
    assert (await repl.harness.store.header(repl.session_id)).title == "Parser location"
    assert len(provider.requests) == 3
    assert [request.system[0].text == TITLE_PROMPT for request in provider.requests] == [False, True, False]
    assert "where is the parser?" in provider.requests[1].messages[0].content


async def test_a_title_drops_reasoning_spans_and_is_capped_at_fifty_characters(tmp_path: Path) -> None:
    headline = "Investigating why the release pipeline keeps timing out"
    repl, _ = await make_repl(tmp_path, [Sample(text="ok"), Sample(text=f"<think>weighing it up</think>\n{headline}")])

    await repl.handle("the release pipeline keeps timing out")

    assert (await repl.harness.store.header(repl.session_id)).title == headline[:50]


async def test_a_failing_title_sample_leaves_the_turn_intact_and_the_session_untitled(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [Sample(text="done")])

    await repl.handle("hello")

    header = await repl.harness.store.header(repl.session_id)
    assert header.title is None
    assert "done" in io.text
    assert only(await history(repl.harness.store, repl.session_id), TurnCompleted)[0].stop_reason == "completed"


async def test_the_summariser_is_asked_with_agnis_own_compaction_instruction(tmp_path: Path) -> None:
    provider = TinyProvider([Sample(text=BRIEF), Sample(text="the lexer still needs tests"), Sample(text="Lexer")])
    repl, _ = await make_repl(tmp_path, provider=provider)
    events = []
    for index in range(1, 6):
        events += chat_turn(f"t{index}", input=f"question {index}", text="w" * 100_000)
    await repl.harness.store.append(repl.session_id, events, expect_seq=1)

    await repl.handle("what is left to do?")

    assert provider.requests[0].messages[-1].content == COMPACTION_INSTRUCTION
    assert "## Critical Context" in COMPACTION_INSTRUCTION


def test_the_environment_block_names_the_branch_and_the_dirty_files(tmp_path: Path) -> None:
    project = git_repo(tmp_path / "project", "feature/parser")
    git_commit(project, "tracked.py", "x = 1\n")
    (project / "tracked.py").write_text("x = 2\n")
    (project / "app.py").write_text("y = 1\n")

    block = environment_block(project)

    assert block.startswith("<env>")
    assert block.endswith("</env>")
    assert f"Working directory: {project}" in block
    assert "Git branch: feature/parser" in block
    assert "?? app.py" in block
    assert " M tracked.py" in block
    assert "(git status truncated)" not in block


def test_the_environment_block_survives_a_directory_that_is_no_git_repository(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()

    block = environment_block(plain)

    assert "not a git repository" in block
    assert "Git branch" not in block
    assert f"Working directory: {plain}" in block


def test_a_git_status_longer_than_the_cap_is_truncated(tmp_path: Path) -> None:
    project = git_repo(tmp_path / "project")
    for index in range(80):
        (project / f"generated_module_number_{index:04d}_scratch.py").write_text("x\n")

    block = environment_block(project)

    assert "(git status truncated)" in block
    assert len(block) < MAX_STATUS + 500
