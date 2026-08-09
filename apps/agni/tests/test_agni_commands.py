from __future__ import annotations

from pathlib import Path

from support import BRIEF, MODELS, TinyProvider, chat_turn, history, make_repl, only

from agni.commands import COMMAND_NAMES, HELP
from agni.skills import MergedSkills, skill_roots
from tantra.events import CompactionApplied
from tantra.providers.fake import Sample


def write_skill(root: Path, name: str, description: str, body: str) -> None:
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text(f"---\nname: {name}\ndescription: {description}\n---\n\n{body}\n")


def write_broken_skill(root: Path, name: str) -> Path:
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text(f"---\nname: {name}\n---\n\nno description\n")
    return directory / "SKILL.md"


async def test_new_starts_a_fresh_session(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [])
    first = repl.session_id

    await repl.handle("/new")

    assert repl.session_id != first
    assert await repl.harness.store.header(repl.session_id) is not None
    assert repl.session_id[:8] in io.text


async def test_resume_lists_recent_sessions_and_re_enters_the_chosen_one(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [], replies=["2"])
    await repl.harness.create_session("build")
    stored = [header for header in await repl.harness.store.list(limit=15) if header.parent_id is None]

    await repl.handle("/resume")

    assert len(stored) == 2
    assert repl.session_id == stored[1].id
    assert stored[0].id[:8] in io.text
    assert stored[1].id[:8] in io.text
    assert "build" in io.text


async def test_resume_shows_the_title_of_a_session_that_has_one(tmp_path: Path) -> None:
    repl, io = await make_repl(
        tmp_path,
        [Sample(text="the parser lives in src/parse.py"), Sample(text="Parser location")],
        replies=["1"],
    )

    await repl.handle("where is the parser?")
    await repl.handle("/resume")

    assert "Parser location" in io.text


async def test_resume_ignores_a_choice_that_is_not_on_the_list(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [], replies=["9"])
    first = repl.session_id

    await repl.handle("/resume")

    assert repl.session_id == first
    assert "nothing picked" in io.text


async def test_model_switches_the_model_used_by_the_next_turn(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [Sample(text="ok")], replies=["2"])

    await repl.handle("/model")
    await repl.handle("hello")

    assert repl.harness.default_model == MODELS[1]
    assert repl.harness.provider.requests[-1].model == MODELS[1]
    assert f"model is now {MODELS[1]}" in io.text


async def test_skills_lists_the_merged_catalogue_with_the_project_shadowing_the_shared_root(
    tmp_path: Path,
) -> None:
    shared = tmp_path / "shared"
    project = tmp_path / "project"
    write_skill(shared, "review", "the shared review checklist", "shared body")
    write_skill(shared, "release", "how releases are cut", "release body")
    write_skill(project, "review", "this project's review checklist", "project body")
    skills = MergedSkills(skill_roots(shared, project, tmp_path / "absent"))
    repl, io = await make_repl(tmp_path, [], skills=skills)

    await repl.handle("/skills")

    assert "this project's review checklist" in io.text
    assert "the shared review checklist" not in io.text
    assert "how releases are cut" in io.text
    assert (await skills.load("review")).body == "project body"


async def test_skills_names_the_files_it_had_to_skip(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    project = tmp_path / "project"
    write_skill(shared, "review", "the shared review checklist", "shared body")
    broken = write_broken_skill(project, "half-written")
    repl, io = await make_repl(tmp_path, [], skills=MergedSkills(skill_roots(shared, project)))

    await repl.handle("/skills")

    assert "review: the shared review checklist" in io.text
    assert f"skipped {broken}" in io.text
    assert "no 'description'" in io.text


async def test_merged_skills_aggregate_the_skipped_files_of_every_root(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    project = tmp_path / "project"
    write_skill(shared, "review", "the shared review checklist", "shared body")
    first = write_broken_skill(shared, "bad-shared")
    second = write_broken_skill(project, "bad-project")
    skills = MergedSkills(skill_roots(shared, project))

    listing = await skills.index()

    assert [info.name for info in listing] == ["review"]
    assert [path for path, _ in skills.skipped] == [first, second]


async def test_compact_summarises_a_session_that_outgrew_the_window(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, provider=TinyProvider([Sample(text=BRIEF)]))
    store = repl.harness.store
    events = []
    for index in range(1, 6):
        events += chat_turn(f"t{index}", input=f"question {index}", text="w" * 100_000)
    await store.append(repl.session_id, events, expect_seq=1)

    await repl.handle("/compact")

    applied = only(await history(store, repl.session_id), CompactionApplied)
    assert len(applied) == 1
    assert applied[0].summary == BRIEF
    assert applied[0].floor_turn_id == "t4"
    assert applied[0].tokens_after < applied[0].tokens_before
    assert "compacted context" in io.text
    assert (await store.header(repl.session_id)).lease is None


async def test_compact_reports_a_session_another_writer_holds_and_appends_nothing(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, provider=TinyProvider([Sample(text=BRIEF)]))
    store = repl.harness.store
    events = []
    for index in range(1, 6):
        events += chat_turn(f"t{index}", input=f"question {index}", text="w" * 100_000)
    await store.append(repl.session_id, events, expect_seq=1)
    assert await store.acquire_lease(repl.session_id, "another-agni", 60.0)
    before = (await store.header(repl.session_id)).last_seq

    await repl.handle("/compact")

    assert "busy" in io.text
    assert repl.running
    assert not only(await history(store, repl.session_id), CompactionApplied)
    assert (await store.header(repl.session_id)).last_seq == before


async def test_compact_leaves_a_session_that_still_fits_alone(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [Sample(text="ok")])

    await repl.handle("hello")
    await repl.handle("/compact")

    assert not only(await history(repl.harness.store, repl.session_id), CompactionApplied)
    assert "nothing to compact" in io.text


async def test_skills_says_so_when_no_skills_directory_is_configured(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [])

    await repl.handle("/skills")

    assert "no skills directory" in io.text


async def test_help_lists_every_command_and_exit_stops_the_loop(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [])

    await repl.handle("/help")
    await repl.handle("/exit")

    assert all(name in io.text for name in COMMAND_NAMES)
    assert not repl.running


async def test_a_command_argument_answers_the_prompt_without_asking(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [])

    await repl.handle("/model 2")

    assert repl.harness.default_model == MODELS[1]
    assert io.prompts == []


def test_the_help_table_lists_exactly_the_commands_that_dispatch() -> None:
    assert tuple(name for name, _ in HELP) == COMMAND_NAMES
