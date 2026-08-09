from __future__ import annotations

from pathlib import Path

import pytest
from support import BRIEF, MODELS, ScriptedIo, TinyProvider, call, chat_turn, history, make_repl, only, open_store

from agni.main import build_harness
from agni.repl import CYAN, Repl
from tantra import Agent, Context, Emitted, FreeText, Harness, Hook, SessionEvent, tool
from tantra.events import (
    AskRaised,
    ChildSessionSpawned,
    CompactionApplied,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
    TurnStarted,
)
from tantra.providers.fake import FakeProvider, Sample


async def test_a_scripted_turn_streams_its_text_into_the_output(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [Sample(text="the parser lives in src/parse.py")])

    await repl.handle("where does parsing happen?")

    assert "the parser lives in src/parse.py" in io.text


async def test_a_turn_over_a_full_context_window_compacts_and_still_finishes(tmp_path: Path) -> None:
    repl, io = await make_repl(
        tmp_path,
        provider=TinyProvider([Sample(text=BRIEF), Sample(text="the lexer still needs tests")]),
    )
    events: list[SessionEvent] = []
    for index in range(1, 6):
        events += chat_turn(f"t{index}", input=f"question {index}", text="w" * 100_000)
    await repl.harness.store.append(repl.session_id, events, expect_seq=1)

    await repl.handle("what is left to do?")

    logged = await history(repl.harness.store, repl.session_id)
    assert len(only(logged, CompactionApplied)) == 1
    assert only(logged, TurnCompleted)[-1].stop_reason == "completed"
    assert "compacted context" in io.text
    assert "the lexer still needs tests" in io.text


async def test_a_blank_line_and_an_unknown_command_never_start_a_turn(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [])

    await repl.handle("   ")
    await repl.handle("/nope")

    assert not only(await history(repl.harness.store, repl.session_id), TurnStarted)
    assert "unknown command /nope" in io.text


async def test_approving_a_write_prompts_once_and_writes_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    samples = [
        Sample(tool_calls=[call("write", {"path": "notes.md", "content": "hi\n"})]),
        Sample(text="written"),
    ]
    repl, io = await make_repl(tmp_path, samples, replies=["y"])

    await repl.handle("write notes.md")

    assert (tmp_path / "notes.md").read_text() == "hi\n"
    assert io.prompts == ["  allow? [y/N] "]
    assert "Run write?" in io.text
    events = await history(repl.harness.store, repl.session_id)
    assert only(events, TurnCompleted)[0].stop_reason == "completed"


async def test_declining_a_write_leaves_the_file_alone_and_tells_the_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    samples = [
        Sample(tool_calls=[call("write", {"path": "notes.md", "content": "hi\n"})]),
        Sample(text="left it alone"),
    ]
    repl, io = await make_repl(tmp_path, samples, replies=["n"])

    await repl.handle("write notes.md")

    assert not (tmp_path / "notes.md").exists()
    events = await history(repl.harness.store, repl.session_id)
    completed = only(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result == "denied by user"
    assert "left it alone" in io.text


async def test_build_delegates_to_explore_and_the_child_events_show_on_the_parent_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "parse.py").write_text("def lex():\n    return []\n")
    samples = [
        Sample(tool_calls=[call("explore", {"task": "where is the lexer?"})]),
        Sample(tool_calls=[call("grep", {"pattern": "def lex", "include": "*.py"}, cid="c2")]),
        Sample(text="parse.py line 1"),
        Sample(text="the lexer is in parse.py"),
    ]
    repl, io = await make_repl(tmp_path, samples)

    await repl.handle("find the lexer")

    events = await history(repl.harness.store, repl.session_id)
    spawned = only(events, ChildSessionSpawned)[0]
    assert spawned.agent == "explore"
    assert [event.name for event in only(events, ToolCallRequested)] == ["explore"]
    assert only(events, ToolCallCompleted)[0].result == "parse.py line 1"

    child = await history(repl.harness.store, spawned.child_session_id)
    assert [event.name for event in only(child, ToolCallRequested)] == ["grep"]
    assert f"  {CYAN}⏺ grep" in io.text
    assert io.prompts == []


async def test_a_free_text_ask_round_trips_the_answer_back_into_the_tool(tmp_path: Path) -> None:
    @tool
    async def consult(question: str, ctx: Context) -> str:
        """Ask the human a question."""
        reply = await ctx.ask(FreeText(prompt=question))
        return f"they said {reply.text}"

    class Asker(Agent):
        tools = [consult]

    store = await open_store(tmp_path)
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("consult", {"question": "tabs or spaces?"})]), Sample(text="noted")]),
        store,
        [Asker],
        default_model=MODELS[0],
    )
    session_id = (await harness.create_session(Asker)).id
    io = ScriptedIo(["tabs"])
    repl = Repl(harness, session_id, MODELS, io.io)

    await repl.handle("ask me about indentation")

    events = await history(store, session_id)
    assert only(events, AskRaised)[0].request.prompt == "tabs or spaces?"
    assert only(events, ToolCallCompleted)[0].result == "they said tabs"
    assert "tabs or spaces?" in io.text


async def test_an_abandoned_turn_resumes_on_the_next_message_instead_of_restarting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app.py").write_text("print('hi')\n")
    store = await open_store(tmp_path)
    interrupted = build_harness(
        FakeProvider([Sample(tool_calls=[call("read", {"path": "app.py"})])]),
        store,
        model=MODELS[0],
    )
    session_id = (await interrupted.create_session("build")).id

    stream = interrupted.run(session_id, "what does app.py do?")
    async for emitted in stream:
        if isinstance(emitted.event, ToolCallRequested):
            break
    await stream.aclose()

    events = await history(store, session_id)
    assert only(events, TurnStarted)
    assert not only(events, TurnCompleted)

    fresh = build_harness(
        FakeProvider([Sample(text="it prints hi"), Sample(text="nothing else")]),
        store,
        model=MODELS[0],
    )
    io = ScriptedIo()
    repl = Repl(fresh, session_id, MODELS, io.io)

    await repl.handle("go on then")

    events = await history(store, session_id)
    assert repl.session_id == session_id
    assert len(only(events, TurnStarted)) == 2
    assert [event.name for event in only(events, ToolCallRequested)] == ["read"]
    assert only(events, ToolCallCompleted)[0].result == "print('hi')\n"
    assert [event.stop_reason for event in only(events, TurnCompleted)] == ["completed", "completed"]
    assert "resuming the interrupted turn" in io.text


RAN: list[str] = []


@tool
async def survey() -> str:
    """Look around."""
    RAN.append("survey")
    return "two files"


class Surveyor(Agent):
    tools = [survey]


class Interrupter(Hook):
    def __init__(self) -> None:
        self.repl: Repl | None = None

    async def on_event(self, emitted: Emitted) -> None:
        if isinstance(emitted.event, ToolCallRequested) and self.repl is not None:
            self.repl.interrupt()


async def interrupted_repl(tmp_path: Path, texts: list[str]) -> tuple[Repl, ScriptedIo, Interrupter]:
    RAN.clear()
    store = await open_store(tmp_path)
    hook = Interrupter()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("survey", {})]), *(Sample(text=text) for text in texts)]),
        store,
        [Surveyor],
        default_model=MODELS[0],
        hooks=[hook],
    )
    session_id = (await harness.create_session(Surveyor)).id
    io = ScriptedIo(["1"])
    repl = Repl(harness, session_id, MODELS, io.io)
    hook.repl = repl
    await repl.handle("survey the repo")
    return repl, io, hook


async def test_an_interrupt_mid_turn_returns_to_the_prompt_and_the_next_message_resumes(tmp_path: Path) -> None:
    repl, io, hook = await interrupted_repl(tmp_path, ["two files, both tiny", "carrying on"])
    store = repl.harness.store

    assert "interrupted" in io.text
    assert repl.running
    assert RAN == []
    assert not only(await history(store, repl.session_id), TurnCompleted)

    hook.repl = None
    await repl.handle("carry on")

    events = await history(store, repl.session_id)
    assert RAN == ["survey"]
    assert len(only(events, ToolCallRequested)) == 1
    assert len(only(events, TurnStarted)) == 2
    assert "resuming the interrupted turn" in io.text
    assert [event.stop_reason for event in only(events, TurnCompleted)] == ["completed", "completed"]


async def test_resume_after_an_interrupt_finishes_the_turn_instead_of_stopping_on_a_stale_flag(
    tmp_path: Path,
) -> None:
    repl, io, hook = await interrupted_repl(tmp_path, ["two files, both tiny"])
    store = repl.harness.store
    hook.repl = None

    await repl.handle("/resume")

    events = await history(store, repl.session_id)
    assert RAN == ["survey"]
    assert len(only(events, TurnStarted)) == 1
    assert only(events, ToolCallCompleted)[0].result == "two files"
    assert [event.stop_reason for event in only(events, TurnCompleted)] == ["completed"]
    assert "two files, both tiny" in io.text


async def test_a_session_held_by_another_writer_is_reported_and_the_repl_survives(tmp_path: Path) -> None:
    repl, io = await make_repl(tmp_path, [Sample(text="never sampled")])
    assert await repl.harness.store.acquire_lease(repl.session_id, "another-agni", 60.0)

    await repl.handle("hello")

    assert "busy" in io.text
    assert repl.running
    assert not only(await history(repl.harness.store, repl.session_id), TurnStarted)
