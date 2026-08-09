from __future__ import annotations

import json
from pathlib import Path

import pytest
from support import call, history, make_repl, only

from agni.config import load_config
from tantra import BuiltinMemory, FileSystemStore
from tantra.events import ToolCallCompleted
from tantra.providers.fake import Sample

REMEMBER = {
    "kind": "preference",
    "title": "tabs over spaces",
    "body": "the user indents this project with tabs",
    "entities": ["indentation"],
}


async def memory_for(root: Path) -> BuiltinMemory:
    store = FileSystemStore(root)
    await store.setup()
    return BuiltinMemory(store)


async def test_a_written_memory_lands_as_a_json_file_that_a_later_session_recalls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("AGNI_MODELS", "fast/model")
    monkeypatch.setenv("AGNI_HOME", str(tmp_path / "home"))
    config = load_config()

    writing, _ = await make_repl(
        config.home,
        [Sample(tool_calls=[call("memory_write", REMEMBER)]), Sample(text="noted")],
        memory=await memory_for(config.memory_root),
    )
    await writing.handle("remember that I indent with tabs")

    files = list((config.memory_root / "_memory").glob("*.json"))
    assert len(files) == 1
    stored = json.loads(files[0].read_text())
    assert stored["title"] == "tabs over spaces"
    assert stored["kind"] == "preference"

    recalling, _ = await make_repl(
        config.home,
        [
            Sample(tool_calls=[call("memory_recall", {"query": "how does the user indent this project"})]),
            Sample(text="you indent with tabs"),
        ],
        memory=await memory_for(config.memory_root),
    )
    assert recalling.session_id != writing.session_id

    await recalling.handle("how do I indent here?")

    hits = only(await history(recalling.harness.store, recalling.session_id), ToolCallCompleted)[0].result
    assert [hit["title"] for hit in hits] == ["tabs over spaces"]
    assert hits[0]["mode"] == "keyword"


async def test_recall_finds_nothing_before_anything_is_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("AGNI_MODELS", "fast/model")
    monkeypatch.setenv("AGNI_HOME", str(tmp_path / "home"))
    config = load_config()

    repl, _ = await make_repl(
        config.home,
        [Sample(tool_calls=[call("memory_recall", {"query": "indentation"})]), Sample(text="nothing saved yet")],
        memory=await memory_for(config.memory_root),
    )

    await repl.handle("what do you know about indentation?")

    assert only(await history(repl.harness.store, repl.session_id), ToolCallCompleted)[0].result == []
