import pytest

from sarathi.agent import SKILLS_DIR, Sarathi, Subagent, _wire_tools
from sarathi.config import get_settings
from tantra import FakeProvider, FileSystemSkills, MemoryStore, Runtime, SessionHeader


def test_subagent_and_delegation_contract() -> None:
    assert Sarathi.subagents == [Subagent]
    assert Subagent.subagents == []
    assert Sarathi.skills == Subagent.skills == ["research"]
    assert "Work inline by default" in Sarathi.prompt
    assert "independent or parallel work would materially help" in Sarathi.prompt
    assert "omitting it only when normal is intended" in Sarathi.prompt
    assert "spawn('subagent', task, name=...)" in Sarathi.prompt
    assert "Never ask a human" in Subagent.prompt
    assert "finish(result) exactly once" in Subagent.prompt


def test_framework_tool_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "brave-key")
    get_settings.cache_clear()
    _wire_tools.cache_clear()
    _wire_tools()
    runtime = Runtime(
        FakeProvider([]),
        MemoryStore(),
        [Sarathi],
        default_model="test",
        skills=FileSystemSkills(SKILLS_DIR),
    )
    root = SessionHeader(id="1" * 32, root_id="1" * 32, agent="sarathi")
    child = SessionHeader(
        id="2" * 32,
        root_id=root.id,
        parent_id=root.id,
        agent="subagent",
        depth=1,
    )

    assert set(runtime._framework_tools(root, Sarathi)) == {
        "web_search",
        "web_fetch",
        "read_doc",
        "memory_recall",
        "memory_write",
        "skill",
        "spawn",
        "send",
        "status",
    }
    assert set(runtime._framework_tools(child, Subagent)) == {
        "web_search",
        "web_fetch",
        "read_doc",
        "memory_recall",
        "skill",
        "send",
        "finish",
    }


async def test_research_skill_is_available_to_root_and_subagent() -> None:
    catalogue = FileSystemSkills(SKILLS_DIR)
    assert [(entry.name, entry.description) for entry in await catalogue.index()] == [
        (
            "research",
            "Load for current, sourced, or comparative investigation that needs web evidence and cross-checking.",
        )
    ]
    skill = await catalogue.load("research")
    assert all(level in skill.body for level in ("`shallow`", "`normal`", "`deep`"))
    assert "If the task omits a level, use `normal`" in skill.body
    assert "Fetch only URLs provided by the user or discovered through search" in skill.body
    assert "Cite every URL actually consulted" in skill.body
    assert "confirmed findings from inference or uncertainty" in skill.body
    assert "limitations" in skill.body
