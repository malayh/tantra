import pytest

from sarathi.agent import SKILLS_DIR, Sarathi, Subagent, _wire_tools
from sarathi.config import get_settings
from tantra import Context, FakeProvider, FileSystemSkills, MemoryStore, Runtime, SessionHeader, Skill, SkillInfo


def test_subagent_and_delegation_contract() -> None:
    assert Sarathi.subagents == [Subagent]
    assert Subagent.subagents == []
    assert Sarathi.skills is Subagent.skills is None
    assert "You are Sarathi, a helpful AI assistant in a chat app" in Sarathi.prompt
    assert "Work inline by default" in Sarathi.prompt
    assert "Delegate only when the user explicitly requests a subagent" in Sarathi.prompt
    assert "independent or parallel work would materially help" in Sarathi.prompt
    assert "When delegating, optionally use a short, descriptive display name" in Sarathi.prompt
    assert "attached PDF or Word file" in Sarathi.prompt
    assert "research" not in Sarathi.prompt.lower()
    assert "memory_write" not in Sarathi.prompt
    assert "memory_recall" not in Sarathi.prompt
    assert "spawn" not in Sarathi.prompt.lower()
    assert "finished]" not in Sarathi.prompt
    assert "synthesize" not in Sarathi.prompt.lower()
    assert "Never ask a human" in Subagent.prompt
    assert "Complete the assigned task" in Subagent.prompt
    assert "Deliver one final result to your parent" in Subagent.prompt
    assert "blocked or incomplete result" in Subagent.prompt
    assert "skill" not in Subagent.prompt.lower()
    assert "finish(" not in Subagent.prompt
    assert "research" not in Subagent.prompt.lower()
    assert "turn-ended" not in Subagent.prompt.lower()


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

    root_tools = runtime._framework_tools(root, Sarathi)
    child_tools = runtime._framework_tools(child, Subagent)

    assert set(root_tools) == {
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
    assert root_tools["spawn"].schema.description.endswith("Available agent types: subagent.")
    assert root_tools["spawn"].schema.parameters["properties"]["agent_name"]["enum"] == ["subagent"]
    assert "delivered later as a new parent input" in root_tools["spawn"].schema.description
    assert set(child_tools) == {
        "web_search",
        "web_fetch",
        "read_doc",
        "memory_recall",
        "skill",
        "send",
        "finish",
    }
    assert child_tools["finish"].schema.description == (
        "Permanently close this child agent and deliver its result to its parent."
    )


async def test_both_actors_inherit_the_full_skill_catalogue() -> None:
    class Catalogue:
        def __init__(self) -> None:
            self.loaded: list[str] = []

        async def index(self) -> list[SkillInfo]:
            return [
                SkillInfo(name="research", description="Investigate with sources."),
                SkillInfo(name="review", description="Review work independently."),
            ]

        async def load(self, name: str) -> Skill:
            self.loaded.append(name)
            return Skill(name=name, description=f"{name} skill", body=f"{name} instructions")

    async def emit(_: str) -> None:
        return None

    catalogue = Catalogue()
    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Sarathi], default_model="test", skills=catalogue)
    root = SessionHeader(id="1" * 32, root_id="1" * 32, agent="sarathi")
    child = SessionHeader(
        id="2" * 32,
        root_id=root.id,
        parent_id=root.id,
        agent="subagent",
        depth=1,
    )
    ctx = Context(
        session_id=root.id,
        turn_id="turn",
        call_id="call",
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )

    assert [entry.name for entry in await runtime._skill_index(Sarathi)] == ["research", "review"]
    assert [entry.name for entry in await runtime._skill_index(Subagent)] == ["research", "review"]
    assert await runtime._framework_tools(root, Sarathi)["skill"].invoke({"name": "review"}, ctx) == (
        "review instructions"
    )
    assert await runtime._framework_tools(child, Subagent)["skill"].invoke({"name": "review"}, ctx) == (
        "review instructions"
    )
    assert catalogue.loaded == ["review", "review"]
    await runtime.aclose()


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
    assert "When delegating research" in skill.body
    assert "include the requested `shallow`, `normal`, or `deep` level in the child task" in skill.body
    assert "Omit it only when `normal` is intended" in skill.body
    assert "Fetch only URLs" not in skill.body
    assert "Cite every URL actually consulted" in skill.body
    assert "confirmed findings from inference or uncertainty" in skill.body
    assert "limitations" in skill.body
