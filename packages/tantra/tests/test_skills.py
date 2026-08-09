from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import ApprovalResponse
from tantra.context import SKILLS_PREAMBLE
from tantra.errors import TantraError
from tantra.events import AskAnswered, AskRaised, ChildSessionSpawned, ToolCallCompleted, TurnCompleted
from tantra.harness import Harness
from tantra.providers.base import SampleRequest, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.skills import FileSystemSkills, Skill, SkillInfo
from tantra.stores.memory import MemoryStore
from tantra.tools import tool

COLD_EMAIL = """---
name: cold-email
description: Write cold outbound emails that earn a reply.
---

# Cold email

Open on the reader's problem, never on your product. The first line must be about them.

Keep the whole thing under 120 words with exactly one ask. Attachments get you filtered.
"""

OUTREACH = """---
name: outreach
description: Build and sequence an outbound prospect list.
---

# Outreach

Segment before you send. A list of 40 people who share a problem beats 4000 who share a job title.

Three touches over eight days, then stop and move on.
"""

SEO = """---
name: seo
description: Audit a site for technical and on-page SEO problems.
---

# SEO

Crawl first, opine second. Indexation and Core Web Vitals outrank keyword density every time.

Fix templates, not pages — one broken template is a thousand broken pages.
"""


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    skills = tmp_path / "skills"
    write(skills / "cold-email" / "SKILL.md", COLD_EMAIL)
    write(skills / "cold-email" / "references" / "frameworks.md", "PAS. AIDA. BAB.\n")
    write(skills / "outreach" / "SKILL.md", OUTREACH)
    write(skills / "seo" / "SKILL.md", SEO)
    return skills


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


def index_block(request: SampleRequest) -> str:
    return request.system[-1].text


def listed(request: SampleRequest) -> list[str]:
    return [line for line in index_block(request).splitlines() if line.startswith("- ")]


def approx_tokens(request: SampleRequest) -> int:
    chunks = [block.text for block in request.system]
    chunks += [json.dumps(message.model_dump(), default=str) for message in request.messages]
    chunks += [json.dumps(schema.model_dump(), default=str) for schema in request.tools]
    return sum(len(chunk) for chunk in chunks) // 4


class RecordingSkills:
    def __init__(self, root: Path) -> None:
        self.inner = FileSystemSkills(root)
        self.loaded: list[str] = []

    async def index(self) -> list[SkillInfo]:
        return await self.inner.index()

    async def load(self, name: str) -> Skill:
        self.loaded.append(name)
        return await self.inner.load(name)


class Writer(Agent):
    prompt = "You are a writer."


async def build(samples: list[Sample], agent: type[Agent] = Writer, **kwargs: Any) -> tuple[Harness, MemoryStore, str]:
    store = MemoryStore()
    harness = Harness(FakeProvider(samples), store, [agent], default_model="fake/model", **kwargs)
    header = await harness.create_session(agent)
    return harness, store, header.id


async def test_the_index_reports_every_skill_directory(root: Path) -> None:
    (root / "not-a-skill").mkdir()
    (root / "not-a-skill" / "notes.md").write_text("no frontmatter here")

    listing = await FileSystemSkills(root).index()

    assert [(info.name, info.description) for info in listing] == [
        ("cold-email", "Write cold outbound emails that earn a reply."),
        ("outreach", "Build and sequence an outbound prospect list."),
        ("seo", "Audit a site for technical and on-page SEO problems."),
    ]


async def test_load_returns_the_body_without_the_frontmatter_and_lists_the_files(root: Path) -> None:
    loaded = await FileSystemSkills(root).load("cold-email")

    assert loaded.body.startswith("# Cold email")
    assert "Attachments get you filtered." in loaded.body
    assert "---" not in loaded.body
    assert "description:" not in loaded.body
    assert loaded.files == ("references/frameworks.md",)
    assert (await FileSystemSkills(root).load("seo")).files == ()


async def test_quoted_values_crlf_and_nested_keys_all_parse(tmp_path: Path) -> None:
    body = (
        "---\r\n"
        'name: "cold-email"\r\n'
        "description: 'Write cold emails.'\r\n"
        "metadata:\r\n"
        "  name: nested-and-ignored\r\n"
        "  description: also ignored\r\n"
        "---\r\n"
        "\r\n"
        "Body text.\r\n"
    )
    write(tmp_path / "skills" / "cold-email" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("cold-email")

    assert loaded.name == "cold-email"
    assert loaded.description == "Write cold emails."
    assert loaded.body == "Body text."


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("---\ndescription: no name here\n---\n\nBody.\n", "no 'name'"),
        ("---\nname: nameless\n---\n\nBody.\n", "no 'description'"),
        ("---\nname: unclosed\ndescription: never fenced\n\nBody.\n", "never closed"),
        ("name: bare\ndescription: no fence at all\n\nBody.\n", "frontmatter fence"),
        ("---\nname: |\ndescription: an empty block scalar\n---\n\nBody.\n", "no 'name'"),
    ],
)
async def test_malformed_frontmatter_is_skipped_naming_the_file(tmp_path: Path, text: str, message: str) -> None:
    write(tmp_path / "skills" / "broken" / "SKILL.md", text)
    skills = FileSystemSkills(tmp_path / "skills")

    assert await skills.index() == []

    [(path, reason)] = skills.skipped
    assert path == tmp_path / "skills" / "broken" / "SKILL.md"
    assert message in reason


async def test_a_folded_block_scalar_description_becomes_one_spaced_line(tmp_path: Path) -> None:
    body = "---\nname: folded\ndescription: >\n  wrapped across\n  three indented\n  lines\n---\n\nBody text.\n"
    write(tmp_path / "skills" / "folded" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("folded")

    assert loaded.description == "wrapped across three indented lines"
    assert loaded.body == "Body text."


async def test_a_literal_block_scalar_description_keeps_its_newlines(tmp_path: Path) -> None:
    body = "---\nname: literal\ndescription: |\n  first line\n  second line\n---\n\nBody text.\n"
    write(tmp_path / "skills" / "literal" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("literal")

    assert loaded.description == "first line\nsecond line"


async def test_an_indented_fence_inside_a_literal_block_stays_in_the_description(tmp_path: Path) -> None:
    body = "---\nname: fenced\ndescription: |\n  first line\n  ---\n  third line\n---\n\nBody text.\n"
    write(tmp_path / "skills" / "fenced" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("fenced")

    assert loaded.description == "first line\n---\nthird line"
    assert loaded.body == "Body text."


@pytest.mark.parametrize("marker", [">", ">-", ">+", "|", "|-", "|+"])
async def test_every_chomping_variant_parses(tmp_path: Path, marker: str) -> None:
    body = f"---\nname: chomped\ndescription: {marker}\n  one\n---\n\nBody text.\n"
    write(tmp_path / "skills" / "chomped" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("chomped")

    assert loaded.description == "one"


async def test_a_caveman_shaped_skill_folds_its_multi_line_description(tmp_path: Path) -> None:
    body = (
        "---\n"
        "name: caveman\n"
        "description: >\n"
        "  Ultra-compressed communication mode. Cuts token usage ~75% by speaking\n"
        "  like caveman while keeping full technical accuracy. Supports intensity\n"
        "  levels: lite, full (default), ultra.\n"
        "---\n"
        "\n"
        "# Caveman\n"
    )
    write(tmp_path / "skills" / "caveman" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("caveman")

    assert loaded.name == "caveman"
    assert loaded.description == (
        "Ultra-compressed communication mode. Cuts token usage ~75% by speaking like caveman while keeping full "
        "technical accuracy. Supports intensity levels: lite, full (default), ultra."
    )
    assert loaded.body == "# Caveman"


async def test_a_nested_mapping_beside_a_block_scalar_is_still_ignored(tmp_path: Path) -> None:
    body = (
        "---\n"
        "name: nested\n"
        "metadata:\n"
        "  version: 2.0.0\n"
        "  name: nested-and-ignored\n"
        "description: >\n"
        "  folded after the mapping\n"
        "---\n"
        "\n"
        "Body text.\n"
    )
    write(tmp_path / "skills" / "nested" / "SKILL.md", body)

    loaded = await FileSystemSkills(tmp_path / "skills").load("nested")

    assert loaded.name == "nested"
    assert loaded.description == "folded after the mapping"


async def test_a_utf8_bom_before_the_fence_is_tolerated(tmp_path: Path) -> None:
    write(tmp_path / "skills" / "seo" / "SKILL.md", "﻿" + SEO)

    loaded = await FileSystemSkills(tmp_path / "skills").load("seo")

    assert loaded.name == "seo"
    assert loaded.description == "Audit a site for technical and on-page SEO problems."
    assert loaded.body.startswith("# SEO")


async def test_two_directories_declaring_one_name_keep_the_first_and_skip_the_second(root: Path) -> None:
    write(root / "cold-email-v2" / "SKILL.md", COLD_EMAIL)
    skills = FileSystemSkills(root)

    listing = await skills.index()

    assert [info.name for info in listing] == ["cold-email", "outreach", "seo"]
    [(path, reason)] = skills.skipped
    assert path == root / "cold-email-v2" / "SKILL.md"
    assert "duplicate skill name 'cold-email'" in reason
    assert str(root / "cold-email" / "SKILL.md") in reason


async def test_a_broken_skill_directory_is_skipped_and_its_siblings_still_index(root: Path) -> None:
    write(root / "half-written" / "SKILL.md", "---\nname: half-written\n---\n\nBody.\n")
    write(root / "unfenced" / "SKILL.md", "---\nname: unfenced\ndescription: never fenced\n\nBody.\n")
    skills = FileSystemSkills(root)

    listing = await skills.index()

    assert [info.name for info in listing] == ["cold-email", "outreach", "seo"]
    assert [path for path, _ in skills.skipped] == [
        root / "half-written" / "SKILL.md",
        root / "unfenced" / "SKILL.md",
    ]
    assert "no 'description'" in skills.skipped[0][1]
    assert "never closed" in skills.skipped[1][1]
    assert (await skills.load("seo")).name == "seo"


async def test_a_skill_file_that_is_not_utf8_is_skipped_not_raised(root: Path) -> None:
    (root / "binary").mkdir()
    (root / "binary" / "SKILL.md").write_bytes(b"---\nname: binary\ndescription: caf\xe9\n---\n\nBody.\n")
    skills = FileSystemSkills(root)

    listing = await skills.index()

    assert [info.name for info in listing] == ["cold-email", "outreach", "seo"]
    [(path, reason)] = skills.skipped
    assert path == root / "binary" / "SKILL.md"
    assert "unreadable skill file" in reason


async def test_rescanning_refreshes_the_skipped_list_instead_of_growing_it(root: Path) -> None:
    write(root / "half-written" / "SKILL.md", "---\nname: half-written\n---\n\nBody.\n")
    skills = FileSystemSkills(root)

    await skills.index()
    await skills.index()
    await skills.load("seo")

    assert [path for path, _ in skills.skipped] == [root / "half-written" / "SKILL.md"]


async def test_loading_a_skipped_skill_raises_like_an_unknown_one(root: Path) -> None:
    write(root / "half-written" / "SKILL.md", "---\nname: half-written\n---\n\nBody.\n")
    skills = FileSystemSkills(root)

    with pytest.raises(TantraError, match="unknown skill 'half-written'"):
        await skills.load("half-written")

    assert [path for path, _ in skills.skipped] == [root / "half-written" / "SKILL.md"]


async def test_three_skills_add_one_block_of_three_lines_and_under_200_tokens(root: Path) -> None:
    with_skills, _, sid = await build([Sample(text="ok")], skills=FileSystemSkills(root))
    await collect(with_skills.run(sid, "how do I write a cold email?"))

    without, _, bare_sid = await build([Sample(text="ok")])
    await collect(without.run(bare_sid, "how do I write a cold email?"))

    request = with_skills.provider.requests[0]
    baseline = without.provider.requests[0]

    assert len(request.system) == 2
    assert baseline.system == request.system[:1]
    assert index_block(request).splitlines()[0] == SKILLS_PREAMBLE
    assert listed(request) == [
        "- cold-email: Write cold outbound emails that earn a reply.",
        "- outreach: Build and sequence an outbound prospect list.",
        "- seo: Audit a site for technical and on-page SEO problems.",
    ]
    assert "Attachments get you filtered." not in index_block(request)
    assert "Crawl first" not in index_block(request)
    assert approx_tokens(request) - approx_tokens(baseline) < 200


async def test_calling_the_skill_tool_places_the_body_and_the_file_listing_in_context(root: Path) -> None:
    harness, _, sid = await build(
        [
            Sample(tool_calls=[call("skill", '{"name": "cold-email"}')]),
            Sample(text="here is your email"),
        ],
        skills=FileSystemSkills(root),
    )

    events = await collect(harness.run(sid, "write me a cold email"))

    result = picks(events, ToolCallCompleted)[0]
    assert not result.is_error
    assert "Open on the reader's problem" in result.result
    assert "Attachments get you filtered." in result.result
    assert "## Files\nreferences/frameworks.md" in result.result
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_an_unknown_skill_name_is_an_error_result_not_a_failed_turn(root: Path) -> None:
    harness, _, sid = await build(
        [
            Sample(tool_calls=[call("skill", '{"name": "nope"}')]),
            Sample(text="recovered"),
        ],
        skills=FileSystemSkills(root),
    )

    events = await collect(harness.run(sid, "go"))

    result = picks(events, ToolCallCompleted)[0]
    assert result.is_error
    assert result.result == "unknown skill 'nope'"
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_a_filtered_agent_indexes_and_loads_only_its_own_skills(root: Path) -> None:
    class Emailer(Agent):
        prompt = "You are a writer."
        skills = ["cold-email"]

    harness, _, sid = await build(
        [
            Sample(tool_calls=[call("skill", '{"name": "outreach"}')]),
            Sample(text="recovered"),
        ],
        agent=Emailer,
        skills=FileSystemSkills(root),
    )

    events = await collect(harness.run(sid, "go"))

    assert listed(harness.provider.requests[0]) == ["- cold-email: Write cold outbound emails that earn a reply."]
    result = picks(events, ToolCallCompleted)[0]
    assert result.is_error
    assert result.result == "skill 'outreach' is not available to this agent"
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_a_filter_naming_a_missing_skill_raises_when_the_turn_runs(root: Path) -> None:
    class Typo(Agent):
        skills = ["nope"]

    harness, _, sid = await build([Sample(text="never sampled")], agent=Typo, skills=FileSystemSkills(root))

    with pytest.raises(TantraError, match=r"unknown skills \['nope'\]"):
        await collect(harness.run(sid, "go"))


async def test_a_harness_without_skills_offers_no_skill_tool_and_one_system_block() -> None:
    harness, _, sid = await build([Sample(text="ok")])

    await collect(harness.run(sid, "go"))

    request = harness.provider.requests[0]
    assert len(request.system) == 1
    assert [schema.name for schema in request.tools] == []


async def test_an_agent_opting_out_gets_neither_the_tool_nor_the_block(root: Path) -> None:
    class Solo(Agent):
        prompt = "You are a writer."
        skills = []

    harness, _, sid = await build([Sample(text="ok")], agent=Solo, skills=FileSystemSkills(root))

    await collect(harness.run(sid, "go"))

    request = harness.provider.requests[0]
    assert len(request.system) == 1
    assert [schema.name for schema in request.tools] == []


async def test_a_user_tool_named_skill_collides_at_construction(root: Path) -> None:
    @tool
    async def skill(name: str) -> str:
        """Shadows the built-in skill tool."""
        return name

    class Clasher(Agent):
        tools = [skill]

    with pytest.raises(TantraError, match="duplicate tool name 'skill'"):
        Harness(FakeProvider([]), MemoryStore(), [Clasher], default_model="fake/model", skills=FileSystemSkills(root))


async def test_the_skills_block_survives_a_suspend_and_a_resume_from_a_fresh_harness(root: Path) -> None:
    @tool
    async def draft(topic: str) -> str:
        """Draft an email about a topic."""
        return f"drafted {topic}"

    class Drafter(Agent):
        prompt = "You are a writer."
        tools = [draft]
        permissions = {"draft": "ask"}

    store = MemoryStore()
    first = Harness(
        FakeProvider([Sample(tool_calls=[call("draft", '{"topic": "p99"}')])]),
        store,
        [Drafter],
        default_model="fake/model",
        skills=FileSystemSkills(root),
    )
    sid = (await first.create_session(Drafter)).id

    opening = await collect(first.run(sid, "draft something"))
    raised = picks(opening, AskRaised)[0]
    assert len(listed(first.provider.requests[0])) == 3

    del first
    second = Harness(
        FakeProvider([Sample(text="done")]),
        store,
        [Drafter],
        default_model="fake/model",
        skills=FileSystemSkills(root),
    )

    resumed = await collect(second.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"
    request = second.provider.requests[0]
    assert len(request.system) == 2
    assert len(listed(request)) == 3
    assert "skill" in [schema.name for schema in request.tools]


async def test_a_filtered_out_name_is_refused_before_the_catalogue_is_touched(root: Path) -> None:
    class Emailer(Agent):
        prompt = "You are a writer."
        skills = ["cold-email"]

    recording = RecordingSkills(root)
    harness, _, sid = await build(
        [
            Sample(tool_calls=[call("skill", '{"name": "outreach"}')]),
            Sample(text="recovered"),
        ],
        agent=Emailer,
        skills=recording,
    )

    events = await collect(harness.run(sid, "go"))

    assert picks(events, ToolCallCompleted)[0].is_error
    assert recording.loaded == []


async def test_a_broken_skills_root_fails_a_resume_before_the_answer_is_persisted(root: Path) -> None:
    @tool
    async def draft(topic: str) -> str:
        """Draft an email about a topic."""
        return f"drafted {topic}"

    class Auditor(Agent):
        prompt = "You audit."
        tools = [draft]
        skills = ["seo"]
        permissions = {"draft": "ask"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("draft", '{"topic": "p99"}')]), Sample(text="done")]),
        store,
        [Auditor],
        default_model="fake/model",
        skills=FileSystemSkills(root),
    )
    sid = (await harness.create_session(Auditor)).id

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]
    shutil.rmtree(root / "seo")

    with pytest.raises(TantraError, match=r"unknown skills \['seo'\]"):
        await collect(harness.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    logged = [stamped.event async for stamped in store.read(sid)]
    assert not [event for event in logged if isinstance(event, AskAnswered)]
    header = await store.header(sid)
    assert header.status == "awaiting_input"
    assert header.pending_ask == raised.ask_id

    write(root / "seo" / "SKILL.md", SEO)
    resumed = await collect(harness.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    assert picks(resumed, AskAnswered)[0].ask_id == raised.ask_id
    assert picks(resumed, ToolCallCompleted)[0].result == "drafted p99"
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"


async def test_the_skills_block_rides_every_sample_of_a_multi_sample_turn(root: Path) -> None:
    @tool
    async def draft(topic: str) -> str:
        """Draft an email about a topic."""
        return f"drafted {topic}"

    class Drafter(Agent):
        prompt = "You are a writer."
        tools = [draft]

    harness, _, sid = await build(
        [
            Sample(tool_calls=[call("skill", '{"name": "cold-email"}', cid="c1")]),
            Sample(tool_calls=[call("draft", '{"topic": "p99"}', cid="c2")]),
            Sample(text="done"),
        ],
        agent=Drafter,
        skills=FileSystemSkills(root),
    )

    events = await collect(harness.run(sid, "go"))

    assert picks(events, TurnCompleted)[0].stop_reason == "completed"
    assert len(harness.provider.requests) == 3
    for request in harness.provider.requests:
        assert len(request.system) == 2
        assert len(listed(request)) == 3


async def test_a_child_skill_call_asks_when_a_rule_less_ancestor_contributes_the_harness_default(root: Path) -> None:
    class Researcher(Agent):
        prompt = "You research."

    class Manager(Agent):
        prompt = "You manage."
        subagents = [Researcher]
        permissions = {"researcher": "allow"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("researcher", '{"task": "audit the site"}', cid="p1")]),
                Sample(tool_calls=[call("skill", '{"name": "seo"}', cid="k1")]),
            ]
        ),
        store,
        [Manager],
        default_model="fake/model",
        default_permission="ask",
        skills=FileSystemSkills(root),
    )
    sid = (await harness.create_session(Manager)).id

    events = await collect(harness.run(sid, "audit it"))

    child_sid = picks(events, ChildSessionSpawned)[0].child_session_id
    raised = picks(events, AskRaised)[0]
    assert raised.request.extra == {"permission": "skill"}
    assert [event.session_id for event in events if isinstance(event.event, AskRaised)] == [child_sid]
    assert not picks(events, TurnCompleted)


async def test_a_parent_skill_allow_rule_lets_the_child_load_without_asking(root: Path) -> None:
    class Researcher(Agent):
        prompt = "You research."

    class Manager(Agent):
        prompt = "You manage."
        subagents = [Researcher]
        permissions = {"researcher": "allow", "skill": "allow"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("researcher", '{"task": "audit the site"}', cid="p1")]),
                Sample(tool_calls=[call("skill", '{"name": "seo"}', cid="k1")]),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Manager],
        default_model="fake/model",
        default_permission="ask",
        skills=FileSystemSkills(root),
    )
    sid = (await harness.create_session(Manager)).id

    events = await collect(harness.run(sid, "audit it"))

    assert not picks(events, AskRaised)
    loaded = [event for event in picks(events, ToolCallCompleted) if event.call_id == "k1"][0]
    assert not loaded.is_error
    assert "Crawl first, opine second." in loaded.result
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"
