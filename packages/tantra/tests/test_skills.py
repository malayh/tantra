from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from tantra.agent import Agent
from tantra.context import build_sample_request
from tantra.errors import TantraError
from tantra.providers.base import SampleRequest, ToolCall, ToolSchema
from tantra.skills import FileSystemSkills, Skill, SkillInfo

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


def test_execution_environment_is_one_final_capability_block() -> None:
    tools = [
        ToolSchema(name="skill", description="Skill description.", parameters={"type": "object"}),
        ToolSchema(name="finish", description="Finish description.", parameters={"type": "object"}),
    ]

    request = build_sample_request(
        model="m",
        prompt=Writer.prompt,
        events=[],
        tools=tools,
        skills=[SkillInfo(name="release", description="Ship safely.")],
        child_lifecycle=True,
    )

    assert request.system[0].text == Writer.prompt
    assert len(request.system) == 2
    environment = request.system[-1].text
    assert environment == (
        "Execution environment\n\n"
        "Skills\n\n"
        "Available skills can be loaded on demand with the skill tool. Read a file a loaded skill refers to with "
        "skill_file, passing that skill's name and the path exactly as written. Load a skill when its description "
        "matches the task or the user explicitly requests it:\n"
        "- release: Ship safely.\n\n"
        "Child lifecycle\n\n"
        "When your turn ends, your parent receives your final message as your reply, or your submitted output if you "
        "submit one. Put the complete result there. Use send only for messages during a turn. Your parent may send "
        "you more work."
    )
    assert "Skill description." not in environment
    assert "Finish description." not in environment
    assert "Tantra" not in environment
    assert "Harness" not in environment
    assert "You are" not in environment
    assert request.tools == tools


def test_execution_environment_is_omitted_without_relevant_capabilities() -> None:
    request = build_sample_request(model="m", prompt="", events=[], tools=[])

    assert request.system == []


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


async def test_read_returns_a_file_relative_to_the_named_skills_folder(root: Path) -> None:
    write(root / "drafts" / "SKILL.md", "---\nname: drafting\ndescription: Draft things.\n---\n\nBody.\n")
    write(root / "drafts" / "outline.md", "Outline.\n")
    skills = FileSystemSkills(root)

    assert await skills.read("cold-email", "references/frameworks.md") == "PAS. AIDA. BAB.\n"
    assert await skills.read("drafting", "outline.md") == "Outline.\n"


async def test_read_climbs_into_a_sibling_skill_with_dotdot(root: Path) -> None:
    write(root / "outreach" / "notes.md", "Three touches.\n")

    assert await FileSystemSkills(root).read("cold-email", "../outreach/notes.md") == "Three touches.\n"


async def test_reading_a_folder_lists_its_files_relative_to_the_anchor_skill(root: Path) -> None:
    skills = FileSystemSkills(root)

    assert await skills.read("cold-email", "references") == "references/frameworks.md"
    assert await skills.read("cold-email", ".") == "SKILL.md\nreferences/frameworks.md"
    assert await skills.read("seo", "../cold-email") == "../cold-email/SKILL.md\n../cold-email/references/frameworks.md"


async def test_reading_the_root_lists_the_whole_catalogue_as_paths_that_read_back(root: Path) -> None:
    skills = FileSystemSkills(root)

    listing = (await skills.read("seo", "..")).split("\n")

    assert listing == [
        "../cold-email/SKILL.md",
        "../cold-email/references/frameworks.md",
        "../outreach/SKILL.md",
        "SKILL.md",
    ]
    assert [await skills.read("seo", line) for line in listing] == [COLD_EMAIL, "PAS. AIDA. BAB.\n", OUTREACH, SEO]


async def test_a_symlinked_skill_folder_reads_and_lists_like_a_plain_folder(root: Path, tmp_path: Path) -> None:
    write(tmp_path / "elsewhere" / "linked" / "SKILL.md", "---\nname: linked\ndescription: Linked in.\n---\n\nBody.\n")
    write(tmp_path / "elsewhere" / "linked" / "guide.md", "Guide.\n")
    write(tmp_path / "elsewhere" / "outreach" / "SKILL.md", "decoy beside the link target")
    (root / "linked").symlink_to(tmp_path / "elsewhere" / "linked")
    skills = FileSystemSkills(root)

    assert "linked" in [info.name for info in await skills.index()]
    assert await skills.read("linked", "guide.md") == "Guide.\n"
    assert await skills.read("linked", ".") == "SKILL.md\nguide.md"
    assert await skills.read("linked", "../outreach/SKILL.md") == OUTREACH
    assert (await skills.read("linked", "..")).split("\n") == [
        "../cold-email/SKILL.md",
        "../cold-email/references/frameworks.md",
        "../outreach/SKILL.md",
        "../seo/SKILL.md",
        "SKILL.md",
        "guide.md",
    ]
    assert "../linked/guide.md" in (await skills.read("seo", "..")).split("\n")
    assert (await skills.load("linked")).files == ("guide.md",)


async def test_a_symlinked_subfolder_appears_in_read_listings_and_load_files(root: Path, tmp_path: Path) -> None:
    write(tmp_path / "shared-docs" / "tone.md", "Plain words.\n")
    (root / "cold-email" / "shared").symlink_to(tmp_path / "shared-docs")
    skills = FileSystemSkills(root)

    assert (await skills.load("cold-email")).files == ("references/frameworks.md", "shared/tone.md")
    assert await skills.read("cold-email", ".") == "SKILL.md\nreferences/frameworks.md\nshared/tone.md"
    assert await skills.read("cold-email", "shared") == "shared/tone.md"
    assert await skills.read("cold-email", "shared/tone.md") == "Plain words.\n"


async def test_a_real_folder_is_listed_under_its_own_name_not_a_symlink_alias(root: Path) -> None:
    write(root / "cold-email" / "docs" / "x.md", "X.\n")
    (root / "cold-email" / "adocs").symlink_to("docs")
    skills = FileSystemSkills(root)

    assert (await skills.load("cold-email")).files == ("docs/x.md", "references/frameworks.md")
    assert await skills.read("cold-email", ".") == "SKILL.md\ndocs/x.md\nreferences/frameworks.md"
    assert await skills.read("cold-email", "adocs/x.md") == "X.\n"


async def test_a_link_into_a_sibling_skill_lists_under_the_siblings_path_when_the_listing_reaches_it(
    root: Path,
) -> None:
    (root / "cold-email" / "tooutreach").symlink_to("../outreach")
    skills = FileSystemSkills(root)

    assert (await skills.read("cold-email", "..")).split("\n") == [
        "../outreach/SKILL.md",
        "../seo/SKILL.md",
        "SKILL.md",
        "references/frameworks.md",
    ]
    assert await skills.read("cold-email", ".") == "SKILL.md\nreferences/frameworks.md\ntooutreach/SKILL.md"


async def test_an_alias_inside_a_symlinked_skill_folder_loses_to_its_real_subfolder(root: Path, tmp_path: Path) -> None:
    write(tmp_path / "elsewhere" / "linked" / "SKILL.md", "---\nname: linked\ndescription: Linked in.\n---\n\nBody.\n")
    write(tmp_path / "elsewhere" / "linked" / "docs" / "x.md", "X.\n")
    (tmp_path / "elsewhere" / "linked" / "adocs").symlink_to("docs")
    (root / "linked").symlink_to(tmp_path / "elsewhere" / "linked")
    skills = FileSystemSkills(root)

    listing = (await skills.read("seo", "..")).split("\n")
    assert "../linked/docs/x.md" in listing
    assert not [line for line in listing if line.startswith("../linked/adocs/")]
    anchored = (await skills.read("linked", "..")).split("\n")
    assert "docs/x.md" in anchored
    assert not [line for line in anchored if line.startswith("adocs/")]
    assert (await skills.load("linked")).files == ("docs/x.md",)


async def test_a_symlink_cycle_terminates(root: Path) -> None:
    (root / "cold-email" / "references" / "back").symlink_to("..")
    skills = FileSystemSkills(root)

    assert await skills.read("cold-email", ".") == "SKILL.md\nreferences/frameworks.md"
    assert (await skills.load("cold-email")).files == ("references/frameworks.md",)
    assert "../outreach/SKILL.md" in (await skills.read("cold-email", "..")).split("\n")


@pytest.mark.parametrize("path", ["../..", "../../secret.txt", "/etc/passwd", "references/../../../secret.txt"])
async def test_paths_above_the_catalogue_root_are_refused_even_when_the_target_exists(root: Path, path: str) -> None:
    (root.parent / "secret.txt").write_text("top secret")
    assert (root / "cold-email" / path).exists()

    with pytest.raises(TantraError, match="outside the skills catalogue") as raised:
        await FileSystemSkills(root).read("cold-email", path)

    assert "'cold-email'" in str(raised.value)
    assert repr(path) in str(raised.value)


async def test_reading_a_missing_path_raises(root: Path) -> None:
    with pytest.raises(TantraError, match="no file or folder at 'references/missing.md'"):
        await FileSystemSkills(root).read("cold-email", "references/missing.md")


async def test_reading_a_file_that_is_not_utf8_raises(root: Path) -> None:
    (root / "cold-email" / "references" / "latin1.md").write_bytes(b"caf\xe9")

    with pytest.raises(TantraError, match="'references/latin1.md' is not UTF-8 text"):
        await FileSystemSkills(root).read("cold-email", "references/latin1.md")


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads files whatever their mode")
async def test_reading_an_unreadable_file_raises_without_the_storage_path(root: Path) -> None:
    locked = root / "cold-email" / "references" / "locked.md"
    locked.write_text("hidden")
    locked.chmod(0)

    with pytest.raises(TantraError, match="cannot read 'references/locked.md': Permission denied") as raised:
        await FileSystemSkills(root).read("cold-email", "references/locked.md")

    assert "'cold-email'" in str(raised.value)
    assert str(root) not in str(raised.value)


async def test_reading_from_an_unknown_skill_raises(root: Path) -> None:
    with pytest.raises(TantraError, match="unknown skill 'nope'"):
        await FileSystemSkills(root).read("nope", "SKILL.md")
