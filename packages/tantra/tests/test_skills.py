from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tantra.agent import Agent
from tantra.errors import TantraError
from tantra.providers.base import SampleRequest, ToolCall
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
