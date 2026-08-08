from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol

from tantra.errors import TantraError

SKILL_TOOL = "skill"
SKILL_FILE = "SKILL.md"
FENCE = "---"
BOM = "﻿"
BLOCK_SCALARS = frozenset({">", "|", ">-", "|-", ">+", "|+"})


@dataclass(frozen=True)
class SkillInfo:
    name: str
    description: str


@dataclass(frozen=True)
class Skill:
    name: str
    description: str
    body: str
    files: tuple[str, ...] = ()


class Skills(Protocol):
    """A catalogue of skills, disclosed progressively.

    `index` is cheap and rides in every system prompt; `load` pulls one skill's full body only once
    the model has asked for it by name.
    """

    async def index(self) -> Sequence[SkillInfo]:
        """Return every available skill's name and description."""

    async def load(self, name: str) -> Skill:
        """Return one skill in full. Raises `TantraError` when `name` is not in the catalogue."""


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def _parse(path: Path) -> Skill:
    lines = path.read_text(encoding="utf-8").lstrip(BOM).replace("\r\n", "\n").split("\n")
    if not lines or lines[0].strip() != FENCE:
        raise TantraError(f"{path}: skill file must open with a '{FENCE}' frontmatter fence")
    closing = next((index for index, line in enumerate(lines[1:], 1) if line.strip() == FENCE), None)
    if closing is None:
        raise TantraError(f"{path}: frontmatter fence is never closed")
    fields: dict[str, str] = {}
    for line in lines[1:closing]:
        if not line or line[0].isspace() or ":" not in line:
            continue
        key, _, value = line.partition(":")
        fields[key.rstrip()] = _unquote(value.strip())
    for key in ("name", "description"):
        if not fields.get(key):
            raise TantraError(f"{path}: frontmatter has no {key!r}")
        if fields[key] in BLOCK_SCALARS:
            raise TantraError(f"{path}: frontmatter {key!r} is a YAML block scalar; write it on one line")
    return Skill(name=fields["name"], description=fields["description"], body="\n".join(lines[closing + 1 :]).strip())


def _files(directory: Path) -> tuple[str, ...]:
    skill_file = directory / SKILL_FILE
    return tuple(
        sorted(
            path.relative_to(directory).as_posix()
            for path in directory.rglob("*")
            if path.is_file() and path != skill_file
        )
    )


class FileSystemSkills:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _scan(self) -> list[tuple[Path, Skill]]:
        if not self.root.is_dir():
            raise TantraError(f"skills root {self.root} is not a directory")
        seen: dict[str, Path] = {}
        entries: list[tuple[Path, Skill]] = []
        for directory in sorted(self.root.iterdir()):
            path = directory / SKILL_FILE
            if not directory.is_dir() or not path.is_file():
                continue
            skill = _parse(path)
            if skill.name in seen:
                raise TantraError(f"duplicate skill name {skill.name!r}: {seen[skill.name]} and {path}")
            seen[skill.name] = path
            entries.append((directory, skill))
        return entries

    async def index(self) -> list[SkillInfo]:
        return [SkillInfo(name=skill.name, description=skill.description) for _, skill in self._scan()]

    async def load(self, name: str) -> Skill:
        for directory, skill in self._scan():
            if skill.name == name:
                return replace(skill, files=_files(directory))
        raise TantraError(f"unknown skill {name!r}")
