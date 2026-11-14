from __future__ import annotations

import os
import posixpath
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Protocol

from tantra.errors import TantraError

SKILL_TOOL = "skill"
SKILL_FILE_TOOL = "skill_file"
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

    async def read(self, name: str, path: str) -> str:
        """Return one file's UTF-8 text, or list one folder, at `path` in the catalogue.

        `path` is a POSIX path relative to skill `name`'s folder, normalised as text inside the catalogue and never
        resolved through storage; `..` may climb into sibling skills. A folder returns every file under it,
        recursively and sorted, one per line, each relative to the skill's folder. Raises `TantraError` for an
        unknown skill, a path that is absolute or above the catalogue root, nothing at the path, or a file that is
        not UTF-8 text.
        """


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def _dedent(block: list[str]) -> list[str]:
    filled = [line for line in block if line.strip()]
    if not filled:
        return []
    margin = min(len(line) - len(line.lstrip()) for line in filled)
    return [line[margin:] if line.strip() else "" for line in block]


def _fold(marker: str, block: list[str]) -> str:
    if marker.startswith(">"):
        return " ".join(line.strip() for line in block if line.strip())
    return "\n".join(_dedent(block)).strip()


def _fields(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    index = 0
    while index < len(lines):
        line = lines[index]
        index += 1
        if not line or line[0].isspace() or ":" not in line:
            continue
        key, _, value = line.partition(":")
        marker = value.strip()
        if marker not in BLOCK_SCALARS:
            fields[key.rstrip()] = _unquote(marker)
            continue
        block: list[str] = []
        while index < len(lines) and (not lines[index].strip() or lines[index][0].isspace()):
            block.append(lines[index])
            index += 1
        fields[key.rstrip()] = _fold(marker, block)
    return fields


def _parse(path: Path) -> Skill:
    lines = path.read_text(encoding="utf-8").lstrip(BOM).replace("\r\n", "\n").split("\n")
    if not lines or lines[0].strip() != FENCE:
        raise TantraError(f"{path}: skill file must open with a '{FENCE}' frontmatter fence")
    closing = next((index for index, line in enumerate(lines[1:], 1) if line.rstrip() == FENCE), None)
    if closing is None:
        raise TantraError(f"{path}: frontmatter fence is never closed")
    fields = _fields(lines[1:closing])
    for key in ("name", "description"):
        if not fields.get(key):
            raise TantraError(f"{path}: frontmatter has no {key!r}")
    return Skill(name=fields["name"], description=fields["description"], body="\n".join(lines[closing + 1 :]).strip())


def _virtual(folder: str, path: str) -> str | None:
    virtual = posixpath.normpath(posixpath.join(folder, path))
    if posixpath.isabs(virtual) or virtual == ".." or virtual.startswith("../"):
        return None
    return virtual


def _walk(base: Path) -> list[str]:
    claimed = {os.path.realpath(dirpath) for dirpath, _, _ in os.walk(base)}
    seen: set[str] = set()
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(base, followlinks=True):
        real = os.path.realpath(dirpath)
        if real in seen:
            dirnames.clear()
            continue
        seen.add(real)
        dirnames[:] = sorted(
            name
            for name in dirnames
            if not os.path.islink(os.path.join(dirpath, name))
            or os.path.realpath(os.path.join(dirpath, name)) not in claimed
        )
        for name in dirnames:
            if os.path.islink(os.path.join(dirpath, name)):
                claimed.update(os.path.realpath(path) for path, _, _ in os.walk(os.path.join(dirpath, name)))
        relative = Path(dirpath).relative_to(base)
        found += [
            (relative / filename).as_posix()
            for filename in filenames
            if os.path.isfile(os.path.join(dirpath, filename))
        ]
    return found


def _files(directory: Path) -> tuple[str, ...]:
    return tuple(sorted(path for path in _walk(directory) if path != SKILL_FILE))


class FileSystemSkills:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.skipped: list[tuple[Path, str]] = []

    def _scan(self) -> list[tuple[Path, Skill]]:
        if not self.root.is_dir():
            raise TantraError(f"skills root {self.root} is not a directory")
        seen: dict[str, Path] = {}
        entries: list[tuple[Path, Skill]] = []
        skipped: list[tuple[Path, str]] = []
        for directory in sorted(self.root.iterdir()):
            path = directory / SKILL_FILE
            if not directory.is_dir() or not path.is_file():
                continue
            try:
                skill = _parse(path)
            except TantraError as exc:
                skipped.append((path, str(exc).removeprefix(f"{path}: ")))
                continue
            except (OSError, ValueError) as exc:
                skipped.append((path, f"unreadable skill file: {exc}"))
                continue
            if skill.name in seen:
                skipped.append((path, f"duplicate skill name {skill.name!r}: already declared by {seen[skill.name]}"))
                continue
            seen[skill.name] = path
            entries.append((directory, skill))
        self.skipped = skipped
        return entries

    async def index(self) -> list[SkillInfo]:
        return [SkillInfo(name=skill.name, description=skill.description) for _, skill in self._scan()]

    async def load(self, name: str) -> Skill:
        for directory, skill in self._scan():
            if skill.name == name:
                return replace(skill, files=_files(directory))
        raise TantraError(f"unknown skill {name!r}")

    async def read(self, name: str, path: str) -> str:
        directory = next((directory for directory, skill in self._scan() if skill.name == name), None)
        if directory is None:
            raise TantraError(f"unknown skill {name!r}")
        folder = directory.name
        virtual = _virtual(folder, path)
        if virtual is None:
            raise TantraError(f"skill {name!r}: path {path!r} is outside the skills catalogue")
        target = self.root / virtual
        if target.is_dir():
            return "\n".join(
                sorted(
                    PurePosixPath(virtual, rel).relative_to(folder, walk_up=True).as_posix() for rel in _walk(target)
                )
            )
        if target.is_file():
            try:
                return target.read_bytes().decode("utf-8")
            except UnicodeDecodeError as exc:
                raise TantraError(f"skill {name!r}: {path!r} is not UTF-8 text") from exc
            except OSError as exc:
                raise TantraError(f"skill {name!r}: cannot read {path!r}: {exc.strerror}") from exc
        raise TantraError(f"skill {name!r}: no file or folder at {path!r}")
