from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from tantra import FileSystemSkills, Skill, SkillInfo, TantraError


class MergedSkills:
    """Skills read from several roots at once; a later root shadows an earlier one on a name clash."""

    def __init__(self, roots: Sequence[str | Path]) -> None:
        self.sources = [FileSystemSkills(root) for root in roots]

    async def index(self) -> list[SkillInfo]:
        merged: dict[str, SkillInfo] = {}
        for source in self.sources:
            for info in await source.index():
                merged[info.name] = info
        return list(merged.values())

    async def load(self, name: str) -> Skill:
        for source in reversed(self.sources):
            if any(info.name == name for info in await source.index()):
                return await source.load(name)
        raise TantraError(f"unknown skill {name!r}")


def skill_roots(*candidates: Path) -> list[Path]:
    return [root for root in candidates if root.is_dir()]
