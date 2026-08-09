from __future__ import annotations

import re
import shlex
from pathlib import Path

from tantra import Denial, Hook, TurnContext
from tantra.events import ToolCallRequested

SEPARATORS = re.compile(r"&&|\|\||[;\n|&]")
DEVICE_WRITE = re.compile(r">\s*/dev/(?!null\b|stdout\b|stderr\b|tty\b|fd/)")
RECURSIVE = frozenset({"r", "R", "recursive"})
FORCE = frozenset({"f", "force"})


def _segments(command: str) -> list[list[str]]:
    segments: list[list[str]] = []
    for piece in SEPARATORS.split(command):
        try:
            parts = shlex.split(piece)
        except ValueError:
            continue
        while parts and "=" in parts[0] and not parts[0].startswith("-"):
            parts.pop(0)
        if parts:
            segments.append(parts)
    return segments


def _flags(parts: list[str]) -> set[str]:
    found: set[str] = set()
    for part in parts:
        if part.startswith("--"):
            found.add(part[2:])
        elif part.startswith("-") and len(part) > 1:
            found.update(part[1:])
    return found


def destructive(command: str) -> str | None:
    if DEVICE_WRITE.search(command):
        return "redirects output into a device file"
    for parts in _segments(command):
        name = Path(parts[0]).name
        flags = _flags(parts[1:])
        if name in ("sudo", "doas", "su"):
            return "runs a command as another user"
        if name == "rm" and flags & RECURSIVE:
            return "deletes a directory tree with rm -r"
        if name == "dd":
            return "writes raw blocks with dd"
        if name == "mkfs" or name.startswith("mkfs."):
            return "formats a filesystem"
        if name == "chmod" and flags & RECURSIVE and "777" in parts:
            return "opens a whole tree with chmod -R 777"
        if name == "git" and "push" in parts and (flags & FORCE or any(part.startswith("--force") for part in parts)):
            return "force-pushes a git branch"
    return None


class BashGuard(Hook):
    """Refuses destructive shell commands, so an unattended run cannot wreck the machine."""

    async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial | None:
        if call.name != "bash":
            return None
        reason = destructive(str(call.args.get("command", "")))
        if reason is None:
            return None
        return Denial(f"the command {reason}; find another way or ask the user to run it")
