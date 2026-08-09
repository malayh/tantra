from __future__ import annotations

import asyncio
import os
import re
import shlex
import signal
from pathlib import Path
from typing import Literal

from tantra.context import TurnContext
from tantra.events import ToolCallRequested
from tantra.hooks import Denial, Escalation, Hook
from tantra.tools import Context, Tool, tool

MAX_CHARS = 64_000

FORK_BOMB = re.compile(r":\(\)\s*\{|:\|:\s*&")
SEPARATORS = frozenset({"&&", "||", ";", ";;", "|", "|&", "&"})
PIPES = frozenset({"|", "|&"})
RECURSIVE = frozenset({"r", "R", "recursive"})
SHELLS = frozenset({"sh", "bash", "zsh", "dash", "ksh"})
INTERPRETERS = frozenset({"python", "python3", "node", "nodejs", "perl", "ruby"})
PREFIXES = frozenset({"nohup", "nice", "stdbuf", "command", "exec", "time", "setsid", "ionice"})
GROUPING = frozenset({"(", "{", "!"})
BUNDLE_MAX = 3
HALT = frozenset({"shutdown", "reboot", "halt", "poweroff"})
PRIVILEGE = frozenset({"sudo", "doas", "su"})
ROOTS = frozenset({"/", "~", "$HOME", "${HOME}"})
HOMES = ("~", "$HOME", "${HOME}")
XARGS_VALUE_FLAGS = frozenset({"-n", "-P", "-I", "-i", "-d", "-s", "-L", "-a", "-E", "-e"})
EXPANSIONS = ("$", "`")
DURATION = re.compile(r"^\d+(\.\d+)?[smhd]?$")

CODE_MARKERS = (
    ("rmtree", "deletes a directory tree"),
    ("rm -rf", "shells out to a recursive delete"),
    ("rm -fr", "shells out to a recursive delete"),
    ("rm -r ", "shells out to a recursive delete"),
    ("mkfs", "formats a filesystem"),
    ("/dev/sd", "writes to a raw block device"),
    ("/dev/nvme", "writes to a raw block device"),
    ("/dev/disk", "writes to a raw block device"),
    ("shutdown", "halts the machine"),
    ("reboot", "restarts the machine"),
    ("sudo ", "escalates privileges"),
)


def _cap(text: str) -> str:
    if len(text) <= MAX_CHARS:
        return text
    return f"{text[:MAX_CHARS]}\n[truncated: {len(text) - MAX_CHARS} chars omitted]"


def _kill_group(process: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        process.kill()


def bash(*, timeout: float = 120.0) -> Tool:
    @tool(permission="ask")
    async def bash(command: str, ctx: Context) -> str:
        """Run a shell command in the working directory and return its combined output.

        Usage:
        - This tool is for terminal operations — builds, tests, linters, git, package managers,
          docker, and one-off scripts. Prefer a dedicated tool when one exists for the job: it
          gives you a real error instead of a shell exit code.
        - Never use `echo` or `printf` to talk to the user. Anything you want read goes in your
          reply, not into the transcript of a command.
        - stdout and stderr come back interleaved, so ordering between the two is approximate. A
          non-zero exit appends `[exit status N]` — read it, a silent failure is still a failure.
        - Output over 64000 characters is cut with a `[truncated: N chars omitted]` note. Treat
          that as a signal to narrow the command (grep, `--quiet`, a smaller path), not to run it
          again unchanged.
        - The command runs in its own process group and is killed with everything it spawned once
          the timeout expires. The timeout is fixed by the application and you cannot raise it, so
          split a long job into steps rather than hoping a slow one finishes.
        - There is no interactive terminal. Pass non-interactive flags (`--yes`, `--no-pager`,
          `--non-interactive`) and never start something that waits for input or never returns,
          such as a dev server or a watcher in the foreground.
        - Quote paths that may contain spaces, and chain steps with `&&` so a failure stops the
          rest instead of running against a broken state.
        - Commit only when the user has explicitly asked. Never push, force-push, `reset --hard`
          or delete a branch on your own initiative.
        """
        await ctx.emit(f"$ {command}")
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(Path.cwd()),
            start_new_session=True,
        )
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except TimeoutError:
            _kill_group(process)
            await process.wait()
            raise TimeoutError(f"command timed out after {timeout}s: {command}") from None
        output = _cap(stdout.decode("utf-8", errors="replace"))
        if process.returncode:
            return f"{output}\n[exit status {process.returncode}]".lstrip()
        return output

    return bash


def _flags(parts: list[str]) -> set[str]:
    found: set[str] = set()
    for part in parts:
        if part.startswith("--"):
            found.add(part[2:])
        elif part.startswith("-") and len(part) > 1:
            found.update(part[1:])
    return found


def _operands(parts: list[str]) -> list[str]:
    return [part for part in parts if not part.startswith("-")]


def _tokenize(command: str) -> list[str]:
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    return list(lexer)


def _bundled_value(parts: list[str], letters: frozenset[str]) -> str | None:
    for index, part in enumerate(parts[:-1]):
        if part.startswith("--") or not part.startswith("-") or len(part) < 2:
            continue
        body = part[1:]
        if body in letters:
            return parts[index + 1]
        if body.isalpha() and len(body) <= BUNDLE_MAX and set(body) & letters:
            return parts[index + 1]
    return None


def _reads_stdin(parts: list[str]) -> bool:
    return any(part.startswith("<") for part in parts)


def _strip_redirects(parts: list[str]) -> list[str]:
    kept: list[str] = []
    index = 0
    while index < len(parts):
        part = parts[index]
        if part.startswith((">", "<")):
            if kept and kept[-1].isdigit():
                kept.pop()
            index += 2
            continue
        kept.append(part)
        index += 1
    return kept


def _ungroup(parts: list[str]) -> list[str]:
    index = 0
    while index < len(parts) and parts[index] in GROUPING:
        index += 1
    return parts[index:]


def _strip_assignments(parts: list[str]) -> list[str]:
    while parts and "=" in parts[0] and not parts[0].startswith("-"):
        parts = parts[1:]
    return parts


def _drop_flags(parts: list[str], value_flags: frozenset[str] = frozenset()) -> list[str]:
    index = 0
    while index < len(parts) and parts[index].startswith("-"):
        if parts[index] in value_flags:
            index += 1
        index += 1
    return parts[index:]


def _is_home(target: str) -> bool:
    return any(target == home or target.startswith(f"{home}/") for home in HOMES)


def _outside(target: str) -> str | None:
    if target == "/":
        return "the filesystem root"
    if _is_home(target):
        return "your home directory"
    if any(marker in target for marker in EXPANSIONS):
        return "built by a shell expansion whose value cannot be checked before it runs"
    try:
        resolved = Path(target).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return "a path that cannot be resolved"
    if resolved.is_relative_to(Path.cwd().resolve()):
        return None
    return "a path outside the working directory"


def _rm(parts: list[str], *, runtime_targets: bool = False) -> str | None:
    if not _flags(parts) & RECURSIVE:
        return None
    targets = _operands(parts)
    if runtime_targets or not targets:
        return "recursively deletes paths supplied at runtime with `rm -r`, so the targets cannot be checked"
    for target in targets:
        where = _outside(target)
        if where is not None:
            return f"recursively deletes {target!r} with `rm -r`, which is {where}"
    return None


class ShellGuard(Hook):
    def __init__(self, *, on_trip: Literal["deny", "ask"] = "deny", deny_extra: list[str] | None = None) -> None:
        self.on_trip = on_trip
        self.deny_extra = list(deny_extra or [])

    async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial | Escalation | None:
        if call.name != "bash":
            return None
        command = call.args.get("command")
        if not isinstance(command, str):
            return None
        reason = self.inspect(command)
        if reason is None:
            return None
        if self.on_trip == "deny":
            return Denial(
                f"the command {reason}. ShellGuard refused to run it — do not retry it, "
                f"take a narrower approach or ask the user to run it themselves"
            )
        return Escalation(f"the command {reason}. ShellGuard requires the user to approve it before it runs")

    def inspect(self, command: str) -> str | None:
        for pattern in self.deny_extra:
            if pattern in command:
                return f"contains the denied pattern {pattern!r}"
        if FORK_BOMB.search(command):
            return "defines a fork bomb that would exhaust the machine's process table"
        try:
            tokens = _tokenize(command)
        except ValueError as exc:
            return f"could not be parsed as a shell command ({exc}), so it cannot be checked for destructive operations"
        for piped, segment in _split(tokens):
            reason = self._segment(segment, piped=piped)
            if reason is not None:
                return reason
        return None

    def _segment(self, parts: list[str], *, piped: bool = False) -> str | None:
        fed = piped or _reads_stdin(parts)
        parts = _strip_assignments(_ungroup(_strip_redirects(parts)))
        if not parts:
            return None
        name = Path(parts[0]).name
        rest = parts[1:]
        if name in PRIVILEGE:
            return f"runs with another user's privileges via `{name}`"
        if name in HALT:
            return f"shuts down or restarts the machine with `{name}`"
        if name == "mkfs" or name.startswith("mkfs."):
            return f"formats a filesystem with `{name}`, destroying everything on the target device"
        if name == "env":
            return self._segment(_strip_assignments(_drop_flags(rest)), piped=fed)
        if name in PREFIXES:
            return self._segment(_drop_flags(rest), piped=fed)
        if name == "timeout":
            tail = _drop_flags(rest)
            return self._segment(tail[1:] if tail and DURATION.match(tail[0]) else tail, piped=fed)
        if name == "eval":
            return self.inspect(" ".join(rest)) if rest else None
        if name == "xargs":
            return self._xargs(_drop_flags(rest, XARGS_VALUE_FLAGS))
        if name in SHELLS:
            nested = _bundled_value(rest, frozenset({"c"}))
            if nested is not None:
                return self.inspect(nested)
            if fed:
                return f"feeds `{name}` a script on its input, so what would actually run cannot be checked beforehand"
            return None
        if name in INTERPRETERS:
            code = _bundled_value(rest, frozenset({"c", "e"}))
            return self._code(code, name) if code is not None else None
        if name == "find":
            return self._find(rest)
        if name == "rm":
            return _rm(rest)
        if name == "dd":
            return _dd(rest)
        if name in ("chmod", "chown", "chgrp") and _flags(rest) & RECURSIVE:
            return _chmod(name, rest)
        return None

    def _xargs(self, parts: list[str]) -> str | None:
        if not parts:
            return None
        if Path(parts[0]).name == "rm":
            return _rm(parts[1:], runtime_targets=True)
        return self._segment(parts)

    def _find(self, parts: list[str]) -> str | None:
        paths: list[str] = []
        index = 0
        while index < len(parts) and not parts[index].startswith("-"):
            paths.append(parts[index])
            index += 1
        options = parts[index:]
        if "-delete" in options:
            for path in paths or ["."]:
                where = _outside(path)
                if where is not None:
                    return f"recursively deletes everything under {path!r} with `find -delete`, which is {where}"
            return None
        for marker in ("-exec", "-execdir"):
            if marker in options:
                expanded: list[str] = []
                for part in options[options.index(marker) + 1 :]:
                    if part in (";", "\\;", "+"):
                        break
                    if "{}" in part:
                        expanded.extend(paths or ["."])
                    else:
                        expanded.append(part)
                return self._segment(expanded)
        return None

    def _code(self, code: str, interpreter: str) -> str | None:
        for pattern in self.deny_extra:
            if pattern in code:
                return f"contains the denied pattern {pattern!r}"
        lowered = code.lower()
        for marker, effect in CODE_MARKERS:
            if marker in lowered:
                return f"runs an inline {interpreter} program that {effect}"
        return None


def _split(tokens: list[str]) -> list[tuple[bool, list[str]]]:
    segments: list[tuple[bool, list[str]]] = []
    current: list[str] = []
    piped = False
    for token in tokens:
        if token in SEPARATORS:
            if current:
                segments.append((piped, current))
            current = []
            piped = token in PIPES
            continue
        current.append(token)
    if current:
        segments.append((piped, current))
    return segments


def _dd(parts: list[str]) -> str | None:
    for part in parts:
        if part.startswith("of=/dev/") and not part.startswith(("of=/dev/null", "of=/dev/stdout", "of=/dev/stderr")):
            return f"writes raw blocks straight to the device {part[3:]!r} with `dd`, destroying its contents"
    return None


def _chmod(name: str, parts: list[str]) -> str | None:
    for target in _operands(parts):
        if target in ROOTS:
            return f"recursively rewrites permissions or ownership of {target!r} with `{name} -R`"
    return None
