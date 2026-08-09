from __future__ import annotations

import asyncio
import os
import re
import signal
from pathlib import Path

from tantra import Context, tool

MAX_MATCHES = 200
MAX_GLOB = 500
MAX_CHARS = 64_000
BASH_TIMEOUT = 120.0
SKIP_DIRS = frozenset({".git", ".venv", "node_modules", "__pycache__", ".mypy_cache", ".ruff_cache", "dist", "build"})


def _target(path: str) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else Path.cwd() / candidate


def _display(path: Path) -> str:
    try:
        return path.relative_to(Path.cwd()).as_posix()
    except ValueError:
        return path.as_posix()


def _text(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(f"no such file: {_display(path)}")
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{_display(path)} is not utf-8 text") from exc


def _cap(text: str) -> str:
    if len(text) <= MAX_CHARS:
        return text
    return f"{text[:MAX_CHARS]}\n[truncated: {len(text) - MAX_CHARS} chars omitted]"


def _walk(root: Path) -> list[Path]:
    found: list[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if SKIP_DIRS & set(path.relative_to(root).parts):
            continue
        found.append(path)
    return found


@tool
async def read(path: str) -> str:
    """Return the text of one file, truncated if it is very large.

    `path` is relative to the working directory unless it starts with `/` or `~`. Read a file
    before editing it: `edit` needs the exact text it is replacing. A truncated read says so on
    its last line — grep for the part you need rather than editing against a cut-off file.
    """
    return _cap(_text(_target(path)))


@tool
async def write(path: str, content: str) -> str:
    """Create a file or overwrite it whole with `content`.

    Missing parent directories are created. Use `edit` to change part of a file that already
    exists — writing it whole loses anything you did not include.
    """
    target = _target(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return f"wrote {len(content)} chars to {_display(target)}"


@tool
async def edit(path: str, old: str, new: str) -> str:
    """Replace one exact occurrence of `old` with `new` in a file.

    `old` must appear exactly once, matched byte for byte including indentation, so include
    enough surrounding lines to make it unique. Nothing is written when the match is missing or
    ambiguous — read the file and retry with a longer `old`.
    """
    target = _target(path)
    content = _text(target)
    hits = content.count(old)
    if hits == 0:
        raise ValueError(f"{_display(target)}: `old` does not appear in the file")
    if hits > 1:
        raise ValueError(f"{_display(target)}: `old` appears {hits} times; make it unique")
    target.write_text(content.replace(old, new), encoding="utf-8")
    return f"edited {_display(target)}"


@tool
async def glob(pattern: str, path: str = ".") -> list[str]:
    """List files whose paths match a glob pattern, e.g. `**/*.py` or `src/**/test_*.py`.

    `path` is the directory to search from, defaulting to the working directory. Returns paths
    relative to the working directory, sorted, and says so when the list was truncated.
    """
    root = _target(path)
    if not root.is_dir():
        raise NotADirectoryError(f"no such directory: {_display(root)}")
    found = [
        _display(match)
        for match in sorted(root.glob(pattern))
        if match.is_file() and not SKIP_DIRS & set(match.relative_to(root).parts)
    ]
    if len(found) > MAX_GLOB:
        return [*found[:MAX_GLOB], f"[truncated: {len(found) - MAX_GLOB} more matches]"]
    return found


@tool
async def grep(pattern: str, path: str = ".", include: str = "*") -> list[str]:
    """Search file contents for a regular expression and return `file:line: text` matches.

    `path` is a file or a directory to search under, `include` a glob filtering which file names
    are searched (`*.py`, `Makefile`). Binary and non-utf-8 files are skipped, as are `.git`,
    `node_modules` and other build directories.
    """
    try:
        expression = re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"invalid regular expression {pattern!r}: {exc}") from exc
    root = _target(path)
    if root.is_file():
        targets = [root]
    elif root.is_dir():
        targets = [file for file in _walk(root) if file.match(include)]
    else:
        raise FileNotFoundError(f"no such file or directory: {_display(root)}")

    matches: list[str] = []
    for file in targets:
        try:
            content = file.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for number, line in enumerate(content.splitlines(), 1):
            if expression.search(line):
                matches.append(f"{_display(file)}:{number}: {line.strip()}")
                if len(matches) >= MAX_MATCHES:
                    return [*matches, f"[truncated at {MAX_MATCHES} matches]"]
    return matches


@tool
async def bash(command: str, ctx: Context, timeout: float = BASH_TIMEOUT) -> str:
    """Run a shell command in the working directory and return its combined output.

    stdout and stderr come back interleaved, with the exit status appended when it is not zero and
    a truncation note when the output is very large. The command and anything it spawned are
    killed after `timeout` seconds. Prefer the file tools for reading and editing; use this for
    builds, tests, git and anything else the other tools cannot do.
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


def _kill_group(process: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        process.kill()
