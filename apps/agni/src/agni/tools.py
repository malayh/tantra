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
    """Read one file from disk and return its text.

    Usage:
    - `path` is taken relative to the working directory unless it starts with `/` or `~`.
    - Read a file before you edit it: `edit` matches the exact bytes this returned.
    - Output is capped at 64000 characters; beyond that the last line reads
      `[truncated: N chars omitted]`. Never edit against a truncated read — narrow the target
      down with `grep` first.
    - Do not use this to find something inside a large file, and do not use it to list a
      directory. `grep` searches contents, `glob` searches names.
    - It returns the whole file, so there is nothing to page through: one call per file.
    - A file that is not valid utf-8 comes back as an error rather than as noise.
    """
    return _cap(_text(_target(path)))


@tool
async def write(path: str, content: str) -> str:
    """Create a file, or overwrite an existing one end to end with `content`.

    Usage:
    - Missing parent directories are created for you. Returns the character count and the path.
    - Do not use this on a file that already exists when only part of it needs to change — use
      `edit`. A whole-file write drops everything you did not include, including code you never
      read.
    - Read a file before overwriting it. Only a file you are creating needs no read first.
    - Never create documentation, README or summary files on your own initiative. Write them
      only when the user asks for them.
    """
    target = _target(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return f"wrote {len(content)} chars to {_display(target)}"


@tool
async def edit(path: str, old: str, new: str) -> str:
    """Replace one exact occurrence of `old` with `new` inside a file.

    Usage:
    - `old` must match byte for byte, indentation and line breaks included, and must appear
      exactly once in the file.
    - Nothing is written when `old` is absent, and nothing is written when it appears more than
      once — the error says how many times it matched. Retry with a longer `old` carrying more
      surrounding lines until it is unique.
    - Read the file first. Editing against remembered or truncated text is how the match fails.
    - Pass an empty `new` to delete the matched passage.
    - Do not use this to create a file or to rewrite one end to end — that is `write`.
    - One call changes one place. Call it again for the next change rather than stretching `old`
      across unrelated code.
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

    Usage:
    - `path` is the directory to search from and defaults to the working directory. Results come
      back relative to the working directory, sorted.
    - Use this to find files by name. Do not use it to find files by what is inside them — that
      is `grep` — and do not use it to read one file you can already name.
    - Capped at 500 matches, followed by a `[truncated: N more matches]` line. A truncated list
      means the pattern was too broad: narrow it instead of paging through.
    - `.git`, `.venv`, `node_modules`, `__pycache__`, `dist` and `build` are skipped.
    - For an open-ended hunt that will take several rounds of `glob` and `grep`, delegate to the
      `explore` sub-agent and keep the dead ends out of this conversation.
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
    """Search file contents for a regular expression and return `file:line: text` for each match.

    Usage:
    - `pattern` is a Python regular expression matched line by line. `path` is a file or a
      directory to search under; `include` is a glob over file names (`*.py`, `Makefile`).
    - Use this to find where something is defined or used. Do not use it to find files by name —
      that is `glob` — and read the file it names when you need the match in context.
    - Capped at 200 matches, followed by a `[truncated at 200 matches]` line. Treat that as a
      signal to tighten `pattern` or `include`, not to run the same search again.
    - Binary and non-utf-8 files are skipped, as are `.git`, `.venv`, `node_modules`,
      `__pycache__`, `dist` and `build`.
    - For a search that has to try many candidate names and directories, delegate to the
      `explore` sub-agent instead.
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

    Usage:
    - IMPORTANT: this tool is for terminal operations — builds, tests, linters, git, package
      managers, docker. Do NOT use it for file operations: reading, writing, editing, searching
      and finding files each have a dedicated tool that is safer for the job — an edit that
      refuses an ambiguous target, output truncated at a labelled boundary, a real error rather
      than a shell exit code.
    - Never use `echo` or `printf` to talk to the user. Anything you want read goes in your reply.
    - stdout and stderr come back interleaved. A non-zero exit appends `[exit status N]`, and
      output over 64000 characters is cut with a `[truncated: N chars omitted]` note.
    - The command runs in its own process group and is killed with everything it spawned after
      `timeout` seconds, 120 by default. Raise `timeout` for a slow test suite rather than
      splitting the run into pieces.
    - There is no interactive terminal. Pass non-interactive flags (`--yes`, `--no-pager`) and
      never start something that waits for input or never returns, such as a dev server in the
      foreground.
    - Commit only when the user has explicitly asked. Never push, force-push, `reset --hard` or
      delete a branch on your own initiative.
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
