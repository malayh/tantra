from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from agni.agents import build_agents
from agni.config import Config, ConfigError, load_config
from agni.guard import BashGuard
from agni.prompts import COMPACTION_INSTRUCTION, environment_block
from agni.repl import Io, Repl, terminal_io
from agni.skills import MergedSkills, skill_roots
from tantra import (
    BuiltinMemory,
    CompactionConfig,
    FileSystemStore,
    Harness,
    Memory,
    OpenAICompatible,
    Provider,
    PruneThenSummarize,
    Skills,
    SQLiteStore,
    Store,
)

USER_SKILLS = (".agents", "skills")
PROJECT_SKILLS = "skills"


def build_harness(
    provider: Provider,
    store: Store,
    *,
    model: str,
    memory: Memory | None = None,
    skills: Skills | None = None,
    auto: bool = False,
) -> Harness:
    return Harness(
        provider,
        store,
        build_agents(environment_block(Path.cwd()), auto),
        default_model=model,
        skills=skills,
        memory=memory,
        compactor=PruneThenSummarize(CompactionConfig(), instruction=COMPACTION_INSTRUCTION),
        hooks=[BashGuard()] if auto else [],
        default_permission="allow",
    )


async def serve(config: Config, *, auto: bool, resume: str | None, io: Io | None = None) -> int:
    store = SQLiteStore(config.database)
    await store.setup()
    memory_store = FileSystemStore(config.memory_root)
    await memory_store.setup()
    roots = skill_roots(Path.home().joinpath(*USER_SKILLS), Path.cwd() / PROJECT_SKILLS)
    skills = MergedSkills(roots) if roots else None
    harness = build_harness(
        OpenAICompatible(config.endpoint, config.api_key),
        store,
        model=config.model,
        memory=BuiltinMemory(memory_store),
        skills=skills,
        auto=auto,
    )
    if skills is not None:
        await skills.index()
        for path, reason in skills.skipped:
            print(f"agni: skipping skill {path}: {reason}", file=sys.stderr)
    if resume is not None and await store.header(resume) is None:
        print(f"agni: no session {resume}", file=sys.stderr)
        return 1
    session_id = resume if resume is not None else (await harness.create_session("build")).id
    await Repl(harness, session_id, config.models, io or terminal_io(config.home / "history")).run()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="agni", description="terminal coding agent")
    parser.add_argument("--auto", action="store_true", help="run unattended: no prompts, destructive commands blocked")
    parser.add_argument("--resume", metavar="SID", help="re-enter an existing session")
    args = parser.parse_args()
    try:
        config = load_config()
    except ConfigError as exc:
        print(f"agni: {exc}", file=sys.stderr)
        return 1
    return asyncio.run(serve(config, auto=args.auto, resume=args.resume))
