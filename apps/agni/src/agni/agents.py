from __future__ import annotations

from agni.prompts import AUTO_NOTE, BUILD_PROMPT, EXPLORE_PROMPT
from agni.tools import bash, edit, glob, grep, read, write
from tantra import Agent, memory_recall, memory_write

READ_RULES = {
    "read": "allow",
    "glob": "allow",
    "grep": "allow",
    "explore": "allow",
    "skill": "allow",
    "memory_*": "allow",
}

ASK_RULES = {"write": "ask", "edit": "ask", "bash": "ask"}

AUTO_RULES = {"write": "allow", "edit": "allow", "bash": "allow"}


class Explore(Agent):
    """Delegate a read-only investigation of this codebase to a sub-agent and get a written report back.

    Usage:
    - Put one self-contained question in `task`. The sub-agent starts with no memory of this conversation, so
      spell out the names, paths and constraints it needs.
    - Use it for open-ended searching that would take several rounds of `grep` and `glob` — "where is
      authentication enforced", "what calls this function", "how does the migration system work". The searching
      burns the sub-agent's context, not yours; only the report comes back.
    - Do not use it when you already know the file: call `read`. Do not use it for a single pattern you could
      match yourself: call `grep`.
    - It cannot write, edit or run commands, so never delegate a change to it.
    - It answers in prose with paths, line numbers and short excerpts, not with file contents. Read the files it
      names before you edit them.
    """

    name = "explore"
    prompt = EXPLORE_PROMPT
    tools = [read, glob, grep]
    permissions = {"read": "allow", "glob": "allow", "grep": "allow", "skill": "allow"}


class Build(Agent):
    name = "build"
    prompt = BUILD_PROMPT
    tools = [read, write, edit, glob, grep, bash, memory_write, memory_recall]
    subagents = [Explore]
    permissions = {**READ_RULES, **ASK_RULES}


class AutoBuild(Build):
    name = "build"
    permissions = {**READ_RULES, **AUTO_RULES}


def build_agents(env: str, auto: bool) -> list[type[Agent]]:
    explore = type(
        "Explore",
        (Explore,),
        {"name": "explore", "__doc__": Explore.__doc__, "prompt": f"{EXPLORE_PROMPT}\n\n{env}"},
    )
    base = AutoBuild if auto else Build
    body = f"{BUILD_PROMPT}\n\n{AUTO_NOTE}" if auto else BUILD_PROMPT
    build = type("Build", (base,), {"name": "build", "prompt": f"{body}\n\n{env}", "subagents": [explore]})
    return [build, explore]
