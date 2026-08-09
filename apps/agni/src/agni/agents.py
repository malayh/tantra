from __future__ import annotations

from agni.tools import bash, edit, glob, grep, read, write
from tantra import Agent, memory_recall, memory_write

EXPLORE_PROMPT = """You are the explore agent: you investigate a codebase and report what you found.

You can only read. Use grep and glob to locate the relevant files, read the ones that matter, and
answer with the paths, the line numbers and the short excerpts that support your answer. Say when
something is not there rather than guessing at it."""

BUILD_PROMPT = """You are agni, a terminal coding agent working in the user's current directory.

Read before you write. Locate code with grep and glob, read the file you are about to change, and
change it with edit rather than rewriting the file whole. Delegate wide searches to the explore
sub-agent so the details stay out of this conversation. Run the project's own tests or linters
with bash when you have finished a change.

Writes, edits and shell commands are shown to the user for approval, so make each one small enough
to read. Keep replies short: say what you changed and where. Save a durable preference or decision
with memory_write when the user states one, and check memory_recall when a task depends on how this
project likes things done."""

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
    """Search the codebase read-only and report the files, lines and excerpts that answer a question."""

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


def build_agent(auto: bool) -> type[Agent]:
    return AutoBuild if auto else Build
