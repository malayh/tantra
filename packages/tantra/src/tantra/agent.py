from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from tantra.errors import TantraError

if TYPE_CHECKING:
    from tantra.context import TurnContext
    from tantra.tools import Tool

_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


class Agent:
    """Declarative agent: values and function references only, never live I/O.

    A resume runs in another process and looks the agent up by name from the persisted header, so
    an `Agent` holding a client or a pool could not survive it.
    """

    name: str | None = None
    model: str | None = None
    prompt: str | Callable[[TurnContext], str | Awaitable[str]] = ""
    tools: list[Tool] = []
    subagents: list[type[Agent]] = []
    permissions: dict[str, str] = {}
    max_steps: int = 40
    output_schema: type[BaseModel] | None = None


def agent_name(agent: type[Agent]) -> str:
    declared = agent.__dict__.get("name")
    if declared:
        return str(declared)
    return _BOUNDARY.sub("_", agent.__name__).lower()


def build_name_table(agents: Iterable[type[Agent]]) -> dict[str, type[Agent]]:
    table: dict[str, type[Agent]] = {}
    pending: list[Any] = list(agents)
    while pending:
        agent = pending.pop(0)
        if not (isinstance(agent, type) and issubclass(agent, Agent)):
            raise TantraError(f"{agent!r} is not an Agent subclass")
        name = agent_name(agent)
        seen = table.get(name)
        if seen is agent:
            continue
        if seen is not None:
            raise TantraError(f"duplicate agent name {name!r}: {seen.__qualname__} and {agent.__qualname__}")
        table[name] = agent
        pending.extend(agent.subagents)
    return table
