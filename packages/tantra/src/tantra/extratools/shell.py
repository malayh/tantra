from __future__ import annotations

from typing import Literal

from tantra.hooks import Hook
from tantra.tools import Tool


def bash(*, timeout: float = 120.0) -> Tool:
    raise NotImplementedError


class ShellGuard(Hook):
    def __init__(self, *, on_trip: Literal["deny", "ask"] = "deny", deny_extra: list[str] | None = None) -> None:
        raise NotImplementedError
