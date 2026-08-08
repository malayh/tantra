from __future__ import annotations

from collections.abc import Mapping
from fnmatch import fnmatchcase

from tantra.errors import TantraError

PERMISSIONS = ("allow", "ask", "deny")

_STRICTNESS = {"allow": 0, "ask": 1, "deny": 2}


def check_permission(label: str, value: str) -> str:
    if value not in PERMISSIONS:
        raise TantraError(f"{label}: invalid permission {value!r}; expected one of {list(PERMISSIONS)}")
    return value


def decide(name: str, rules: Mapping[str, str], tool_permission: str | None, default: str) -> str:
    """Resolve a tool name to `allow`, `ask` or `deny`.

    The longest matching glob wins; equal-length patterns resolve to the most restrictive value.
    With no matching rule the tool's declared permission applies, then `default`.
    """
    matched: str | None = None
    width = -1
    for pattern, value in rules.items():
        if not fnmatchcase(name, pattern):
            continue
        if len(pattern) > width:
            matched, width = value, len(pattern)
        elif len(pattern) == width and _STRICTNESS[value] > _STRICTNESS[matched]:
            matched = value
    if matched is not None:
        return matched
    return tool_permission or default
