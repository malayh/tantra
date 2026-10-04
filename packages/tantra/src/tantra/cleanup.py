from __future__ import annotations

import base64
import hashlib
import json
import math
import sqlite3
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Literal
from uuid import UUID

from tantra.errors import CommandTimeout, CoordinatorUnavailable, LeaseLost, TantraError
from tantra.events import SessionHeader

CleanupOutcome = Literal["candidate", "deleted", "absent", "active", "changed", "failed", "unknown"]
OUTCOMES: tuple[CleanupOutcome, ...] = ("candidate", "deleted", "absent", "active", "changed", "failed", "unknown")


@dataclass(frozen=True)
class CleanupSelector:
    root_ids: Collection[UUID] | None = None
    metadata: Mapping[str, Any] | None = None
    inactive_before: datetime | None = None

    def __post_init__(self) -> None:
        if self.root_ids is not None:
            if isinstance(self.root_ids, str | bytes) or not isinstance(self.root_ids, Collection):
                raise TypeError("root_ids must be a collection of UUIDs")
            if any(not isinstance(value, UUID) for value in self.root_ids):
                raise TypeError("root_ids must contain UUIDs")
            object.__setattr__(self, "root_ids", tuple(dict.fromkeys(self.root_ids)))
        if self.metadata is not None:
            if not isinstance(self.metadata, Mapping):
                raise TypeError("metadata must be a mapping")
            for key, value in self.metadata.items():
                if not isinstance(key, str):
                    raise TypeError("metadata keys must be strings")
                if value is not None and type(value) not in (str, bool, int, float):
                    raise TypeError("metadata values must be JSON scalars")
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError("metadata numbers must be finite")
            object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        if self.inactive_before is not None:
            if not isinstance(self.inactive_before, datetime):
                raise TypeError("inactive_before must be a datetime")
            if self.inactive_before.utcoffset() is None:
                raise ValueError("inactive_before must be timezone-aware")
            object.__setattr__(self, "inactive_before", self.inactive_before.astimezone(UTC))
        if self.root_ids is None and not self.metadata and self.inactive_before is None:
            raise ValueError("cleanup requires a scoped selector")


@dataclass(frozen=True)
class CleanupResult:
    root_id: UUID
    outcome: CleanupOutcome
    error_code: str | None = None


@dataclass(frozen=True)
class CleanupReport:
    results: tuple[CleanupResult, ...]
    next_after: str | None = None
    error_code: str | None = None

    @property
    def counts(self) -> dict[CleanupOutcome, int]:
        return {outcome: sum(result.outcome == outcome for result in self.results) for outcome in OUTCOMES}


@dataclass(frozen=True)
class CleanupCandidate:
    root_id: str
    created_at: datetime
    updated_at: datetime
    revision: str
    active: bool

    @property
    def cursor(self) -> str:
        data = [1, self.created_at.astimezone(UTC).isoformat(), str(UUID(hex=self.root_id))]
        return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).decode().rstrip("=")


class CleanupChanged(TantraError): ...


def cleanup_cursor(after: str | None) -> tuple[datetime, str] | None:
    if after is None:
        return None
    if not isinstance(after, str):
        raise TypeError("after must be a cleanup cursor string")
    if not after or len(after) > 256:
        raise ValueError("invalid cleanup cursor")
    try:
        raw = base64.b64decode(after + "=" * (-len(after) % 4), altchars=b"-_", validate=True)
        version, stamp, sid = json.loads(raw)
        if type(version) is not int or version != 1:
            raise ValueError
        created = datetime.fromisoformat(stamp)
        if created.utcoffset() is None:
            raise ValueError
        return created.astimezone(UTC), UUID(sid).hex
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("invalid cleanup cursor") from exc


def check_revision(expected: str | None, actual: str) -> None:
    if expected is not None and expected != actual:
        raise CleanupChanged("selected tree changed")


def tree_revision(headers: Sequence[SessionHeader]) -> str:
    value = [header.model_dump(mode="json") for header in sorted(headers, key=lambda header: header.id)]
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def cleanup_matches(header: SessionHeader, selector: CleanupSelector) -> bool:
    if selector.root_ids is not None and UUID(hex=header.id) not in selector.root_ids:
        return False
    for key, wanted in (selector.metadata or {}).items():
        if key not in header.metadata:
            return False
        value = header.metadata[key]
        if (isinstance(value, bool) != isinstance(wanted, bool)) or value != wanted:
            return False
    return True


def cleanup_trees(
    headers: Sequence[SessionHeader], selector: CleanupSelector, *, after: str | None = None
) -> list[list[SessionHeader]]:
    by_id = {header.id: header for header in headers}
    if selector.root_ids is not None:
        for root in selector.root_ids:
            header = by_id.get(root.hex)
            if header is not None and (header.parent_id is not None or header.root_id not in (None, header.id)):
                raise TantraError("cleanup root_ids contains a live child")
    cursor = cleanup_cursor(after)
    roots = sorted(
        (h for h in headers if h.parent_id is None and h.root_id in (None, h.id)),
        key=lambda h: (h.created_at, h.id),
    )
    children: dict[str, list[SessionHeader]] = {}
    for header in headers:
        if header.parent_id is not None:
            children.setdefault(header.parent_id, []).append(header)
    trees = []
    for root in roots:
        if cursor is not None and (root.created_at, root.id) <= cursor:
            continue
        if not cleanup_matches(root, selector):
            continue
        tree = [root]
        for header in tree:
            tree.extend(children.get(header.id, ()))
        updated = max(header.updated_at for header in tree)
        if selector.inactive_before is not None and updated >= selector.inactive_before:
            continue
        trees.append(tree)
    return trees


def cleanup_infrastructure(exc: Exception) -> bool:
    if isinstance(
        exc, CoordinatorUnavailable | CommandTimeout | LeaseLost | OSError | TimeoutError | sqlite3.OperationalError
    ):
        return True
    try:
        import psycopg
    except ImportError:
        return False
    return isinstance(exc, psycopg.OperationalError | psycopg.InterfaceError)
