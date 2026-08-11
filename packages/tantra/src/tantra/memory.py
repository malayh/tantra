from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from tantra.errors import TantraError
from tantra.providers.base import Embedder
from tantra.stores.base import matches_metadata
from tantra.tools import Context, Tool, tool

KEYWORD = "keyword"
VECTOR = "vector"
CANDIDATE_FLOOR = 20
ROW_METHODS = ("memory_put", "memory_get", "memory_all")
NO_MEMORY = "no memory configured: this harness was built without Harness(memory=...)"

MemoryScope = Callable[[Context], dict[str, Any]]

_SPLIT = re.compile(r"[^a-z0-9]+")


def _now() -> datetime:
    return datetime.now(UTC)


class MemoryWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str
    title: str
    body: str
    tags: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MemoryRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    kind: str
    title: str
    body: str
    tags: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None
    created_at: datetime = Field(default_factory=_now)
    deleted: bool = False
    superseded_by: str | None = None


@dataclass(frozen=True)
class MemoryHit:
    memory: MemoryRecord
    score: float
    mode: str


class Memory(Protocol):
    """Durable rows an agent writes and recalls across turns, by verbs rather than injection.

    Nothing is recalled automatically: the model asks for what it wants through the `memory_recall`
    and `memory_write` tools. `metadata` scopes rows the way session metadata scopes sessions —
    recall filters on it and tantra enforces nothing.
    """

    async def write(self, m: MemoryWrite) -> str:
        """Store a new memory row and return its id."""

    async def get(self, mid: str) -> MemoryRecord | None:
        """Return one row by id, including deleted and superseded ones, or None when unknown."""

    async def recall(
        self,
        q: str,
        *,
        k: int = 5,
        kind: str | None = None,
        tags: list[str] | None = None,
        entity: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[MemoryHit]:
        """Return up to `k` live rows matching `q`, best first.

        Deleted and superseded rows never match. `tags` requires every listed tag; `metadata`
        matches as a subset. Each hit reports the `mode` that produced it, so a backend degrading
        to keyword-only says so rather than pretending it ran a vector search.
        """

    async def supersede(self, old_id: str, new: MemoryWrite, *, scope: dict[str, Any] | None = None) -> str:
        """Write `new` and point `old_id` at it. The old row stays readable via `get`.

        A row whose metadata does not match `scope` is refused as unknown, so a caller holding the
        wrong scope cannot learn that the id exists.
        """

    async def delete(self, mid: str, *, scope: dict[str, Any] | None = None) -> bool:
        """Soft-delete a row and report whether it is now gone: `recall` stops returning it, `get` still does.

        Never raises: an unknown id and a row whose metadata does not match `scope` both return
        False, and deleting a row that is already deleted returns True again.
        """


def _tokens(text: str) -> set[str]:
    return {token for token in _SPLIT.split(text.lower()) if token}


def _row_tokens(row: MemoryRecord) -> set[str]:
    return _tokens(" ".join([row.title, row.body, *row.tags, *row.entities]))


def _embed_text(row: MemoryRecord) -> str:
    parts = [row.title, row.body, " ".join(row.entities)]
    return "\n".join(part for part in parts if part)


def _vector(values: Any) -> list[float]:
    vector = [float(value) for value in values]
    if not all(math.isfinite(value) for value in vector):
        raise ValueError("embedding holds non-finite values")
    return vector


@dataclass(frozen=True)
class _Filter:
    kind: str | None
    tags: set[str] | None
    entity: str | None
    metadata: dict[str, Any] | None

    def passes(self, row: MemoryRecord) -> bool:
        if row.deleted or row.superseded_by is not None:
            return False
        if self.kind is not None and row.kind.casefold() != self.kind:
            return False
        if self.tags and not self.tags <= {tag.casefold() for tag in row.tags}:
            return False
        if self.entity is not None and self.entity not in {name.casefold() for name in row.entities}:
            return False
        if not matches_metadata(row.metadata, self.metadata):
            return False
        return True


class BuiltinMemory:
    def __init__(self, store: Any, embedder: Embedder | None = None) -> None:
        missing = [name for name in ROW_METHODS if not hasattr(store, name)]
        if missing:
            raise TantraError(f"{type(store).__name__} stores no memory rows: missing {missing}")
        self.store = store
        self.embedder = embedder

    async def _embed(self, row: MemoryRecord) -> None:
        if self.embedder is None:
            return
        try:
            vectors = await self.embedder.embed([_embed_text(row)])
            row.embedding = _vector(vectors[0])
        except Exception:
            row.embedding = None

    async def write(self, m: MemoryWrite) -> str:
        row = MemoryRecord(
            id=uuid4().hex,
            kind=m.kind,
            title=m.title,
            body=m.body,
            tags=list(m.tags),
            entities=list(m.entities),
            metadata=dict(m.metadata),
        )
        await self._embed(row)
        await self.store.memory_put(row)
        return row.id

    async def get(self, mid: str) -> MemoryRecord | None:
        return await self.store.memory_get(mid)

    async def recall(
        self,
        q: str,
        *,
        k: int = 5,
        kind: str | None = None,
        tags: list[str] | None = None,
        entity: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[MemoryHit]:
        wanted = _tokens(q)
        if not wanted or k <= 0:
            return []
        rule = _Filter(
            kind=kind.casefold() if kind is not None else None,
            tags={tag.casefold() for tag in tags} if tags else None,
            entity=entity.casefold() if entity is not None else None,
            metadata=metadata,
        )
        best: dict[str, MemoryHit] = {}
        for hit in await self._keyword(wanted, rule) + await self._vector_pass(q, k, rule):
            current = best.get(hit.memory.id)
            if current is None or hit.score > current.score:
                best[hit.memory.id] = hit
        hits = sorted(best.values(), key=lambda hit: (hit.score, hit.memory.created_at), reverse=True)
        return hits[:k]

    async def _keyword(self, wanted: set[str], rule: _Filter) -> list[MemoryHit]:
        hits = []
        for row in await self.store.memory_all(metadata=rule.metadata):
            if not rule.passes(row):
                continue
            score = len(wanted & _row_tokens(row)) / len(wanted)
            if score > 0:
                hits.append(MemoryHit(memory=row, score=score, mode=KEYWORD))
        return hits

    async def _vector_pass(self, q: str, k: int, rule: _Filter) -> list[MemoryHit]:
        search = getattr(self.store, "memory_search", None)
        if self.embedder is None or search is None:
            return []
        try:
            vector = _vector((await self.embedder.embed([q]))[0])
        except Exception:
            return []
        found = await search(vector, max(4 * k, CANDIDATE_FLOOR))
        if found is None:
            return []
        hits = []
        for row, distance in found:
            score = 1.0 - distance
            if rule.passes(row) and score > 0:
                hits.append(MemoryHit(memory=row, score=score, mode=VECTOR))
        return hits

    async def supersede(self, old_id: str, new: MemoryWrite, *, scope: dict[str, Any] | None = None) -> str:
        old = await self.store.memory_get(old_id)
        if old is None or not matches_metadata(old.metadata, scope):
            raise TantraError(f"unknown memory {old_id!r}")
        if old.deleted:
            raise TantraError(f"memory {old_id!r} is deleted and cannot be superseded")
        if old.superseded_by is not None:
            raise TantraError(f"memory {old_id!r} is already superseded by {old.superseded_by!r}")
        new_id = await self.write(new)
        old.superseded_by = new_id
        await self.store.memory_put(old)
        return new_id

    async def delete(self, mid: str, *, scope: dict[str, Any] | None = None) -> bool:
        row = await self.store.memory_get(mid)
        if row is None or not matches_metadata(row.metadata, scope):
            return False
        row.deleted = True
        await self.store.memory_put(row)
        return True

    async def backfill(self) -> int:
        if self.embedder is None:
            raise TantraError("backfill needs an embedder; this BuiltinMemory was built without one")
        rows = [row for row in await self.store.memory_all() if row.embedding is None]
        if not rows:
            return 0
        vectors = await self.embedder.embed([_embed_text(row) for row in rows])
        repaired = [(row, _vector(vector)) for row, vector in zip(rows, vectors, strict=True)]
        for row, vector in repaired:
            row.embedding = vector
            await self.store.memory_put(row)
        return len(repaired)


def memory_tools(scope: MemoryScope | None = None) -> tuple[Tool, Tool]:
    @tool
    async def memory_write(
        kind: str,
        title: str,
        body: str,
        ctx: Context,
        tags: list[str] | None = None,
        entities: list[str] | None = None,
    ) -> str:
        """Save one durable memory and return its id.

        Write a single self-contained fact, decision or preference per call: `title` is a one-line
        summary, `body` carries the detail worth re-reading months later. `kind` groups rows for
        filtered recall — pick a short lowercase label and reuse it ("preference", "decision", "fact").
        `tags` and `entities` are recall filters, so put the names of people, services, files or
        projects the memory is about into `entities`. Do not save anything you can look up again.
        """
        if ctx.memory is None:
            raise TantraError(NO_MEMORY)
        return await ctx.memory.write(
            MemoryWrite(
                kind=kind,
                title=title,
                body=body,
                tags=list(tags or []),
                entities=list(entities or []),
                metadata=dict(scope(ctx)) if scope else {},
            )
        )

    @tool
    async def memory_recall(
        query: str,
        ctx: Context,
        k: int = 5,
        kind: str | None = None,
        tags: list[str] | None = None,
        entity: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search saved memories and return the best matches, highest score first.

        Matching is keyword overlap against each memory's title, body, tags and entities, so query with
        the words you expect to find written there — a short phrase beats a single word. `kind`, `tags`
        and `entity` narrow the search; `tags` requires every tag listed. Deleted and superseded
        memories are never returned, and an empty list means nothing was saved about this yet.
        """
        if ctx.memory is None:
            raise TantraError(NO_MEMORY)
        hits = await ctx.memory.recall(
            query,
            k=k,
            kind=kind,
            tags=list(tags) if tags else None,
            entity=entity,
            metadata=scope(ctx) if scope else None,
        )
        return [
            {
                "id": hit.memory.id,
                "kind": hit.memory.kind,
                "title": hit.memory.title,
                "body": hit.memory.body,
                "tags": list(hit.memory.tags),
                "entities": list(hit.memory.entities),
                "score": hit.score,
                "mode": hit.mode,
            }
            for hit in hits
        ]

    return memory_write, memory_recall


memory_write, memory_recall = memory_tools()
