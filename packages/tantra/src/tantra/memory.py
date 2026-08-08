from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from tantra.errors import TantraError
from tantra.providers.base import Embedder
from tantra.tools import Context, tool

KEYWORD = "keyword"
ROW_METHODS = ("memory_put", "memory_get", "memory_all")
NO_MEMORY = "no memory configured: this harness was built without Harness(memory=...)"

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

    async def supersede(self, old_id: str, new: MemoryWrite) -> str:
        """Write `new` and point `old_id` at it. The old row stays readable via `get`."""

    async def delete(self, mid: str) -> None:
        """Soft-delete a row: `recall` stops returning it, `get` still does."""


def _tokens(text: str) -> set[str]:
    return {token for token in _SPLIT.split(text.lower()) if token}


def _row_tokens(row: MemoryRecord) -> set[str]:
    return _tokens(" ".join([row.title, row.body, *row.tags, *row.entities]))


def _embed_text(row: MemoryRecord) -> str:
    parts = [row.title, row.body, " ".join(row.entities)]
    return "\n".join(part for part in parts if part)


def _vector(values: Any) -> list[float]:
    return [float(value) for value in values]


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
        want_kind = kind.casefold() if kind is not None else None
        want_tags = {tag.casefold() for tag in tags} if tags else None
        want_entity = entity.casefold() if entity is not None else None
        hits: list[MemoryHit] = []
        for row in await self.store.memory_all():
            if row.deleted or row.superseded_by is not None:
                continue
            if want_kind is not None and row.kind.casefold() != want_kind:
                continue
            if want_tags and not want_tags <= {tag.casefold() for tag in row.tags}:
                continue
            if want_entity is not None and want_entity not in {name.casefold() for name in row.entities}:
                continue
            if metadata and any(row.metadata.get(key) != value for key, value in metadata.items()):
                continue
            score = len(wanted & _row_tokens(row)) / len(wanted)
            if score > 0:
                hits.append(MemoryHit(memory=row, score=score, mode=KEYWORD))
        hits.sort(key=lambda hit: (hit.score, hit.memory.created_at), reverse=True)
        return hits[:k]

    async def supersede(self, old_id: str, new: MemoryWrite) -> str:
        old = await self.store.memory_get(old_id)
        if old is None:
            raise TantraError(f"unknown memory {old_id!r}")
        if old.deleted:
            raise TantraError(f"memory {old_id!r} is deleted and cannot be superseded")
        if old.superseded_by is not None:
            raise TantraError(f"memory {old_id!r} is already superseded by {old.superseded_by!r}")
        new_id = await self.write(new)
        old.superseded_by = new_id
        await self.store.memory_put(old)
        return new_id

    async def delete(self, mid: str) -> None:
        row = await self.store.memory_get(mid)
        if row is None:
            raise TantraError(f"unknown memory {mid!r}")
        row.deleted = True
        await self.store.memory_put(row)

    async def backfill(self) -> int:
        if self.embedder is None:
            raise TantraError("backfill needs an embedder; this BuiltinMemory was built without one")
        rows = [
            row
            for row in await self.store.memory_all()
            if row.embedding is None and not row.deleted and row.superseded_by is None
        ]
        if not rows:
            return 0
        vectors = await self.embedder.embed([_embed_text(row) for row in rows])
        repaired = [(row, _vector(vector)) for row, vector in zip(rows, vectors, strict=True)]
        for row, vector in repaired:
            row.embedding = vector
            await self.store.memory_put(row)
        return len(repaired)


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
    hits = await ctx.memory.recall(query, k=k, kind=kind, tags=list(tags) if tags else None, entity=entity)
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
