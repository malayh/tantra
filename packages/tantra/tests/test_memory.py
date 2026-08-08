from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.errors import CorruptLog, TantraError
from tantra.events import ToolCallCompleted, TurnCompleted
from tantra.harness import Harness
from tantra.memory import BuiltinMemory, MemoryRecord, MemoryWrite, memory_recall, memory_write
from tantra.providers.base import ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.fs import FileSystemStore
from tantra.stores.memory import MemoryStore

DEPLOYS = MemoryWrite(
    kind="preference",
    title="Deploys run on Fridays",
    body="The team ships to production every Friday afternoon.",
    tags=["process"],
    entities=["deploy"],
)
MIMIR = MemoryWrite(
    kind="fact",
    title="The p99 panel reads from Mimir",
    body="Latency percentiles for the checkout service come from Mimir, not Prometheus.",
    tags=["dashboard", "metrics"],
    entities=["mimir", "checkout"],
)
POSTGRES = MemoryWrite(
    kind="decision",
    title="Sessions live in Postgres",
    body="Session rows are stored in Postgres under a dedicated tantra schema.",
    tags=["storage"],
    entities=["postgres"],
)

QUERY = "where does the p99 panel get its latency data"


class FakeEmbedder:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[float(len(text)), float(sum(text.encode()) % 97)] for text in texts]


class RaisingEmbedder:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        raise RuntimeError("embedding service unreachable")


class JunkEmbedder:
    async def embed(self, texts: list[str]) -> list[list[Any]]:
        return [["junk"] for _ in texts]


class Bare:
    pass


class Librarian(Agent):
    prompt = "You remember things."
    tools = [memory_write, memory_recall]


@pytest.fixture(params=["memory", "fs"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    if request.param == "memory":
        return MemoryStore()
    return FileSystemStore(tmp_path / "store")


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def results(events: list[Any]) -> dict[str, ToolCallCompleted]:
    return {item.event.call_id: item.event for item in events if isinstance(item.event, ToolCallCompleted)}


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [item.event for item in events if isinstance(item.event, kind)]


async def build(samples: list[Sample], *, with_memory: bool = True) -> tuple[Harness, BuiltinMemory | None, str]:
    store = MemoryStore()
    memory = BuiltinMemory(store) if with_memory else None
    harness = Harness(FakeProvider(samples), store, [Librarian], default_model="fake/model", memory=memory)
    header = await harness.create_session(Librarian)
    return harness, memory, header.id


async def test_three_memories_recall_with_the_match_first_and_every_hit_marked_keyword(store: Any) -> None:
    memory = BuiltinMemory(store)
    for row in (DEPLOYS, MIMIR, POSTGRES):
        await memory.write(row)

    hits = await memory.recall(QUERY)

    assert len(hits) == 2
    assert [hit.memory.title for hit in hits][0] == MIMIR.title
    assert hits[0].score > hits[-1].score
    assert {hit.mode for hit in hits} == {"keyword"}
    assert [hit.memory.title for hit in await memory.recall(QUERY, k=1)] == [MIMIR.title]
    assert await memory.recall(QUERY, k=0) == []
    assert await memory.recall(QUERY, k=-1) == []


async def test_rows_scoring_equally_come_back_newest_first(store: Any) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for position in (1, 3, 0, 2):
        await store.memory_put(
            MemoryRecord(
                id=f"row{position}",
                kind="fact",
                title=MIMIR.title,
                body=f"Revision {position}.",
                created_at=base + timedelta(days=position),
            )
        )

    hits = await BuiltinMemory(store).recall("p99 panel mimir")

    assert {hit.score for hit in hits} == {1.0}
    assert [hit.memory.id for hit in hits] == ["row3", "row2", "row1", "row0"]


async def test_supersede_hides_the_old_row_from_recall_but_get_still_returns_it(store: Any) -> None:
    memory = BuiltinMemory(store)
    old_id = await memory.write(MIMIR)
    replacement = MemoryWrite(
        kind="fact",
        title="The p99 panel reads from Thanos",
        body="Latency percentiles for the checkout service moved off Mimir onto Thanos.",
        entities=["thanos", "checkout"],
    )

    new_id = await memory.supersede(old_id, replacement)

    assert new_id != old_id
    assert [hit.memory.id for hit in await memory.recall(QUERY)] == [new_id]
    stale = await memory.get(old_id)
    assert stale is not None
    assert stale.title == MIMIR.title
    assert stale.superseded_by == new_id


async def test_an_unreachable_embedder_still_commits_the_write_and_backfill_repairs_it(store: Any) -> None:
    broken = RaisingEmbedder()
    mid = await BuiltinMemory(store, broken).write(MIMIR)
    written = await BuiltinMemory(store).get(mid)
    assert broken.calls
    assert written is not None
    assert written.embedding is None

    embedder = FakeEmbedder()
    repaired = BuiltinMemory(store, embedder)
    assert await repaired.backfill() == 1

    row = await repaired.get(mid)
    assert row is not None
    assert row.embedding == (await FakeEmbedder().embed(embedder.calls[0]))[0]
    assert row.model_dump(exclude={"embedding"}) == written.model_dump(exclude={"embedding"})


async def test_a_junk_vector_at_write_time_is_the_embed_failed_case_and_leaves_the_store_readable(
    store: Any,
) -> None:
    memory = BuiltinMemory(store, JunkEmbedder())

    mid = await memory.write(MIMIR)

    row = await memory.get(mid)
    assert row is not None
    assert row.embedding is None
    assert [hit.memory.id for hit in await memory.recall(QUERY)] == [mid]
    assert [record.id for record in await store.memory_all()] == [mid]


async def test_a_junk_vector_at_backfill_time_raises_without_poisoning_any_row(store: Any) -> None:
    mid = await BuiltinMemory(store).write(MIMIR)
    memory = BuiltinMemory(store, JunkEmbedder())

    with pytest.raises(ValueError):
        await memory.backfill()

    row = await memory.get(mid)
    assert row is not None
    assert row.embedding is None
    assert [hit.memory.id for hit in await memory.recall(QUERY)] == [mid]
    assert await BuiltinMemory(store, FakeEmbedder()).backfill() == 1


async def test_a_corrupt_memory_row_file_raises_corrupt_log_naming_the_path(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path / "store")
    mid = await BuiltinMemory(store).write(MIMIR)
    path = tmp_path / "store" / "_memory" / f"{mid}.json"
    path.write_text('{"id": 4}')

    with pytest.raises(CorruptLog) as raised:
        await store.memory_all()
    assert str(path) in str(raised.value)

    with pytest.raises(CorruptLog) as caught:
        await store.memory_get(mid)
    assert str(path) in str(caught.value)


async def test_a_memory_write_may_not_smuggle_in_control_fields() -> None:
    with pytest.raises(ValidationError):
        MemoryWrite(kind="fact", title="t", body="b", deleted=True)
    with pytest.raises(ValidationError):
        MemoryWrite(kind="fact", title="t", body="b", id="forged")


async def test_write_then_get_returns_every_field_unchanged(store: Any) -> None:
    memory = BuiltinMemory(store)
    scoped = MemoryWrite(**MIMIR.model_dump() | {"metadata": {"company": 42}})

    mid = await memory.write(scoped)

    row = await memory.get(mid)
    assert row is not None
    assert row.id == mid
    assert row.metadata == {"company": 42}
    assert row.model_dump(exclude={"id", "created_at", "embedding"}) == {
        **scoped.model_dump(),
        "deleted": False,
        "superseded_by": None,
    }
    assert await memory.get("nope") is None


async def test_delete_hides_a_row_from_recall_while_get_still_reports_it(store: Any) -> None:
    memory = BuiltinMemory(store)
    mid = await memory.write(MIMIR)

    await memory.delete(mid)

    assert await memory.recall(QUERY) == []
    row = await memory.get(mid)
    assert row is not None
    assert row.deleted is True


async def test_deleting_or_superseding_an_unknown_id_raises(store: Any) -> None:
    memory = BuiltinMemory(store)

    with pytest.raises(TantraError, match="unknown memory 'ghost'"):
        await memory.delete("ghost")
    with pytest.raises(TantraError, match="unknown memory 'ghost'"):
        await memory.supersede("ghost", MIMIR)


async def test_a_dead_row_may_not_be_superseded_into_a_forked_chain(store: Any) -> None:
    memory = BuiltinMemory(store)
    once = await memory.write(MIMIR)
    replacement = MemoryWrite(kind="fact", title="Thanos serves p99", body="Moved off Mimir.")
    await memory.supersede(once, replacement)

    with pytest.raises(TantraError, match="already superseded"):
        await memory.supersede(once, replacement)

    dropped = await memory.write(DEPLOYS)
    await memory.delete(dropped)
    with pytest.raises(TantraError, match="is deleted and cannot be superseded"):
        await memory.supersede(dropped, replacement)

    assert len([row for row in await store.memory_all() if not row.deleted and row.superseded_by is None]) == 1


async def test_filters_narrow_recall_by_kind_tags_entity_and_metadata(store: Any) -> None:
    memory = BuiltinMemory(store)
    await memory.write(MemoryWrite(**MIMIR.model_dump() | {"metadata": {"company": 42, "user": 7}}))
    await memory.write(
        MemoryWrite(
            kind="preference",
            title="Show p99 latency by default",
            body="The default panel for the checkout service is p99 latency.",
            tags=["dashboard"],
            entities=["checkout"],
            metadata={"company": 7},
        )
    )

    assert [hit.memory.kind for hit in await memory.recall(QUERY, kind="fact")] == ["fact"]
    assert [hit.memory.kind for hit in await memory.recall(QUERY, tags=["dashboard", "metrics"])] == ["fact"]
    assert len(await memory.recall(QUERY, tags=["dashboard"])) == 2
    assert await memory.recall(QUERY, tags=["dashboard", "nope"]) == []
    assert [hit.memory.kind for hit in await memory.recall(QUERY, entity="mimir")] == ["fact"]
    assert len(await memory.recall(QUERY, entity="checkout")) == 2
    assert [hit.memory.kind for hit in await memory.recall(QUERY, metadata={"company": 42})] == ["fact"]
    assert [hit.memory.kind for hit in await memory.recall(QUERY, metadata={"company": 42, "user": 7})] == ["fact"]
    assert await memory.recall(QUERY, metadata={"company": 42, "user": 9}) == []


async def test_kind_tag_and_entity_filters_ignore_case(store: Any) -> None:
    memory = BuiltinMemory(store)
    await memory.write(
        MemoryWrite(
            kind="Fact",
            title=MIMIR.title,
            body=MIMIR.body,
            tags=["Dashboard"],
            entities=["Mimir", "Checkout"],
        )
    )

    assert len(await memory.recall(QUERY, entity="mimir")) == 1
    assert len(await memory.recall(QUERY, kind="fact")) == 1
    assert len(await memory.recall(QUERY, tags=["dashboard"])) == 1
    assert await memory.recall(QUERY, entity="thanos") == []


async def test_a_query_matching_nothing_returns_nothing_rather_than_everything(store: Any) -> None:
    memory = BuiltinMemory(store)
    for row in (DEPLOYS, MIMIR, POSTGRES):
        await memory.write(row)

    assert await memory.recall("kubernetes ingress annotations") == []
    assert await memory.recall("") == []
    assert await memory.recall("   ?!  ") == []


async def test_backfill_without_an_embedder_raises(store: Any) -> None:
    with pytest.raises(TantraError, match="backfill needs an embedder"):
        await BuiltinMemory(store).backfill()


async def test_backfill_embeds_the_live_rows_in_one_call_and_skips_dead_ones(store: Any) -> None:
    memory = BuiltinMemory(store)
    deleted_id = await memory.write(DEPLOYS)
    superseded_id = await memory.write(MIMIR)
    await memory.write(POSTGRES)
    await memory.delete(deleted_id)
    await memory.supersede(superseded_id, MemoryWrite(kind="fact", title="Thanos serves p99", body="Moved."))

    embedder = FakeEmbedder()
    repaired = BuiltinMemory(store, embedder)
    assert await repaired.backfill() == 2

    assert len(embedder.calls) == 1
    assert len(embedder.calls[0]) == 2
    assert (await repaired.get(deleted_id)).embedding is None
    assert (await repaired.get(superseded_id)).embedding is None
    assert all(hit.memory.embedding is not None for hit in await repaired.recall("postgres thanos"))


async def test_backfill_with_nothing_missing_returns_zero_without_calling_the_embedder(store: Any) -> None:
    embedder = FakeEmbedder()
    memory = BuiltinMemory(store, embedder)
    await memory.write(MIMIR)
    assert len(embedder.calls) == 1

    assert await memory.backfill() == 0
    assert len(embedder.calls) == 1


async def test_two_memory_instances_over_one_store_see_each_others_writes(store: Any) -> None:
    mid = await BuiltinMemory(store).write(MIMIR)

    reader = BuiltinMemory(store)

    assert (await reader.get(mid)).title == MIMIR.title
    assert [hit.memory.id for hit in await reader.recall(QUERY)] == [mid]


async def test_a_store_without_memory_rows_is_refused_at_construction() -> None:
    with pytest.raises(TantraError, match="Bare stores no memory rows"):
        BuiltinMemory(Bare())


async def test_memory_rows_are_invisible_to_session_listing(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path / "store")
    await store.setup()
    await BuiltinMemory(store).write(MIMIR)

    assert await store.list() == []


async def test_the_tools_write_and_recall_through_a_turn_without_leaking_vectors_or_metadata() -> None:
    written = json.dumps(
        {
            "kind": "fact",
            "title": MIMIR.title,
            "body": MIMIR.body,
            "tags": ["dashboard"],
            "entities": ["mimir"],
        }
    )
    harness, memory, sid = await build(
        [
            Sample(tool_calls=[call("memory_write", written, cid="w1")]),
            Sample(tool_calls=[call("memory_recall", json.dumps({"query": QUERY}), cid="r1")]),
            Sample(text="noted"),
        ]
    )

    events = await collect(harness.run(sid, "remember where p99 comes from"))

    completed = results(events)
    assert not completed["w1"].is_error
    mid = completed["w1"].result
    assert (await memory.get(mid)).title == MIMIR.title

    recalled = completed["r1"].result
    assert not completed["r1"].is_error
    assert [row["title"] for row in recalled] == [MIMIR.title]
    assert recalled[0]["id"] == mid
    assert recalled[0]["mode"] == "keyword"
    assert set(recalled[0]) == {"id", "kind", "title", "body", "tags", "entities", "score", "mode"}
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_a_harness_without_memory_turns_a_write_into_an_error_result_not_a_failed_turn() -> None:
    harness, _, sid = await build(
        [
            Sample(tool_calls=[call("memory_write", json.dumps({"kind": "fact", "title": "t", "body": "b"}), "w1")]),
            Sample(text="recovered"),
        ],
        with_memory=False,
    )

    events = await collect(harness.run(sid, "remember this"))

    completed = results(events)["w1"]
    assert completed.is_error
    assert "no memory configured" in completed.result
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"
