import sqlite3
from pathlib import Path

import pytest

from tantra.errors import CorruptLog, SessionBusy, SessionExists, SessionNotFound, TantraError
from tantra.events import InputQueued, SessionHeader, TextPart, TurnStarted
from tantra.stores.memory import MemoryStore
from tantra.stores.sqlite import SQLiteStore


@pytest.fixture(params=("memory", "sqlite"))
async def store(request: pytest.FixtureRequest, tmp_path: Path) -> MemoryStore | SQLiteStore:
    result: MemoryStore | SQLiteStore
    if request.param == "memory":
        result = MemoryStore()
    else:
        result = SQLiteStore(tmp_path / "tantra.db")
    await result.setup()
    return result


async def create_tree(store: MemoryStore | SQLiteStore) -> None:
    await store.create(SessionHeader(id="root", root_id="root", agent="test"))
    await store.create(SessionHeader(id="child", root_id="root", parent_id="root", agent="test"))
    await store.create(SessionHeader(id="legacy", parent_id="child", agent="test"))
    await store.create(SessionHeader(id="other", root_id="other", agent="test"))


async def test_delete_tree_purges_legacy_descendants_and_preserves_other_roots(
    store: MemoryStore | SQLiteStore,
) -> None:
    await create_tree(store)

    assert await store.delete_tree("root") == ["root", "child", "legacy"]
    assert await store.header("root") is None
    assert await store.header("child") is None
    assert await store.header("legacy") is None
    assert await store.header("other") is not None


async def test_unknown_delete_and_live_child_rejection(store: MemoryStore | SQLiteStore) -> None:
    await create_tree(store)

    assert await store.delete_tree("missing") == []
    with pytest.raises(TantraError, match="not a root"):
        await store.delete_tree("child")
    assert await store.header("child") is not None


async def test_header_and_journal_activity_block_delete_but_force_succeeds(
    store: MemoryStore | SQLiteStore,
) -> None:
    headers = (
        SessionHeader(id="queued", root_id="queued", agent="test", status="queued"),
        SessionHeader(id="running", root_id="running", agent="test", status="running"),
        SessionHeader(id="awaiting", root_id="awaiting", agent="test", status="awaiting_input"),
        SessionHeader(id="ask", root_id="ask", agent="test", pending_ask="ask-1"),
        SessionHeader(id="turn", root_id="turn", agent="test", current_turn_id="turn-1"),
    )
    for header in headers:
        await store.create(header)
        with pytest.raises(SessionBusy):
            await store.delete_tree(header.id)
        assert await store.delete_tree(header.id, allow_active=True) == [header.id]

    for sid, event in (
        ("pending", InputQueued(command_id="pending-1", input="go")),
        ("started", TurnStarted(turn_id="started-1", input="go")),
    ):
        await store.create(SessionHeader(id=sid, root_id=sid, agent="test"))
        await store.append(sid, [event])
        await store.put_header(SessionHeader(id=sid, root_id=sid, agent="test"))
        called = []
        with pytest.raises(SessionBusy):
            await store.delete_tree(sid, before_delete=lambda ids, target=called: target.extend(ids))
        assert called == []
        assert await store.delete_tree(sid, allow_active=True) == [sid]


async def test_deleted_ids_and_deleted_ancestry_cannot_be_reused(store: MemoryStore | SQLiteStore) -> None:
    await create_tree(store)
    await store.delete_tree("root")

    with pytest.raises(SessionExists):
        await store.create(SessionHeader(id="root", root_id="root", agent="test"))
    with pytest.raises(SessionExists):
        await store.create(SessionHeader(id="child", root_id="child", agent="test"))
    with pytest.raises(SessionNotFound):
        await store.create(SessionHeader(id="new-root-child", root_id="root", parent_id="fresh", agent="test"))
    with pytest.raises(SessionNotFound):
        await store.create(SessionHeader(id="new-parent-child", root_id="fresh", parent_id="child", agent="test"))

    await store.create(SessionHeader(id="fresh", root_id="fresh", agent="test"))
    await store.create(SessionHeader(id="fresh-child", root_id="fresh", parent_id="fresh", agent="test"))
    assert await store.header("fresh-child") is not None


async def test_sqlite_force_delete_does_not_decode_the_journal(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "tantra.db")
    await store.setup()
    await store.create(SessionHeader(id="root", root_id="root", agent="test"))
    with sqlite3.connect(store.path) as conn:
        conn.execute("INSERT INTO events (session_id, seq, stamped) VALUES (?, ?, ?)", ("root", 1, "{}"))

    called = []
    with pytest.raises(CorruptLog):
        await store.delete_tree("root", before_delete=lambda ids: called.extend(ids))
    assert called == []
    assert await store.delete_tree("root", allow_active=True) == ["root"]


async def test_sqlite_callback_failure_rolls_back_without_marking_ids(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "tantra.db")
    await store.setup()
    await create_tree(store)
    seen = []

    def fail(ids: list[str]) -> None:
        seen.extend(ids)
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        await store.delete_tree("root", before_delete=fail)

    assert seen == ["root", "child", "legacy"]
    assert await store.header("root") is not None
    with sqlite3.connect(store.path) as conn:
        assert conn.execute("SELECT count(*) FROM deleted_sessions").fetchone()[0] == 0


async def test_sqlite_purge_failure_rolls_back_events_sessions_and_markers(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "tantra.db")
    await store.setup()
    await store.create(SessionHeader(id="root", root_id="root", agent="test"))
    await store.create(SessionHeader(id="child", root_id="root", parent_id="root", agent="test"))
    await store.append("child", [TextPart(sample_id="sample", text="kept")])
    with sqlite3.connect(store.path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_child_delete BEFORE DELETE ON sessions WHEN OLD.id = 'child' "
            "BEGIN SELECT RAISE(ABORT, 'stop'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="stop"):
        await store.delete_tree("root")

    assert await store.header("root") is not None
    assert await store.header("child") is not None
    assert [item.event for item in await store.read_page("child")] == [TextPart(sample_id="sample", text="kept")]
    with sqlite3.connect(store.path) as conn:
        assert conn.execute("SELECT count(*) FROM deleted_sessions").fetchone()[0] == 0
