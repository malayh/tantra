import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from tantra.cleanup import CleanupChanged, CleanupSelector
from tantra.errors import CorruptLog, SessionBusy, TantraError
from tantra.events import InputQueued, SessionHeader, TextPart
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


def make_header(
    *,
    sid: str | None = None,
    root_id: str | None = None,
    parent_id: str | None = None,
    created_at: datetime,
    updated_at: datetime,
    metadata: dict[str, object] | None = None,
) -> SessionHeader:
    actor_id = sid or uuid4().hex
    return SessionHeader(
        id=actor_id,
        root_id=root_id or actor_id,
        parent_id=parent_id,
        agent="test",
        created_at=created_at,
        updated_at=updated_at,
        metadata=metadata or {},
    )


async def test_select_cleanup_filters_are_typed_and_combined(store: MemoryStore | SQLiteStore) -> None:
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    nullable = make_header(created_at=stamp, updated_at=stamp, metadata={"nil": None, "flag": True, "group": "x"})
    missing = make_header(
        created_at=stamp + timedelta(seconds=1),
        updated_at=stamp,
        metadata={"flag": 1, "group": "x"},
    )
    numbered = make_header(
        created_at=stamp + timedelta(seconds=2),
        updated_at=stamp,
        metadata={"nil": None, "flag": 1, "group": "y"},
    )
    for header in (nullable, missing, numbered):
        await store.create(header)

    nil = await store.select_cleanup(CleanupSelector(metadata={"nil": None}), limit=10)
    boolean = await store.select_cleanup(CleanupSelector(metadata={"flag": True}), limit=10)
    combined = await store.select_cleanup(
        CleanupSelector(
            root_ids=(UUID(hex=nullable.id), UUID(hex=missing.id)),
            metadata={"group": "x", "flag": True},
        ),
        limit=10,
    )

    assert [row.root_id for row in nil] == [nullable.id, numbered.id]
    assert [row.root_id for row in boolean] == [nullable.id]
    assert [row.root_id for row in combined] == [nullable.id]
    assert await store.select_cleanup(CleanupSelector(root_ids=()), limit=10) == []
    assert await store.select_cleanup(CleanupSelector(metadata={"group": "x"}), limit=0) == []


async def test_select_cleanup_rejects_children_and_uses_whole_tree_age(
    store: MemoryStore | SQLiteStore,
) -> None:
    old = datetime(2024, 1, 1, tzinfo=UTC)
    cutoff = old + timedelta(days=1)
    eligible = make_header(created_at=old, updated_at=old)
    equality = make_header(created_at=old + timedelta(seconds=1), updated_at=cutoff)
    recent = make_header(created_at=old + timedelta(seconds=2), updated_at=old)
    child = make_header(
        root_id=recent.id,
        parent_id=recent.id,
        created_at=old + timedelta(seconds=3),
        updated_at=cutoff + timedelta(seconds=1),
    )
    for header in (eligible, equality, recent, child):
        await store.create(header)

    rows = await store.select_cleanup(CleanupSelector(inactive_before=cutoff), limit=10)

    assert [row.root_id for row in rows] == [eligible.id]
    with pytest.raises(TantraError, match="live child"):
        await store.select_cleanup(CleanupSelector(root_ids=(UUID(hex=child.id),)), limit=10)


async def test_mutations_refresh_age_but_reads_do_not(store: MemoryStore | SQLiteStore) -> None:
    old = datetime(2024, 1, 1, tzinfo=UTC)
    cutoff = datetime.now(UTC)
    put = make_header(created_at=old, updated_at=old)
    patch = make_header(created_at=old + timedelta(seconds=1), updated_at=old)
    append = make_header(created_at=old + timedelta(seconds=2), updated_at=old)
    read = make_header(created_at=old + timedelta(seconds=3), updated_at=old)
    for header in (put, patch, append, read):
        await store.create(header)

    supplied = put.model_copy(deep=True)
    supplied.title = "changed"
    supplied.updated_at = old
    await store.put_header(supplied)
    await store.patch_header(patch.id, title="changed")
    await store.append(append.id, [TextPart(sample_id="sample", text="changed")])
    await store.header(read.id)
    await store.read_page(read.id)
    [item async for item in store.read(read.id)]
    await store.list()

    rows = await store.select_cleanup(CleanupSelector(inactive_before=cutoff), limit=10)

    assert [row.root_id for row in rows] == [read.id]


async def test_cleanup_cursor_survives_deleted_roots(store: MemoryStore | SQLiteStore) -> None:
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    roots = [make_header(created_at=stamp + timedelta(seconds=index), updated_at=stamp) for index in range(3)]
    for header in roots:
        await store.create(header)

    selector = CleanupSelector(root_ids=tuple(UUID(hex=header.id) for header in roots))
    first = await store.select_cleanup(selector, limit=2)
    cursor = first[-1].cursor
    for candidate in first:
        await store.delete_tree(candidate.root_id, allow_active=True, expected_revision=candidate.revision)

    remaining = await store.select_cleanup(selector, limit=2, after=cursor)

    assert [row.root_id for row in remaining] == [roots[2].id]


async def test_revision_change_stops_delete_before_callback(store: MemoryStore | SQLiteStore) -> None:
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    root = make_header(created_at=stamp, updated_at=stamp)
    await store.create(root)
    candidate = (await store.select_cleanup(CleanupSelector(root_ids=(UUID(hex=root.id),)), limit=1))[0]
    await store.patch_header(root.id, metadata={"changed": True})
    called: list[str] = []

    with pytest.raises(CleanupChanged):
        await store.delete_tree(
            root.id,
            allow_active=True,
            before_delete=lambda ids: called.extend(ids),
            expected_revision=candidate.revision,
        )

    assert called == []
    assert await store.header(root.id) is not None


async def test_reparented_root_is_changed_for_guarded_delete(store: MemoryStore | SQLiteStore) -> None:
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    root = make_header(created_at=stamp, updated_at=stamp)
    parent = make_header(created_at=stamp, updated_at=stamp)
    await store.create(root)
    await store.create(parent)
    candidate = (await store.select_cleanup(CleanupSelector(root_ids=(UUID(hex=root.id),)), limit=1))[0]
    moved = root.model_copy(update={"root_id": parent.id, "parent_id": parent.id})
    await store.put_header(moved)
    called: list[str] = []

    with pytest.raises(CleanupChanged):
        await store.delete_tree(
            root.id,
            allow_active=True,
            before_delete=lambda ids: called.extend(ids),
            expected_revision=candidate.revision,
        )
    with pytest.raises(TantraError, match="not a root"):
        await store.delete_tree(root.id, allow_active=True)

    assert called == []
    assert await store.header(root.id) is not None


async def test_busy_selection_and_force_delete(store: MemoryStore | SQLiteStore) -> None:
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    root = make_header(created_at=stamp, updated_at=stamp)
    await store.create(root)
    await store.append(root.id, [InputQueued(command_id=uuid4().hex, input="go")])

    candidate = (await store.select_cleanup(CleanupSelector(root_ids=(UUID(hex=root.id),)), limit=1))[0]

    assert candidate.active is True
    with pytest.raises(SessionBusy):
        await store.delete_tree(root.id, expected_revision=candidate.revision)
    assert await store.delete_tree(root.id, allow_active=True, expected_revision=candidate.revision) == [root.id]


async def test_callback_failure_preserves_revision_and_tree(store: MemoryStore | SQLiteStore) -> None:
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    root = make_header(created_at=stamp, updated_at=stamp)
    await store.create(root)
    selector = CleanupSelector(root_ids=(UUID(hex=root.id),))
    candidate = (await store.select_cleanup(selector, limit=1))[0]

    def fail(ids: list[str]) -> None:
        raise RuntimeError(ids[0])

    with pytest.raises(RuntimeError, match=root.id):
        await store.delete_tree(root.id, before_delete=fail, expected_revision=candidate.revision)

    current = (await store.select_cleanup(selector, limit=1))[0]
    assert current.revision == candidate.revision


async def test_sqlite_corrupt_journal_is_active_and_force_skips_decode(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "tantra.db")
    await store.setup()
    stamp = datetime(2024, 1, 1, tzinfo=UTC)
    root = make_header(created_at=stamp, updated_at=stamp)
    await store.create(root)
    with sqlite3.connect(store.path) as conn:
        conn.execute("INSERT INTO events (session_id, seq, stamped) VALUES (?, ?, ?)", (root.id, 1, "{}"))

    candidate = (await store.select_cleanup(CleanupSelector(root_ids=(UUID(hex=root.id),)), limit=1))[0]
    called: list[str] = []

    assert candidate.active is True
    with pytest.raises(CorruptLog):
        await store.delete_tree(
            root.id,
            before_delete=lambda ids: called.extend(ids),
            expected_revision=candidate.revision,
        )
    assert called == []
    assert await store.delete_tree(root.id, allow_active=True, expected_revision=candidate.revision) == [root.id]
