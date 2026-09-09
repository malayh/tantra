import asyncio
import fcntl
import os
import threading
import uuid
from pathlib import Path

import pytest

from tantra.errors import CorruptLog
from tantra.events import InputQueued, SessionHeader, Stamped, TextPart
from tantra.stores.fs import FileSystemStore

CONTENDERS = 4
ROUNDS = 10


def _header() -> SessionHeader:
    return SessionHeader(id=uuid.uuid4().hex, agent="build")


def _append_in_thread(root: Path, sid: str, text: str, barrier: threading.Barrier | None = None) -> int | None:
    async def attempt() -> int:
        store = FileSystemStore(root)
        if barrier is not None:
            barrier.wait(timeout=30)
        return await store.append(sid, [TextPart(sample_id="s1", text=text)])

    return asyncio.run(attempt())


async def test_layout(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)

    assert (tmp_path / header.id / "session.json").is_file()
    assert (tmp_path / header.id / "events.jsonl").is_file()
    assert (tmp_path / header.id / ".lock").is_file()


async def test_truncated_final_line_is_skipped(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)
    events = [TextPart(sample_id="s1", text=f"part {index}") for index in range(3)]
    await store.append(header.id, events)

    torn = Stamped(seq=4, event=TextPart(sample_id="s1", text="x" * 8192)).model_dump_json()
    with open(tmp_path / header.id / "events.jsonl", "a", encoding="utf-8") as handle:
        handle.write(torn[:2000])

    stamped = [s async for s in store.read(header.id)]
    assert [s.seq for s in stamped] == [1, 2, 3]
    assert [s.event for s in stamped] == events

    event = TextPart(sample_id="s1", text="four")
    assert await store.append(header.id, [event]) == 4

    recovered = [s async for s in store.read(header.id)]
    assert recovered == [Stamped(seq=index + 1, event=item) for index, item in enumerate([*events, event])]


async def test_enqueue_recovers_a_truncated_final_line(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text="one")])

    queued = InputQueued(command_id="fresh", input="go")
    torn = Stamped(seq=2, event=InputQueued(command_id="lost", input="stop")).model_dump_json()
    with open(tmp_path / header.id / "events.jsonl", "a", encoding="utf-8") as handle:
        handle.write(torn[:20])

    result = await store.enqueue(header.id, queued)

    assert result.seq == 2
    assert result.duplicate is False
    assert await store.read_page(header.id) == [
        Stamped(seq=1, event=TextPart(sample_id="s1", text="one")),
        Stamped(seq=2, event=queued),
    ]


async def test_read_page_does_not_parse_beyond_the_bound(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)
    events = [TextPart(sample_id="s1", text=f"part {index}") for index in range(2)]
    await store.append(header.id, events)

    path = tmp_path / header.id / "events.jsonl"
    with open(path, "a", encoding="utf-8") as handle:
        handle.write('{"seq":3}\n')

    assert await store.read_page(header.id, limit=0) == []
    assert await store.read_page(header.id, limit=2) == [
        Stamped(seq=index + 1, event=event) for index, event in enumerate(events)
    ]


async def test_a_complete_line_that_cannot_be_decoded_raises(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text=f"part {i}") for i in range(2)])

    torn = Stamped(seq=3, event=TextPart(sample_id="s1", text="x" * 8192)).model_dump_json()
    with open(tmp_path / header.id / "events.jsonl", "a", encoding="utf-8") as handle:
        handle.write(torn[:2000] + "\n")

    stamped = []
    with pytest.raises(CorruptLog):
        async for s in store.read(header.id):
            stamped.append(s)
    assert [s.seq for s in stamped] == [1, 2]


async def test_unknown_event_type_raises_instead_of_being_dropped(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text=f"part {i}") for i in range(2)])

    with open(tmp_path / header.id / "events.jsonl", "a", encoding="utf-8") as handle:
        handle.write('{"seq":3,"event":{"type":"telepathy_part","version":9,"vibes":"good"}}\n')
        handle.write(Stamped(seq=4, event=TextPart(sample_id="s1", text="after")).model_dump_json() + "\n")

    stamped = []
    with pytest.raises(CorruptLog):
        async for s in store.read(header.id):
            stamped.append(s)
    assert [s.seq for s in stamped] == [1, 2]


async def test_append_reconciles_a_header_that_lost_its_write(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text=f"part {i}") for i in range(2)])

    path = tmp_path / header.id / "session.json"
    lost = SessionHeader.model_validate_json(path.read_text(encoding="utf-8"))
    lost.last_seq = 1
    path.write_text(lost.model_dump_json(), encoding="utf-8")

    assert await store.append(header.id, [TextPart(sample_id="s1", text="three")]) == 3
    loaded = await store.header(header.id)
    assert loaded is not None
    assert loaded.last_seq == 3
    assert [s.seq for s in [x async for x in store.read(header.id)]] == [1, 2, 3]


async def test_concurrent_appends_from_threads_are_serialized(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()

    for _ in range(ROUNDS):
        header = _header()
        await store.create(header)
        barrier = threading.Barrier(CONTENDERS)

        results = await asyncio.gather(
            *(asyncio.to_thread(_append_in_thread, tmp_path, header.id, "x" * 8192, barrier) for _ in range(CONTENDERS))
        )
        assert sorted(results) == list(range(1, CONTENDERS + 1))

        stamped = [s async for s in store.read(header.id)]
        assert [s.seq for s in stamped] == list(range(1, CONTENDERS + 1))
        assert all(len(item.event.text) == 8192 for item in stamped)

        loaded = await store.header(header.id)
        assert loaded is not None
        assert loaded.last_seq == CONTENDERS


async def test_append_waits_for_an_external_flock(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    header = _header()
    await store.create(header)

    fd = os.open(tmp_path / header.id / ".lock", os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    task = asyncio.create_task(asyncio.to_thread(_append_in_thread, tmp_path, header.id, "one"))
    await asyncio.sleep(0.25)
    blocked = not task.done()
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
    result = await asyncio.wait_for(task, timeout=10)

    assert blocked, "append did not wait for the session lock"
    assert result == 1
