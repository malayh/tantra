from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from tantra import Agent, FileSystemStore, FreeText, Runtime, SQLiteStore, TantraError
from tantra.events import AskRaised, SessionHeader
from tantra.providers.fake import FakeProvider
from tantra.stores.memory import MemoryStore


class Root(Agent):
    pass


class FallbackStore(MemoryStore):
    lookup_ask = None


async def add_child(store: MemoryStore, root_id: UUID) -> UUID:
    child_id = uuid4()
    await store.create(
        SessionHeader(
            id=child_id.hex,
            root_id=root_id.hex,
            parent_id=root_id.hex,
            agent="child",
            depth=1,
            model="m",
        )
    )
    return child_id


async def test_lookup_ask_finds_durable_root_ask_without_live_state_or_activation() -> None:
    store = FallbackStore()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    ask_id = uuid4()
    event = AskRaised(ask_id=ask_id.hex, call_id="call", request=FreeText(prompt="continue?"))
    seq = await store.append(root_id.hex, [event])
    await store.patch_header(root_id.hex, status="idle", pending_ask=None)
    before = await store.header(root_id.hex)

    located = await runtime.lookup_ask(root_id, ask_id)

    assert located is not None
    assert located.actor_id == root_id
    assert located.seq == seq
    assert located.event == event
    assert runtime.active == {}
    assert runtime.asks == {}
    assert await store.header(root_id.hex) == before
    await runtime.aclose()


async def test_lookup_ask_finds_child_and_rejects_child_as_root() -> None:
    store = FallbackStore()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    child_id = await add_child(store, root_id)
    ask_id = uuid4()
    event = AskRaised(ask_id=ask_id.hex, call_id="child-call", request=FreeText(prompt="child?"))
    seq = await store.append(child_id.hex, [event])

    located = await runtime.lookup_ask(root_id, ask_id)

    assert located is not None
    assert located.actor_id == child_id
    assert located.seq == seq
    assert located.event == event
    with pytest.raises(TantraError, match="not a root"):
        await runtime.lookup_ask(child_id, ask_id)
    assert runtime.active == {}
    await runtime.aclose()


async def test_lookup_ask_is_scoped_to_the_requested_root() -> None:
    store = FallbackStore()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    first = await runtime.create(Root)
    second = await runtime.create(Root)
    ask_id = uuid4()
    await store.append(
        first.hex,
        [AskRaised(ask_id=ask_id.hex, call_id="call", request=FreeText(prompt="first?"))],
    )

    assert await runtime.lookup_ask(second, ask_id) is None
    assert await runtime.lookup_ask(first, uuid4()) is None
    assert runtime.active == {}
    await runtime.aclose()


async def test_fresh_runtime_recovers_ask_when_pending_projection_and_live_future_are_lost() -> None:
    store = FallbackStore()
    creator = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await creator.create(Root)
    ask_id = uuid4()
    event = AskRaised(ask_id=ask_id.hex, call_id="call", request=FreeText(prompt="recover?"))
    seq = await store.append(root_id.hex, [event])
    await store.patch_header(root_id.hex, status="idle", pending_ask=None)
    await creator.aclose()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")

    located = await runtime.lookup_ask(root_id, ask_id)

    assert located is not None
    assert located.actor_id == root_id
    assert located.seq == seq
    assert located.event == event
    assert runtime.active == {}
    assert runtime.asks == {}
    await runtime.aclose()


async def test_lookup_ask_rejects_ambiguous_durable_history() -> None:
    store = FallbackStore()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    child_id = await add_child(store, root_id)
    ask_id = uuid4()
    root_event = AskRaised(ask_id=ask_id.hex, call_id="root", request=FreeText(prompt="root?"))
    child_event = AskRaised(ask_id=ask_id.hex, call_id="child", request=FreeText(prompt="child?"))
    await store.append(root_id.hex, [root_event])
    await store.append(child_id.hex, [child_event])

    with pytest.raises(ValueError, match="duplicate|ambiguous"):
        await runtime.lookup_ask(root_id, ask_id)

    assert runtime.active == {}
    await runtime.aclose()


@pytest.mark.parametrize("backend", ["memory", "sqlite", "fs"])
async def test_fallback_lookup_returns_independent_nested_descriptors(backend: str, tmp_path: Path) -> None:
    stores = {
        "memory": MemoryStore(),
        "sqlite": SQLiteStore(tmp_path / "sessions.sqlite3"),
        "fs": FileSystemStore(tmp_path / "sessions"),
    }
    store = stores[backend]
    await store.setup()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    ask_id = uuid4()
    event = AskRaised(
        ask_id=ask_id.hex,
        request=FreeText(prompt="continue?", extra={"resource": {"ids": ["original"]}}),
    )
    seq = await store.append(root_id.hex, [event])

    located = await runtime.lookup_ask(root_id, ask_id)
    assert located is not None
    located.event.request.extra["resource"]["ids"].append("mutated")
    again = await runtime.lookup_ask(root_id, ask_id)
    assert again is not None and again.seq == seq
    assert again.event.request.extra == {"resource": {"ids": ["original"]}}
    assert runtime.active == {}
    await runtime.aclose()
