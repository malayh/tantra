from uuid import uuid4

import pytest

from tantra import Agent, FakeProvider, MemoryStore, Runtime, SessionHeader, TantraError
from tantra.events import InputQueued, SessionCreated
from tantra.tools import Context


async def test_unregistered_legacy_actor_history_is_readable_but_send_does_not_mutate_it() -> None:
    class Subagent(Agent):
        pass

    class Root(Agent):
        subagents = [Subagent]

    async def emit(_: str) -> None:
        return None

    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Root], default_model="m")
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    legacy_id = uuid4()
    legacy = SessionHeader(
        id=legacy_id.hex,
        root_id=root.id,
        parent_id=root.id,
        agent="researcher",
        depth=1,
        model="m",
    )
    await store.create(legacy)
    await store.append(
        legacy.id,
        [
            SessionCreated(
                agent="researcher",
                root_id=root.id,
                parent_id=root.id,
                depth=1,
                model="m",
            ),
            InputQueued(command_id=uuid4().hex, input="historical research"),
        ],
    )

    status = await runtime.status(legacy_id)
    tree = await runtime.tree_status(root_id)
    stream = runtime.events(legacy_id)
    first = await anext(stream)
    await stream.aclose()
    before = ((await store.header(legacy.id)).model_dump(), await store.read_page(legacy.id))
    ctx = Context(
        session_id=root.id,
        turn_id="turn",
        call_id="send",
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )

    with pytest.raises(TantraError, match="unknown agent 'researcher'"):
        await runtime._actor_send(root, ctx, legacy_id, "new work")

    after = ((await store.header(legacy.id)).model_dump(), await store.read_page(legacy.id))
    assert status.agent == status.name == "researcher"
    assert [actor.agent for actor in tree] == ["root", "researcher"]
    assert isinstance(first.event, SessionCreated)
    assert before == after
    assert legacy.id not in runtime.active
    await runtime.aclose()
