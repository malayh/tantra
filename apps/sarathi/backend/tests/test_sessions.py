from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
from conftest import SharedStore

from tantra import SessionHeader, TurnSummary

Signup = Callable[..., Awaitable[str]]
NewSession = Callable[..., Awaitable[str]]


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def test_create_session_uses_default_model(client: httpx.AsyncClient, signup: Signup) -> None:
    token = await signup()
    response = await client.post("/api/sessions", json={}, headers=_auth(token))
    assert response.status_code == 201
    body = response.json()
    assert body["model"] == "test-model"
    assert body["title"] is None
    assert body["status"] == "idle"


async def test_create_session_with_explicit_model(client: httpx.AsyncClient, signup: Signup) -> None:
    token = await signup()
    response = await client.post("/api/sessions", json={"model": "other-model"}, headers=_auth(token))
    assert response.status_code == 201
    assert response.json()["model"] == "other-model"


async def test_create_session_unknown_model_rejected(client: httpx.AsyncClient, signup: Signup) -> None:
    token = await signup()
    response = await client.post("/api/sessions", json={"model": "nope"}, headers=_auth(token))
    assert response.status_code == 422


async def test_list_sessions_requires_auth(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/sessions")).status_code == 401


async def test_list_sessions_is_scoped_to_owner(
    client: httpx.AsyncClient, signup: Signup, new_session: NewSession
) -> None:
    first = await signup("a@example.com")
    second = await signup("b@example.com")
    mine = await new_session(first)
    await new_session(second)

    listed = await client.get("/api/sessions", headers=_auth(first))
    assert listed.status_code == 200
    assert [row["id"] for row in listed.json()] == [mine]


async def test_patch_session_updates_model(client: httpx.AsyncClient, signup: Signup, new_session: NewSession) -> None:
    token = await signup()
    sid = await new_session(token)

    patched = await client.patch(f"/api/sessions/{sid}", json={"model": "other-model"}, headers=_auth(token))
    assert patched.status_code == 200
    assert patched.json()["model"] == "other-model"

    listed = await client.get("/api/sessions", headers=_auth(token))
    assert listed.json()[0]["model"] == "other-model"


async def test_patch_session_unknown_model_rejected(
    client: httpx.AsyncClient, signup: Signup, new_session: NewSession
) -> None:
    token = await signup()
    sid = await new_session(token)
    response = await client.patch(f"/api/sessions/{sid}", json={"model": "nope"}, headers=_auth(token))
    assert response.status_code == 422


async def test_patch_other_users_session_not_found(
    client: httpx.AsyncClient, signup: Signup, new_session: NewSession
) -> None:
    owner = await signup("a@example.com")
    intruder = await signup("b@example.com")
    sid = await new_session(owner)

    response = await client.patch(f"/api/sessions/{sid}", json={"model": "other-model"}, headers=_auth(intruder))
    assert response.status_code == 404


async def test_patch_unknown_session_not_found(client: httpx.AsyncClient, signup: Signup) -> None:
    token = await signup()
    response = await client.patch("/api/sessions/missing", json={"model": "other-model"}, headers=_auth(token))
    assert response.status_code == 404


async def test_list_sessions_excludes_child_sessions(
    client: httpx.AsyncClient, store: SharedStore, signup: Signup, new_session: NewSession
) -> None:
    token = await signup()
    root = await new_session(token)
    parent = await store.header(root)
    assert parent is not None
    await store.create(
        SessionHeader(id="child", agent="sarathi", parent_id=root, depth=1, metadata=dict(parent.metadata))
    )

    listed = await client.get("/api/sessions", headers=_auth(token))
    assert [row["id"] for row in listed.json()] == [root]


async def test_list_actors_requires_auth(client: httpx.AsyncClient) -> None:
    root_id = "0" * 32
    assert (await client.get(f"/api/sessions/{root_id}/actors")).status_code == 401


async def test_list_actors_returns_owned_root_tree_without_reading_journals(
    client: httpx.AsyncClient,
    store: SharedStore,
    resources: object,
    signup: Signup,
    new_session: NewSession,
    monkeypatch: object,
) -> None:
    token = await signup()
    root_id = await new_session(token)
    owner = str((await client.get("/api/auth/me", headers=_auth(token))).json()["id"])
    base = datetime(2026, 1, 1, tzinfo=UTC)
    child_late = uuid4()
    child_early = uuid4()
    grandchild = uuid4()
    turn_id = uuid4()
    metadata = {"user": owner, "kind": "root"}
    await store.create(
        SessionHeader(
            id=child_late.hex,
            root_id=root_id,
            parent_id=root_id,
            agent="late",
            depth=1,
            status="running",
            current_turn_id=turn_id.hex,
            created_at=base + timedelta(seconds=2),
            updated_at=base + timedelta(seconds=4),
            metadata=metadata,
        )
    )
    await store.create(
        SessionHeader(
            id=child_early.hex,
            root_id=root_id,
            parent_id=root_id,
            agent="early",
            name="Reviewer",
            depth=1,
            status="failed",
            last_turn=TurnSummary(turn_id, "failed", None, "boom"),
            created_at=base + timedelta(seconds=1),
            updated_at=base + timedelta(seconds=3),
            metadata=metadata,
        )
    )
    await store.create(
        SessionHeader(
            id=grandchild.hex,
            root_id=root_id,
            parent_id=child_early.hex,
            agent="nested",
            depth=2,
            status="idle",
            created_at=base,
            updated_at=base + timedelta(seconds=5),
            metadata=metadata,
        )
    )

    async def fail_read(*args: object, **kwargs: object) -> object:
        raise AssertionError("actor polling read a journal")

    monkeypatch.setattr(store, "read_page", fail_read)
    active_before = dict(resources.runtime.active)
    response = await client.get(f"/api/sessions/{root_id}/actors", headers=_auth(token))

    assert response.status_code == 200
    actors = response.json()
    assert [actor["agent_id"] for actor in actors] == [
        root_id,
        child_early.hex,
        child_late.hex,
        grandchild.hex,
    ]
    assert actors[1] == {
        "agent_id": child_early.hex,
        "root_id": root_id,
        "parent_id": root_id,
        "agent": "early",
        "name": "Reviewer",
        "state": "failed",
        "active": False,
        "current_turn_id": None,
        "last_turn": {
            "turn_id": turn_id.hex,
            "outcome": "failed",
            "stop_reason": None,
            "error": "boom",
        },
        "last_seq": 0,
        "updated_at": actors[1]["updated_at"],
    }
    assert actors[2]["current_turn_id"] == turn_id.hex
    assert all(len(actor["agent_id"]) == 32 and "-" not in actor["agent_id"] for actor in actors)
    assert resources.runtime.active == active_before


async def test_list_actors_rejects_non_root_and_other_owner(
    client: httpx.AsyncClient, store: SharedStore, signup: Signup, new_session: NewSession
) -> None:
    owner = await signup("owner@example.com")
    intruder = await signup("intruder@example.com")
    root_id = await new_session(owner)
    parent = await store.header(root_id)
    assert parent is not None
    child_id = uuid4().hex
    await store.create(
        SessionHeader(
            id=child_id,
            root_id=root_id,
            parent_id=root_id,
            agent="child",
            depth=1,
            metadata=dict(parent.metadata),
        )
    )

    assert (await client.get(f"/api/sessions/{root_id}/actors", headers=_auth(intruder))).status_code == 404
    assert (await client.get(f"/api/sessions/{child_id}/actors", headers=_auth(owner))).status_code == 404
    assert (await client.get("/api/sessions/not-a-uuid/actors", headers=_auth(owner))).status_code == 404
