from collections.abc import Awaitable, Callable

import httpx
from conftest import SharedStore

from tantra import SessionHeader

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
