from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from conftest import SharedStore

from tantra import BuiltinMemory, MemoryWrite

Signup = Callable[..., Awaitable[str]]


async def _uid(client: httpx.AsyncClient, token: str) -> str:
    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    return str(response.json()["id"])


async def _seed(store: SharedStore, uid: str, body: str) -> str:
    return await BuiltinMemory(store).write(
        MemoryWrite(kind="preference", title=body, body=body, tags=["drink"], metadata={"user": uid})
    )


async def _list(client: httpx.AsyncClient, token: str) -> list[dict[str, Any]]:
    response = await client.get("/api/memory", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    rows: list[dict[str, Any]] = response.json()
    return rows


async def test_listing_returns_only_the_callers_rows(
    client: httpx.AsyncClient, store: SharedStore, signup: Signup
) -> None:
    owner = await signup("a@example.com")
    other = await signup("b@example.com")
    await _seed(store, await _uid(client, owner), "prefers tea")

    mine = await _list(client, owner)

    assert [row["body"] for row in mine] == ["prefers tea"]
    assert set(mine[0]) == {"id", "kind", "title", "body", "tags", "created_at"}
    assert mine[0]["tags"] == ["drink"]
    assert await _list(client, other) == []


async def test_deleting_another_users_row_is_a_404_and_leaves_it_listed(
    client: httpx.AsyncClient, store: SharedStore, signup: Signup
) -> None:
    owner = await signup("a@example.com")
    intruder = await signup("b@example.com")
    mid = await _seed(store, await _uid(client, owner), "prefers tea")

    response = await client.delete(f"/api/memory/{mid}", headers={"Authorization": f"Bearer {intruder}"})

    assert response.status_code == 404
    assert response.json()["detail"] == "Memory not found"
    assert [row["id"] for row in await _list(client, owner)] == [mid]


async def test_deleting_an_own_row_drops_it_from_the_list(
    client: httpx.AsyncClient, store: SharedStore, signup: Signup
) -> None:
    token = await signup()
    uid = await _uid(client, token)
    kept = await _seed(store, uid, "prefers tea")
    doomed = await _seed(store, uid, "prefers coffee")

    response = await client.delete(f"/api/memory/{doomed}", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 204
    assert [row["id"] for row in await _list(client, token)] == [kept]

    again = await client.delete(f"/api/memory/{doomed}", headers={"Authorization": f"Bearer {token}"})
    assert again.status_code == 204


async def test_deleting_an_unknown_row_is_a_404(client: httpx.AsyncClient, signup: Signup) -> None:
    token = await signup()

    response = await client.delete("/api/memory/missing", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 404


async def test_memory_requires_authentication(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/memory")).status_code == 401
    assert (await client.delete("/api/memory/anything")).status_code == 401
