from collections.abc import Awaitable, Callable

import httpx

Signup = Callable[..., Awaitable[str]]


async def test_models_lists_the_configured_models(client: httpx.AsyncClient, signup: Signup) -> None:
    token = await signup()

    response = await client.get("/api/models", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    assert response.json() == ["test-model", "other-model"]


async def test_models_requires_authentication(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/models")

    assert response.status_code == 401
