from datetime import UTC, datetime, timedelta

import httpx
import jwt
import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sarathi.auth import TOKEN_TTL, current_user_ws, decode_token
from sarathi.config import get_settings

CREDENTIALS = {"email": "a@example.com", "password": "hunter2hunter2"}


async def test_signup_login_me(client: httpx.AsyncClient) -> None:
    signup = await client.post("/api/auth/signup", json=CREDENTIALS)
    assert signup.status_code == 200
    assert signup.json()["access_token"]

    login = await client.post("/api/auth/login", json=CREDENTIALS)
    assert login.status_code == 200
    token = login.json()["access_token"]

    me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    assert me.json() == {"id": decode_token(token), "email": CREDENTIALS["email"]}


async def test_signup_duplicate_email_conflicts(client: httpx.AsyncClient) -> None:
    assert (await client.post("/api/auth/signup", json=CREDENTIALS)).status_code == 200
    duplicate = await client.post("/api/auth/signup", json={**CREDENTIALS, "password": "other-password"})
    assert duplicate.status_code == 409


async def test_login_wrong_password_unauthorized(client: httpx.AsyncClient) -> None:
    await client.post("/api/auth/signup", json=CREDENTIALS)
    response = await client.post("/api/auth/login", json={**CREDENTIALS, "password": "wrong-password"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid email or password"


async def test_login_unknown_email_unauthorized(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/auth/login", json={"email": "nobody@example.com", "password": "whatever"})
    assert response.status_code == 401


async def test_me_without_token_unauthorized(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/auth/me")).status_code == 401


async def test_me_garbage_token_unauthorized(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/auth/me", headers={"Authorization": "Bearer not-a-jwt"})
    assert response.status_code == 401


async def test_me_expired_token_unauthorized(client: httpx.AsyncClient) -> None:
    signup = await client.post("/api/auth/signup", json=CREDENTIALS)
    user_id = decode_token(signup.json()["access_token"])
    issued = datetime.now(UTC) - TOKEN_TTL - timedelta(minutes=1)
    expired = jwt.encode(
        {"sub": str(user_id), "iat": int(issued.timestamp()), "exp": int((issued + TOKEN_TTL).timestamp())},
        get_settings().SECRET_KEY,
        algorithm="HS256",
    )

    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {expired}"})
    assert response.status_code == 401


async def test_me_token_for_deleted_user_unauthorized(client: httpx.AsyncClient) -> None:
    now = datetime.now(UTC)
    token = jwt.encode(
        {"sub": "4242", "iat": int(now.timestamp()), "exp": int((now + TOKEN_TTL).timestamp())},
        get_settings().SECRET_KEY,
        algorithm="HS256",
    )
    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


async def test_current_user_ws_resolves_token(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    signup = await client.post("/api/auth/signup", json=CREDENTIALS)
    token = signup.json()["access_token"]

    async with session_factory() as session:
        user = await current_user_ws(session, token)
        assert user.email == CREDENTIALS["email"]

        with pytest.raises(HTTPException) as invalid:
            await current_user_ws(session, "not-a-jwt")
        assert invalid.value.status_code == 401

        with pytest.raises(HTTPException) as missing:
            await current_user_ws(session, None)
        assert missing.value.status_code == 401
