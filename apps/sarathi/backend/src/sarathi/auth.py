from datetime import UTC, datetime, timedelta
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.hash import pbkdf2_sha256
from sqlalchemy.ext.asyncio import AsyncSession

from sarathi.config import get_settings
from sarathi.db import DbDep
from sarathi.models import User

TOKEN_TTL = timedelta(hours=24)

bearer_scheme = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    return pbkdf2_sha256.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pbkdf2_sha256.verify(password, password_hash)


def create_token(user_id: int) -> str:
    now = datetime.now(UTC)
    payload = {"sub": str(user_id), "iat": int(now.timestamp()), "exp": int((now + TOKEN_TTL).timestamp())}
    return jwt.encode(payload, get_settings().SECRET_KEY, algorithm="HS256")


def decode_token(token: str) -> int:
    payload = jwt.decode(token, get_settings().SECRET_KEY, algorithms=["HS256"])
    return int(payload["sub"])


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def resolve_user(token: str | None, db: AsyncSession) -> User:
    if not token:
        raise _unauthorized()
    try:
        user_id = decode_token(token)
    except (jwt.PyJWTError, KeyError, TypeError, ValueError) as exc:
        raise _unauthorized() from exc
    user = await db.get(User, user_id)
    if user is None:
        raise _unauthorized()
    return user


async def current_user(
    db: DbDep,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
) -> User:
    return await resolve_user(credentials.credentials if credentials else None, db)


async def current_user_ws(db: DbDep, token: Annotated[str | None, Query()] = None) -> User:
    return await resolve_user(token, db)


CurrentUser = Annotated[User, Depends(current_user)]
CurrentUserWS = Annotated[User, Depends(current_user_ws)]
