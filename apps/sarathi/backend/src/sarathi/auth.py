from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.hash import pbkdf2_sha256

from sarathi.config import get_settings
from sarathi.db import DbDep
from sarathi.models import User

TOKEN_TTL = timedelta(hours=24)
SOCKET_TICKET_TTL = timedelta(seconds=30)
LOGIN_PURPOSE = "login"
SOCKET_TICKET_PURPOSE = "socket_ticket"
SOCKET_TICKET_AUDIENCE = "sarathi-session-socket"

bearer_scheme = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    return pbkdf2_sha256.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pbkdf2_sha256.verify(password, password_hash)


def create_token(user_id: int) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
        "purpose": LOGIN_PURPOSE,
        "iat": int(now.timestamp()),
        "exp": int((now + TOKEN_TTL).timestamp()),
    }
    return jwt.encode(payload, get_settings().SECRET_KEY, algorithm="HS256")


def decode_login(token: str) -> tuple[int, datetime]:
    payload = jwt.decode(
        token,
        get_settings().SECRET_KEY,
        algorithms=["HS256"],
        options={"require": ["sub", "iat", "exp"]},
    )
    if payload.get("purpose", LOGIN_PURPOSE) != LOGIN_PURPOSE:
        raise jwt.InvalidTokenError("invalid token purpose")
    return int(payload["sub"]), datetime.fromtimestamp(int(payload["exp"]), UTC)


def decode_token(token: str) -> int:
    return decode_login(token)[0]


@dataclass(frozen=True)
class SocketTicket:
    root_id: UUID
    user_id: str
    capability: str
    login_expires_at: datetime


def create_socket_ticket(
    user_id: int,
    root_id: UUID,
    capability: str,
    login_expires_at: datetime,
) -> tuple[str, datetime]:
    now = datetime.now(UTC)
    expires_at = datetime.fromtimestamp(
        min(int((now + SOCKET_TICKET_TTL).timestamp()), int(login_expires_at.timestamp())), UTC
    )
    payload = {
        "aud": SOCKET_TICKET_AUDIENCE,
        "purpose": SOCKET_TICKET_PURPOSE,
        "root_id": str(root_id),
        "user_id": str(user_id),
        "capability": capability,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "login_exp": int(login_expires_at.timestamp()),
    }
    return jwt.encode(payload, get_settings().SECRET_KEY, algorithm="HS256"), expires_at


def decode_socket_ticket(token: str) -> SocketTicket:
    payload = jwt.decode(
        token,
        get_settings().SECRET_KEY,
        algorithms=["HS256"],
        audience=SOCKET_TICKET_AUDIENCE,
        options={"require": ["aud", "purpose", "root_id", "user_id", "capability", "iat", "exp", "login_exp"]},
    )
    if payload.get("purpose") != SOCKET_TICKET_PURPOSE:
        raise jwt.InvalidTokenError("invalid token purpose")
    raw_root = payload["root_id"]
    root_id = UUID(str(raw_root))
    if str(root_id) != raw_root:
        raise jwt.InvalidTokenError("invalid root id")
    user_id = str(int(payload["user_id"]))
    capability = payload["capability"]
    if capability not in ("read", "write"):
        raise jwt.InvalidTokenError("invalid capability")
    login_expires_at = datetime.fromtimestamp(int(payload["login_exp"]), UTC)
    if login_expires_at <= datetime.now(UTC):
        raise jwt.ExpiredSignatureError("login expired")
    return SocketTicket(root_id, user_id, capability, login_expires_at)


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )


@dataclass(frozen=True)
class LoginSession:
    user: User
    expires_at: datetime


async def current_login(
    db: DbDep,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
) -> LoginSession:
    if credentials is None:
        raise _unauthorized()
    try:
        user_id, expires_at = decode_login(credentials.credentials)
    except (jwt.PyJWTError, KeyError, TypeError, ValueError) as exc:
        raise _unauthorized() from exc
    user = await db.get(User, user_id)
    if user is None:
        raise _unauthorized()
    return LoginSession(user, expires_at)


async def current_user(
    login: Annotated[LoginSession, Depends(current_login)],
) -> User:
    return login.user


CurrentUser = Annotated[User, Depends(current_user)]
CurrentLogin = Annotated[LoginSession, Depends(current_login)]
