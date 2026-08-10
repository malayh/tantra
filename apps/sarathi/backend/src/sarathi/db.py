from collections.abc import AsyncIterator
from functools import cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from sarathi.config import get_settings


class Base(DeclarativeBase):
    pass


@cache
def get_engine() -> AsyncEngine:
    return create_async_engine(get_settings().DATABASE_URL, pool_pre_ping=True)


@cache
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


async def get_db() -> AsyncIterator[AsyncSession]:
    async with get_session_factory()() as session:
        yield session


DbDep = Annotated[AsyncSession, Depends(get_db)]
