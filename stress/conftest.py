from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from tantra import FileSystemStore, MemoryStore, PostgresStore, SQLiteStore, Store

try:
    import psycopg
    from psycopg import sql

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False

IMAGE = "pgvector/pgvector:pg17"
PASSWORD = "tantra"
STARTUP_DEADLINE = 60.0
DOCKER_TIMEOUT = 120

BACKENDS = ("memory", "fs", "sqlite", "postgres")


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    external = os.environ.get("TANTRA_POSTGRES_DSN")
    if external:
        yield external
        return
    container, dsn = _start_postgres()
    try:
        yield dsn
    finally:
        subprocess.run(["docker", "stop", container], capture_output=True, timeout=DOCKER_TIMEOUT)


@pytest.fixture(params=BACKENDS)
async def store(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Store]:
    backend = request.param
    dsn, schema = "", f"s_{uuid.uuid4().hex[:8]}"
    if backend == "memory":
        made: Store = MemoryStore()
    elif backend == "fs":
        made = FileSystemStore(tmp_path / "sessions")
    elif backend == "sqlite":
        made = SQLiteStore(tmp_path / "sessions.db")
    else:
        dsn = request.getfixturevalue("postgres_dsn")
        made = PostgresStore(dsn, schema=schema)
    await made.setup()
    try:
        yield made
    finally:
        await close_store(made)
        if dsn:
            drop_schema(dsn, schema)


async def close_store(store: Store) -> None:
    closer = getattr(store, "close", None)
    if closer is not None:
        await closer()


def drop_schema(dsn: str, schema: str) -> None:
    """Discard a per-test schema so an external `TANTRA_POSTGRES_DSN` does not accumulate them."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def _start_postgres() -> tuple[str, str]:
    if not HAS_PSYCOPG:
        pytest.skip("psycopg is not installed: install tantra-harness[postgres]")
    container = ""
    try:
        port = _free_port()
        container = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "-e",
                f"POSTGRES_PASSWORD={PASSWORD}",
                "-p",
                f"{port}:5432",
                IMAGE,
                "-c",
                "max_connections=300",
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=DOCKER_TIMEOUT,
        ).stdout.strip()
        dsn = f"postgresql://postgres:{PASSWORD}@127.0.0.1:{port}/postgres"
        _await_ready(dsn)
        return container, dsn
    except (OSError, subprocess.SubprocessError, TimeoutError) as exc:
        if container:
            subprocess.run(["docker", "stop", container], capture_output=True, timeout=DOCKER_TIMEOUT)
        pytest.skip(f"no docker postgres available: {exc}")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _await_ready(dsn: str) -> None:
    deadline = time.monotonic() + STARTUP_DEADLINE
    while True:
        try:
            psycopg.connect(dsn, connect_timeout=3).close()
            return
        except psycopg.Error as exc:
            if time.monotonic() > deadline:
                raise TimeoutError(f"postgres refused connections for {STARTUP_DEADLINE}s: {exc}") from exc
            time.sleep(0.25)
