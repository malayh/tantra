from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator

import pytest

try:
    import psycopg

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False

IMAGE = "pgvector/pgvector:pg17"
PASSWORD = "tantra"
STARTUP_DEADLINE = 60.0
DOCKER_TIMEOUT = 120


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


@pytest.fixture
def pg_schema() -> str:
    return f"t_{uuid.uuid4().hex[:8]}"


def _start_postgres() -> tuple[str, str]:
    if not HAS_PSYCOPG:
        pytest.skip("psycopg is not installed: install tantra[postgres]")
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
