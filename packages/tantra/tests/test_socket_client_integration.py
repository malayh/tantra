from __future__ import annotations

import json
import select
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
CLIENT = ROOT / "packages" / "tantra-client"
SERVER = CLIENT / "test" / "server.py"
INTEGRATION = CLIENT / "test" / "integration.mjs"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def output(process: subprocess.Popen[str]) -> str:
    stdout = process.stdout.read() if process.stdout is not None else ""
    stderr = process.stderr.read() if process.stderr is not None else ""
    return stdout + stderr


def test_native_client_against_two_real_endpoints(postgres_dsn: str, pg_schema: str) -> None:
    npm = shutil.which("npm")
    node = shutil.which("node")
    if npm is None or node is None:
        pytest.skip("Node and npm are required for the TypeScript client integration")
    subprocess.run([npm, "run", "build", "--silent"], cwd=CLIENT, check=True, timeout=60)
    server = subprocess.Popen(
        [sys.executable, str(SERVER), postgres_dsn, pg_schema, str(free_port())],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert server.stdout is not None
        readable, _, _ = select.select([server.stdout], [], [], 20)
        if not readable:
            server.terminate()
            server.wait(timeout=5)
            raise AssertionError(f"integration server did not start:\n{output(server)}")
        line = server.stdout.readline()
        if not line:
            server.wait(timeout=5)
            raise AssertionError(f"integration server exited during startup:\n{output(server)}")
        ready = json.loads(line)
        result = subprocess.run(
            [node, str(INTEGRATION), ready["baseUrl"], ready["rootId"], ready["childId"]],
            cwd=CLIENT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
