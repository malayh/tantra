from __future__ import annotations

import json
import select
import shutil
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any

import pytest
from acp_support import VALIDATOR

ROOT = Path(__file__).resolve().parents[3]
CLIENT = ROOT / "packages" / "tantra-acp-client"
SERVER = CLIENT / "test" / "server.py"
INTEGRATION = CLIENT / "test" / "integration.mjs"
CLIENT_METHODS = {
    "initialize",
    "session/new",
    "session/list",
    "session/resume",
    "session/prompt",
    "session/cancel",
    "session/close",
    "session/delete",
    "session/set_config_option",
    "_tantra/session/claim_writer",
}


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def output(process: subprocess.Popen[str]) -> str:
    stdout = process.stdout.read() if process.stdout is not None else ""
    stderr = process.stderr.read() if process.stderr is not None else ""
    return stdout + stderr


def control(base_url: str, path: str, *, post: bool = False) -> Any:
    request = urllib.request.Request(f"{base_url}{path}", data=b"{}" if post else None)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def schema_errors(connections: list[list[dict[str, Any]]]) -> list[str]:
    problems = []
    for frames in connections:
        sent = [item["frame"] for item in frames if item["from"] == "server"]
        methods = {frame["id"]: frame["method"] for frame in sent if "method" in frame and "id" in frame}
        for item in frames:
            if item["from"] == "client":
                problems += VALIDATOR.frame_errors(item["frame"], methods)
    return problems


def test_typescript_client_against_two_real_endpoints(postgres_dsn: str, pg_schema: str) -> None:
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
        base_url = json.loads(line)["baseUrl"]
        result = subprocess.run(
            [node, str(INTEGRATION), base_url], cwd=CLIENT, capture_output=True, text=True, timeout=90
        )
        assert result.returncode == 0, result.stdout + result.stderr
        connections = control(base_url, "/control/frames")
        control(base_url, "/control/shutdown", post=True)
        assert server.wait(timeout=30) == 0, output(server)
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
    sent = {item["frame"].get("method") for frames in connections for item in frames if item["from"] == "client"}
    answered = [
        item["frame"]
        for frames in connections
        for item in frames
        if item["from"] == "client" and "result" in item["frame"]
    ]
    assert len(connections) >= 3
    assert CLIENT_METHODS <= sent
    assert answered
    assert schema_errors(connections) == []
