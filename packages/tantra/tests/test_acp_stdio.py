from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT = """
import asyncio
import fcntl
import json
import os
import sys

from tantra import Agent, Runtime
from tantra.acp import AcpAuth, AcpServer
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.sqlite import SQLiteStore


class Bot(Agent):
    pass


def snapshot():
    return [(os.fstat(fd), fcntl.fcntl(fd, fcntl.F_GETFL)) for fd in (0, 1)]


async def scope(auth, meta):
    print("stray print from scope")
    os.write(1, b"stray fd write from scope\\n")
    print(json.dumps({"stdin_is_devnull": os.path.samestat(os.fstat(0), os.stat(os.devnull))}), file=sys.stderr)
    return {"user": str(auth.principal)}


async def allow(access):
    return True


async def main():
    before = snapshot()
    store = SQLiteStore(sys.argv[1])
    await store.setup()
    runtime = Runtime(FakeProvider([Sample(text="hello from stdio")]), store, [Bot], default_model="m")
    server = AcpServer(runtime, agent=Bot, scope=scope, policy=allow, name="stdio-bot")
    try:
        await server.serve_stdio(AcpAuth(principal="local"))
    except Exception as exc:
        print(json.dumps({"serve_failed": type(exc).__name__}), file=sys.stderr)
    finally:
        await runtime.aclose()
    after = snapshot()
    restored = all(os.path.samestat(a[0], b[0]) and a[1] == b[1] for a, b in zip(before, after))
    print(json.dumps({"restored": restored}), file=sys.stderr)
    print("after serving", flush=True)


asyncio.run(main())
"""


def ended(message: dict[str, Any]) -> bool:
    update = message.get("params", {}).get("update", {})
    return update.get("sessionUpdate") == "state_update" and update.get("stopReason") == "end_turn"


def write_script(tmp_path: Path) -> Path:
    script = tmp_path / "acp_stdio_app.py"
    script.write_text(SCRIPT)
    return script


def frame(request_id: int, method: str, params: dict[str, Any]) -> bytes:
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode() + b"\n"


async def test_stdio_carries_only_json_rpc_and_completes_a_turn(tmp_path: Path) -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(write_script(tmp_path)),
        str(tmp_path / "sessions.db"),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    lines: list[str] = []

    async def request(request_id: int, method: str, params: dict[str, Any]) -> Any:
        process.stdin.write(frame(request_id, method, params))
        await process.stdin.drain()
        while True:
            reply = await read()
            if reply.get("id") == request_id:
                return reply["result"]

    async def read() -> dict[str, Any]:
        line = (await asyncio.wait_for(process.stdout.readline(), timeout=15)).decode()
        assert line, "stdio server closed early"
        lines.append(line)
        return json.loads(line)

    try:
        initialized = await request(1, "initialize", {"protocolVersion": 2, "info": {"name": "t", "version": "0"}})
        session_id = (await request(2, "session/new", {"cwd": str(tmp_path), "mcpServers": []}))["sessionId"]
        prompted = await request(
            3, "session/prompt", {"sessionId": session_id, "prompt": [{"type": "text", "text": "hi"}]}
        )
        while not any(ended(json.loads(line)) for line in lines):
            await read()
        process.stdin.close()
        rest, errors = await asyncio.wait_for(process.communicate(), timeout=15)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    lines.extend(line for line in rest.decode().splitlines(keepends=True) if line.strip())
    log = errors.decode()
    assert process.returncode == 0, log
    assert initialized["info"]["name"] == "stdio-bot"
    assert "messageId" in prompted
    assert lines[-1].strip() == "after serving"
    assert all(json.loads(line)["jsonrpc"] == "2.0" for line in lines[:-1])
    assert not any("stray" in line for line in lines)
    assert "stray print from scope" in log
    assert "stray fd write from scope" in log
    assert '{"stdin_is_devnull": true}' in log
    assert '{"restored": true}' in log
    texts = [
        message["params"]["update"]["content"]
        for message in map(json.loads, lines[:-1])
        if message.get("params", {}).get("update", {}).get("sessionUpdate") == "agent_message"
    ]
    assert texts == [[{"type": "text", "text": "hello from stdio"}]]


async def test_an_oversized_stdio_frame_ends_the_connection_and_restores_descriptors(tmp_path: Path) -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(write_script(tmp_path)),
        str(tmp_path / "sessions.db"),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    oversized = b'{"jsonrpc": "2.0", "id": 2, "method": "session/list", "params": {"pad": "' + b"x" * 1_200_000
    payload = frame(1, "initialize", {"protocolVersion": 2}) + oversized + b'"}}\n'
    try:
        output, errors = await asyncio.wait_for(process.communicate(payload), timeout=30)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    lines = [line for line in output.decode().splitlines() if line.strip()]
    assert process.returncode == 0, errors.decode()
    assert lines[-1] == "after serving"
    assert all(json.loads(line).get("id") != 2 for line in lines[:-1])
    assert '{"restored": true}' in errors.decode()


async def test_a_failed_stdio_open_restores_descriptors(tmp_path: Path) -> None:
    regular = tmp_path / "stdin.txt"
    regular.write_text("{}\n")
    with regular.open("rb") as stdin:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(write_script(tmp_path)),
            str(tmp_path / "sessions.db"),
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(), timeout=30)
    log = errors.decode()
    assert process.returncode == 0, log
    assert output.decode().splitlines() == ["after serving"]
    assert '{"serve_failed": "ValueError"}' in log
    assert '{"restored": true}' in log
