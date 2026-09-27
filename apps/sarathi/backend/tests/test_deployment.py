import asyncio
import logging
import socket
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket
from websockets.asyncio.client import connect

SARATHI_DIR = Path(__file__).resolve().parents[2]


def test_nginx_logs_do_not_include_query_bearing_request_fields() -> None:
    config = (SARATHI_DIR / "nginx.conf").read_text()

    assert "error_log /dev/stderr crit;" in config
    assert '"$request"' not in config
    assert "$request_uri" not in config
    assert "$args" not in config


def test_uvicorn_access_logs_are_disabled() -> None:
    dockerfile = (SARATHI_DIR / "backend" / "Dockerfile").read_text()
    justfile = (SARATHI_DIR / "backend" / "justfile").read_text()

    assert '"--no-access-log"' in dockerfile
    assert '"--log-level", "warning"' in dockerfile
    assert "uv run uvicorn sarathi.main:app --reload --port 8000 --no-access-log --log-level warning" in justfile


async def test_uvicorn_warning_level_suppresses_websocket_query_logs() -> None:
    application = FastAPI()

    @application.websocket("/socket")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    messages: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    loggers = {name: logging.getLogger(name) for name in ("uvicorn.error", "uvicorn.access", "uvicorn.asgi")}
    original = {name: (logger.level, list(logger.handlers), logger.propagate) for name, logger in loggers.items()}
    logger = loggers["uvicorn.error"]
    handler = Capture()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.setblocking(False)
    server: uvicorn.Server | None = None
    task: asyncio.Task[None] | None = None
    try:
        config = uvicorn.Config(application, log_config=None, log_level="warning", access_log=False, lifespan="off")
        logger.addHandler(handler)
        server = uvicorn.Server(config)
        task = asyncio.create_task(server.serve(sockets=[listener]))
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started
        port = listener.getsockname()[1]
        async with connect(f"ws://127.0.0.1:{port}/socket?token=redacted", proxy=None):
            pass
        assert not any("WebSocket" in message or "token=" in message for message in messages)
    finally:
        try:
            if server is not None and task is not None:
                server.should_exit = True
                await asyncio.wait_for(task, 2)
        finally:
            listener.close()
            for name, (level, handlers, propagate) in original.items():
                loggers[name].setLevel(level)
                loggers[name].handlers = handlers
                loggers[name].propagate = propagate

    assert {
        name: (logger.level, list(logger.handlers), logger.propagate) for name, logger in loggers.items()
    } == original


def test_compose_isolates_e2e_and_forces_internal_nginx_url() -> None:
    compose = (SARATHI_DIR / "docker-compose.yaml").read_text()
    override = (SARATHI_DIR / "docker-compose.e2e.yaml").read_text()

    assert "API_URL_INTERNAL: http://nginx:8000" in compose
    assert "http://127.0.0.1:8000/api/health" in compose
    assert override.startswith("name: tantra011e2e\n")
    assert "e2e_provider:app" in override
    assert "E2E_GATE_URL: http://gate:8090" in override
    assert "E2E_COORDINATOR_LEASE_TTL: ${E2E_COORDINATOR_LEASE_TTL:-6}" in override
    assert "127.0.0.1:${SARATHI_E2E_GATE_PORT:-18090}:8090" in override
    assert "gate:" not in compose
