from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from starlette.routing import WebSocketRoute

from sarathi.agent import RuntimeResources, close_resources, make_resources
from sarathi.api import acp, auth, memory, uploads
from sarathi.config import get_settings
from sarathi.telemetry import setup_instrumentation, shutdown_telemetry
from tantra.acp import AcpWebSocket


def mount_acp(application: FastAPI, resources: RuntimeResources) -> tuple[WebSocketRoute, AcpWebSocket]:
    endpoint = acp.build_acp_endpoint(resources)
    route = WebSocketRoute("/api/acp", endpoint)
    application.router.routes.append(route)
    return route, endpoint


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    Path(get_settings().UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    resources = await make_resources()
    application.state.resources = resources
    route, endpoint = mount_acp(application, resources)
    try:
        yield
    finally:
        application.router.routes.remove(route)
        await endpoint.aclose()
        await close_resources(resources)
        shutdown_telemetry()


def unique_id(route: APIRoute) -> str:
    return route.name


def create_app() -> FastAPI:
    application = FastAPI(
        title="sarathi",
        separate_input_output_schemas=False,
        generate_unique_id_function=unique_id,
        lifespan=lifespan,
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=get_settings().cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @application.get("/api/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    for router in (auth.router, memory.router, uploads.router, acp.router):
        application.include_router(router)
    setup_instrumentation(application)
    return application


app = create_app()
