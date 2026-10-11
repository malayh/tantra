from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.routing import WebSocketRoute

from sarathi.agent import RuntimeResources, close_resources, make_resources
from sarathi.api import auth, memory, meta, sessions, uploads, ws
from sarathi.config import get_settings
from sarathi.db import get_session_factory
from sarathi.telemetry import setup_instrumentation, shutdown_telemetry


def mount_session_socket(
    application: FastAPI,
    resources: RuntimeResources,
    session_factory: async_sessionmaker[AsyncSession],
) -> WebSocketRoute:
    route = WebSocketRoute("/api/ws/sessions/{session_id}", ws.build_session_socket(resources, session_factory))
    application.router.routes.append(route)
    return route


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    Path(get_settings().UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    resources = await make_resources()
    application.state.resources = resources
    socket_route = mount_session_socket(application, resources, get_session_factory())
    try:
        yield
    finally:
        application.router.routes.remove(socket_route)
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

    for router in (
        auth.router,
        sessions.router,
        memory.router,
        uploads.router,
        meta.router,
        meta.models_router,
    ):
        application.include_router(router)
    setup_instrumentation(application)
    return application


app = create_app()
