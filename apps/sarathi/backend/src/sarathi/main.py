from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute

from sarathi.api import auth, memory, meta, sessions, uploads, ws
from sarathi.config import get_settings


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    Path(get_settings().UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    yield


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

    for router in (auth.router, sessions.router, memory.router, uploads.router, ws.router, meta.router):
        application.include_router(router)
    return application


app = create_app()
