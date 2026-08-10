from fastapi import APIRouter, HTTPException

from sarathi.auth import CurrentUser
from sarathi.config import get_settings
from sarathi.schemas import ClientFrame, ServerFrame

router = APIRouter(prefix="/api/meta", tags=["meta"])
models_router = APIRouter(prefix="/api", tags=["meta"])


@router.post("/ws-types")
async def ws_types(frame: ClientFrame) -> ServerFrame:
    raise HTTPException(status_code=400, detail=repr(frame))


@models_router.get("/models")
async def list_models(user: CurrentUser) -> list[str]:
    return get_settings().models
