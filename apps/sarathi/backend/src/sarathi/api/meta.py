from fastapi import APIRouter, HTTPException

from sarathi.schemas import ClientFrame, ServerFrame

router = APIRouter(prefix="/api/meta", tags=["meta"])


@router.post("/ws-types")
async def ws_types(frame: ClientFrame) -> ServerFrame:
    raise HTTPException(status_code=400, detail=repr(frame))
