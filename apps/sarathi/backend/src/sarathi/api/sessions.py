from fastapi import APIRouter, HTTPException, status

from sarathi.agent import FactoryDep, Sarathi, close_harness
from sarathi.auth import CurrentUser
from sarathi.config import get_settings
from sarathi.schemas import CreateSessionRequest, PatchSessionRequest, SessionOut
from tantra import SessionHeader

router = APIRouter(prefix="/api/sessions", tags=["sessions"])


def _out(header: SessionHeader) -> SessionOut:
    return SessionOut(
        id=header.id,
        title=header.title,
        status=header.status,
        model=header.metadata.get("model"),
        updated_at=header.updated_at,
    )


def _resolve_model(model: str | None) -> str:
    settings = get_settings()
    if model is None:
        return settings.default_model
    if model not in settings.models:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=f"Unknown model {model!r}")
    return model


@router.get("")
async def list_sessions(user: CurrentUser, factory: FactoryDep) -> list[SessionOut]:
    harness = factory(None)
    try:
        headers = await harness.store.list(metadata={"user": str(user.id), "kind": "root"})
        return [_out(header) for header in headers if header.parent_id is None]
    finally:
        await close_harness(harness)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_session(body: CreateSessionRequest, user: CurrentUser, factory: FactoryDep) -> SessionOut:
    model = _resolve_model(body.model)
    harness = factory(model)
    try:
        metadata = {"user": str(user.id), "kind": "root", "model": model, "title": None}
        return _out(await harness.create_session(Sarathi, metadata=metadata))
    finally:
        await close_harness(harness)


@router.patch("/{session_id}")
async def patch_session(
    session_id: str, body: PatchSessionRequest, user: CurrentUser, factory: FactoryDep
) -> SessionOut:
    harness = factory(None)
    try:
        header = await harness.store.header(session_id)
        if header is None or header.metadata.get("user") != str(user.id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
        patched = await harness.store.patch_header(session_id, metadata={"model": _resolve_model(body.model)})
        return _out(patched)
    finally:
        await close_harness(harness)
