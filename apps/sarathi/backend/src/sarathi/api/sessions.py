from uuid import uuid4

from fastapi import APIRouter, HTTPException, status

from sarathi.agent import ResourcesDep, Sarathi
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
        model=header.model,
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
async def list_sessions(user: CurrentUser, resources: ResourcesDep) -> list[SessionOut]:
    headers = await resources.store.list(metadata={"user": str(user.id), "kind": "root"})
    return [_out(header) for header in headers if header.parent_id is None]


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_session(body: CreateSessionRequest, user: CurrentUser, resources: ResourcesDep) -> SessionOut:
    model = _resolve_model(body.model)
    metadata = {"user": str(user.id), "kind": "root"}
    sid = await resources.runtime.create(Sarathi, session_id=uuid4(), model=model, metadata=metadata)
    header = await resources.store.header(sid.hex)
    assert header is not None
    return _out(header)


@router.patch("/{session_id}")
async def patch_session(
    session_id: str,
    body: PatchSessionRequest,
    user: CurrentUser,
    resources: ResourcesDep,
) -> SessionOut:
    header = await resources.store.header(session_id)
    if header is None or header.metadata.get("user") != str(user.id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    patched = await resources.store.patch_header(session_id, model=_resolve_model(body.model))
    return _out(patched)
