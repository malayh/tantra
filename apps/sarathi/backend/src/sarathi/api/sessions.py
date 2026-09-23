from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, status

from sarathi.agent import ResourcesDep, Sarathi
from sarathi.auth import CurrentUser
from sarathi.config import get_settings
from sarathi.schemas import ActorStatusOut, CreateSessionRequest, PatchSessionRequest, SessionOut, TurnSummaryOut
from tantra import ActorStatus, SessionHeader

router = APIRouter(prefix="/api/sessions", tags=["sessions"])


def _out(header: SessionHeader) -> SessionOut:
    return SessionOut(
        id=header.id,
        title=header.title,
        status=header.status,
        model=header.model,
        updated_at=header.updated_at,
    )


def _actor_out(actor: ActorStatus) -> ActorStatusOut:
    last_turn = actor.last_turn
    return ActorStatusOut(
        agent_id=actor.agent_id.hex,
        root_id=actor.root_id.hex,
        parent_id=actor.parent_id.hex if actor.parent_id is not None else None,
        agent=actor.agent,
        name=actor.name,
        state=actor.state,
        active=actor.active,
        current_turn_id=actor.current_turn_id.hex if actor.current_turn_id is not None else None,
        last_turn=(
            TurnSummaryOut(
                turn_id=last_turn.turn_id.hex,
                outcome=last_turn.outcome,
                stop_reason=last_turn.stop_reason,
                error=last_turn.error,
            )
            if last_turn is not None
            else None
        ),
        last_seq=actor.last_seq,
        updated_at=actor.updated_at,
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


@router.get("/{root_id}/actors")
async def list_actors(root_id: str, user: CurrentUser, resources: ResourcesDep) -> list[ActorStatusOut]:
    try:
        root = UUID(hex=root_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found") from None
    header = await resources.store.header(root.hex)
    if header is None or header.parent_id is not None or header.metadata.get("user") != str(user.id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    return [_actor_out(actor) for actor in await resources.runtime.tree_status(root)]


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
