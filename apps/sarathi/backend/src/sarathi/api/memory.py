from fastapi import APIRouter, HTTPException, status

from sarathi.agent import ResourcesDep
from sarathi.auth import CurrentUser
from sarathi.schemas import MemoryOut
from tantra import MemoryRecord

router = APIRouter(prefix="/api/memory", tags=["memory"])


def _out(row: MemoryRecord) -> MemoryOut:
    return MemoryOut(
        id=row.id,
        kind=row.kind,
        title=row.title,
        body=row.body,
        tags=list(row.tags),
        created_at=row.created_at,
    )


@router.get("", response_model=list[MemoryOut])
async def list_memory(user: CurrentUser, resources: ResourcesDep) -> list[MemoryOut]:
    rows = await resources.store.memory_all(metadata={"user": str(user.id)})
    rows.sort(key=lambda row: row.created_at, reverse=True)
    return [_out(row) for row in rows]


@router.delete("/{memory_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_memory(memory_id: str, user: CurrentUser, resources: ResourcesDep) -> None:
    if not await resources.memory.delete(memory_id, scope={"user": str(user.id)}):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Memory not found")
