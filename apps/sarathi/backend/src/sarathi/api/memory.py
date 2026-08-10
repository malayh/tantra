from fastapi import APIRouter, HTTPException, status

from sarathi.agent import FactoryDep, close_harness
from sarathi.auth import CurrentUser
from sarathi.schemas import MemoryOut
from tantra import BuiltinMemory, MemoryRecord

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


def _owned(row: MemoryRecord, uid: str) -> bool:
    return not row.deleted and row.superseded_by is None and row.metadata.get("user") == uid


@router.get("", response_model=list[MemoryOut])
async def list_memory(user: CurrentUser, factory: FactoryDep) -> list[MemoryOut]:
    harness = factory(None)
    try:
        rows = [row for row in await harness.store.memory_all() if _owned(row, str(user.id))]
        rows.sort(key=lambda row: row.created_at, reverse=True)
        return [_out(row) for row in rows]
    finally:
        await close_harness(harness)


@router.delete("/{memory_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_memory(memory_id: str, user: CurrentUser, factory: FactoryDep) -> None:
    harness = factory(None)
    try:
        row = await harness.store.memory_get(memory_id)
        if row is None or not _owned(row, str(user.id)):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Memory not found")
        await BuiltinMemory(harness.store).delete(memory_id)
    finally:
        await close_harness(harness)
