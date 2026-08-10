from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, HTTPException, UploadFile, status

from sarathi.auth import CurrentUser
from sarathi.config import get_settings
from sarathi.schemas import Attachment

router = APIRouter(prefix="/api/uploads", tags=["uploads"])

ALLOWED_SUFFIXES = {".pdf", ".docx"}
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
CHUNK_SIZE = 1024 * 1024


@router.post("", status_code=status.HTTP_201_CREATED, response_model=Attachment)
async def create_upload(user: CurrentUser, file: UploadFile) -> Attachment:
    name = Path(file.filename or "").name
    if Path(name).suffix.lower() not in ALLOWED_SUFFIXES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Only .pdf and .docx files can be uploaded",
        )

    directory = Path(get_settings().UPLOAD_DIR) / str(user.id)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{uuid4().hex}_{name}"

    total = 0
    try:
        with target.open("wb") as sink:
            while chunk := await file.read(CHUNK_SIZE):
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    break
                sink.write(chunk)
    except BaseException:
        target.unlink(missing_ok=True)
        raise

    if total > MAX_UPLOAD_BYTES:
        target.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"Uploads are limited to {MAX_UPLOAD_BYTES} bytes",
        )
    return Attachment(path=str(target), name=name)
