import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx

Signup = Callable[..., Awaitable[str]]

PDF_BYTES = b"%PDF-1.4 pretend this is a document"
OVERSIZED = b"x" * (21 * 1024 * 1024)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _uid(client: httpx.AsyncClient, token: str) -> str:
    response = await client.get("/api/auth/me", headers=_auth(token))
    assert response.status_code == 200
    return str(response.json()["id"])


async def _upload(client: httpx.AsyncClient, token: str, filename: str, body: bytes) -> httpx.Response:
    return await client.post(
        "/api/uploads",
        files={"file": (filename, body, "application/octet-stream")},
        headers=_auth(token),
    )


async def test_pdf_upload_is_stored_under_the_user_directory(
    client: httpx.AsyncClient, signup: Signup, upload_dir: Path
) -> None:
    token = await signup()
    response = await _upload(client, token, "paper.pdf", PDF_BYTES)

    assert response.status_code == 201
    body = response.json()
    assert set(body) == {"path", "name"}
    assert body["name"] == "paper.pdf"

    saved = Path(body["path"])
    assert saved.parent == upload_dir / await _uid(client, token)
    assert saved.read_bytes() == PDF_BYTES

    prefix, _, rest = saved.name.partition("_")
    assert rest == "paper.pdf"
    assert uuid.UUID(hex=prefix).hex == prefix


async def test_unsupported_extension_is_rejected(client: httpx.AsyncClient, signup: Signup, upload_dir: Path) -> None:
    token = await signup()
    response = await _upload(client, token, "notes.txt", b"plain text")

    assert response.status_code == 415
    assert not list(upload_dir.rglob("*.txt"))


async def test_oversized_upload_is_rejected_and_leaves_nothing_on_disk(
    client: httpx.AsyncClient, signup: Signup, upload_dir: Path
) -> None:
    token = await signup()
    response = await _upload(client, token, "huge.pdf", OVERSIZED)

    assert response.status_code == 413
    assert [path for path in upload_dir.rglob("*") if path.is_file()] == []


async def test_upload_requires_authentication(client: httpx.AsyncClient, upload_dir: Path) -> None:
    response = await client.post("/api/uploads", files={"file": ("paper.pdf", PDF_BYTES, "application/pdf")})

    assert response.status_code == 401
    assert not upload_dir.exists()


async def test_traversal_filename_is_reduced_to_its_basename(
    client: httpx.AsyncClient, signup: Signup, upload_dir: Path
) -> None:
    token = await signup()
    response = await _upload(client, token, "../../evil.pdf", PDF_BYTES)

    assert response.status_code == 201
    saved = Path(response.json()["path"])
    assert response.json()["name"] == "evil.pdf"
    assert saved.parent == upload_dir / await _uid(client, token)
    assert saved.name.endswith("_evil.pdf")
    assert saved.is_file()
