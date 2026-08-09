from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path

from tantra.tools import Tool, tool

try:
    import docx
    from pypdf import PdfReader
except ImportError as exc:
    raise ImportError("pypdf/python-docx not installed: install tantra-harness[doc]") from exc

MAX_CHARS = 64_000
SUPPORTED = ".pdf, .docx"


def extract_pdf(data: bytes) -> str:
    try:
        pages = [page.extract_text() or "" for page in PdfReader(BytesIO(data)).pages]
    except Exception as exc:
        raise RuntimeError(_corrupt("PDF", exc)) from exc
    return _nonempty("\n\n".join(pages), "PDF")


def extract_docx(data: bytes) -> str:
    try:
        paragraphs = [paragraph.text for paragraph in docx.Document(BytesIO(data)).paragraphs]
    except Exception as exc:
        raise RuntimeError(_corrupt("Word document", exc)) from exc
    return _nonempty("\n".join(paragraphs), "Word document")


def read_doc() -> Tool:
    @tool
    async def read_doc(path: str) -> str:
        """Read a PDF or Word document on this machine and return its text.

        Usage:
        - Pass a path to a local file, absolute or relative to the working directory. This tool
          reads the filesystem only; it never downloads anything, so a URL will not work here —
          use `web_fetch` for documents that live on the web.
        - Supported types are `.pdf` and `.docx`, chosen by the file extension. Plain text, source
          code, CSV and Markdown are not read here — open those with the file or shell tools.
        - Only the text comes back. Tables lose their layout, and images, charts, headers and
          footers are dropped, so do not claim a figure said something you cannot see.
        - Page and paragraph breaks survive as blank lines. There are no page numbers, so cite the
          document by name rather than by page.
        - Long documents are cut with a `[truncated at N chars]` marker. That is everything you get:
          reading the same path again returns the same truncated text, so summarise what arrived
          instead of asking for the rest.
        - Every failure names what went wrong — a missing path, an unsupported type, a corrupt
          file, or a scan with no text layer. Read the message and act on it; running the same call
          again produces the same error.
        - A document that is images of text (a scan or a photographed page) has no text to extract.
          Say so plainly instead of guessing at the contents.
        """
        return _cap(await asyncio.to_thread(_read, path))

    return read_doc


def _read(path: str) -> str:
    file = Path(path)
    if not file.is_file():
        raise RuntimeError(
            f"read_doc found no file at {path!r} — check the path against the working directory, "
            "or list the directory first to find the document's real name"
        )
    suffix = file.suffix.lower()
    if suffix not in (".pdf", ".docx"):
        raise RuntimeError(
            f"read_doc cannot read {path!r}: it only reads {SUPPORTED} files, not "
            f"{suffix or 'files without an extension'} — open this one with a file or shell tool instead"
        )
    try:
        data = file.read_bytes()
    except OSError as exc:
        raise RuntimeError(
            f"read_doc could not read the file at {path!r} ({exc}) — the path exists but is not readable, "
            "so check its permissions or ask the user for a copy you can open"
        ) from exc
    return extract_pdf(data) if suffix == ".pdf" else extract_docx(data)


def _corrupt(kind: str, exc: Exception) -> str:
    return (
        f"the data could not be read as a {kind} ({exc}) — it is corrupt, truncated, or not really a {kind} "
        "despite its name; check the source of the file rather than reading it again"
    )


def _nonempty(text: str, kind: str) -> str:
    if not text.strip():
        raise RuntimeError(
            f"the {kind} holds no extractable text — it is most likely a scan or images of text with no text "
            "layer, which nothing here can read; ask the user for a text version or use another source"
        )
    return text


def _cap(text: str) -> str:
    if len(text) <= MAX_CHARS:
        return text
    return f"{text[:MAX_CHARS]}\n[truncated at {MAX_CHARS} chars]"
