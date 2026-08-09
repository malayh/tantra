from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import docx
import pytest
from pypdf import PdfWriter

from tantra.extratools import doc
from tantra.extratools.doc import extract_docx, extract_pdf, read_doc
from tantra.tools import Context

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_PDF = FIXTURES / "sample.pdf"
SAMPLE_DOCX = FIXTURES / "sample.docx"
PDF_SENTENCE = "The lighthouse keeper counted forty-one gulls before breakfast."
DOCX_SENTENCE = "Marginalia in the ledger mentions a missing brass telescope."


def make_ctx() -> Context:
    async def emit(message: str) -> None:
        return None

    return Context(
        session_id="s1",
        turn_id="t1",
        call_id="c1",
        depth=0,
        deps=None,
        store=None,
        emit=emit,
    )


def blank_pdf() -> bytes:
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def empty_docx() -> bytes:
    buffer = BytesIO()
    docx.Document().save(buffer)
    return buffer.getvalue()


def test_the_tool_offers_only_the_path_to_the_model() -> None:
    tool = read_doc()

    assert tool.name == "read_doc"
    assert set(tool.schema.parameters["properties"]) == {"path"}


def test_extract_pdf_reads_the_text_out_of_pdf_bytes() -> None:
    assert PDF_SENTENCE in extract_pdf(SAMPLE_PDF.read_bytes())


def test_extract_docx_reads_every_paragraph_out_of_docx_bytes() -> None:
    text = extract_docx(SAMPLE_DOCX.read_bytes())

    assert DOCX_SENTENCE in text
    assert "The second paragraph is here only to prove paragraphs are joined." in text


@pytest.mark.parametrize(("path", "sentence"), [(SAMPLE_PDF, PDF_SENTENCE), (SAMPLE_DOCX, DOCX_SENTENCE)])
async def test_the_tool_extracts_both_supported_types_by_suffix(path: Path, sentence: str) -> None:
    result = await read_doc().invoke({"path": str(path)}, make_ctx())

    assert sentence in result


async def test_an_unsupported_suffix_names_the_types_that_do_work(tmp_path: Path) -> None:
    note = tmp_path / "notes.txt"
    note.write_text("plain text is not a document format here")

    with pytest.raises(RuntimeError) as info:
        await read_doc().invoke({"path": str(note)}, make_ctx())

    assert ".pdf, .docx" in str(info.value)
    assert ".txt" in str(info.value)


async def test_a_missing_file_names_the_path_that_was_not_found(tmp_path: Path) -> None:
    missing = tmp_path / "absent.pdf"

    with pytest.raises(RuntimeError) as info:
        await read_doc().invoke({"path": str(missing)}, make_ctx())

    assert str(missing) in str(info.value)
    assert "check the path" in str(info.value)


@pytest.mark.parametrize(("name", "kind"), [("broken.pdf", "PDF"), ("broken.docx", "Word document")])
async def test_a_corrupt_document_is_described_rather_than_raising_the_parser_error(
    tmp_path: Path, name: str, kind: str
) -> None:
    broken = tmp_path / name
    broken.write_bytes(b"this is not a document, it is a sentence pretending to be one")

    with pytest.raises(RuntimeError) as info:
        await read_doc().invoke({"path": str(broken)}, make_ctx())

    assert f"could not be read as a {kind}" in str(info.value)
    assert "corrupt" in str(info.value)


async def test_an_unreadable_file_names_the_path_rather_than_leaking_the_errno(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("root reads a file whatever its mode, so the permission error never fires")
    locked = tmp_path / "locked.pdf"
    locked.write_bytes(SAMPLE_PDF.read_bytes())
    locked.chmod(0o000)

    with pytest.raises(RuntimeError) as info:
        await read_doc().invoke({"path": str(locked)}, make_ctx())

    assert str(locked) in str(info.value)
    assert "could not read the file" in str(info.value)
    assert "permissions" in str(info.value)


async def test_an_uppercase_suffix_is_dispatched_like_a_lowercase_one(tmp_path: Path) -> None:
    shouted = tmp_path / "SAMPLE.PDF"
    shouted.write_bytes(SAMPLE_PDF.read_bytes())

    result = await read_doc().invoke({"path": str(shouted)}, make_ctx())

    assert PDF_SENTENCE in result


def test_a_pdf_with_no_text_layer_blames_the_scan_and_not_the_reader() -> None:
    with pytest.raises(RuntimeError) as info:
        extract_pdf(blank_pdf())

    assert "the PDF holds no extractable text" in str(info.value)
    assert "scan" in str(info.value)


def test_a_word_document_with_no_text_blames_the_scan_and_not_the_reader() -> None:
    with pytest.raises(RuntimeError) as info:
        extract_docx(empty_docx())

    assert "the Word document holds no extractable text" in str(info.value)
    assert "scan" in str(info.value)


async def test_a_long_document_is_truncated_with_an_explicit_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doc, "extract_pdf", lambda data: "z" * 70_000)

    result = await read_doc().invoke({"path": str(SAMPLE_PDF)}, make_ctx())

    assert result == "z" * 64_000 + "\n[truncated at 64000 chars]"
