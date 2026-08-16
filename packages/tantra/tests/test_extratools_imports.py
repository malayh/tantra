from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator

import pytest


def _evict_extratools() -> None:
    for name in [n for n in sys.modules if n == "tantra.extratools" or n.startswith("tantra.extratools.")]:
        del sys.modules[name]


@pytest.fixture(autouse=True)
def _clean_extratools() -> Iterator[None]:
    _evict_extratools()
    yield
    _evict_extratools()


def test_public_factories_import() -> None:
    from tantra.extratools.doc import read_doc
    from tantra.extratools.shell import ShellGuard, bash
    from tantra.extratools.web import web_fetch, web_search

    for factory in (read_doc, bash, ShellGuard, web_fetch, web_search):
        assert callable(factory)


def test_shell_needs_no_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    for missing in ("curl_cffi", "trafilatura", "pypdf", "docx"):
        monkeypatch.setitem(sys.modules, missing, None)

    module = importlib.import_module("tantra.extratools.shell")

    assert callable(module.bash)
    assert callable(module.ShellGuard)


@pytest.mark.parametrize("missing", ["curl_cffi", "trafilatura", "tenacity"])
def test_web_import_error_names_web_extra(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    monkeypatch.setitem(sys.modules, missing, None)

    with pytest.raises(ImportError, match=r"tantra-harness\[web\]"):
        importlib.import_module("tantra.extratools.web")


@pytest.mark.parametrize("missing", ["pypdf", "docx"])
def test_doc_import_error_names_doc_extra(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    monkeypatch.setitem(sys.modules, missing, None)

    with pytest.raises(ImportError, match=r"tantra-harness\[doc\]"):
        importlib.import_module("tantra.extratools.doc")
