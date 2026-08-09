from __future__ import annotations

from importlib.util import find_spec

from tantra.tools import Tool

if find_spec("pypdf") is None or find_spec("docx") is None:
    raise ImportError("pypdf/python-docx not installed: install tantra-harness[doc]")


def read_doc() -> Tool:
    raise NotImplementedError
