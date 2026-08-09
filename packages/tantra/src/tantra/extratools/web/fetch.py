from __future__ import annotations

from importlib.util import find_spec

from tantra.tools import Tool

if find_spec("curl_cffi") is None or find_spec("trafilatura") is None:
    raise ImportError("curl-cffi/trafilatura not installed: install tantra-harness[web]")


def web_fetch(*, max_chars: int = 64_000, timeout: float = 20.0, ssrf_guard: bool = True) -> Tool:
    raise NotImplementedError
