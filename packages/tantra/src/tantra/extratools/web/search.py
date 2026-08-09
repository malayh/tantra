from __future__ import annotations

import httpx

from tantra.tools import Tool


def web_search(api_key: str, *, http_client: httpx.AsyncClient | None = None) -> Tool:
    raise NotImplementedError
