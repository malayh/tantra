"""Manual live smoke for web_fetch through a proxy — run by a human, never collected by pytest.

    TANTRA_TEST_PROXY=http://user:pass@host:port uv run python stress/live_fetch_proxy.py

Fetches https://api.ipify.org?format=json once directly and once through the proxy, prints both IPs,
and exits non-zero if the proxied fetch fails or returns the same IP as the direct one.
"""

from __future__ import annotations

import asyncio
import json
import os

from tantra import Context
from tantra.extratools.web import web_fetch

URL = "https://api.ipify.org?format=json"


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


def parse_ip(result: str) -> str:
    body = [line for line in result.splitlines() if line.strip()][-1]
    return str(json.loads(body)["ip"])


async def main() -> int:
    proxy = os.environ.get("TANTRA_TEST_PROXY")
    if not proxy:
        print("live_fetch_proxy needs TANTRA_TEST_PROXY exported; refusing to run.")
        return 2

    direct = parse_ip(await web_fetch().invoke({"url": URL}, make_ctx()))
    print(f"direct: {direct}")

    try:
        proxied = parse_ip(await web_fetch(proxy=proxy).invoke({"url": URL}, make_ctx()))
    except Exception as exc:
        print(f"proxied: FAILED — {exc}")
        return 1
    print(f"proxied: {proxied}")

    if proxied == direct:
        print("the proxied fetch came from the same IP as the direct one; the proxy is not being used.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
