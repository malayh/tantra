from __future__ import annotations

from collections.abc import AsyncIterator

from tantra.loop import Emitted


async def collect(stream: AsyncIterator[Emitted]) -> list[Emitted]:
    """Drain an event stream into a list. The turn advances only while someone consumes it."""
    return [emitted async for emitted in stream]
