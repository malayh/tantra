from __future__ import annotations

import asyncio
import fcntl
import os
import sys
from typing import Any, Protocol, TextIO

MAX_FRAME = 1024 * 1024


class Transport(Protocol):
    async def send(self, frame: str) -> None: ...

    async def receive(self) -> str | None: ...

    async def close(self) -> None: ...


class MemoryTransport:
    def __init__(self) -> None:
        self.peer: MemoryTransport | None = None
        self.closed = False
        self._eof = False
        self._inbox: asyncio.Queue[str | None] = asyncio.Queue()

    async def send(self, frame: str) -> None:
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport is closed")
        self.peer._inbox.put_nowait(frame)

    async def receive(self) -> str | None:
        if self._eof:
            return None
        frame = await self._inbox.get()
        if frame is None:
            self._eof = True
        return frame

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._inbox.put_nowait(None)
        if self.peer is not None:
            self.peer._inbox.put_nowait(None)


def memory_pair() -> tuple[MemoryTransport, MemoryTransport]:
    left, right = MemoryTransport(), MemoryTransport()
    left.peer, right.peer = right, left
    return left, right


def _restore(saved: tuple[int, int], flags: tuple[int, int], stdout: TextIO) -> None:
    for fd, flag in zip(saved, flags, strict=True):
        fcntl.fcntl(fd, fcntl.F_SETFL, flag)
    sys.stdout = stdout
    for target, fd in enumerate(saved):
        os.dup2(fd, target)
        os.close(fd)


class StdioTransport:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        source: asyncio.BaseTransport,
        saved: tuple[int, int],
        flags: tuple[int, int],
        stdout: TextIO,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.closed = False
        self._source = source
        self._saved = saved
        self._flags = flags
        self._stdout = stdout

    @classmethod
    async def open(cls) -> StdioTransport:
        loop = asyncio.get_running_loop()
        sys.stdout.flush()
        stdout = sys.stdout
        saved = (os.dup(0), os.dup(1))
        flags = (fcntl.fcntl(0, fcntl.F_GETFL), fcntl.fcntl(1, fcntl.F_GETFL))
        files: list[Any] = []
        try:
            files.append(os.fdopen(os.dup(saved[0]), "rb", buffering=0))
            files.append(os.fdopen(os.dup(saved[1]), "wb", buffering=0))
            devnull = os.open(os.devnull, os.O_RDONLY)
            os.dup2(devnull, 0)
            os.close(devnull)
            os.dup2(2, 1)
            sys.stdout = sys.stderr
            reader = asyncio.StreamReader(limit=MAX_FRAME)
            source, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), files[0])
            try:
                sink, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, files[1])
            except BaseException:
                source.close()
                raise
        except BaseException:
            for file in files:
                file.close()
            _restore(saved, flags, stdout)
            raise
        writer = asyncio.StreamWriter(sink, protocol, None, loop)
        return cls(reader, writer, source, saved, flags, stdout)

    async def send(self, frame: str) -> None:
        if self.closed or self.writer.is_closing():
            raise ConnectionError("transport is closed")
        self.writer.write(frame.encode("utf-8") + b"\n")
        await self.writer.drain()

    async def receive(self) -> str | None:
        while not self.closed:
            try:
                line = await self.reader.readuntil(b"\n")
            except asyncio.IncompleteReadError as exc:
                line = exc.partial
                if not line.strip():
                    return None
            except asyncio.LimitOverrunError:
                return None
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                return text
        return None

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._source.close()
        self.writer.close()
        _restore(self._saved, self._flags, self._stdout)
