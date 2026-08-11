from __future__ import annotations

import fcntl
import os
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from tantra.errors import CorruptLog, SeqConflict, SessionExists, SessionNotFound
from tantra.events import Lease, SessionEvent, SessionHeader, Stamped
from tantra.memory import MemoryRecord
from tantra.stores.base import select_headers

HEADER_FILE = "session.json"
EVENTS_FILE = "events.jsonl"
LOCK_FILE = ".lock"
MEMORY_DIR = "_memory"
TAIL_CHUNK = 65536


class FileSystemStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    async def setup(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    async def create(self, header: SessionHeader) -> None:
        directory = self.root / header.id
        try:
            directory.mkdir(parents=True, exist_ok=False)
        except FileExistsError as exc:
            raise SessionExists(header.id) from exc
        (directory / EVENTS_FILE).touch()
        (directory / LOCK_FILE).touch()
        stored = header.model_copy(deep=True)
        stored.lease = None
        self._write_header(stored)

    async def header(self, sid: str) -> SessionHeader | None:
        header = self._read_header(sid)
        if header is None:
            return None
        header.lease = self._read_lease(sid)
        return header

    async def put_header(self, h: SessionHeader) -> None:
        if not (self.root / h.id).is_dir():
            raise SessionNotFound(h.id)
        with self._flock(h.id, fcntl.LOCK_EX):
            current = self._read_header(h.id)
            if current is None:
                raise SessionNotFound(h.id)
            stored = h.model_copy(deep=True)
            stored.last_seq = current.last_seq
            stored.lease = None
            self._write_header(stored)

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        if not (self.root / sid).is_dir():
            raise SessionNotFound(sid)
        with self._flock(sid, fcntl.LOCK_EX):
            header = self._read_header(sid)
            if header is None:
                raise SessionNotFound(sid)
            tail = self._tail_seq(sid)
            if tail > header.last_seq:
                header.last_seq = tail
                self._write_header(header)
            if expect_seq is not None and expect_seq != header.last_seq:
                raise SeqConflict(f"{sid}: expected seq {header.last_seq}, got {expect_seq}")
            seq = header.last_seq
            lines = []
            for event in events:
                seq += 1
                lines.append(Stamped(seq=seq, event=event).model_dump_json() + "\n")
            if lines:
                with open(self.root / sid / EVENTS_FILE, "a", encoding="utf-8") as handle:
                    handle.write("".join(lines))
                    handle.flush()
                    os.fsync(handle.fileno())
            header.last_seq = seq
            header.updated_at = datetime.now(UTC)
            self._write_header(header)
            return seq

    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        path = self.root / sid / EVENTS_FILE
        if not path.exists():
            return
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if not line.endswith("\n"):
                    return
                if not line.strip():
                    continue
                stamped = _parse(sid, line)
                if stamped.seq > from_seq:
                    yield stamped

    async def list(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        limit: int = 50,
        before: str | None = None,
    ) -> list[SessionHeader]:
        headers = []
        if self.root.is_dir():
            for entry in self.root.iterdir():
                if not entry.is_dir():
                    continue
                header = self._read_header(entry.name)
                if header is not None:
                    headers.append(header)
        rows = select_headers(headers, metadata=metadata, parent_id=parent_id, limit=limit, before=before)
        for header in rows:
            header.lease = self._read_lease(header.id)
        return rows

    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool:
        if not (self.root / sid).is_dir():
            raise SessionNotFound(sid)
        with self._flock(sid, fcntl.LOCK_EX) as fd:
            now = datetime.now(UTC)
            lease = _load_lease(fd)
            if lease is not None and lease.holder != holder and lease.expires_at > now:
                return False
            _store_lease(fd, Lease(holder=holder, expires_at=now + timedelta(seconds=ttl)))
            return True

    async def release_lease(self, sid: str, holder: str) -> None:
        if not (self.root / sid).is_dir():
            return
        with self._flock(sid, fcntl.LOCK_EX) as fd:
            lease = _load_lease(fd)
            if lease is not None and lease.holder == holder:
                _store_lease(fd, None)

    async def memory_put(self, row: MemoryRecord) -> None:
        directory = self.root / MEMORY_DIR
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{row.id}.json"
        tmp = path.with_name(f"{row.id}.json.tmp")
        tmp.write_text(row.model_dump_json(), encoding="utf-8")
        os.replace(tmp, path)

    async def memory_get(self, mid: str) -> MemoryRecord | None:
        path = self.root / MEMORY_DIR / f"{mid}.json"
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        return _parse_row(path, raw)

    async def memory_all(self) -> list[MemoryRecord]:
        directory = self.root / MEMORY_DIR
        if not directory.is_dir():
            return []
        return [_parse_row(path, path.read_text(encoding="utf-8")) for path in sorted(directory.glob("*.json"))]

    @contextmanager
    def _flock(self, sid: str, mode: int) -> Iterator[int]:
        fd = os.open(self.root / sid / LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, mode)
            yield fd
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _tail_seq(self, sid: str) -> int:
        path = self.root / sid / EVENTS_FILE
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return 0
        line = _tail_line(path, size)
        return _parse(sid, line).seq if line else 0

    def _read_lease(self, sid: str) -> Lease | None:
        if not (self.root / sid).is_dir():
            return None
        with self._flock(sid, fcntl.LOCK_SH) as fd:
            return _load_lease(fd)

    def _read_header(self, sid: str) -> SessionHeader | None:
        try:
            raw = (self.root / sid / HEADER_FILE).read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        return SessionHeader.model_validate_json(raw)

    def _write_header(self, header: SessionHeader) -> None:
        path = self.root / header.id / HEADER_FILE
        tmp = path.with_name(HEADER_FILE + ".tmp")
        tmp.write_text(header.model_dump_json(), encoding="utf-8")
        os.replace(tmp, path)


def _parse_row(path: Path, raw: str) -> MemoryRecord:
    try:
        return MemoryRecord.model_validate_json(raw)
    except ValidationError as exc:
        raise CorruptLog(f"{path}: unreadable memory row") from exc


def _parse(sid: str, line: str | bytes) -> Stamped:
    try:
        return Stamped.model_validate_json(line)
    except ValidationError as exc:
        raise CorruptLog(f"{sid}: unreadable event log line") from exc


def _tail_line(path: Path, size: int) -> bytes:
    if size == 0:
        return b""
    with open(path, "rb") as handle:
        end = size
        buffer = b""
        while end > 0:
            start = max(0, end - TAIL_CHUNK)
            handle.seek(start)
            buffer = handle.read(end - start) + buffer
            end = start
            if not buffer.endswith(b"\n"):
                cut = buffer.rfind(b"\n")
                if cut < 0:
                    continue
                buffer = buffer[: cut + 1]
            body = buffer.rstrip(b"\n")
            cut = body.rfind(b"\n")
            if cut >= 0:
                return body[cut + 1 :]
            if start == 0:
                return body
    return b""


def _load_lease(fd: int) -> Lease | None:
    os.lseek(fd, 0, os.SEEK_SET)
    raw = os.read(fd, 4096).decode("utf-8").strip()
    if not raw:
        return None
    try:
        return Lease.model_validate_json(raw)
    except ValidationError:
        return None


def _store_lease(fd: int, lease: Lease | None) -> None:
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    if lease is not None:
        os.write(fd, lease.model_dump_json().encode("utf-8"))
    os.fsync(fd)
