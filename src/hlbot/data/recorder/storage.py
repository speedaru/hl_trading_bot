"""Crash-safe Parquet storage: buffered part files per flush, compacted into one file per day.

Layout under the data root:
    <coin>/<stream>/<YYYY-MM-DD>.parquet               compacted day (final, never rewritten
                                                       except to merge late parts)
    <coin>/<stream>/<YYYY-MM-DD>.parts/part-*.parquet  one file per flush, until compaction

Why parts: a Parquet file is only readable once its footer is written at close. Appending to
one open file per day would make the whole day unreadable after a hard crash. Each part is
complete on its own, so a crash loses only rows still in memory.
"""

import io
import itertools
import logging
import os
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from hlbot.data.recorder import schemas
from hlbot.data.recorder.parse import Row

log = logging.getLogger(__name__)

PARTS_SUFFIX = ".parts"
_part_seq = itertools.count()


def utc_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).date()


def day_file(root: Path, coin: str, stream: str, day: date) -> Path:
    return root / coin / stream / f"{day.isoformat()}.parquet"


def parts_dir(root: Path, coin: str, stream: str, day: date) -> Path:
    return root / coin / stream / f"{day.isoformat()}{PARTS_SUFFIX}"


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write via temp file + fsync + rename, so `path` is either absent/old or complete."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync_dir(d: Path) -> None:
    # Makes the rename itself durable across power loss. POSIX only; Windows has no dir fds.
    o_directory: int | None = getattr(os, "O_DIRECTORY", None)
    if o_directory is not None:
        fd = os.open(d, os.O_RDONLY | o_directory)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd", compression_level=3)
    return buf.getvalue()


def write_parts(root: Path, coin: str, stream: str, schema: pl.Schema, rows: list[Row]) -> int:
    """Write rows as one new part file per UTC day they touch. Returns rows written.

    Blocking (disk I/O); the recorder calls it from a worker thread.
    """
    if not rows:
        return 0
    df = pl.DataFrame(rows, schema=schema)
    part_col = schemas.partition_column(stream)
    df = df.with_columns(pl.col(part_col).dt.date().alias("_day"))
    stamp = int(time.time() * 1000)
    for (day,), group in df.group_by("_day", maintain_order=True):
        assert isinstance(day, date)
        name = f"part-{stamp}-{os.getpid()}-{next(_part_seq):06d}.parquet"
        path = parts_dir(root, coin, stream, day) / name
        atomic_write_bytes(path, _parquet_bytes(group.drop("_day")))
    return df.height


class StreamBuffer:
    """In-memory rows for one (coin, stream), flushed every `max_rows` or `max_seconds`."""

    def __init__(
        self,
        coin: str,
        stream: str,
        *,
        schema: pl.Schema,
        max_rows: int,
        max_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.coin = coin
        self.stream = stream
        self.schema = schema
        self.max_rows = max_rows
        self.max_seconds = max_seconds
        self._clock = clock
        self._rows: list[Row] = []
        self._oldest: float | None = None  # clock time the oldest buffered row arrived

    def __len__(self) -> int:
        return len(self._rows)

    def add(self, row: Row) -> None:
        if self._oldest is None:
            self._oldest = self._clock()
        self._rows.append(row)

    def extend(self, rows: list[Row]) -> None:
        for r in rows:
            self.add(r)

    def due(self) -> bool:
        if not self._rows or self._oldest is None:
            return False
        return len(self._rows) >= self.max_rows or (
            self._clock() - self._oldest >= self.max_seconds
        )

    def take(self) -> list[Row]:
        """Detach the buffered rows for writing. Call `restore` if the write fails."""
        rows, self._rows, self._oldest = self._rows, [], None
        return rows

    def restore(self, rows: list[Row]) -> None:
        """Put rows back in front after a failed write, keeping their age for the next try."""
        self._rows = rows + self._rows
        self._oldest = self._clock() - self.max_seconds  # due again immediately


# ---- compaction ---------------------------------------------------------------------------


def compact_day(parts: Path, stream: str) -> Path:
    """Merge a `<date>.parts` dir (plus any existing day file) into `<date>.parquet`.

    Idempotent: rows are de-duplicated on the stream's key, so re-running after a crash
    between "day file written" and "parts deleted" gives the same file.
    """
    day_path = parts.with_name(parts.name.removesuffix(PARTS_SUFFIX) + ".parquet")
    part_files = sorted(parts.glob("part-*.parquet"))  # names sort chronologically
    sources = ([day_path] if day_path.exists() else []) + part_files
    if part_files:
        frames = [pl.read_parquet(p) for p in sources]
        # diagonal_relaxed: tolerates a changed column set (e.g. book levels changed in config)
        df = pl.concat(frames, how="diagonal_relaxed")
        key = schemas.unique_key(stream)
        df = df.unique(subset=key, keep="first", maintain_order=True)
        df = df.sort(schemas.partition_column(stream), maintain_order=True)
        atomic_write_bytes(day_path, _parquet_bytes(df))
    # Delete only what was merged (plus .tmp leftovers of a crash mid-write), so a part that
    # appeared meanwhile survives for the next run.
    for p in [*part_files, *parts.glob("*.tmp")]:
        p.unlink()
    try:
        parts.rmdir()
    except OSError:
        log.warning("%s not empty after compaction; will merge on the next run", parts)
    return day_path


def compact_ready(root: Path, today: date) -> list[Path]:
    """Compact every parts dir for a day strictly before `today` (UTC). Returns day files."""
    done: list[Path] = []
    for parts in sorted(root.glob(f"*/*/*{PARTS_SUFFIX}")):
        day = date.fromisoformat(parts.name.removesuffix(PARTS_SUFFIX))
        if day < today:
            stream = parts.parent.name
            done.append(compact_day(parts, stream))
            log.info("compacted %s", done[-1])
    return done


# ---- reading ------------------------------------------------------------------------------


def stream_files(root: Path, coin: str, stream: str, day: date) -> list[Path]:
    """All files holding rows for one day: the compacted file and/or not-yet-compacted parts."""
    files: list[Path] = []
    f = day_file(root, coin, stream, day)
    if f.exists():
        files.append(f)
    files += sorted(parts_dir(root, coin, stream, day).glob("part-*.parquet"))
    return files


def read_day(root: Path, coin: str, stream: str, day: date) -> pl.DataFrame | None:
    """One day of a stream, de-duplicated, or None if nothing was recorded."""
    files = stream_files(root, coin, stream, day)
    if not files:
        return None
    df = pl.concat([pl.read_parquet(p) for p in files], how="diagonal_relaxed")
    df = df.unique(subset=schemas.unique_key(stream), keep="first", maintain_order=True)
    return df.sort(schemas.partition_column(stream), maintain_order=True)
