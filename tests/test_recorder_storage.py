import shutil
from datetime import date
from pathlib import Path

import polars as pl

from hlbot.data.recorder import schemas
from hlbot.data.recorder.parse import Row, gap_row
from hlbot.data.recorder.state import (
    RECENT_TRADE_KEYS,
    RecorderState,
    downtime_gaps,
    load_state,
    save_state,
)
from hlbot.data.recorder.storage import (
    StreamBuffer,
    compact_day,
    compact_ready,
    day_file,
    parts_dir,
    read_day,
    write_parts,
)

# 2026-09-24 23:59:30 UTC and 2026-09-25 00:00:30 UTC
BEFORE_MIDNIGHT = 1_790_294_370_000
AFTER_MIDNIGHT = 1_790_294_430_000
DAY1, DAY2 = date(2026, 9, 24), date(2026, 9, 25)


def trade(ms: int, tid: int, recv_offset: int = 5) -> Row:
    return {
        "exch_time": ms,
        "recv_time": ms + recv_offset,
        "tid": tid,
        "px": 100.0,
        "sz": 1.0,
        "side": "B",
        "hash": "0xabc",
        "buyer": "0x1",
        "seller": "0x2",
    }


def write_trades(root: Path, rows: list[Row]) -> None:
    write_parts(root, "SOL", schemas.TRADES, schemas.TRADES_SCHEMA, rows)


def test_rows_split_at_utc_midnight(tmp_path: Path) -> None:
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT, 1), trade(AFTER_MIDNIGHT, 2)])
    d1 = list(parts_dir(tmp_path, "SOL", "trades", DAY1).glob("*.parquet"))
    d2 = list(parts_dir(tmp_path, "SOL", "trades", DAY2).glob("*.parquet"))
    assert len(d1) == 1 and len(d2) == 1
    assert pl.read_parquet(d1[0])["tid"].to_list() == [1]
    assert pl.read_parquet(d2[0])["tid"].to_list() == [2]


def test_each_part_is_readable_on_its_own(tmp_path: Path) -> None:
    # A crash after any number of flushes leaves only complete files behind.
    for i in range(3):
        write_trades(tmp_path, [trade(BEFORE_MIDNIGHT - 1000 * i, i)])
    parts = sorted(parts_dir(tmp_path, "SOL", "trades", DAY1).glob("*.parquet"))
    assert len(parts) == 3
    for p in parts:
        assert pl.read_parquet(p).schema == schemas.TRADES_SCHEMA


def test_leftover_tmp_file_is_ignored_and_cleaned(tmp_path: Path) -> None:
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT, 1)])
    pdir = parts_dir(tmp_path, "SOL", "trades", DAY1)
    (pdir / "part-9999-1-000000.parquet.tmp").write_bytes(b"half a file")  # crash mid-write
    df = read_day(tmp_path, "SOL", "trades", DAY1)
    assert df is not None and df.height == 1
    compact_day(pdir, "trades")
    assert not pdir.exists()


def test_compaction_merges_sorts_and_dedupes(tmp_path: Path) -> None:
    t0 = BEFORE_MIDNIGHT - 60_000
    write_trades(tmp_path, [trade(t0 + 2000, 20), trade(t0 + 1000, 10)])
    # Same trade seen again (e.g. in a reconnect snapshot), received later.
    write_trades(tmp_path, [trade(t0 + 1000, 10, recv_offset=900), trade(t0 + 3000, 30)])
    out = compact_day(parts_dir(tmp_path, "SOL", "trades", DAY1), "trades")
    assert out == day_file(tmp_path, "SOL", "trades", DAY1)
    df = pl.read_parquet(out)
    assert df["tid"].to_list() == [10, 20, 30]
    assert df.schema == schemas.TRADES_SCHEMA
    # keep="first": the earliest-written copy wins
    first = df.filter(pl.col("tid") == 10)["recv_time"].dt.epoch("ms")[0]
    assert first == t0 + 1000 + 5


def test_same_tid_at_different_times_is_not_a_duplicate(tmp_path: Path) -> None:
    # tid is a hash; the unique key is (time, tid).
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT - 5000, 7), trade(BEFORE_MIDNIGHT, 7)])
    df = pl.read_parquet(compact_day(parts_dir(tmp_path, "SOL", "trades", DAY1), "trades"))
    assert df.height == 2


def test_compaction_is_idempotent_after_crash_before_cleanup(tmp_path: Path) -> None:
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT - 1000, 1), trade(BEFORE_MIDNIGHT, 2)])
    pdir = parts_dir(tmp_path, "SOL", "trades", DAY1)
    backup = tmp_path / "backup"
    shutil.copytree(pdir, backup)
    first = pl.read_parquet(compact_day(pdir, "trades"))
    shutil.copytree(backup, pdir)  # simulate: day file written, then crash before deleting
    second = pl.read_parquet(compact_day(pdir, "trades"))
    assert first.equals(second)


def test_late_part_is_merged_into_existing_day_file(tmp_path: Path) -> None:
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT - 1000, 1)])
    compact_day(parts_dir(tmp_path, "SOL", "trades", DAY1), "trades")
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT, 2)])  # arrives after compaction
    df = pl.read_parquet(compact_day(parts_dir(tmp_path, "SOL", "trades", DAY1), "trades"))
    assert df["tid"].to_list() == [1, 2]


def test_compact_ready_skips_today(tmp_path: Path) -> None:
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT, 1), trade(AFTER_MIDNIGHT, 2)])
    done = compact_ready(tmp_path, today=DAY2)
    assert done == [day_file(tmp_path, "SOL", "trades", DAY1)]
    assert parts_dir(tmp_path, "SOL", "trades", DAY2).exists()


def test_read_day_combines_compacted_and_parts(tmp_path: Path) -> None:
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT - 1000, 1)])
    compact_day(parts_dir(tmp_path, "SOL", "trades", DAY1), "trades")
    write_trades(tmp_path, [trade(BEFORE_MIDNIGHT, 2)])
    df = read_day(tmp_path, "SOL", "trades", DAY1)
    assert df is not None and df["tid"].to_list() == [1, 2]
    assert read_day(tmp_path, "SOL", "trades", DAY2) is None


def test_book_compaction_tolerates_changed_level_count(tmp_path: Path) -> None:
    def book(ms: int, levels: int) -> Row:
        row: Row = {"sample_time": ms, "exch_time": ms, "recv_time": ms, "age_ms": 0}
        row["source"] = "ws"
        for side in ("bid", "ask"):
            for i in range(levels):
                row |= {f"{side}_px_{i}": 1.0, f"{side}_sz_{i}": 1.0, f"{side}_n_{i}": 1}
        return row

    write_parts(
        tmp_path, "BTC", "book", schemas.book_schema(2), [book(BEFORE_MIDNIGHT - 30_000, 2)]
    )
    write_parts(tmp_path, "BTC", "book", schemas.book_schema(3), [book(BEFORE_MIDNIGHT, 3)])
    df = pl.read_parquet(compact_day(parts_dir(tmp_path, "BTC", "book", DAY1), "book"))
    assert df.height == 2
    assert df["bid_px_2"].to_list() == [None, 1.0]


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_buffer_due_by_rows_or_age() -> None:
    clock = FakeClock()
    buf = StreamBuffer(
        "SOL", "trades", schema=schemas.TRADES_SCHEMA, max_rows=3, max_seconds=60, clock=clock
    )
    assert not buf.due()
    buf.add(trade(1, 1))
    clock.t = 59
    assert not buf.due()
    clock.t = 60
    assert buf.due()  # oldest row is 60s old
    buf.take()
    buf.extend([trade(1, 1), trade(2, 2)])
    assert not buf.due()
    buf.add(trade(3, 3))
    assert buf.due()  # max_rows reached


def test_buffer_restore_after_failed_write() -> None:
    clock = FakeClock()
    buf = StreamBuffer(
        "SOL", "trades", schema=schemas.TRADES_SCHEMA, max_rows=100, max_seconds=60, clock=clock
    )
    buf.add(trade(1, 1))
    rows = buf.take()
    assert len(buf) == 0
    buf.add(trade(2, 2))  # arrived while the write was in flight
    buf.restore(rows)
    assert [r["tid"] for r in buf.take()] == [1, 2]


def test_state_roundtrip_and_trade_key_window(tmp_path: Path) -> None:
    st = RecorderState()
    rows = [trade(1000 + i, i) for i in range(RECENT_TRADE_KEYS + 50)]
    st.record_flush("SOL", "trades", rows)
    st.record_flush("SOL", "ctx", [{"sample_time": 60_000}, {"sample_time": 120_000}])
    st.clean_shutdown = True
    path = tmp_path / "_meta" / "state.json"
    save_state(path, st)
    loaded = load_state(path)
    assert loaded.last_row_ms == {"SOL": {"trades": 1000 + RECENT_TRADE_KEYS + 49, "ctx": 120_000}}
    keys = loaded.recent_trade_keys["SOL"]
    assert len(keys) == RECENT_TRADE_KEYS
    assert keys[-1] == (1000 + RECENT_TRADE_KEYS + 49, RECENT_TRADE_KEYS + 49)
    assert loaded.clean_shutdown


def test_state_never_moves_backwards() -> None:
    st = RecorderState()
    st.record_flush("SOL", "ctx", [{"sample_time": 120_000}])
    st.record_flush("SOL", "ctx", [{"sample_time": 60_000}])  # e.g. a retried older flush
    assert st.last_row_ms["SOL"]["ctx"] == 120_000


def test_missing_or_corrupt_state_is_a_fresh_start(tmp_path: Path) -> None:
    assert load_state(tmp_path / "nope.json").last_row_ms == {}
    bad = tmp_path / "state.json"
    bad.write_text("{not json")
    assert load_state(bad).last_row_ms == {}


def test_downtime_gaps() -> None:
    st = RecorderState(last_row_ms={"SOL": {"ctx": 60_000, "book": 90_000}, "OLD": {"ctx": 1}})
    gaps = downtime_gaps(st, ("SOL", "BTC"), ["ctx", "book", "book_coarse"], 300_000)
    assert set(gaps) == {"SOL"}  # BTC has no history; OLD is no longer configured
    assert gaps["SOL"] == [
        gap_row("ctx", 60_000, 300_000, "process_down"),
        gap_row("book", 90_000, 300_000, "process_down"),
    ]
    st.clean_shutdown = True
    assert downtime_gaps(st, ("SOL",), ["ctx"], 300_000)["SOL"][0]["reason"] == "restart_clean"
