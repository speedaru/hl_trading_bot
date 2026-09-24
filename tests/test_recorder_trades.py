"""TradeTracker: hand-checked buckets, dedupe, and coverage across disconnects/restarts."""

import polars as pl
import pytest

from hlbot.data.recorder import schemas
from hlbot.data.recorder.parse import Row, Trade
from hlbot.data.recorder.trades import MINUTE_MS, TradeTracker

T0 = 1_790_290_800_000  # 2026-09-24 22:20:00 UTC, a minute boundary
GRACE = 10_000


def tr(ms: int, tid: int, px: float = 100.0, sz: float = 1.0, side: str = "B") -> Trade:
    return Trade("SOL", ms, ms + 300, tid, px, sz, side, "0xh", "0xbuyer", "0xseller")


def minute(i: int) -> int:
    return T0 + i * MINUTE_MS


def closed_after(i: int) -> int:
    """A time at which minute i is closable (over + grace)."""
    return minute(i + 1) + GRACE


def by_minute(rows: list[Row]) -> dict[int, Row]:
    return {int(str(r["minute"])): r for r in rows}


def started(
    start_ms: int = T0,
    seed_keys: list[tuple[int, int]] | None = None,
    last_trade_ms: int | None = None,
) -> TradeTracker:
    return TradeTracker(
        "SOL",
        start_ms=start_ms,
        grace_ms=GRACE,
        seed_keys=seed_keys or [],
        last_trade_ms=last_trade_ms,
    )


def test_bucket_values_hand_checked() -> None:
    t = started()
    t.on_trades([tr(T0 - 5000, 1)], T0)  # snapshot; before start -> not bucketed
    trades = [
        tr(minute(1) + 1000, 2, px=100, sz=2, side="B"),
        tr(minute(1) + 2000, 3, px=102, sz=1, side="A"),
        tr(minute(1) + 1500, 4, px=99, sz=1, side="B"),  # out of order by time
    ]
    new, gaps = t.on_trades(trades, minute(1) + 3000)
    assert len(new) == 3 and gaps == []
    rows = by_minute(t.finalize(closed_after(1)))
    r = rows[minute(1)]
    assert r["n_trades"] == 3
    assert r["base_vol"] == 4
    assert r["notional_vol"] == pytest.approx(2 * 100 + 102 + 99)
    assert r["buy_base_vol"] == 3 and r["sell_base_vol"] == 1
    assert r["buy_notional"] == pytest.approx(299) and r["sell_notional"] == pytest.approx(102)
    assert r["vwap"] == pytest.approx(401 / 4)
    assert (r["open"], r["high"], r["low"], r["close"]) == (100, 102, 99, 102)  # open/close by time
    assert r["complete"] is True
    # minute 0 contains the start time: bucket memory before start is unknown -> incomplete
    assert rows[minute(0)]["complete"] is False
    pl.DataFrame(list(rows.values()), schema=schemas.TRADES_1M_SCHEMA)  # fits the schema


def test_empty_minutes_are_emitted_while_connected() -> None:
    t = started()
    t.on_trades([tr(T0 - 1000, 1)], T0)
    rows = by_minute(t.finalize(closed_after(3)))
    assert sorted(rows) == [minute(i) for i in range(4)]
    assert rows[minute(2)]["n_trades"] == 0 and rows[minute(2)]["complete"] is True
    assert rows[minute(2)]["vwap"] is None


def test_minute_not_closed_before_grace() -> None:
    t = started()
    t.on_trades([], T0)
    assert all(r["minute"] != minute(1) for r in t.finalize(minute(2) + GRACE - 1))


def test_nothing_closes_before_first_snapshot() -> None:
    t = started()
    assert t.finalize(closed_after(5)) == []
    assert t.awaiting_snapshot


def test_duplicates_are_dropped() -> None:
    t = started()
    t.on_trades([tr(T0 + 100, 1)], T0 + 200)
    new, _ = t.on_trades([tr(T0 + 100, 1), tr(T0 + 200, 2)], T0 + 300)
    assert [x.tid for x in new] == [2]


def test_same_tid_different_time_is_a_new_trade() -> None:
    t = started()
    t.on_trades([tr(T0 + 100, 7)], T0 + 200)
    new, _ = t.on_trades([tr(T0 + 900, 7)], T0 + 1000)
    assert len(new) == 1


def test_late_trade_counted_not_bucketed() -> None:
    t = started()
    t.on_trades([], T0)
    t.finalize(closed_after(1))
    new, _ = t.on_trades([tr(minute(1) + 5, 9)], closed_after(1) + 1)
    assert len(new) == 1  # still stored raw
    assert t.late_trades == 1


def test_reconnect_with_overlapping_snapshot_loses_nothing() -> None:
    t = started()
    t.on_trades([tr(T0 - 1000, 1)], T0)
    t.on_trades([tr(minute(1) + 1000, 2), tr(minute(1) + 2000, 3)], minute(1) + 2500)
    t.on_disconnect(last_msg_recv_ms=minute(1) + 3000)
    snapshot = [tr(minute(1) + 2000, 3), tr(minute(1) + 4000, 4)]  # overlaps tid 3
    new, gaps = t.on_trades(snapshot, minute(1) + 8000)
    assert gaps == []
    assert [x.tid for x in new] == [4]
    rows = by_minute(t.finalize(closed_after(1)))
    assert rows[minute(1)]["n_trades"] == 3 and rows[minute(1)]["complete"] is True


def test_reconnect_without_overlap_writes_gap_and_marks_minutes() -> None:
    t = started()
    t.on_trades([tr(T0 - 1000, 1)], T0)
    t.on_trades([tr(minute(1) + 1000, 2)], minute(1) + 1500)
    # Connection went silent at minute(1)+30s; snapshot after reconnect starts at minute 4.
    t.on_disconnect(last_msg_recv_ms=minute(1) + 30_000)
    snapshot = [tr(minute(4) + 20_000, 10), tr(minute(4) + 21_000, 11)]
    _, gaps = t.on_trades(snapshot, minute(4) + 30_000)
    assert len(gaps) == 1
    g = gaps[0]
    assert g["stream"] == "trades" and g["reason"] == "trades_lost"
    assert g["gap_start"] == minute(1) + 25_000  # last message minus delivery margin
    assert g["gap_end"] == minute(4) + 20_000  # oldest snapshot trade
    rows = by_minute(t.finalize(closed_after(4)))
    assert rows[minute(1)]["complete"] is False  # partly inside the lost window
    assert minute(2) not in rows and minute(3) not in rows  # entirely inside: no row
    assert rows[minute(4)]["complete"] is False
    assert rows[minute(4)]["n_trades"] == 2


def test_minutes_held_while_disconnected() -> None:
    t = started()
    t.on_trades([], T0)
    t.on_disconnect(last_msg_recv_ms=minute(1) + 30_000)
    rows = t.finalize(closed_after(5))
    # minute 0 ends before the outage start and may close; minute 1 must wait
    assert [r["minute"] for r in rows] == [minute(0)]
    # Snapshot reaches back past the outage start (minute(1)+25s): nothing was lost.
    _, gaps = t.on_trades([tr(minute(1) + 20_000, 5), tr(minute(5), 6)], minute(6))
    assert gaps == []
    assert not t.awaiting_snapshot
    rows = by_minute(t.finalize(closed_after(5)))
    assert sorted(rows) == [minute(i) for i in range(1, 6)]
    assert all(r["complete"] for r in rows.values())
    assert rows[minute(1)]["n_trades"] == 1 and rows[minute(5)]["n_trades"] == 1


def test_snapshot_starting_exactly_at_outage_start_is_not_proof() -> None:
    # Other trades in that same millisecond may have been cut off the 30-trade snapshot.
    t = started()
    t.on_trades([], T0)
    t.on_disconnect(last_msg_recv_ms=minute(1) + 30_000)
    _, gaps = t.on_trades([tr(minute(1) + 25_000, 5)], minute(2))
    assert len(gaps) == 1


def test_no_snapshot_after_reconnect_is_unknown_coverage() -> None:
    t = started()
    t.on_trades([], T0)
    t.on_disconnect(last_msg_recv_ms=minute(1))
    gaps = t.resolve_without_snapshot(minute(2))
    assert gaps[0]["reason"] == "trades_unknown"
    assert gaps[0]["gap_start"] == minute(1) - 5000
    assert t.resolve_without_snapshot(minute(3)) == []  # only once


def test_restart_with_seeded_keys_detects_overlap() -> None:
    # Before the restart, trades up to tid 3 were written; the process restarts shortly after.
    seed = [(T0 - 3000, 2), (T0 - 2000, 3)]
    t = started(start_ms=T0 + 5000, seed_keys=seed, last_trade_ms=T0 - 2000)
    snapshot = [tr(T0 - 2000, 3), tr(T0 + 1000, 4)]
    new, gaps = t.on_trades(snapshot, T0 + 6000)
    assert gaps == []
    assert [x.tid for x in new] == [4]  # the already-written trade isn't stored twice


def test_restart_after_long_downtime_writes_gap() -> None:
    seed = [(T0 - 2000, 3)]
    t = started(start_ms=minute(30), seed_keys=seed, last_trade_ms=T0 - 2000)
    snapshot = [tr(minute(29) + 50_000, 40)]
    _, gaps = t.on_trades(snapshot, minute(30) + 1000)
    assert gaps[0]["gap_start"] == T0 - 2000
    assert gaps[0]["gap_end"] == minute(29) + 50_000
    rows = t.finalize(closed_after(31))
    assert [r["minute"] for r in rows] == [minute(30), minute(31)]
    assert [r["complete"] for r in rows] == [False, True]


def test_first_run_has_no_gap_row() -> None:
    t = started()
    _, gaps = t.on_trades([tr(T0 - 50_000, 1)], T0 + 100)
    assert gaps == []
