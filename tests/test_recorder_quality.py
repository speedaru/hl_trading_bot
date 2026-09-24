"""Quality report on synthetic recorded days: a perfect day, then one defect at a time."""

from datetime import date
from pathlib import Path

import pytest

from hlbot.data.recorder import schemas
from hlbot.data.recorder.config import BookStreamConfig, RecorderConfig
from hlbot.data.recorder.parse import Row, gap_row
from hlbot.data.recorder.quality import assess_day, main, recorded_days
from hlbot.data.recorder.storage import write_parts

DAY = date(2026, 9, 20)
D0 = 1_789_862_400_000  # 2026-09-20 00:00:00 UTC
MIN = 60_000

CFG = RecorderConfig(
    coins=("BTC",),
    data_dir=Path("unused"),
    books=(BookStreamConfig(stream="book", levels=1, interval_s=30),),
)


def write(root: Path, stream: str, rows: list[Row]) -> None:
    levels = 1 if schemas.is_book(stream) else None
    write_parts(root, "BTC", stream, schemas.schema_for(stream, levels), rows)


def ctx_rows(skip_minutes: range = range(0)) -> list[Row]:
    rows: list[Row] = []
    for i in range(1440):
        if i in skip_minutes:
            continue
        t = D0 + i * MIN
        row: Row = {"sample_time": t, "recv_time": t - 300, "age_ms": 300, "source": "ws"}
        row |= dict.fromkeys(("funding", "open_interest", "mark_px", "oracle_px"), 1.0)
        row |= {"day_ntl_vlm": 1.0, "prev_day_px": 1.0}
        rows.append(row)
    return rows


def book_rows(skew_ms: int = 400, source: str = "ws") -> list[Row]:
    rows: list[Row] = []
    for i in range(2880):
        t = D0 + i * 30_000
        rows.append(
            {
                "sample_time": t,
                "exch_time": t - 2000 - skew_ms,
                "recv_time": t - 2000,
                "age_ms": 2000,
                "source": source,
                "bid_px_0": 99.0,
                "bid_sz_0": 1.0,
                "bid_n_0": 1,
                "ask_px_0": 101.0,
                "ask_sz_0": 1.0,
                "ask_n_0": 1,
            }
        )
    return rows


def funding_rows() -> list[Row]:
    return [
        {
            "sample_time": D0 + i * MIN,
            "recv_time": D0 + i * MIN + 200,
            "venue": v,
            "funding_rate": 1e-5,
            "interval_hours": 1,
            "next_funding_time": D0,
        }
        for i in range(1440)
        for v in ("HlPerp", "BinPerp", "BybitPerp")
    ]


def perfect_day(root: Path, *, skew_ms: int = 400, bucket_off_by_one: bool = False) -> None:
    write(root, schemas.CTX, ctx_rows())
    write(root, schemas.FUNDING_PRED, funding_rows())
    write(root, "book", book_rows(skew_ms))
    trades: list[Row] = []
    buckets: list[Row] = []
    for i in range(1440):
        m = D0 + i * MIN
        for k in range(2):
            trades.append(
                {
                    "exch_time": m + 1000 * (k + 1),
                    "recv_time": m + 1000 * (k + 1) + 400,
                    "tid": i * 10 + k,
                    "px": 100.0,
                    "sz": 1.0,
                    "side": "B",
                    "hash": "0x",
                    "buyer": "0xb",
                    "seller": "0xs",
                }
            )
        n = 3 if (bucket_off_by_one and i == 100) else 2
        buckets.append(
            {"minute": m, "n_trades": n, "base_vol": 2.0, "notional_vol": 200.0, "complete": True}
        )
    write(root, schemas.TRADES, trades)
    write(root, schemas.TRADES_1M, buckets)


def test_perfect_day_is_ok(tmp_path: Path) -> None:
    perfect_day(tmp_path)
    r = assess_day(tmp_path, CFG, "BTC", DAY)
    assert r.coverage == {"ctx": 1.0, "funding_pred": 1.0, "book": 1.0}
    assert r.trades_1m_complete == 1.0
    assert r.n_trades == 2880
    assert r.bucket_mismatch_min == 0
    assert r.skew_median_ms == 400
    assert r.gap_rows == 0
    assert (r.verdict, r.reasons) == ("OK", [])


def test_two_hour_hole_is_bad(tmp_path: Path) -> None:
    perfect_day(tmp_path)
    (tmp_path / "BTC" / "ctx").rename(tmp_path / "old_ctx")
    write(tmp_path, schemas.CTX, ctx_rows(skip_minutes=range(600, 720)))
    write(tmp_path, schemas.GAPS, [gap_row("ctx", D0 + 599 * MIN, D0 + 720 * MIN, "process_down")])
    r = assess_day(tmp_path, CFG, "BTC", DAY)
    assert r.coverage["ctx"] == pytest.approx(1320 / 1440)
    assert r.longest_gap_min == pytest.approx(121)
    assert r.verdict == "BAD"
    assert any("longest gap" in x for x in r.reasons)


def test_gap_spanning_midnight_is_clipped_to_the_day(tmp_path: Path) -> None:
    perfect_day(tmp_path)
    # Down from 23:50 the day before until 00:04: only 4 minutes belong to DAY.
    write(tmp_path, schemas.GAPS, [gap_row("ctx", D0 - 10 * MIN, D0 + 4 * MIN, "process_down")])
    r = assess_day(tmp_path, CFG, "BTC", DAY)
    assert r.gap_rows == 1
    assert r.longest_gap_min == pytest.approx(4)
    assert r.verdict == "OK"  # 4 min < warn threshold


def test_ten_minute_trade_gap_warns(tmp_path: Path) -> None:
    perfect_day(tmp_path)
    write(tmp_path, schemas.GAPS, [gap_row("trades", D0 + 60 * MIN, D0 + 70 * MIN, "trades_lost")])
    r = assess_day(tmp_path, CFG, "BTC", DAY)
    assert r.trades_lost_min == pytest.approx(10)
    assert r.verdict == "WARN"


def test_clock_skew_warns_then_bad(tmp_path: Path) -> None:
    perfect_day(tmp_path / "a", skew_ms=3000)
    assert assess_day(tmp_path / "a", CFG, "BTC", DAY).verdict == "WARN"
    perfect_day(tmp_path / "b", skew_ms=-6000)  # local clock far behind the exchange
    assert assess_day(tmp_path / "b", CFG, "BTC", DAY).verdict == "BAD"


def test_bucket_disagreeing_with_raw_trades_is_flagged(tmp_path: Path) -> None:
    perfect_day(tmp_path, bucket_off_by_one=True)
    r = assess_day(tmp_path, CFG, "BTC", DAY)
    assert r.bucket_mismatch_min == 1
    assert r.verdict == "WARN"


def test_missing_day_is_bad(tmp_path: Path) -> None:
    r = assess_day(tmp_path, CFG, "BTC", DAY)
    assert r.verdict == "BAD"
    assert "no ctx data" in r.reasons


def test_cli_exit_code_and_csv(tmp_path: Path) -> None:
    perfect_day(tmp_path)
    cfg_file = tmp_path / "rec.toml"
    cfg_file.write_text(
        'coins = ["BTC"]\ndata_dir = "unused"\n'
        '[[books]]\nstream = "book"\nlevels = 1\ninterval_s = 30\n'
    )
    assert recorded_days(tmp_path, ("BTC",)) == [DAY]
    out = tmp_path / "q.csv"
    code = main(["--config", str(cfg_file), "--data", str(tmp_path), "--csv", str(out)])
    assert code == 0
    assert "OK" in out.read_text()
    # Asking for a range that includes an empty day reports it as BAD.
    args = ["--config", str(cfg_file), "--data", str(tmp_path)]
    assert main([*args, "--start", "2026-09-20", "--end", "2026-09-21"]) == 1
