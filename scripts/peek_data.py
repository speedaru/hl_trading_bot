"""Dev tool: print a snapshot of today's (UTC) recorded data for BTC, ETH and SOL.

Run with: uv run python scripts/peek_data.py
"""

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from hlbot.data.recorder.storage import read_day, stream_files

DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "live"
COINS = ("BTC", "ETH", "SOL")
STREAMS = ("trades", "trades_1m", "ctx", "book")
N_LAST = 3

TIME_COLUMN = {
    "trades": "exch_time",
    "trades_1m": "minute",
    "ctx": "sample_time",
    "book": "sample_time",
}
# Columns worth showing per stream (the full rows are too wide for a terminal).
SHOW = {
    "trades": ["exch_time", "px", "sz", "side", "tid"],
    "trades_1m": ["minute", "n_trades", "open", "high", "low", "close", "notional_vol", "complete"],
    "ctx": ["sample_time", "source", "mark_px", "oracle_px", "funding", "open_interest", "age_ms"],
    "book": [
        "sample_time", "source", "bid_sz_0", "bid_px_0", "ask_px_0", "ask_sz_0", "spread_bps",
        "age_ms",
    ],
}  # fmt: skip


def fmt_time(t: object) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S") if isinstance(t, datetime) else "-"


def peek_stream(root: Path, coin: str, stream: str, day: date) -> None:
    files = stream_files(root, coin, stream, day)
    # read_day merges the compacted file and any parts, de-duplicated, so nothing is missed
    # if both exist (e.g. a late part written after compaction).
    df = read_day(root, coin, stream, day)
    if df is None or df.is_empty():
        print(f"  [{stream}] no data yet for {day}")
        return

    time_col = TIME_COLUMN[stream]
    # read_day returns rows sorted by the time column
    first = df.row(0, named=True)[time_col]
    last = df.row(-1, named=True)[time_col]
    kinds = sorted({"parts" if f.parent.name.endswith(".parts") else "compacted" for f in files})
    print(
        f"  [{stream}] {df.height:,} rows  {fmt_time(first)} -> {fmt_time(last)} UTC"
        f"  (from {' + '.join(kinds)})"
    )

    if stream == "book" and {"bid_px_0", "ask_px_0"} <= set(df.columns):
        mid = (pl.col("bid_px_0") + pl.col("ask_px_0")) / 2
        spread = (pl.col("ask_px_0") - pl.col("bid_px_0")) / mid * 1e4
        df = df.with_columns(spread.round(2).alias("spread_bps"))

    if stream == "ctx":
        latest = df.row(-1, named=True)
        funding = "-" if latest["funding"] is None else f"{latest['funding']:+.6%}/h"
        print(
            f"    latest mark {latest['mark_px']}  funding {funding}"
            f"  (sampled {fmt_time(latest['sample_time'])})"
        )

    if stream == "trades_1m":
        gaps = df.filter(~pl.col("complete"))
        if gaps.is_empty():
            print("    all minutes complete")
        else:
            print(f"    {gaps.height} incomplete minute(s) (complete = false):")
            print(gaps.select("minute", "n_trades"))

    cols = [c for c in SHOW[stream] if c in df.columns]
    print(df.tail(N_LAST).select(cols))


def main(root: Path = DATA_ROOT) -> None:
    day = datetime.now(UTC).date()
    print(f"Recorded data for {day} (UTC) in {root}")
    # ASCII tables: the Windows console (cp1252) can't print polars' default box characters.
    with pl.Config(
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=50,
        tbl_cols=-1,
        tbl_width_chars=160,
    ):
        for coin in COINS:
            print(f"\n=== {coin} " + "=" * 60)
            for stream in STREAMS:
                peek_stream(root, coin, stream, day)


if __name__ == "__main__":
    main()
