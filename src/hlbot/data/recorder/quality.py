"""Data-quality report for recorded days. Run after pulling files home, before research.

    uv run python -m hlbot.data.recorder.quality                      # every complete day
    uv run python -m hlbot.data.recorder.quality --start 2026-09-20 --end 2026-09-26
    uv run python -m hlbot.data.recorder.quality --csv quality.csv

Per coin per UTC day: sample coverage vs the configured cadence, 1m trade-bucket
completeness, a bucket-vs-raw-trades consistency check, gap rows (count, longest, trade
minutes lost), clock skew, REST-fallback share and book staleness. Each day gets a verdict:
OK (use it), WARN (usable, read the reasons), BAD (exclude it or handle the holes).
Exit code 1 if any day is BAD.
"""

import argparse
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from hlbot.data.recorder import schemas
from hlbot.data.recorder.config import RecorderConfig, load_config
from hlbot.data.recorder.storage import PARTS_SUFFIX, read_day

DAY_MS = 86_400_000

# Verdict thresholds. A day is WARN/BAD if any check crosses its level.
COVERAGE_WARN, COVERAGE_BAD = 0.99, 0.90  # worst sampled stream: samples / expected
TRADES_COMPLETE_WARN, TRADES_COMPLETE_BAD = 0.99, 0.95  # complete 1m buckets / 1440
LONGEST_GAP_WARN_MIN, LONGEST_GAP_BAD_MIN = 5.0, 60.0
# recv - exch includes publish + network latency (~0.4-1s observed), so only seconds matter.
SKEW_WARN_MS, SKEW_BAD_MS = 2_000, 5_000
REST_SHARE_WARN = 0.20  # websocket was stale this often at sample time


@dataclass
class DayReport:
    coin: str
    day: date
    coverage: dict[str, float] = field(default_factory=lambda: {})  # stream -> fraction
    trades_1m_complete: float = 0.0
    n_trades: int | None = None  # raw trades recorded (None if raw storage is off)
    bucket_mismatch_min: int = 0  # complete minutes whose n_trades != raw count
    gap_rows: int = 0
    longest_gap_min: float = 0.0
    trades_lost_min: float = 0.0
    skew_median_ms: float | None = None
    skew_p5_ms: float | None = None
    skew_p95_ms: float | None = None
    rest_share: float = 0.0
    book_age_p95_s: float | None = None
    verdict: str = "OK"
    reasons: list[str] = field(default_factory=lambda: [])


def _expected_samples(interval_s: int) -> int:
    return 86_400 // interval_s


def _day_bounds_ms(day: date) -> tuple[int, int]:
    start = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)
    return start, start + DAY_MS


def _read_all(root: Path, coin: str, stream: str) -> pl.DataFrame | None:
    """Every row of a (small) stream across all days, e.g. gaps, which can span days."""
    d = root / coin / stream
    files = sorted(d.glob("*.parquet")) + sorted(d.glob(f"*{PARTS_SUFFIX}/part-*.parquet"))
    if not files:
        return None
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed").unique()


def _ms(col: str) -> pl.Expr:
    return pl.col(col).dt.epoch("ms")


def _num(v: object) -> float:
    """A numeric polars aggregate (median/quantile of a non-empty numeric column)."""
    assert isinstance(v, int | float), f"expected a number, got {v!r}"
    return float(v)


def _gap_stats(gaps: pl.DataFrame | None, day: date) -> tuple[int, float, float]:
    """(gap rows overlapping the day, longest clipped gap in minutes, trade minutes lost)."""
    if gaps is None or gaps.is_empty():
        return 0, 0.0, 0.0
    lo, hi = _day_bounds_ms(day)
    g = gaps.with_columns(
        _ms("gap_start").clip(lo, hi).alias("s"), _ms("gap_end").clip(lo, hi).alias("e")
    ).filter(pl.col("e") > pl.col("s"))
    if g.is_empty():
        return 0, 0.0, 0.0
    dur_min = (pl.col("e") - pl.col("s")) / 60_000
    longest = g.select(dur_min.max()).item()
    trades = g.filter(pl.col("stream") == schemas.TRADES).select(dur_min.sum()).item()
    return g.height, float(longest), float(trades or 0.0)


def assess_day(root: Path, cfg: RecorderConfig, coin: str, day: date) -> DayReport:
    r = DayReport(coin=coin, day=day)

    # Sampled streams: distinct sample slots vs expected (funding_pred has one row per venue).
    rest_rows = total_rows = 0
    sampled = {schemas.CTX: cfg.ctx_interval_s, schemas.FUNDING_PRED: cfg.funding_pred_interval_s}
    sampled |= {b.stream: b.interval_s for b in cfg.books}
    for stream, interval in sampled.items():
        df = read_day(root, coin, stream, day)
        n = 0 if df is None else df["sample_time"].n_unique()
        r.coverage[stream] = n / _expected_samples(interval)
        if df is not None and "source" in df.columns:
            rest_rows += df.filter(pl.col("source") == "rest").height
            total_rows += df.height
    r.rest_share = rest_rows / total_rows if total_rows else 0.0

    # Clock skew and staleness from the first book stream.
    if cfg.books:
        book = read_day(root, coin, cfg.books[0].stream, day)
        if book is not None and not book.is_empty():
            d = _ms("recv_time") - _ms("exch_time")
            s = book.select(
                d.median().alias("med"),
                d.quantile(0.05).alias("p5"),
                d.quantile(0.95).alias("p95"),
                pl.col("age_ms").quantile(0.95).alias("age95"),
            )
            r.skew_median_ms = _num(s["med"].item())
            r.skew_p5_ms = _num(s["p5"].item())
            r.skew_p95_ms = _num(s["p95"].item())
            r.book_age_p95_s = _num(s["age95"].item()) / 1000

    # Trades: complete buckets, and buckets vs raw trades on complete minutes.
    buckets = read_day(root, coin, schemas.TRADES_1M, day)
    if buckets is not None:
        r.trades_1m_complete = buckets.filter(pl.col("complete")).height / 1440
    if cfg.store_raw_trades:
        raw = read_day(root, coin, schemas.TRADES, day)
        r.n_trades = 0 if raw is None else raw.height
        if raw is not None and buckets is not None:
            per_min = raw.group_by(pl.col("exch_time").dt.truncate("1m").alias("minute")).agg(
                pl.len().alias("raw_n")
            )
            joined = buckets.filter(pl.col("complete")).join(per_min, on="minute", how="left")
            mismatch = joined.filter(pl.col("n_trades") != pl.col("raw_n").fill_null(0))
            r.bucket_mismatch_min = mismatch.height

    r.gap_rows, r.longest_gap_min, r.trades_lost_min = _gap_stats(
        _read_all(root, coin, schemas.GAPS), day
    )
    r.verdict, r.reasons = verdict(r)
    return r


def _grade(badness: float, warn_at: float, bad_at: float) -> str | None:
    """'BAD' / 'WARN' / None for a value where larger is worse."""
    if badness > bad_at:
        return "BAD"
    return "WARN" if badness > warn_at else None


def verdict(r: DayReport) -> tuple[str, list[str]]:
    # (level, reason) per check; level None = passed.
    checks: list[tuple[str | None, str]] = []
    for stream, cov in r.coverage.items():
        level = _grade(1 - cov, 1 - COVERAGE_WARN, 1 - COVERAGE_BAD)
        checks.append((level, f"no {stream} data" if cov == 0 else f"{stream} coverage {cov:.1%}"))
    complete = r.trades_1m_complete
    checks.append(
        (
            _grade(1 - complete, 1 - TRADES_COMPLETE_WARN, 1 - TRADES_COMPLETE_BAD),
            f"complete trade minutes {complete:.1%}",
        )
    )
    # Expected only from late trades (kept raw, not bucketed; see heartbeat `late=`).
    checks.append(
        (
            "WARN" if r.bucket_mismatch_min else None,
            f"{r.bucket_mismatch_min} complete minutes disagree with raw trades",
        )
    )
    checks.append(
        (
            _grade(r.longest_gap_min, LONGEST_GAP_WARN_MIN, LONGEST_GAP_BAD_MIN),
            f"longest gap {r.longest_gap_min:.1f} min",
        )
    )
    if r.skew_median_ms is not None:
        checks.append(
            (
                _grade(abs(r.skew_median_ms), SKEW_WARN_MS, SKEW_BAD_MS),
                f"clock skew {r.skew_median_ms:+.0f} ms",
            )
        )
    checks.append(
        (
            _grade(r.rest_share, REST_SHARE_WARN, 1.0),
            f"{r.rest_share:.0%} of samples via REST fallback",
        )
    )
    bad = [reason for level, reason in checks if level == "BAD"]
    warn = [reason for level, reason in checks if level == "WARN"]
    if bad:
        return "BAD", bad + warn
    return ("WARN", warn) if warn else ("OK", [])


def recorded_days(root: Path, coins: tuple[str, ...]) -> list[date]:
    days: set[date] = set()
    for coin in coins:
        for f in (root / coin).glob("*/*"):
            stem = f.name.removesuffix(PARTS_SUFFIX).removesuffix(".parquet")
            try:
                days.add(date.fromisoformat(stem))
            except ValueError:
                continue
    return sorted(days)


def report_frame(reports: list[DayReport], cfg: RecorderConfig) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for r in reports:
        d = asdict(r)
        cov = d.pop("coverage")
        reasons = d.pop("reasons")
        row: dict[str, object] = {"day": r.day.isoformat(), "coin": r.coin, "verdict": r.verdict}
        for stream in [schemas.CTX, schemas.FUNDING_PRED, *(b.stream for b in cfg.books)]:
            row[f"cov_{stream}"] = round(cov.get(stream, 0.0), 4)
        row |= {k: v for k, v in d.items() if k not in ("coin", "day", "verdict")}
        row["reasons"] = "; ".join(reasons)
        rows.append(row)
    # Optional fields are None on days without book/trade data. If every reported day is like
    # that, polars would infer dtype Null for the column, which numeric ops (round) reject.
    return pl.DataFrame(rows, schema_overrides=_NULLABLE_DTYPES)


_NULLABLE_DTYPES: dict[str, type[pl.DataType]] = {
    "n_trades": pl.Int64,
    "skew_median_ms": pl.Float64,
    "skew_p5_ms": pl.Float64,
    "skew_p95_ms": pl.Float64,
    "book_age_p95_s": pl.Float64,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Data-quality report for recorded Hyperliquid data.")
    p.add_argument("--config", type=Path, default=Path("config/recorder.toml"))
    p.add_argument("--data", type=Path, help="data root (default: data_dir from the config)")
    p.add_argument("--start", type=date.fromisoformat, help="first UTC day (inclusive)")
    p.add_argument("--end", type=date.fromisoformat, help="last UTC day (inclusive)")
    p.add_argument("--include-today", action="store_true", help="also report today (partial)")
    p.add_argument("--csv", type=Path, help="also write the table to this CSV file")
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    root: Path = args.data or cfg.data_dir
    today = datetime.now(UTC).date()
    days = [
        d
        for d in recorded_days(root, cfg.coins)
        if (args.start is None or d >= args.start)
        and (args.end is None or d <= args.end)
        and (args.include_today or d < today)
    ]
    if args.start and args.end:  # also report days with no data at all in the range
        span = (args.end - args.start).days + 1
        days = sorted(set(days) | {args.start + timedelta(i) for i in range(span)})
    if not days:
        print(f"no recorded days found under {root}")
        return 1

    reports = [assess_day(root, cfg, coin, d) for d in days for coin in cfg.coins]
    table = report_frame(reports, cfg)
    if args.csv:
        table.write_csv(args.csv)
    # ASCII tables: the Windows console can't encode polars' default box characters.
    # Coverage columns are percentages; the full-precision table goes to --csv.
    pct = [
        (pl.col(c) * 100).round(1).alias(c.removeprefix("cov_") + "%")
        for c in table.columns
        if c.startswith("cov_")
    ]
    summary = table.select(
        "day",
        "coin",
        "verdict",
        *pct,
        (pl.col("trades_1m_complete") * 100).round(1).alias("trades_ok%"),
        pl.col("longest_gap_min").round(1).alias("max_gap_min"),
        pl.col("trades_lost_min").round(1).alias("trades_lost_min"),
        # Stays null (not 0) when there was no book data: 0 would read as a perfect clock.
        pl.col("skew_median_ms").cast(pl.Float64).round(0).alias("skew_ms"),
        (pl.col("rest_share") * 100).round(1).alias("rest%"),
    )
    with pl.Config(
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=400,
    ):
        print(summary)
    for r in reports:
        if r.reasons:
            print(f"{r.day} {r.coin} {r.verdict}: " + "; ".join(r.reasons))
    counts = table["verdict"].value_counts()
    print("summary:", dict(zip(counts["verdict"], counts["count"], strict=True)))
    return 1 if (table["verdict"] == "BAD").any() else 0


if __name__ == "__main__":
    sys.exit(main())
