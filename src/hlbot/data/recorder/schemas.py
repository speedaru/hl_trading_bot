"""Column schemas for every recorded stream, and how each stream is partitioned into days.

All time columns are UTC datetimes in milliseconds. Naming convention:
- `exch_time`: timestamp set by the exchange (only where the message carries one)
- `recv_time`: local clock when the message arrived
- `sample_time`: the wall-clock slot a sampled row belongs to (a multiple of its interval)
"""

import polars as pl

TIME = pl.Datetime("ms", "UTC")

CTX = "ctx"
FUNDING_PRED = "funding_pred"
TRADES = "trades"
TRADES_1M = "trades_1m"
GAPS = "gaps"

CTX_SCHEMA = pl.Schema(
    {
        "sample_time": TIME,
        "recv_time": TIME,
        # Asset contexts carry no exchange timestamp. This is the newest exchange time seen on
        # any feed (book/trade messages) at sample time, as a proxy. Null for REST samples.
        "exch_time_ref": TIME,
        "age_ms": pl.Int64,  # sample_time - recv_time: how old the value was when sampled
        "source": pl.String,  # "ws" | "rest"
        "funding": pl.Float64,  # live estimate for the current hour (settles at the next hour)
        "open_interest": pl.Float64,  # in coins, not USD
        "mark_px": pl.Float64,
        "oracle_px": pl.Float64,
        "mid_px": pl.Float64,
        "premium": pl.Float64,
        "impact_bid_px": pl.Float64,
        "impact_ask_px": pl.Float64,
        "day_ntl_vlm": pl.Float64,
        "day_base_vlm": pl.Float64,
        "prev_day_px": pl.Float64,
    }
)

FUNDING_PRED_SCHEMA = pl.Schema(
    {
        "sample_time": TIME,
        "recv_time": TIME,
        "venue": pl.String,  # "HlPerp" | "BinPerp" | "BybitPerp" (as sent by the API)
        "funding_rate": pl.Float64,  # per funding interval, not per hour
        "interval_hours": pl.Int64,
        # Stored raw. HlPerp's value was observed lagging by >= 1 interval; don't trust it.
        "next_funding_time": TIME,
    }
)

TRADES_SCHEMA = pl.Schema(
    {
        "exch_time": TIME,
        "recv_time": TIME,
        # 50-bit hash of (buyer_oid, seller_oid); unique only together with (time, coin).
        "tid": pl.Int64,
        "px": pl.Float64,
        "sz": pl.Float64,
        "side": pl.String,  # "B" = taker bought, "A" = taker sold
        "hash": pl.String,  # shared by all fills of one taker order
        "buyer": pl.String,
        "seller": pl.String,
    }
)

TRADES_1M_SCHEMA = pl.Schema(
    {
        "minute": TIME,  # bucket start, by exchange trade time
        "n_trades": pl.Int64,
        "base_vol": pl.Float64,
        "notional_vol": pl.Float64,
        "buy_base_vol": pl.Float64,  # taker-buy side
        "sell_base_vol": pl.Float64,
        "buy_notional": pl.Float64,
        "sell_notional": pl.Float64,
        "vwap": pl.Float64,  # null when n_trades == 0
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        # False if any part of the minute fell inside a window where trades may be missing.
        "complete": pl.Boolean,
    }
)

GAPS_SCHEMA = pl.Schema(
    {
        "stream": pl.String,
        "gap_start": TIME,  # last time data is known good
        "gap_end": TIME,  # first time data is known good again
        # "process_down" | "restart_clean" | "missed_samples" | "trades_lost" | "trades_unknown"
        "reason": pl.String,
        "detail": pl.String,
    }
)


def book_schema(levels: int) -> pl.Schema:
    cols: dict[str, pl.DataType | type[pl.DataType]] = {
        "sample_time": TIME,
        "exch_time": TIME,
        "recv_time": TIME,
        "age_ms": pl.Int64,
        "source": pl.String,
    }
    for side in ("bid", "ask"):
        for i in range(levels):
            cols[f"{side}_px_{i}"] = pl.Float64
            cols[f"{side}_sz_{i}"] = pl.Float64
            cols[f"{side}_n_{i}"] = pl.Int64  # number of orders at the level
    return pl.Schema(cols)


_FIXED: dict[str, pl.Schema] = {
    CTX: CTX_SCHEMA,
    FUNDING_PRED: FUNDING_PRED_SCHEMA,
    TRADES: TRADES_SCHEMA,
    TRADES_1M: TRADES_1M_SCHEMA,
    GAPS: GAPS_SCHEMA,
}

# Which column decides the UTC day file a row lands in.
_PARTITION_COLUMN: dict[str, str] = {
    CTX: "sample_time",
    FUNDING_PRED: "sample_time",
    TRADES: "exch_time",
    TRADES_1M: "minute",
    GAPS: "gap_end",
}

# Columns that identify a row, for de-duplication during compaction (None = the whole row).
_UNIQUE_KEY: dict[str, list[str] | None] = {
    CTX: ["sample_time"],
    FUNDING_PRED: ["sample_time", "venue"],
    TRADES: ["exch_time", "tid"],
    TRADES_1M: ["minute"],
    GAPS: None,
}


def is_book(stream: str) -> bool:
    return stream.startswith("book")


def schema_for(stream: str, book_levels: int | None = None) -> pl.Schema:
    if is_book(stream):
        if book_levels is None:
            raise ValueError(f"book stream {stream!r} needs book_levels")
        return book_schema(book_levels)
    return _FIXED[stream]


def partition_column(stream: str) -> str:
    return "sample_time" if is_book(stream) else _PARTITION_COLUMN[stream]


def unique_key(stream: str) -> list[str] | None:
    return ["sample_time"] if is_book(stream) else _UNIQUE_KEY[stream]
