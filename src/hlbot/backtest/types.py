"""Small shared types for the backtest engine."""

from enum import Enum, StrEnum

import polars as pl


class Direction(Enum):
    LONG = 1
    SHORT = -1


class SkipReason(StrEnum):
    """Why a strategy signal did not become a trade."""

    NO_STOP = "no_stop"  # stop could not be computed (e.g. ATR still warming up)
    FEE_FILTER = "fee_filter"
    LIQUIDATION = "liquidation_too_close"
    DAILY_LOSS_RAIL = "daily_loss_rail"
    WEEKLY_LOSS_RAIL = "weekly_loss_rail"


class ExitReason(StrEnum):
    STOP = "stop"
    TARGET = "target"
    TIME = "time_stop"
    END_OF_DATA = "end_of_data"


# Column schema of trades.parquet. Explicit so an empty run still writes a valid file.
TRADE_SCHEMA: dict[str, type[pl.DataType] | pl.DataType] = {
    "signal_bar": pl.Int64,
    "entry_bar": pl.Int64,
    "exit_bar": pl.Int64,
    "entry_time": pl.Datetime("us", "UTC"),
    "exit_time": pl.Datetime("us", "UTC"),
    "direction": pl.String,
    "entry_price": pl.Float64,
    "exit_price": pl.Float64,
    "stop_price": pl.Float64,
    "target_price": pl.Float64,
    "qty": pl.Float64,
    "notional": pl.Float64,
    "leverage": pl.Float64,
    "leverage_capped": pl.Boolean,
    "risk_usd": pl.Float64,
    "gross_pnl": pl.Float64,
    "fees": pl.Float64,
    "slippage": pl.Float64,
    "funding": pl.Float64,
    "net_pnl": pl.Float64,
    "r_multiple": pl.Float64,
    "exit_reason": pl.String,
    "equity_after": pl.Float64,
}


def trades_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=TRADE_SCHEMA, orient="row")
