"""True range and Wilder's Average True Range (ATR).

Bars read: the value at bar t uses only bars 0..t (high/low/close of t, close of t-1).
Appending future bars never changes past values (see tests/test_atr.py).
"""

import polars as pl


def true_range(high: list[float], low: list[float], close: list[float]) -> list[float]:
    """TR_t = max(high_t - low_t, |high_t - close_{t-1}|, |low_t - close_{t-1}|).

    The first bar has no previous close, so TR_0 = high_0 - low_0.
    """
    tr: list[float] = []
    for i in range(len(high)):
        if i == 0:
            tr.append(high[0] - low[0])
        else:
            prev_close = close[i - 1]
            tr.append(max(high[i] - low[i], abs(high[i] - prev_close), abs(low[i] - prev_close)))
    return tr


def atr(df: pl.DataFrame, period: int = 14) -> pl.Series:
    """Wilder's ATR over columns `high`, `low`, `close`.

    - First value (at index period-1) is the simple mean of the first `period` TRs.
    - After that: ATR_t = (ATR_{t-1} * (period - 1) + TR_t) / period.
    - Indices before period-1 are null (not enough data yet: "warm-up").
    """
    if period < 1:
        raise ValueError("period must be >= 1")
    tr = true_range(
        df["high"].cast(pl.Float64).to_list(),
        df["low"].cast(pl.Float64).to_list(),
        df["close"].cast(pl.Float64).to_list(),
    )
    out: list[float | None] = []
    prev: float | None = None
    for i, value in enumerate(tr):
        if i < period - 1:
            out.append(None)
            continue
        if prev is None:
            prev = sum(tr[:period]) / period
        else:
            prev = (prev * (period - 1) + value) / period
        out.append(prev)
    return pl.Series("atr", out, dtype=pl.Float64)
