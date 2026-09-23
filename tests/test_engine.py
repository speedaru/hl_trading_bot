"""Hand-checked fill/exit rules on tiny scripted price paths (slippage off for clean numbers)."""

from datetime import timedelta
from typing import Any

import polars as pl
import pytest

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.engine import BacktestEngine, parse_timeframe
from hlbot.backtest.exits import ExitRule
from tests.conftest import LongOnce, bars_from_prices

NO_SLIP = CostConfig(slippage_bps=0.0, default_funding_rate_hourly=0.0)
RULE = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0)  # stop 98, target 104
FLAT = (100.0, 100.0, 100.0, 100.0)


def run(
    prices: list[tuple[float, float, float, float]],
    rule: ExitRule = RULE,
    costs: CostConfig = NO_SLIP,
) -> list[dict[str, Any]]:
    data = bars_from_prices(prices)
    engine = BacktestEngine(data, LongOnce(rule), costs, RiskConfig(), timeframe="1h")
    return engine.run().trades.to_dicts()


def test_signal_fills_at_next_bar_open() -> None:
    (trade,) = run([FLAT, (101.0, 101.5, 100.5, 101.0), FLAT])
    assert trade["signal_bar"] == 0
    assert trade["entry_bar"] == 1
    assert trade["entry_price"] == pytest.approx(101.0)  # bar 1 open, not bar 0 close


def test_bar_touching_stop_and_target_counts_as_stop() -> None:
    (trade,) = run([FLAT, (100.0, 105.0, 97.0, 100.0), FLAT])
    assert trade["exit_reason"] == "stop"
    assert trade["exit_bar"] == 1
    assert trade["exit_price"] == pytest.approx(98.0)


def test_gap_through_stop_exits_at_open() -> None:
    (trade,) = run([FLAT, (100.0, 101.0, 99.5, 100.0), (95.0, 96.0, 94.0, 95.0)])
    assert trade["exit_reason"] == "stop"
    assert trade["exit_price"] == pytest.approx(95.0)  # worse than the 98 stop
    assert trade["r_multiple"] < -2


def test_target_hit() -> None:
    (trade,) = run([FLAT, (100.0, 104.5, 99.0, 104.0), FLAT])
    assert trade["exit_reason"] == "target"
    assert trade["exit_price"] == pytest.approx(104.0)
    # +2R gross, minus taker fees on both legs (expressed in R)
    fees_r = (0.00045 * 100 + 0.00045 * 104) / 2.0
    assert trade["r_multiple"] == pytest.approx(2.0 - fees_r)


def test_time_stop_exits_at_close() -> None:
    rule = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0, max_bars_held=2)
    (trade,) = run([FLAT, FLAT, (100.0, 101.0, 99.0, 100.5), FLAT], rule=rule)
    assert trade["exit_reason"] == "time_stop"
    assert trade["exit_bar"] == 2  # entry bar 1 counts as bar 1 held
    assert trade["exit_price"] == pytest.approx(100.5)


FUNDED = CostConfig(slippage_bps=0.0, default_funding_rate_hourly=0.0001)


def _one_bar_funding(trade: dict[str, Any], price: float = 100.0) -> float:
    return trade["qty"] * price * 0.0001


def test_funding_intrabar_exit_charges_half_of_exit_bar() -> None:
    (trade,) = run([FLAT, FLAT, FLAT, (100.0, 104.5, 99.0, 104.0)], costs=FUNDED)
    # held through the close of bars 1 and 2, then exits intrabar on bar 3 -> +1/2 bar
    # (the half bar is valued at bar 3's close, 104)
    expected = 2 * _one_bar_funding(trade) + 0.5 * _one_bar_funding(trade, 104.0)
    assert trade["funding"] == pytest.approx(expected)
    assert trade["net_pnl"] == pytest.approx(
        trade["gross_pnl"] - trade["fees"] - trade["slippage"] - trade["funding"]
    )


def test_funding_time_stop_charges_full_exit_bar() -> None:
    rule = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0, max_bars_held=2)
    (trade,) = run([FLAT, FLAT, FLAT, FLAT], rule=rule, costs=FUNDED)
    # entry bar 1 + exit bar 2, both held to the close
    assert trade["exit_reason"] == "time_stop"
    assert trade["funding"] == pytest.approx(2 * _one_bar_funding(trade))


def test_funding_end_of_data_charges_full_last_bar() -> None:
    (trade,) = run([FLAT, FLAT, FLAT], costs=FUNDED)
    assert trade["exit_reason"] == "end_of_data"
    assert trade["funding"] == pytest.approx(2 * _one_bar_funding(trade))  # bars 1 and 2


def test_funding_gap_exit_at_open_charges_nothing_for_exit_bar() -> None:
    (trade,) = run([FLAT, FLAT, (95.0, 96.0, 94.0, 95.0)], costs=FUNDED)
    assert trade["exit_reason"] == "stop"
    assert trade["funding"] == pytest.approx(_one_bar_funding(trade))  # bar 1 only


def test_open_position_closed_at_end_of_data() -> None:
    (trade,) = run([FLAT, FLAT, (100.0, 101.0, 100.0, 101.0)])
    assert trade["exit_reason"] == "end_of_data"
    assert trade["exit_price"] == pytest.approx(101.0)


# ---------------------------------------------------------------- data validation
def _engine(data: pl.DataFrame, timeframe: str = "1h") -> BacktestEngine:
    return BacktestEngine(data, LongOnce(RULE), NO_SLIP, RiskConfig(), timeframe=timeframe)


def test_parse_timeframe() -> None:
    assert parse_timeframe("15m") == timedelta(minutes=15)
    assert parse_timeframe("4h") == timedelta(hours=4)
    assert parse_timeframe("1d") == timedelta(days=1)
    for bad in ("", "h", "0h", "4x", "1.5h"):
        with pytest.raises(ValueError):
            parse_timeframe(bad)


def test_missing_bar_is_rejected() -> None:
    data = bars_from_prices([FLAT] * 6)
    with pytest.raises(ValueError, match="irregular bar spacing"):
        _engine(pl.concat([data.head(2), data.tail(3)]))  # drops one hour


def test_timeframe_mismatch_is_rejected() -> None:
    with pytest.raises(ValueError, match="irregular bar spacing"):
        _engine(bars_from_prices([FLAT] * 4), timeframe="4h")  # data is hourly


def test_duplicate_or_unsorted_bars_are_rejected() -> None:
    data = bars_from_prices([FLAT] * 4)
    with pytest.raises(ValueError, match="irregular bar spacing"):
        _engine(pl.concat([data, data.tail(1)]))
    with pytest.raises(ValueError, match="irregular bar spacing"):
        _engine(data.reverse())


@pytest.mark.parametrize(
    "bad_bar",
    [
        (100.0, 99.0, 98.0, 99.5),  # high below open
        (100.0, 101.0, 100.5, 100.8),  # low above open
        (100.0, 101.0, 0.0, 100.0),  # zero price
    ],
)
def test_inconsistent_ohlc_is_rejected(bad_bar: tuple[float, float, float, float]) -> None:
    with pytest.raises(ValueError, match="high/low outside open/close"):
        _engine(bars_from_prices([FLAT, bad_bar, FLAT]))


def test_null_funding_rate_is_rejected() -> None:
    data = bars_from_prices([FLAT] * 3).with_columns(
        funding_rate=pl.Series([0.0001, None, 0.0001], dtype=pl.Float64)
    )
    with pytest.raises(ValueError, match="funding_rate has 1 nulls"):
        _engine(data)


def test_funding_rate_column_is_used_per_bar() -> None:
    data = bars_from_prices([FLAT] * 4).with_columns(
        funding_rate=pl.Series([0.0, 0.0001, -0.0002, 0.0], dtype=pl.Float64)
    )
    (trade,) = _engine(data).run().trades.to_dicts()
    # held bars 1..3 to the close; long pays positive rates, receives negative ones
    assert trade["funding"] == pytest.approx(trade["qty"] * 100.0 * (0.0001 - 0.0002 + 0.0))


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("open", float("nan")),
        ("high", float("nan")),
        ("close", float("nan")),
        ("high", float("inf")),
        ("volume", float("nan")),
        ("funding_rate", float("nan")),
        ("funding_rate", float("inf")),
    ],
)
def test_nan_and_inf_are_rejected(column: str, value: float) -> None:
    data = bars_from_prices([FLAT] * 3).with_columns(funding_rate=pl.lit(0.0001))
    patched = data.with_columns(
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.lit(value))
        .otherwise(pl.col(column))
        .alias(column)
    )
    with pytest.raises(ValueError, match="NaN/inf"):
        _engine(patched)


def test_negative_volume_is_rejected() -> None:
    data = bars_from_prices([FLAT] * 3).with_columns(volume=pl.Series([1.0, -1.0, 1.0]))
    with pytest.raises(ValueError, match="negative"):
        _engine(data)
