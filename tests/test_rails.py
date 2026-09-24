"""Risk rails, in-run skips, warm-up, cached ATR stops, baseline determinism, MTM drawdown."""

from datetime import UTC, datetime, timedelta

import pytest

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.engine import BacktestEngine, BacktestResult
from hlbot.backtest.exits import ExitRule
from hlbot.backtest.metrics import compute_metrics
from hlbot.backtest.types import Direction
from hlbot.strategies.base import Strategy
from hlbot.strategies.random_baseline import RandomBaseline
from tests.conftest import (
    TEST_COIN_MAX_LEVERAGE,
    AlwaysLong,
    LongOnce,
    Scripted,
    bars_from_prices,
    make_ohlcv,
)

NO_SLIP = CostConfig(slippage_bps=0.0, default_funding_rate_hourly=0.0)
RULE = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0)  # long: stop 98, target 104
FLAT = (100.0, 100.0, 100.0, 100.0)
LOSER = (100.0, 100.0, 97.0, 100.0)  # opens at entry, trades through the 98 stop
# Every trade here loses 1R plus fees: (0.045% * (100 + 98)) / 2 = 0.04455R -> -1.04455R.


def risk(**overrides: float) -> RiskConfig:
    base: dict[str, float] = {
        "daily_loss_limit_r": 100.0,
        "weekly_loss_limit_r": 100.0,
        "max_trades_per_day": 100,
        "cooldown_bars_after_stop": 0,
    }
    base.update(overrides)
    return RiskConfig(
        coin_max_leverage=TEST_COIN_MAX_LEVERAGE,
        daily_loss_limit_r=base["daily_loss_limit_r"],
        weekly_loss_limit_r=base["weekly_loss_limit_r"],
        max_trades_per_day=int(base["max_trades_per_day"]),
        cooldown_bars_after_stop=int(base["cooldown_bars_after_stop"]),
    )


def run_losers(n_bars: int, rules: RiskConfig) -> BacktestResult:
    """Always-long on bars that stop out every trade on its entry bar (all same UTC day
    if n_bars <= 24; 2024-01-01 is a Monday so a week is 168 bars)."""
    data = bars_from_prices([FLAT] + [LOSER] * (n_bars - 1))
    return BacktestEngine(data, AlwaysLong(RULE), NO_SLIP, rules, timeframe="1h").run()


# ---------------------------------------------------------------- loss rails
def test_daily_loss_rail_stops_trading_after_3r() -> None:
    res = run_losers(12, risk(daily_loss_limit_r=3.0))
    # -1.04R, -2.09R, -3.13R -> the 4th entry is blocked for the rest of the day
    assert res.trades.height == 3
    assert res.skips == {"daily_loss_rail": 8}


def test_daily_loss_rail_resets_next_day() -> None:
    res = run_losers(30, risk(daily_loss_limit_r=3.0))  # bars 0..29 span two UTC days
    next_day = res.trades.filter(res.trades["entry_bar"] >= 24)
    assert res.trades.height == 6
    assert next_day.height == 3


def test_weekly_loss_rail_stops_trading_after_8r() -> None:
    res = run_losers(20, risk(weekly_loss_limit_r=8.0))
    # after 7 losses: -7.31R (still allowed); after 8: -8.36R -> blocked
    assert res.trades.height == 8
    assert res.skips == {"weekly_loss_rail": 11}


def test_max_trades_per_day() -> None:
    res = run_losers(12, risk(max_trades_per_day=5))
    assert res.trades.height == 5
    assert res.skips == {"max_trades_per_day": 6}


def test_cooldown_after_stop_out() -> None:
    res = run_losers(12, risk(cooldown_bars_after_stop=3))
    # stopped on bar 1 -> fills on bars 2, 3, 4 blocked -> next entry bar 5, then 9
    assert res.trades["entry_bar"].to_list() == [1, 5, 9]
    assert res.skips == {"stop_cooldown": 8}


def test_no_cooldown_after_target_exit() -> None:
    winner = (100.0, 104.5, 99.5, 104.0)  # hits the 104 target, not the stop
    data = bars_from_prices([FLAT, winner, (104.0, 104.0, 104.0, 104.0), FLAT])
    engine = BacktestEngine(
        data, AlwaysLong(RULE), NO_SLIP, risk(cooldown_bars_after_stop=3), timeframe="1h"
    )
    res = engine.run()
    assert res.trades["entry_bar"].to_list()[:2] == [1, 2]
    assert "stop_cooldown" not in res.skips


# ---------------------------------------------------------------- in-run skips
def test_fee_filter_skip_inside_a_run() -> None:
    tight = ExitRule(stop_atr_mult=None, stop_pct=0.005, target_r=2.0)  # 0.18R round trip
    data = bars_from_prices([FLAT] * 5)
    res = BacktestEngine(data, AlwaysLong(tight), NO_SLIP, risk(), timeframe="1h").run()
    assert res.trades.height == 0
    assert res.skips == {"fee_filter": 4}


def test_zero_atr_gives_no_stop_skip() -> None:
    atr_rule = ExitRule(stop_atr_mult=1.5, atr_period=3, target_r=2.0)
    data = bars_from_prices([FLAT] * 6)  # flat bars -> ATR = 0 -> no usable stop
    res = BacktestEngine(data, AlwaysLong(atr_rule), NO_SLIP, risk(), timeframe="1h").run()
    assert res.trades.height == 0
    assert res.skips == {"no_stop": 3}  # signals at bars 2, 3, 4 (after warm-up)


# ---------------------------------------------------------------- warm-up
def test_strategy_not_called_before_warmup() -> None:
    atr_rule = ExitRule(stop_atr_mult=1.5, atr_period=14, target_r=2.0)
    strategy = AlwaysLong(atr_rule, warmup_bars=20)
    data = make_ohlcv(40)
    res = BacktestEngine(data, strategy, NO_SLIP, risk(), timeframe="1h").run()
    assert res.warmup_bars == 20
    assert strategy.seen, "never called"
    assert min(height for height, _ in strategy.seen) == 20
    assert "no_stop" not in res.skips


def test_exit_rule_warmup_applies_even_if_strategy_has_none() -> None:
    atr_rule = ExitRule(stop_atr_mult=1.5, atr_period=14, target_r=2.0)
    strategy = AlwaysLong(atr_rule, warmup_bars=1)
    BacktestEngine(make_ohlcv(40), strategy, NO_SLIP, risk(), timeframe="1h").run()
    assert min(height for height, _ in strategy.seen) == 14


# ---------------------------------------------------------------- cached ATR stops
def test_cached_atr_stops_match_reference_computation() -> None:
    """The engine reads a precomputed ATR series; it must equal ATR recomputed on
    bars 0..signal_bar only (the lookahead-safe reference)."""
    atr_rule = ExitRule(stop_atr_mult=1.5, atr_period=14, target_r=2.0)
    data = make_ohlcv(400, seed=7)
    costs = CostConfig()
    res = BacktestEngine(data, AlwaysLong(atr_rule), costs, risk(), timeframe="1h").run()
    assert res.trades.height > 5
    for t in res.trades.to_dicts():
        expected = atr_rule.stop_distance(data.slice(0, t["signal_bar"] + 1), t["entry_price"])
        assert expected is not None
        assert abs(t["entry_price"] - t["stop_price"]) == pytest.approx(expected)


# ---------------------------------------------------------------- baseline
def baseline(seed: int) -> RandomBaseline:
    return RandomBaseline(entry_prob=0.3, long_prob=0.5, seed=seed, exit_rule=RULE)


def run_strategy(strategy: Strategy) -> BacktestResult:
    return BacktestEngine(make_ohlcv(300), strategy, CostConfig(), risk(), timeframe="1h").run()


def test_baseline_same_seed_same_trades() -> None:
    a, b = run_strategy(baseline(5)).trades, run_strategy(baseline(5)).trades
    assert a.height > 0
    assert a.equals(b)


def test_baseline_different_seed_different_trades() -> None:
    assert not run_strategy(baseline(5)).trades.equals(run_strategy(baseline(6)).trades)


def test_baseline_long_prob_is_respected() -> None:
    only_short = RandomBaseline(entry_prob=0.5, long_prob=0.0, seed=1, exit_rule=RULE)
    trades = run_strategy(only_short).trades
    assert trades.height > 0
    assert set(trades["direction"].to_list()) == {"short"}


# ---------------------------------------------------------------- metrics
def test_mark_to_market_drawdown_sees_open_losses() -> None:
    wide = ExitRule(stop_atr_mult=None, stop_pct=0.10, target_r=1.0)  # stop 90, target 110
    data = bars_from_prices([FLAT, (100.0, 100.0, 94.0, 95.0), (95.0, 111.0, 95.0, 110.0), FLAT])
    res = BacktestEngine(data, LongOnce(wide), NO_SLIP, risk(), timeframe="1h").run()
    (trade,) = res.trades.to_dicts()
    assert trade["exit_reason"] == "target"
    metrics = compute_metrics(res.trades, res.initial_equity, res.skips, res.equity_curve)
    # risk $200 at a 10% stop -> 20 coins. At bar 1's close: 20 * (95 - 100) - entry fee 0.9
    assert metrics["max_drawdown_pct"] == pytest.approx(100.9 / 10_000)
    assert metrics["max_drawdown_closed_pct"] == 0.0  # the only closed trade was a winner


def test_end_of_data_exits_reported_separately() -> None:
    data = bars_from_prices([FLAT, LOSER, FLAT, FLAT])
    res = BacktestEngine(data, AlwaysLong(RULE), NO_SLIP, risk(), timeframe="1h").run()
    metrics = compute_metrics(res.trades, res.initial_equity, res.skips, res.equity_curve)
    assert res.trades["exit_reason"].to_list() == ["stop", "end_of_data"]
    assert metrics["n_end_of_data_exits"] == 1
    assert metrics["expectancy_r_excl_end_of_data"] == pytest.approx(res.trades["r_multiple"][0])


# ---------------------------------------------------------------- exit timestamps
def test_exit_time_is_when_the_fill_happens() -> None:
    start = bars_from_prices([FLAT])["open_time"][0]
    hour = timedelta(hours=1)
    gap = LongOnce(RULE)
    (t_gap,) = (
        BacktestEngine(
            bars_from_prices([FLAT, FLAT, (95.0, 96.0, 94.0, 95.0)]),
            gap,
            NO_SLIP,
            risk(),
            timeframe="1h",
        )
        .run()
        .trades.to_dicts()
    )
    assert t_gap["exit_time"] == start + 2 * hour  # at bar 2's open

    (t_intrabar,) = (
        BacktestEngine(
            bars_from_prices([FLAT, LOSER, FLAT]), LongOnce(RULE), NO_SLIP, risk(), timeframe="1h"
        )
        .run()
        .trades.to_dicts()
    )
    assert t_intrabar["exit_time"] == start + 1.5 * hour  # mid-bar estimate

    timed = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0, max_bars_held=1)
    (t_time,) = (
        BacktestEngine(
            bars_from_prices([FLAT, FLAT, FLAT]), LongOnce(timed), NO_SLIP, risk(), timeframe="1h"
        )
        .run()
        .trades.to_dicts()
    )
    assert t_time["exit_time"] == start + 2 * hour  # bar 1's close


def test_loss_at_midnight_close_counts_toward_the_next_day() -> None:
    """A time stop at the close of the 23:00 bar happens at 00:00 the next day, so its R
    belongs to the next day's loss rail (reviewer finding: it was booked a day early)."""
    timed = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0, max_bars_held=2)
    drift_down = (100.0, 100.0, 99.0, 99.0)
    prices = [FLAT] * 22 + [drift_down, (99.0, 99.0, 99.0, 99.0), FLAT, FLAT]
    strategy = Scripted(timed, {21: Direction.LONG, 23: Direction.LONG})
    res = BacktestEngine(
        bars_from_prices(prices), strategy, NO_SLIP, risk(daily_loss_limit_r=0.5), timeframe="1h"
    ).run()
    (trade,) = res.trades.to_dicts()
    assert trade["exit_reason"] == "time_stop"
    assert trade["exit_time"] == datetime(2024, 1, 2, tzinfo=UTC)
    assert trade["r_multiple"] < -0.5
    assert res.skips == {"daily_loss_rail": 1}  # the entry at 2024-01-02 00:00 is blocked


def test_equity_curve_is_keyed_by_close_time() -> None:
    data = bars_from_prices([FLAT] * 3)
    res = BacktestEngine(data, LongOnce(RULE), NO_SLIP, risk(), timeframe="1h").run()
    assert res.equity_curve.columns == ["close_time", "equity"]
    assert res.equity_curve["close_time"][0] == data["open_time"][0] + timedelta(hours=1)
