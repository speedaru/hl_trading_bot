import pytest

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.sizing import SizeResult, liquidation_distance_pct, size_position
from hlbot.backtest.types import SkipReason

COSTS = CostConfig()
RISK = RiskConfig(coin_max_leverage=20.0)


def test_basic_formula() -> None:
    # 10k equity, 2% risk, 2% stop -> risk $200 -> notional 10k -> 1x leverage
    res = size_position(10_000, 100.0, 98.0, RISK, COSTS)
    assert isinstance(res, SizeResult)
    assert res.notional == pytest.approx(10_000)
    assert res.leverage == pytest.approx(1.0)
    assert res.risk_usd == pytest.approx(200)
    assert not res.leverage_capped


def test_tight_stop_is_capped_at_our_10x() -> None:
    # 2% risk with a 0.15% stop would need 13.3x; capped to 10x. Fee filter disabled here.
    risk = RiskConfig(coin_max_leverage=20.0, max_cost_r=10.0)
    res = size_position(10_000, 100.0, 99.85, risk, COSTS)
    assert isinstance(res, SizeResult)
    assert res.leverage_capped
    assert res.notional == pytest.approx(100_000)
    assert res.risk_usd == pytest.approx(150)  # real risk shrinks below 2%


def test_coin_max_leverage_caps_below_10x() -> None:
    # thin alt with 3x max on the exchange: 2% risk / 0.5% stop = 4x -> capped to 3x
    risk = RiskConfig(coin_max_leverage=3.0, max_cost_r=10.0)
    res = size_position(10_000, 100.0, 99.5, risk, COSTS)
    assert isinstance(res, SizeResult)
    assert res.leverage == pytest.approx(3.0)
    assert res.leverage_capped


def test_maintenance_margin_is_half_initial_at_max_leverage() -> None:
    assert RiskConfig(coin_max_leverage=40.0).maintenance_margin_frac == pytest.approx(0.0125)
    assert RiskConfig(coin_max_leverage=3.0).maintenance_margin_frac == pytest.approx(1 / 6)


def test_liquidation_distance_hand_checked() -> None:
    # 10x, maintenance 2.5%: long (0.1 - 0.025) / 0.975, short (0.1 - 0.025) / 1.025
    assert liquidation_distance_pct(10, 0.025, long=True) == pytest.approx(0.075 / 0.975)
    assert liquidation_distance_pct(10, 0.025, long=False) == pytest.approx(0.075 / 1.025)
    # sanity: liquidation price from HL's formula for a 1-coin long at $100, $10 equity
    # liq = 100 - (10 - 0.025 * 100) / 1 / (1 - 0.025) = 92.3077
    assert 100 * (1 - liquidation_distance_pct(10, 0.025, long=True)) == pytest.approx(92.30769)


def test_short_liquidation_is_closer_than_long() -> None:
    risk = RiskConfig(coin_max_leverage=20.0, risk_pct=0.04, max_cost_r=10.0)
    long = size_position(10_000, 100.0, 99.6, risk, COSTS)
    short = size_position(10_000, 100.0, 100.4, risk, COSTS)
    assert isinstance(long, SizeResult)
    assert isinstance(short, SizeResult)
    assert short.liq_distance_pct < long.liq_distance_pct


def test_liquidation_too_close_is_skipped() -> None:
    # With HL's margin rules and risk <= 4%, liquidation is always far beyond 3x the stop
    # (see docs/known_issues.md), so exercise the skip path with a stricter ratio.
    strict = RiskConfig(
        coin_max_leverage=20.0, risk_pct=0.04, max_cost_r=10.0, min_liq_to_stop_ratio=30.0
    )
    # 0.4% stop -> 10x -> liq 7.69% away < 30 * 0.4% = 12% -> skip
    assert size_position(10_000, 100.0, 99.6, strict, COSTS) is SkipReason.LIQUIDATION
    ok = RiskConfig(coin_max_leverage=20.0, risk_pct=0.04, max_cost_r=10.0)
    assert isinstance(size_position(10_000, 100.0, 99.6, ok, COSTS), SizeResult)


def test_fee_filter_skips_tight_stops() -> None:
    # round trip = 2*0.045% + 2*0.015% = 0.12%; with a 1% stop that's 0.12R > 0.1R
    assert size_position(10_000, 100.0, 99.0, RISK, COSTS) is SkipReason.FEE_FILTER
    # with a 1.5% stop it's 0.08R -> allowed
    assert isinstance(size_position(10_000, 100.0, 98.5, RISK, COSTS), SizeResult)


def test_short_side_uses_absolute_distance() -> None:
    res = size_position(10_000, 100.0, 102.0, RISK, COSTS)
    assert isinstance(res, SizeResult)
    assert res.notional == pytest.approx(10_000)


@pytest.mark.parametrize("bad", [0.0, 0.041, 0.10])
def test_risk_above_hard_cap_rejected(bad: float) -> None:
    with pytest.raises(ValueError):
        RiskConfig(coin_max_leverage=20.0, risk_pct=bad)


def test_leverage_above_hard_cap_rejected() -> None:
    with pytest.raises(ValueError):
        RiskConfig(coin_max_leverage=20.0, max_leverage=20)


def test_invalid_coin_max_leverage_rejected() -> None:
    with pytest.raises(ValueError):
        RiskConfig(coin_max_leverage=0.5)
