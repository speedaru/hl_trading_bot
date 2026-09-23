import pytest

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.sizing import SizeResult, size_position
from hlbot.backtest.types import SkipReason

COSTS = CostConfig()


def test_basic_formula() -> None:
    # 10k equity, 2% risk, 2% stop -> risk $200 -> notional 10k -> 1x leverage
    res = size_position(10_000, 100.0, 98.0, RiskConfig(), COSTS)
    assert isinstance(res, SizeResult)
    assert res.notional == pytest.approx(10_000)
    assert res.leverage == pytest.approx(1.0)
    assert res.risk_usd == pytest.approx(200)
    assert not res.leverage_capped


def test_tight_stop_is_leverage_capped() -> None:
    # 2% risk with a 0.15% stop would need 13.3x; capped to 10x. Fee filter disabled here.
    risk = RiskConfig(max_cost_r=10.0)
    res = size_position(10_000, 100.0, 99.85, risk, COSTS)
    assert isinstance(res, SizeResult)
    assert res.leverage_capped
    assert res.notional == pytest.approx(100_000)
    assert res.risk_usd == pytest.approx(150)  # real risk shrinks below 2%


def test_liquidation_too_close_is_skipped() -> None:
    # 4% risk, 0.4% stop -> 10x leverage. Fee filter disabled to isolate the liq check.
    # Default maintenance 2.5%: liq ~ 1/10 - 0.025 = 7.5% away >= 3 * 0.4% -> allowed.
    risk = RiskConfig(risk_pct=0.04, max_cost_r=10.0)
    res = size_position(10_000, 100.0, 99.6, risk, COSTS)
    assert isinstance(res, SizeResult)
    assert res.liq_distance_pct == pytest.approx(0.075)
    # Maintenance 9%: liq ~ 1% away < 1.2% (3x the stop) -> skip.
    risk_mm = RiskConfig(risk_pct=0.04, max_cost_r=10.0, maintenance_margin_frac=0.09)
    assert size_position(10_000, 100.0, 99.6, risk_mm, COSTS) is SkipReason.LIQUIDATION


def test_fee_filter_skips_tight_stops() -> None:
    # round trip = 2*0.045% + 2*0.015% = 0.12%; with a 1% stop that's 0.12R > 0.1R
    assert size_position(10_000, 100.0, 99.0, RiskConfig(), COSTS) is SkipReason.FEE_FILTER
    # with a 1.5% stop it's 0.08R -> allowed
    assert isinstance(size_position(10_000, 100.0, 98.5, RiskConfig(), COSTS), SizeResult)


def test_short_side_uses_absolute_distance() -> None:
    res = size_position(10_000, 100.0, 102.0, RiskConfig(), COSTS)
    assert isinstance(res, SizeResult)
    assert res.notional == pytest.approx(10_000)


@pytest.mark.parametrize("bad", [0.0, 0.041, 0.10])
def test_risk_above_hard_cap_rejected(bad: float) -> None:
    with pytest.raises(ValueError):
        RiskConfig(risk_pct=bad)


def test_leverage_above_hard_cap_rejected() -> None:
    with pytest.raises(ValueError):
        RiskConfig(max_leverage=20)
