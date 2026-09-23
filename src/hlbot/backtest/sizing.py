"""Position sizing from risk, with leverage cap, liquidation check and fee filter."""

from dataclasses import dataclass

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.types import SkipReason


@dataclass(frozen=True)
class SizeResult:
    notional: float
    leverage: float
    leverage_capped: bool
    risk_usd: float  # loss in $ if the stop is hit exactly (before costs)
    stop_distance_pct: float
    liq_distance_pct: float
    round_trip_cost_r: float


def size_position(
    equity: float,
    entry_price: float,
    stop_price: float,
    risk: RiskConfig,
    costs: CostConfig,
) -> SizeResult | SkipReason:
    """notional = equity * risk_pct / stop_distance_pct, then apply caps and filters.

    - Leverage above `max_leverage`: notional is cut to the cap (real risk becomes < risk_pct).
    - Liquidation distance (cross margin approx) = 1/leverage - maintenance margin.
      Skip if it is not at least `min_liq_to_stop_ratio` x the stop distance.
    - Fee filter: round-trip cost (taker + slippage on both legs) in R; skip if > max_cost_r.
      Note cost in R = cost_pct / stop_distance_pct, so it doesn't depend on size at all.
    """
    if equity <= 0 or entry_price <= 0:
        raise ValueError("equity and entry_price must be positive")
    stop_distance_pct = abs(entry_price - stop_price) / entry_price
    if stop_distance_pct <= 0:
        raise ValueError("stop must differ from entry")

    round_trip_cost_pct = 2 * costs.taker_fee + 2 * costs.slippage_frac
    round_trip_cost_r = round_trip_cost_pct / stop_distance_pct
    if round_trip_cost_r > risk.max_cost_r:
        return SkipReason.FEE_FILTER

    notional = equity * risk.risk_pct / stop_distance_pct
    leverage = notional / equity
    capped = leverage > risk.max_leverage
    if capped:
        notional = equity * risk.max_leverage
        leverage = risk.max_leverage

    liq_distance_pct = 1 / leverage - risk.maintenance_margin_frac
    if liq_distance_pct < risk.min_liq_to_stop_ratio * stop_distance_pct:
        return SkipReason.LIQUIDATION

    return SizeResult(
        notional=notional,
        leverage=leverage,
        leverage_capped=capped,
        risk_usd=notional * stop_distance_pct,
        stop_distance_pct=stop_distance_pct,
        liq_distance_pct=liq_distance_pct,
        round_trip_cost_r=round_trip_cost_r,
    )
