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


def liquidation_distance_pct(leverage: float, maintenance_margin: float, *, long: bool) -> float:
    """Distance from entry to liquidation price, as a fraction of entry price.

    From Hyperliquid's formula (docs: trading/liquidations), cross margin, one position:
        liq_price = price - side * margin_available / position_size / (1 - l * side)
        margin_available = account_value - maintenance_margin_required
    With account_value = equity, l = maintenance margin fraction m, this simplifies to
        long:  (1/leverage - m) / (1 - m)
        short: (1/leverage - m) / (1 + m)     <- shorts get liquidated slightly sooner
    Ignores fees and unrealized PnL of other positions (there are none in this engine).
    """
    side = 1 if long else -1
    return (1 / leverage - maintenance_margin) / (1 - side * maintenance_margin)


def size_position(
    equity: float,
    entry_price: float,
    stop_price: float,
    risk: RiskConfig,
    costs: CostConfig,
) -> SizeResult | SkipReason:
    """notional = equity * risk_pct / stop_distance_pct, then apply caps and filters.

    - Leverage above min(our cap, coin's exchange max): notional is cut to the cap
      (real risk becomes < risk_pct).
    - Liquidation must be >= `min_liq_to_stop_ratio` x the stop distance, else skip.
    - Fee filter: round-trip cost (taker + slippage on both legs) in R; skip if > max_cost_r.
      Note cost in R = cost_pct / stop_distance_pct, so it doesn't depend on size at all.
    Direction is implied by the stop: stop below entry = long.
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
    cap = risk.effective_max_leverage
    capped = leverage > cap
    if capped:
        notional = equity * cap
        leverage = cap

    liq_distance_pct = liquidation_distance_pct(
        leverage, risk.maintenance_margin_frac, long=stop_price < entry_price
    )
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
