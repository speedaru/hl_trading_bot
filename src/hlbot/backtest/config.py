"""Cost and risk configuration. Passed into the engine; never hard-coded in strategies."""

from dataclasses import dataclass

HARD_MAX_RISK_PCT = 0.04
HARD_MAX_LEVERAGE = 10.0


@dataclass(frozen=True)
class CostConfig:
    """Trading costs as fractions (0.00045 = 0.045%).

    Fee defaults are Hyperliquid base-tier perps fees; re-check the current fee schedule.
    Funding default is Hyperliquid's baseline interest component (0.01% per 8h, paid hourly)
    and is only used when the data has no `funding_rate` column. Verify before relying on it.
    """

    taker_fee: float = 0.00045
    maker_fee: float = 0.00015
    slippage_bps: float = 1.5  # per side, applied adversely to every market fill
    default_funding_rate_hourly: float = 0.0000125
    # Conservative default: target exits are treated as market orders (taker + slippage).
    target_is_maker: bool = False

    @property
    def slippage_frac(self) -> float:
        return self.slippage_bps / 10_000


@dataclass(frozen=True)
class RiskConfig:
    """Sizing and risk rails. See CLAUDE.md 'Risk model'."""

    risk_pct: float = 0.02
    max_leverage: float = HARD_MAX_LEVERAGE
    min_liq_to_stop_ratio: float = 3.0
    # Approximation of maintenance margin as a fraction of notional. On Hyperliquid it is
    # half the initial margin at the coin's max leverage (e.g. 20x max -> 2.5%).
    # TODO: verify per coin against HL docs before trusting liquidation checks.
    maintenance_margin_frac: float = 0.025
    max_cost_r: float = 0.1  # fee filter: skip if round-trip cost > this many R
    daily_loss_limit_r: float = 3.0
    weekly_loss_limit_r: float = 8.0

    def __post_init__(self) -> None:
        if not 0 < self.risk_pct <= HARD_MAX_RISK_PCT:
            raise ValueError(f"risk_pct must be in (0, {HARD_MAX_RISK_PCT}], got {self.risk_pct}")
        if not 0 < self.max_leverage <= HARD_MAX_LEVERAGE:
            raise ValueError(
                f"max_leverage must be in (0, {HARD_MAX_LEVERAGE}], got {self.max_leverage}"
            )
