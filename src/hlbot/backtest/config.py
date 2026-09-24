"""Cost and risk configuration. Passed into the engine; never hard-coded in strategies."""

from dataclasses import dataclass

HARD_MAX_RISK_PCT = 0.04
HARD_MAX_LEVERAGE = 10.0


@dataclass(frozen=True)
class CostConfig:
    """Trading costs as fractions (0.00045 = 0.045%).

    Fee defaults are Hyperliquid base-tier (tier 0) perps fees, checked against
    https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees on 2026-09-25.
    Funding default is Hyperliquid's baseline interest component (0.01% per 8h, paid hourly)
    and is only used when the data has no `funding_rate` column.
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


@dataclass(frozen=True, kw_only=True)
class RiskConfig:
    """Sizing and risk rails. See CLAUDE.md 'Risk model'.

    `coin_max_leverage` is the exchange's max leverage for the coin being tested (3-40x on
    Hyperliquid; `maxLeverage` in the SDK's `Info.meta()` universe). It is required on
    purpose: a wrong default would make the liquidation check pass for thin alts.
    """

    coin_max_leverage: float
    risk_pct: float = 0.02
    max_leverage: float = HARD_MAX_LEVERAGE  # our own cap; the effective cap is the lower one
    min_liq_to_stop_ratio: float = 3.0
    max_cost_r: float = 0.1  # fee filter: skip if round-trip cost > this many R
    daily_loss_limit_r: float = 3.0
    weekly_loss_limit_r: float = 8.0
    max_trades_per_day: int = 5  # entries per UTC day
    # "No re-entry right after a stop-out": after a stop exit on bar x, no entry fills on
    # bars x+1 .. x+N. Placeholder value, not tuned; changing it counts as a variant.
    cooldown_bars_after_stop: int = 3

    def __post_init__(self) -> None:
        if not 0 < self.risk_pct <= HARD_MAX_RISK_PCT:
            raise ValueError(f"risk_pct must be in (0, {HARD_MAX_RISK_PCT}], got {self.risk_pct}")
        if not 0 < self.max_leverage <= HARD_MAX_LEVERAGE:
            raise ValueError(
                f"max_leverage must be in (0, {HARD_MAX_LEVERAGE}], got {self.max_leverage}"
            )
        if self.coin_max_leverage < 1:
            raise ValueError(f"coin_max_leverage must be >= 1, got {self.coin_max_leverage}")
        if self.max_trades_per_day < 1 or self.cooldown_bars_after_stop < 0:
            raise ValueError("max_trades_per_day must be >= 1 and cooldown_bars_after_stop >= 0")

    @property
    def effective_max_leverage(self) -> float:
        return min(self.max_leverage, self.coin_max_leverage)

    @property
    def maintenance_margin_frac(self) -> float:
        """Hyperliquid docs (trading/liquidations): 'The maintenance margin is half of the
        initial margin at max leverage'. Initial margin at max leverage = 1/max_leverage."""
        return 0.5 / self.coin_max_leverage
