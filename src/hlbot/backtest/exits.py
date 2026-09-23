"""ExitRule: how a trade's stop, target and time stop are set.

A strategy owns one ExitRule; the random baseline reuses the same rule so that any
difference between them comes from entries alone.
"""

from dataclasses import dataclass

import polars as pl

from hlbot.backtest.types import Direction
from hlbot.indicators.atr import atr


@dataclass(frozen=True)
class ExitRule:
    """Stop = either `stop_atr_mult` x ATR(atr_period) or `stop_pct` x entry price.

    Target = `target_r` x stop distance (None = no target). Time stop = exit at the close
    of the `max_bars_held`-th bar held, counting the entry bar as 1 (None = no time stop).
    """

    stop_atr_mult: float | None = 1.5
    stop_pct: float | None = None
    atr_period: int = 14
    target_r: float | None = 2.0
    max_bars_held: int | None = None

    def __post_init__(self) -> None:
        if (self.stop_atr_mult is None) == (self.stop_pct is None):
            raise ValueError("set exactly one of stop_atr_mult or stop_pct")

    def stop_distance(self, history: pl.DataFrame, entry_price: float) -> float | None:
        """Absolute price distance from entry to stop.

        Bars read: `history` only (the engine passes bars 0..signal_bar). Returns None when
        the ATR has not warmed up yet.
        """
        if self.stop_pct is not None:
            return entry_price * self.stop_pct
        assert self.stop_atr_mult is not None
        last_atr = atr(history, self.atr_period)[-1]
        if last_atr is None:
            return None
        return float(last_atr) * self.stop_atr_mult

    def levels(
        self, direction: Direction, entry_price: float, stop_distance: float
    ) -> tuple[float, float | None]:
        """(stop_price, target_price) anchored on the actual fill price."""
        sign = direction.value
        stop = entry_price - sign * stop_distance
        target = (
            None if self.target_r is None else entry_price + sign * self.target_r * stop_distance
        )
        return stop, target
