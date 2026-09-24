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

    @property
    def uses_atr(self) -> bool:
        return self.stop_atr_mult is not None

    @property
    def warmup_bars(self) -> int:
        """Bars of history needed before a stop can be computed (ATR's first value)."""
        return self.atr_period if self.uses_atr else 1

    def stop_distance(self, history: pl.DataFrame, entry_price: float) -> float | None:
        """Absolute price distance from entry to stop (reference implementation).

        Bars read: `history` only (the engine passes bars 0..signal_bar). Returns None when
        the ATR has not warmed up yet. The engine uses `stop_distance_from_atr` with a
        precomputed ATR series instead; a test checks both give the same stops.
        """
        last_atr = atr(history, self.atr_period)[-1] if self.uses_atr else None
        return self.stop_distance_from_atr(last_atr, entry_price)

    def stop_distance_from_atr(self, atr_value: float | None, entry_price: float) -> float | None:
        """Same as `stop_distance`, given the ATR value at the signal bar."""
        if self.stop_pct is not None:
            return entry_price * self.stop_pct
        assert self.stop_atr_mult is not None
        if atr_value is None:
            return None
        return atr_value * self.stop_atr_mult

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
