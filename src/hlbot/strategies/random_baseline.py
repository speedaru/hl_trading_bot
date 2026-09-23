"""Random-entry baseline: same exits and sizing as the strategy under test, random entries.

If a strategy can't beat this after costs, its entry logic has no demonstrated edge.
The baseline copies the strategy's entry frequency AND its long/short mix, so a long-only
strategy isn't credited for simply being long in a rising market.
"""

import random
from collections.abc import Mapping
from dataclasses import dataclass, field

import polars as pl

from hlbot.backtest.exits import ExitRule
from hlbot.backtest.types import Direction


@dataclass
class RandomBaseline:
    """Each time it is asked (i.e. while flat), enters with probability `entry_prob`;
    goes long with probability `long_prob`, else short. Deterministic for a given `seed`.
    """

    entry_prob: float
    long_prob: float
    seed: int
    exit_rule: ExitRule
    name: str = "random_baseline"
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for label, p in (("entry_prob", self.entry_prob), ("long_prob", self.long_prob)):
            if not 0 <= p <= 1:
                raise ValueError(f"{label} must be in [0, 1], got {p}")
        self._rng = random.Random(self.seed)

    @property
    def params(self) -> Mapping[str, object]:
        return {"entry_prob": self.entry_prob, "long_prob": self.long_prob, "seed": self.seed}

    def on_bar(self, history: pl.DataFrame) -> Direction | None:
        # Bars read: none. Draw both numbers every call so the RNG stream doesn't depend
        # on which branch was taken.
        enter = self._rng.random() < self.entry_prob
        go_long = self._rng.random() < self.long_prob
        if not enter:
            return None
        return Direction.LONG if go_long else Direction.SHORT
