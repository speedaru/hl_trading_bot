"""Shared test helpers: seeded synthetic OHLCV and tiny scripted strategies."""

import random
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from hlbot.backtest.exits import ExitRule
from hlbot.backtest.types import Direction


def make_ohlcv(n: int, seed: int = 42, start_price: float = 100.0) -> pl.DataFrame:
    """Hourly random-walk bars. open = previous close, so there are no gaps."""
    rng = random.Random(seed)
    start = datetime(2024, 1, 1, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    price = start_price
    for i in range(n):
        o = price
        c = o * (1 + rng.gauss(0, 0.006))
        h = max(o, c) * (1 + abs(rng.gauss(0, 0.002)))
        lo = min(o, c) * (1 - abs(rng.gauss(0, 0.002)))
        rows.append(
            {
                "open_time": start + timedelta(hours=i),
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "volume": 1000.0,
            }
        )
        price = c
    return pl.DataFrame(rows).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))


def bars_from_prices(prices: list[tuple[float, float, float, float]]) -> pl.DataFrame:
    """Hand-built hourly bars from (open, high, low, close) tuples."""
    start = datetime(2024, 1, 1, tzinfo=UTC)
    return pl.DataFrame(
        {
            "open_time": [start + timedelta(hours=i) for i in range(len(prices))],
            "open": [p[0] for p in prices],
            "high": [p[1] for p in prices],
            "low": [p[2] for p in prices],
            "close": [p[3] for p in prices],
            "volume": [1.0] * len(prices),
        }
    ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))


@dataclass
class AlwaysLong:
    """Goes long every time it is asked; records exactly what history it saw."""

    exit_rule: ExitRule
    name: str = "always_long"
    seen: list[tuple[int, datetime]] = field(default_factory=lambda: [])

    @property
    def params(self) -> Mapping[str, object]:
        return {"stop_pct": self.exit_rule.stop_pct, "target_r": self.exit_rule.target_r}

    def on_bar(self, history: pl.DataFrame) -> Direction | None:
        self.seen.append((history.height, history["open_time"][-1]))
        return Direction.LONG


@dataclass
class LongOnce:
    """Goes long on the first call only."""

    exit_rule: ExitRule
    name: str = "long_once"
    fired: bool = False

    @property
    def params(self) -> Mapping[str, object]:
        return {}

    def on_bar(self, history: pl.DataFrame) -> Direction | None:
        if self.fired:
            return None
        self.fired = True
        return Direction.LONG


@pytest.fixture
def ohlcv_100() -> pl.DataFrame:
    return make_ohlcv(100)
