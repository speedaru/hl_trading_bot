"""The Strategy interface the engine calls."""

from collections.abc import Mapping
from typing import Protocol

import polars as pl

from hlbot.backtest.exits import ExitRule
from hlbot.backtest.types import Direction


class Strategy(Protocol):
    """A strategy decides *whether and which way* to enter. Nothing else.

    `on_bar(history)` is called after bar t closes, with `history` = bars 0..t inclusive,
    but ONLY while flat (no open position, no pending entry). A strategy that keeps internal
    state must not assume it sees every bar; recompute from `history` instead.
    Returning a Direction means "enter at the next bar's open"; None means "do nothing".
    Exits come only from `exit_rule` (stop/target/time stop): a strategy can't close a trade
    on its own signal yet. Sizing and risk rails are the engine's job.

    `warmup_bars`: minimum history length before the strategy can signal (e.g. 200 for an
    EMA200 filter). The engine doesn't call `on_bar` before max(warmup_bars,
    exit_rule.warmup_bars) bars exist, and the random baseline gets the same warm-up, so it
    can't trade in a period the strategy never could.
    """

    @property
    def name(self) -> str: ...

    @property
    def params(self) -> Mapping[str, object]: ...

    @property
    def exit_rule(self) -> ExitRule: ...

    @property
    def warmup_bars(self) -> int: ...

    def on_bar(self, history: pl.DataFrame) -> Direction | None: ...
