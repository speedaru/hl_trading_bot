"""The Strategy interface the engine calls."""

from collections.abc import Mapping
from typing import Protocol

import polars as pl

from hlbot.backtest.exits import ExitRule
from hlbot.backtest.types import Direction


class Strategy(Protocol):
    """A strategy decides *whether and which way* to enter. Nothing else.

    `on_bar(history)` is called after bar t closes, with `history` = bars 0..t inclusive.
    Returning a Direction means "enter at the next bar's open"; None means "do nothing".
    Stops/targets come from `exit_rule`; sizing and risk rails are the engine's job.
    """

    @property
    def name(self) -> str: ...

    @property
    def params(self) -> Mapping[str, object]: ...

    @property
    def exit_rule(self) -> ExitRule: ...

    def on_bar(self, history: pl.DataFrame) -> Direction | None: ...
