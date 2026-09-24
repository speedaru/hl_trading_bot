"""Bar-by-bar backtest engine (single coin, at most one open position).

Timeline for bar t (timestamps are bar OPEN times; bar t is known only after it closes):
  1. A signal from bar t-1, if any, fills at open[t] +/- slippage. Rails and sizing are
     checked here; a signal that fails them is recorded as a skip.
  2. An open position (including one just filled) is checked against bar t's range for
     stop / target / time stop. Funding is charged for the part of the bar the position
     was held: whole bar if still open (or exited) at the close, half if it exited
     intrabar, none if it exited at the open.
  3. Mark-to-market equity is recorded at bar t's close.
  4. If flat and past warm-up, the strategy is called with bars 0..t. A signal is filled
     in step 1 at t+1.

No signal is ever computed on the full dataset: the strategy only receives the prefix
`data.slice(0, t + 1)`, so it cannot see bar t+1. The one thing precomputed over the full
data is the ATR series for stops; ATR is causal (value at t uses bars 0..t only, proven by
the append-future test in tests/test_atr.py) and the engine only reads it at the signal bar.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import polars as pl

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.costs import fee, funding_cost, slipped_price
from hlbot.backtest.sizing import SizeResult, size_position
from hlbot.backtest.types import Direction, ExitReason, SkipReason, trades_frame
from hlbot.indicators.atr import atr
from hlbot.strategies.base import Strategy

REQUIRED_COLUMNS = ("open_time", "open", "high", "low", "close", "volume")


@dataclass
class _Position:
    direction: Direction
    signal_bar: int
    entry_bar: int
    entry_time: datetime
    entry_ref: float  # bar open, before slippage
    entry_price: float  # actual fill
    stop: float
    target: float | None
    qty: float
    size: SizeResult
    entry_fee: float
    funding: float = 0.0


@dataclass
class _Rails:
    """Running state for the risk rails (single coin)."""

    risk: RiskConfig
    daily_r: defaultdict[date, float] = field(default_factory=lambda: defaultdict(float))
    weekly_r: defaultdict[tuple[int, int], float] = field(
        default_factory=lambda: defaultdict(float)
    )
    entries_per_day: Counter[date] = field(default_factory=Counter[date])
    last_stop_bar: int | None = None

    @staticmethod
    def _week(ts: datetime) -> tuple[int, int]:
        iso = ts.isocalendar()
        return iso.year, iso.week

    def blocked(self, ts: datetime, t: int) -> SkipReason | None:
        """Would an entry filling at bar t (time ts) break a rail?"""
        if self.daily_r[ts.date()] <= -self.risk.daily_loss_limit_r:
            return SkipReason.DAILY_LOSS_RAIL
        if self.weekly_r[self._week(ts)] <= -self.risk.weekly_loss_limit_r:
            return SkipReason.WEEKLY_LOSS_RAIL
        if self.entries_per_day[ts.date()] >= self.risk.max_trades_per_day:
            return SkipReason.TRADES_PER_DAY
        if self.last_stop_bar is not None and t <= (
            self.last_stop_bar + self.risk.cooldown_bars_after_stop
        ):
            return SkipReason.STOP_COOLDOWN
        return None

    def record_entry(self, ts: datetime) -> None:
        self.entries_per_day[ts.date()] += 1

    def record_exit(self, ts: datetime, t: int, r: float, reason: ExitReason) -> None:
        self.daily_r[ts.date()] += r
        self.weekly_r[self._week(ts)] += r
        if reason is ExitReason.STOP:
            self.last_stop_bar = t


@dataclass(frozen=True)
class BacktestResult:
    trades: pl.DataFrame
    equity_curve: pl.DataFrame  # close_time, equity (mark-to-market at each bar's close)
    skips: dict[str, int]
    n_bars: int
    warmup_bars: int
    n_strategy_calls: int
    n_signals: int
    n_long_signals: int
    initial_equity: float
    final_equity: float
    data_start: datetime
    data_end: datetime
    bar_hours: float


_TIMEFRAME_UNITS = {"m": "minutes", "h": "hours", "d": "days"}


def parse_timeframe(timeframe: str) -> timedelta:
    """'15m' -> 15 minutes, '1h' -> 1 hour, '4h' -> 4 hours, '1d' -> 1 day."""
    unit = _TIMEFRAME_UNITS.get(timeframe[-1:])
    if unit is None or not timeframe[:-1].isdigit() or int(timeframe[:-1]) <= 0:
        raise ValueError(f"unsupported timeframe {timeframe!r}; use e.g. '15m', '1h', '4h', '1d'")
    return timedelta(**{unit: int(timeframe[:-1])})


def validate_ohlcv(data: pl.DataFrame, bar_interval: timedelta) -> None:
    """Reject data the engine can't simulate honestly.

    - every bar exactly `bar_interval` after the previous one (no gaps, no duplicates):
      a gap would silently stretch ATR windows, time stops and funding across missing time
    - prices positive and each bar internally consistent (low <= open/close <= high)
    - no nulls, NaN or inf in OHLCV or `funding_rate` (if present); volume >= 0
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in data.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    if data.height < 2:
        raise ValueError("need at least 2 bars")
    dtype = data.schema["open_time"]
    if not isinstance(dtype, pl.Datetime) or dtype.time_zone != "UTC":
        raise ValueError("open_time must be Datetime with time_zone='UTC'")
    if data.select(REQUIRED_COLUMNS).null_count().sum_horizontal().item() > 0:
        raise ValueError("OHLCV data contains nulls")
    if "funding_rate" in data.columns and data["funding_rate"].null_count() > 0:
        raise ValueError(
            f"funding_rate has {data['funding_rate'].null_count()} nulls; fill or trim the data"
        )
    # NaN is not null in polars, and NaN comparisons are always False, so the checks above
    # and below would let it through. One NaN funding rate turns equity into NaN.
    numeric = [
        c for c in ("open", "high", "low", "close", "volume", "funding_rate") if c in data.columns
    ]
    for col in numeric:
        n_bad = int((~data[col].cast(pl.Float64).is_finite()).sum())
        if n_bad:
            raise ValueError(f"{col} has {n_bad} NaN/inf value(s)")
    if (data["volume"] < 0).any():
        raise ValueError("volume has negative values")

    step_us = bar_interval // timedelta(microseconds=1)
    steps = data["open_time"].dt.epoch("us").diff().drop_nulls()
    bad = steps.ne(step_us)
    if bad.any():
        first = int(bad.arg_true()[0]) + 1
        raise ValueError(
            f"{int(bad.sum())} irregular bar spacing(s) for interval {bar_interval}; "
            f"first at row {first} (open_time {data['open_time'][first]}). "
            "Gaps or duplicates must be fixed in the loader."
        )

    o, h, lo, c = pl.col("open"), pl.col("high"), pl.col("low"), pl.col("close")
    invalid = data.filter(
        (lo <= 0) | (h < pl.max_horizontal(o, c)) | (lo > pl.min_horizontal(o, c))
    )
    if invalid.height > 0:
        raise ValueError(
            f"{invalid.height} bar(s) with non-positive prices or high/low outside open/close; "
            f"first at open_time {invalid['open_time'][0]}"
        )


class BacktestEngine:
    def __init__(
        self,
        data: pl.DataFrame,
        strategy: Strategy,
        costs: CostConfig,
        risk: RiskConfig,
        *,
        timeframe: str,
        initial_equity: float = 10_000.0,
    ) -> None:
        bar_interval = parse_timeframe(timeframe)
        validate_ohlcv(data, bar_interval)
        self.data = data
        self.timeframe = timeframe
        self.strategy = strategy
        self.costs = costs
        self.risk = risk
        self.initial_equity = initial_equity

        self._times: list[datetime] = data["open_time"].to_list()
        self._open: list[float] = data["open"].cast(pl.Float64).to_list()
        self._high: list[float] = data["high"].cast(pl.Float64).to_list()
        self._low: list[float] = data["low"].cast(pl.Float64).to_list()
        self._close: list[float] = data["close"].cast(pl.Float64).to_list()
        # Row t's funding_rate is the hourly rate in force during bar t (validated: no nulls).
        self._funding: list[float] | None = (
            data["funding_rate"].cast(pl.Float64).to_list()
            if "funding_rate" in data.columns
            else None
        )
        self.bar_interval = bar_interval
        self.bar_hours = bar_interval / timedelta(hours=1)

        exit_rule = strategy.exit_rule
        # Computed once (O(n)) instead of from scratch at every fill (O(n^2)). Causal: only
        # ever read at index signal_bar, and value t depends on bars 0..t only.
        self._atr: list[float | None] | None = (
            atr(data, exit_rule.atr_period).to_list() if exit_rule.uses_atr else None
        )
        # The strategy is first called once this many bars exist (history.height >= warmup).
        self.warmup_bars = max(1, strategy.warmup_bars, exit_rule.warmup_bars)

    # ------------------------------------------------------------------ main loop
    def run(self) -> BacktestResult:
        equity = self.initial_equity
        position: _Position | None = None
        pending: tuple[Direction, int] | None = None  # (direction, signal_bar)
        rows: list[dict[str, object]] = []
        equity_curve: list[float] = []
        skips: Counter[str] = Counter()
        rails = _Rails(self.risk)
        n_calls = 0
        n_signals = 0
        n_long_signals = 0
        n = self.data.height

        for t in range(n):
            # 1. fill a pending entry at this bar's open
            if pending is not None:
                direction, signal_bar = pending
                pending = None
                opened = self._open_position(direction, signal_bar, t, equity, rails)
                if isinstance(opened, SkipReason):
                    skips[opened.value] += 1
                else:
                    position = opened
                    rails.record_entry(self._times[t])

            # 2. manage the open position on this bar
            if position is not None:
                exit_info = self._check_exit(position, t)
                if exit_info is not None:
                    ref_price, reason, bar_fraction = exit_info
                    self._accrue_funding(position, t, bar_fraction)
                    exit_time = self._times[t] + self.bar_interval * bar_fraction
                    row, net_pnl = self._close_position(
                        position,
                        t,
                        exit_time=exit_time,
                        ref_price=ref_price,
                        reason=reason,
                        equity=equity,
                    )
                    equity += net_pnl
                    rows.append(row)
                    rails.record_exit(exit_time, t, net_pnl / position.size.risk_usd, reason)
                    position = None
                else:
                    self._accrue_funding(position, t, 1.0)

            # 3. mark-to-market equity at this bar's close
            equity_curve.append(equity + self._unrealized(position, t))

            # 4. ask the strategy (only when flat, past warm-up, and a next bar exists)
            if position is None and t + 1 >= self.warmup_bars and t < n - 1:
                n_calls += 1
                signal = self.strategy.on_bar(self.data.slice(0, t + 1))
                if signal is not None:
                    n_signals += 1
                    n_long_signals += signal is Direction.LONG
                    pending = (signal, t)

        # Still open after the last bar: its funding was already accrued in full above.
        if position is not None:
            last = n - 1
            row, net_pnl = self._close_position(
                position,
                last,
                exit_time=self._times[last] + self.bar_interval,  # the last bar's close
                ref_price=self._close[last],
                reason=ExitReason.END_OF_DATA,
                equity=equity,
            )
            equity += net_pnl
            rows.append(row)
            equity_curve[-1] = equity  # exit costs are now realized

        return BacktestResult(
            trades=trades_frame(rows),
            equity_curve=pl.DataFrame(
                # Keyed by CLOSE time: the value is only known once the bar has closed.
                {"close_time": self.data["open_time"] + self.bar_interval, "equity": equity_curve}
            ),
            skips=dict(skips),
            n_bars=n,
            warmup_bars=self.warmup_bars,
            n_strategy_calls=n_calls,
            n_signals=n_signals,
            n_long_signals=n_long_signals,
            initial_equity=self.initial_equity,
            final_equity=equity,
            data_start=self._times[0],
            data_end=self._times[-1],
            bar_hours=self.bar_hours,
        )

    # ------------------------------------------------------------------ helpers
    def _unrealized(self, pos: _Position | None, t: int) -> float:
        """Open P&L at bar t's close, net of the entry fee and funding so far.

        Exit fee/slippage are not estimated here; they are realized when the trade closes.
        """
        if pos is None:
            return 0.0
        price_pnl = pos.direction.value * pos.qty * (self._close[t] - pos.entry_price)
        return price_pnl - pos.entry_fee - pos.funding

    def _accrue_funding(self, pos: _Position, t: int, bar_fraction: float) -> None:
        """Charge funding for `bar_fraction` of bar t (1 = held the whole bar).

        Hyperliquid settles funding every hour; on bars longer than 1h we charge
        bar_hours x the row's hourly rate, which is an approximation.
        """
        if bar_fraction <= 0:
            return
        rate = self.costs.default_funding_rate_hourly if self._funding is None else self._funding[t]
        pos.funding += funding_cost(
            pos.qty * self._close[t], pos.direction, rate, self.bar_hours * bar_fraction
        )

    def _open_position(
        self, direction: Direction, signal_bar: int, t: int, equity: float, rails: _Rails
    ) -> _Position | SkipReason:
        now = self._times[t]
        blocked = rails.blocked(now, t)
        if blocked is not None:
            return blocked

        entry_ref = self._open[t]
        entry_price = slipped_price(
            entry_ref, direction, is_entry=True, slippage=self.costs.slippage_frac
        )
        # Stop distance uses only bars up to the signal bar (ATR as of the signal close).
        exit_rule = self.strategy.exit_rule
        atr_value = self._atr[signal_bar] if self._atr is not None else None
        dist = exit_rule.stop_distance_from_atr(atr_value, entry_price)
        if dist is None or dist <= 0:
            return SkipReason.NO_STOP
        stop, target = exit_rule.levels(direction, entry_price, dist)

        sized = size_position(equity, entry_price, stop, self.risk, self.costs)
        if isinstance(sized, SkipReason):
            return sized
        qty = sized.notional / entry_price
        return _Position(
            direction=direction,
            signal_bar=signal_bar,
            entry_bar=t,
            entry_time=now,
            entry_ref=entry_ref,
            entry_price=entry_price,
            stop=stop,
            target=target,
            qty=qty,
            size=sized,
            entry_fee=fee(qty * entry_price, self.costs.taker_fee),
        )

    def _check_exit(self, pos: _Position, t: int) -> tuple[float, ExitReason, float] | None:
        """Return (reference exit price, reason, fraction of bar t held) if exiting on bar t.

        Order of checks is deliberately pessimistic:
          gap through stop at the open -> stop at the open (worse than the stop price)
          bar touches stop (even if it also touched target) -> stop
          then target, then time stop at the close.
        Fraction held (for funding): 0 at the open, 1/2 intrabar (exact time unknown),
        1 at the close.
        """
        o, h, lo = self._open[t], self._high[t], self._low[t]
        long = pos.direction is Direction.LONG
        stop_gapped = o <= pos.stop if long else o >= pos.stop
        if stop_gapped:
            return o, ExitReason.STOP, 0.0
        if pos.target is not None:
            target_gapped = o >= pos.target if long else o <= pos.target
            if target_gapped:
                return o, ExitReason.TARGET, 0.0
        stop_touched = lo <= pos.stop if long else h >= pos.stop
        if stop_touched:
            return pos.stop, ExitReason.STOP, 0.5
        if pos.target is not None:
            target_touched = h >= pos.target if long else lo <= pos.target
            if target_touched:
                return pos.target, ExitReason.TARGET, 0.5
        max_bars = self.strategy.exit_rule.max_bars_held
        if max_bars is not None and t - pos.entry_bar + 1 >= max_bars:
            return self._close[t], ExitReason.TIME, 1.0
        return None

    def _close_position(
        self,
        pos: _Position,
        t: int,
        *,
        exit_time: datetime,
        ref_price: float,
        reason: ExitReason,
        equity: float,
    ) -> tuple[dict[str, object], float]:
        """Build the trade row. Returns (row, net_pnl).

        `exit_time` is when the exit fill happens: bar t's open for a gap exit, mid-bar for
        an intrabar stop/target (exact time unknown), bar t's close for time stops and
        end of data. Rails book the trade's R on this time.
        """
        maker_exit = reason is ExitReason.TARGET and self.costs.target_is_maker
        slippage = 0.0 if maker_exit else self.costs.slippage_frac
        exit_rate = self.costs.maker_fee if maker_exit else self.costs.taker_fee
        exit_price = slipped_price(ref_price, pos.direction, is_entry=False, slippage=slippage)

        sign = pos.direction.value
        gross_pnl = sign * pos.qty * (ref_price - pos.entry_ref)  # before any cost
        after_slippage = sign * pos.qty * (exit_price - pos.entry_price)
        slippage_cost = gross_pnl - after_slippage
        fees = pos.entry_fee + fee(pos.qty * exit_price, exit_rate)
        net_pnl = after_slippage - fees - pos.funding
        row: dict[str, object] = {
            "signal_bar": pos.signal_bar,
            "entry_bar": pos.entry_bar,
            "exit_bar": t,
            "entry_time": pos.entry_time,
            "exit_time": exit_time,
            "direction": pos.direction.name.lower(),
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "stop_price": pos.stop,
            "target_price": pos.target,
            "qty": pos.qty,
            "notional": pos.size.notional,
            "leverage": pos.size.leverage,
            "leverage_capped": pos.size.leverage_capped,
            "risk_usd": pos.size.risk_usd,
            "gross_pnl": gross_pnl,
            "fees": fees,
            "slippage": slippage_cost,
            "funding": pos.funding,
            "net_pnl": net_pnl,
            "r_multiple": net_pnl / pos.size.risk_usd,
            "exit_reason": reason.value,
            "equity_after": equity + net_pnl,
        }
        return row, net_pnl
