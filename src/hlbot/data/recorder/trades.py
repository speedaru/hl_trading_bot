"""Per-coin trade tracking: de-duplication, 1-minute buckets, and "were trades lost?".

Trades are events, not state: a missed trade is gone for good (a missed book snapshot is
superseded by the next one). So coverage is tracked exactly:

- On every (re)subscribe the server sends a snapshot of its last 30 trades. If it overlaps
  trades already seen, nothing was lost across the outage. If not, trades between the outage
  start and the oldest snapshot trade may be missing -> a `trades_lost` gap row.
- Buckets are keyed by exchange trade time. A minute touching a window where trades may be
  missing is emitted with complete=False; a minute entirely inside one is not emitted at all.
- While an outage is unresolved (disconnected, or waiting for the snapshot), minutes that
  might be affected are held back instead of being closed with partial data.
"""

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

from hlbot.data.recorder import schemas
from hlbot.data.recorder.parse import Row, Trade, gap_row

MINUTE_MS = 60_000
_NEG_INF = -(2**62)
# Seen-trade keys kept for de-duplication/overlap checks. The snapshot is 30 trades.
SEEN_KEYS = 2000
# Trades seen by exchange time up to this long before the last received message are assumed
# delivered; an outage is taken to start after that (network latency margin).
DELIVERY_MARGIN_MS = 5_000


def minute_of(ms: int) -> int:
    return ms - ms % MINUTE_MS


@dataclass
class _Bucket:
    n: int = 0
    base: float = 0.0
    notional: float = 0.0
    buy_base: float = 0.0
    buy_notional: float = 0.0
    open: float = 0.0
    high: float = float("-inf")
    low: float = float("inf")
    close: float = 0.0
    first: tuple[int, int] = (2**62, 0)  # (exch_ms, arrival seq) of the open trade
    last: tuple[int, int] = (_NEG_INF, 0)

    def add(self, t: Trade, seq: int) -> None:
        self.n += 1
        self.base += t.sz
        self.notional += t.sz * t.px
        if t.side == "B":
            self.buy_base += t.sz
            self.buy_notional += t.sz * t.px
        self.high = max(self.high, t.px)
        self.low = min(self.low, t.px)
        if (t.exch_ms, seq) < self.first:
            self.first, self.open = (t.exch_ms, seq), t.px
        if (t.exch_ms, seq) > self.last:
            self.last, self.close = (t.exch_ms, seq), t.px


def _bucket_row(minute: int, b: _Bucket | None, complete: bool) -> Row:
    if b is None or b.n == 0:
        return {
            "minute": minute,
            "n_trades": 0,
            **dict.fromkeys(("base_vol", "notional_vol", "buy_base_vol", "sell_base_vol"), 0.0),
            **dict.fromkeys(("buy_notional", "sell_notional"), 0.0),
            **dict.fromkeys(("vwap", "open", "high", "low", "close")),
            "complete": complete,
        }
    return {
        "minute": minute,
        "n_trades": b.n,
        "base_vol": b.base,
        "notional_vol": b.notional,
        "buy_base_vol": b.buy_base,
        "sell_base_vol": b.base - b.buy_base,
        "buy_notional": b.buy_notional,
        "sell_notional": b.notional - b.buy_notional,
        "vwap": b.notional / b.base if b.base > 0 else None,
        "open": b.open,
        "high": b.high,
        "low": b.low,
        "close": b.close,
        "complete": complete,
    }


class TradeTracker:
    def __init__(
        self,
        coin: str,
        *,
        start_ms: int,
        grace_ms: int,
        seed_keys: Iterable[tuple[int, int]] = (),
        last_trade_ms: int | None = None,
    ) -> None:
        """`seed_keys`/`last_trade_ms` come from state.json: trades written before a restart."""
        self.coin = coin
        self.grace_ms = grace_ms
        self._seen_order: deque[tuple[int, int]] = deque()
        self._seen: set[tuple[int, int]] = set()
        for k in seed_keys:
            self._remember(k)
        self._last_trade_ms = last_trade_ms
        self._buckets: dict[int, _Bucket] = {}
        self._seq = 0
        # Bucket memory from before this process started is gone: minutes up to start are
        # never complete, whatever the raw-trade coverage.
        self._lost: list[tuple[int, int]] = [(_NEG_INF, start_ms)]
        self._next_minute = minute_of(start_ms)
        self._start_ms = start_ms
        self._awaiting_snapshot = True
        self._outage_from: int | None = last_trade_ms  # None: first run, nothing to compare
        self.late_trades = 0

    # ---- connection events -----------------------------------------------------------------

    def on_disconnect(self, last_msg_recv_ms: int) -> None:
        """The connection carrying this coin's trades dropped (or went stale)."""
        if self._awaiting_snapshot:
            return  # outage already open
        candidate = last_msg_recv_ms - DELIVERY_MARGIN_MS
        self._outage_from = max(self._last_trade_ms or candidate, candidate)
        self._awaiting_snapshot = True

    def on_trades(self, trades: list[Trade], now_ms: int) -> tuple[list[Trade], list[Row]]:
        """Feed one trades message. Returns (new trades to store raw, gap rows)."""
        gaps: list[Row] = []
        is_snapshot = self._awaiting_snapshot
        if is_snapshot:
            gaps = self._resolve(trades, now_ms)
        new: list[Trade] = []
        for t in trades:
            if t.key in self._seen:
                continue
            self._remember(t.key)
            new.append(t)
            self._last_trade_ms = max(self._last_trade_ms or t.exch_ms, t.exch_ms)
            m = minute_of(t.exch_ms)
            if m < self._next_minute:
                if not is_snapshot:
                    self.late_trades += 1  # its minute is already closed; raw row still kept
                continue
            self._seq += 1
            self._buckets.setdefault(m, _Bucket()).add(t, self._seq)
        return new, gaps

    def resolve_without_snapshot(self, now_ms: int) -> list[Row]:
        """No trades message arrived after reconnecting: coverage can't be proven."""
        if not self._awaiting_snapshot:
            return []
        start = self._outage_from if self._outage_from is not None else self._start_ms
        self._add_lost(start, now_ms)
        self._awaiting_snapshot = False
        self._outage_from = None
        return [gap_row(schemas.TRADES, start, now_ms, "trades_unknown", "no snapshot received")]

    def _resolve(self, snapshot: list[Trade], now_ms: int) -> list[Row]:
        self._awaiting_snapshot = False
        outage_from, self._outage_from = self._outage_from, None
        overlap = any(t.key in self._seen for t in snapshot)
        oldest = min((t.exch_ms for t in snapshot), default=None)
        if outage_from is None:
            # First run: nothing before this snapshot is known. Coverage starts at its oldest
            # trade (other trades in that same millisecond may have been cut off).
            self._add_lost(_NEG_INF, oldest if oldest is not None else now_ms)
            return []
        # The snapshot is the server's last N trades without holes, so it covers
        # [oldest, now]. Nothing was lost if it reaches back past the outage start, or if it
        # contains a trade already seen (then it joins up with what we have).
        if overlap or (oldest is not None and oldest < outage_from):
            return []
        if oldest is None:
            self._add_lost(outage_from, now_ms)
            return [
                gap_row(schemas.TRADES, outage_from, now_ms, "trades_unknown", "empty snapshot")
            ]
        self._add_lost(outage_from, oldest)
        detail = f"{len(snapshot)}-trade snapshot did not overlap trades already seen"
        return [gap_row(schemas.TRADES, outage_from, oldest, "trades_lost", detail)]

    # ---- buckets ----------------------------------------------------------------------------

    def finalize(self, now_ms: int) -> list[Row]:
        """Close every minute that is over (plus grace) and not held by an open outage."""
        rows: list[Row] = []
        while self._next_minute + MINUTE_MS + self.grace_ms <= now_ms:
            m = self._next_minute
            end = m + MINUTE_MS - 1
            if self._awaiting_snapshot:
                hold_from = self._outage_from if self._outage_from is not None else self._start_ms
                if end >= hold_from:
                    break
            bucket = self._buckets.pop(m, None)
            inside = any(a <= m and end <= b for a, b in self._lost)
            if not inside:
                touches = any(m <= b and end >= a for a, b in self._lost)
                rows.append(_bucket_row(m, bucket, complete=not touches))
            self._next_minute += MINUTE_MS
        self._lost = [(a, b) for a, b in self._lost if b >= self._next_minute]
        return rows

    # ---- helpers ------------------------------------------------------------------------------

    def _add_lost(self, a: int, b: int) -> None:
        self._lost.append((a, b))

    def _remember(self, key: tuple[int, int]) -> None:
        if key in self._seen:
            return
        self._seen.add(key)
        self._seen_order.append(key)
        if len(self._seen_order) > SEEN_KEYS:
            self._seen.discard(self._seen_order.popleft())

    @property
    def awaiting_snapshot(self) -> bool:
        return self._awaiting_snapshot

    @property
    def open_buckets(self) -> int:
        return len(self._buckets)
