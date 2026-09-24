"""Heartbeat: one line per minute that shows at a glance whether the recorder is healthy.

Example:
  HB up=3h12m main=up/r2 book=up/r0 skew=+310ms gaps=1 late=0 rest_err=0 write_err=0 buf=412
     | BTC ctx=1/0.4s trades=150/0.2s book=2/3.1s ... | ETH ...
Per stream: rows written since the last heartbeat / age of the newest websocket message.
"""

import statistics
from collections import deque
from dataclasses import dataclass, field


class SkewTracker:
    """recv_time - exch_time over recent book messages.

    Always includes network latency, so a healthy value is a few hundred ms positive. A
    negative median means the local clock is behind the exchange; seconds of drift mean
    NTP isn't working.
    """

    def __init__(self, window: int = 200) -> None:
        self._d: deque[int] = deque(maxlen=window)

    def add(self, recv_ms: int, exch_ms: int) -> None:
        self._d.append(recv_ms - exch_ms)

    def median_ms(self) -> int | None:
        return int(statistics.median(self._d)) if self._d else None


@dataclass
class StreamStat:
    rows_since_hb: int = 0
    rows_total: int = 0
    last_msg_ms: int | None = None  # websocket-fed streams only


@dataclass
class HeartbeatInput:
    now_ms: int
    start_ms: int
    connections: dict[str, tuple[bool, int]]  # name -> (up, reconnects)
    skew_ms: int | None
    gaps: int
    late_trades: int
    rest_errors: int
    write_errors: int
    buffered_rows: int
    streams: dict[str, dict[str, StreamStat]] = field(default_factory=lambda: {})  # coin->...


def _duration(ms: int) -> str:
    s = ms // 1000
    d, rem = divmod(s, 86_400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    return f"{d}d{h}h{m:02d}m" if d else f"{h}h{m:02d}m"


def format_heartbeat(hb: HeartbeatInput) -> str:
    conns = " ".join(f"{n}={'up' if up else 'DOWN'}/r{r}" for n, (up, r) in hb.connections.items())
    skew = "n/a" if hb.skew_ms is None else f"{hb.skew_ms:+d}ms"
    head = (
        f"HB up={_duration(hb.now_ms - hb.start_ms)} {conns} skew={skew} gaps={hb.gaps} "
        f"late={hb.late_trades} rest_err={hb.rest_errors} write_err={hb.write_errors} "
        f"buf={hb.buffered_rows}"
    )
    parts = [head]
    for coin, streams in hb.streams.items():
        items: list[str] = []
        for name, st in streams.items():
            age = "" if st.last_msg_ms is None else f"/{(hb.now_ms - st.last_msg_ms) / 1000:.1f}s"
            items.append(f"{name}={st.rows_since_hb}{age}")
        parts.append(f"{coin} " + " ".join(items))
    return " | ".join(parts)


def heartbeat_json(hb: HeartbeatInput) -> dict[str, object]:
    """Same content, machine-readable, for an external watchdog (`_meta/heartbeat.json`)."""
    return {
        "now_ms": hb.now_ms,
        "uptime_s": (hb.now_ms - hb.start_ms) // 1000,
        "connections": {n: {"up": up, "reconnects": r} for n, (up, r) in hb.connections.items()},
        "skew_ms": hb.skew_ms,
        "gaps": hb.gaps,
        "late_trades": hb.late_trades,
        "rest_errors": hb.rest_errors,
        "write_errors": hb.write_errors,
        "buffered_rows": hb.buffered_rows,
        "streams": {
            coin: {
                name: {
                    "rows_since_hb": st.rows_since_hb,
                    "rows_total": st.rows_total,
                    "msg_age_s": None
                    if st.last_msg_ms is None
                    else (hb.now_ms - st.last_msg_ms) / 1000,
                }
                for name, st in streams.items()
            }
            for coin, streams in hb.streams.items()
        },
    }
