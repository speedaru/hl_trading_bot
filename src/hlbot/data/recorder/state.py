"""What has been durably written, persisted across restarts in `_meta/state.json`.

Updated only after a flush succeeds, so it never claims coverage that a crash could still
lose. On startup it tells the recorder where each stream stopped (-> gap rows for the
downtime) and which trades were already written (-> no duplicates from the reconnect
snapshot, and a precise "were trades lost?" answer across a restart).
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from hlbot.data.recorder import schemas
from hlbot.data.recorder.parse import Row, gap_row
from hlbot.data.recorder.storage import atomic_write_bytes

log = logging.getLogger(__name__)

# Enough to overlap the 30-trade snapshot the server sends on (re)subscribe.
RECENT_TRADE_KEYS = 200


@dataclass
class RecorderState:
    # coin -> stream -> latest partition time (ms) durably written
    last_row_ms: dict[str, dict[str, int]] = field(default_factory=lambda: {})
    # coin -> most recent written trade keys (exch_ms, tid), oldest first
    recent_trade_keys: dict[str, list[tuple[int, int]]] = field(default_factory=lambda: {})
    clean_shutdown: bool = False

    def record_flush(self, coin: str, stream: str, rows: list[Row]) -> None:
        if not rows:
            return
        col = schemas.partition_column(stream)
        latest = max(_as_int(r[col]) for r in rows)
        per_coin = self.last_row_ms.setdefault(coin, {})
        per_coin[stream] = max(latest, per_coin.get(stream, latest))
        if stream == schemas.TRADES:
            keys = self.recent_trade_keys.setdefault(coin, [])
            keys.extend((_as_int(r["exch_time"]), _as_int(r["tid"])) for r in rows)
            keys.sort()
            del keys[:-RECENT_TRADE_KEYS]


def _as_int(v: object) -> int:
    assert isinstance(v, int), f"expected epoch-ms int, got {v!r}"
    return v


def save_state(path: Path, state: RecorderState) -> None:
    payload = {
        "last_row_ms": state.last_row_ms,
        "recent_trade_keys": state.recent_trade_keys,
        "clean_shutdown": state.clean_shutdown,
    }
    atomic_write_bytes(path, json.dumps(payload, indent=1).encode())


def load_state(path: Path) -> RecorderState:
    """Missing file = first run. A corrupt file is logged and treated as a first run too:
    the recorder must start regardless, and the gap is then visible as missing data."""
    if not path.exists():
        return RecorderState()
    try:
        raw = json.loads(path.read_text())
        return RecorderState(
            last_row_ms={
                c: {s: int(v) for s, v in d.items()} for c, d in raw["last_row_ms"].items()
            },
            recent_trade_keys={
                c: [(int(a), int(b)) for a, b in keys]
                for c, keys in raw["recent_trade_keys"].items()
            },
            clean_shutdown=bool(raw["clean_shutdown"]),
        )
    except (ValueError, KeyError, TypeError):
        log.exception("unreadable state file %s; starting without restart gap info", path)
        return RecorderState()


def downtime_gaps(
    state: RecorderState, coins: tuple[str, ...], sampled_streams: list[str], start_ms: int
) -> dict[str, list[Row]]:
    """Gap rows (per coin) covering the time between the last written sample and this start.

    Only for sampled streams; trade coverage is decided precisely by the trade tracker once
    the reconnect snapshot arrives.
    """
    reason = "restart_clean" if state.clean_shutdown else "process_down"
    out: dict[str, list[Row]] = {}
    for coin in coins:
        for stream in sampled_streams:
            last = state.last_row_ms.get(coin, {}).get(stream)
            if last is not None and last < start_ms:
                out.setdefault(coin, []).append(gap_row(stream, last, start_ms, reason))
    return out
