"""Backoff schedule, and WsConnection against a local fake server (no network)."""

import asyncio
import contextlib
from typing import Any

from hlbot.data.recorder.heartbeat import (
    HeartbeatInput,
    SkewTracker,
    StreamStat,
    format_heartbeat,
    heartbeat_json,
)
from hlbot.data.recorder.ws import Backoff, WsConnection
from tests.recorder_fake_exchange import FakeExchange


def test_backoff_doubles_to_cap_with_jitter_and_resets() -> None:
    b = Backoff(1, 8, rng=lambda: 1.0)  # rng=1 -> upper end of the jitter range
    assert [b.next_delay() for _ in range(5)] == [1, 2, 4, 8, 8]
    b.reset()
    assert b.next_delay() == 1
    low = Backoff(1, 8, rng=lambda: 0.0)
    assert [low.next_delay() for _ in range(3)] == [0.5, 1, 2]  # never below half


class Recording:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.messages: list[dict[str, Any]] = []

    def on_open(self, conn: str) -> None:
        self.events.append("open")

    def on_message(self, conn: str, msg: Any, recv_ms: int) -> None:
        self.messages.append(msg)

    def on_close(self, conn: str, last_msg_ms: int) -> None:
        self.events.append("close")


async def _run_for(conn: WsConnection, seconds: float) -> None:
    task = asyncio.create_task(conn.run())
    await asyncio.sleep(seconds)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _conn(url: str, h: Recording, stale_s: float = 2.0) -> WsConnection:
    subs: list[dict[str, Any]] = [{"type": "activeAssetCtx", "coin": "SOL"}]
    return WsConnection(
        "t", url, subs, h, ping_s=0.2, stale_s=stale_s, backoff=Backoff(0.1, 0.2), stable_reset_s=60
    )


def test_reconnects_after_server_drop_and_resubscribes() -> None:
    async def scenario() -> tuple[FakeExchange, Recording, WsConnection]:
        h = Recording()
        async with FakeExchange(("SOL",)) as ex:
            conn = _conn(ex.url, h)
            task = asyncio.create_task(_run_for(conn, 1.5))
            await asyncio.sleep(0.5)
            await ex.drop_all()
            await task
        return ex, h, conn

    ex, h, conn = asyncio.run(scenario())
    assert ex.connections >= 2
    assert conn.stats.reconnects >= 1
    assert h.events[:3] == ["open", "close", "open"]
    assert sum(1 for s in ex.subscriptions if s["type"] == "activeAssetCtx") >= 2
    assert ex.pings >= 1  # app-level pings were sent
    assert any(m.get("channel") == "activeAssetCtx" for m in h.messages)


def test_silent_connection_is_detected_as_stale() -> None:
    async def scenario() -> tuple[FakeExchange, WsConnection]:
        h = Recording()
        async with FakeExchange(("SOL",)) as ex:
            ex.silent = True  # socket stays open, nothing ever arrives
            conn = _conn(ex.url, h, stale_s=0.4)
            await _run_for(conn, 1.5)
        return ex, conn

    ex, conn = asyncio.run(scenario())
    assert ex.connections >= 2
    assert "StaleConnectionError" in conn.stats.last_error


def test_skew_median() -> None:
    s = SkewTracker(window=3)
    assert s.median_ms() is None
    for recv, exch in [(1000, 900), (2000, 1700), (3000, 2800), (4000, 3900)]:
        s.add(recv, exch)
    assert s.median_ms() == 200  # window keeps the last 3: 300, 200, 100


def test_heartbeat_line_and_json() -> None:
    hb = HeartbeatInput(
        now_ms=3_600_000 + 125_000,
        start_ms=0,
        connections={"main": (True, 2), "book": (False, 0)},
        skew_ms=310,
        gaps=1,
        late_trades=0,
        rest_errors=3,
        write_errors=0,
        buffered_rows=42,
        streams={"BTC": {"ctx": StreamStat(1, 10, 3_724_600), "gaps": StreamStat(0, 0, None)}},
    )
    line = format_heartbeat(hb)
    assert line.startswith("HB up=1h02m main=up/r2 book=DOWN/r0 skew=+310ms gaps=1")
    assert "rest_err=3" in line and "buf=42" in line
    assert "| BTC ctx=1/0.4s gaps=0" in line
    js = heartbeat_json(hb)
    assert js["connections"] == {
        "main": {"up": True, "reconnects": 2},
        "book": {"up": False, "reconnects": 0},
    }
