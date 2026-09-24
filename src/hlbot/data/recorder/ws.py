"""One websocket connection that reconnects forever, with backoff and stale detection.

Not the SDK's WebsocketManager: that runs `run_forever()` once in a thread and simply ends
when the server drops the connection, with no reconnect and no way to notice.
"""

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

log = logging.getLogger(__name__)


class StaleConnectionError(Exception):
    """No message of any kind within the stale timeout: the socket is open but dead."""


class Backoff:
    """Exponential backoff with jitter: each delay is uniform in [d/2, d], d doubling to a cap.

    Jitter keeps a fleet of clients (or several connections here) from reconnecting in
    lockstep after a server-side drop.
    """

    def __init__(
        self, initial_s: float, max_s: float, rng: Callable[[], float] = random.random
    ) -> None:
        self.initial_s = initial_s
        self.max_s = max_s
        self._rng = rng
        self._d = initial_s

    def next_delay(self) -> float:
        d = self._d
        self._d = min(self._d * 2, self.max_s)
        return d * (0.5 + 0.5 * self._rng())

    def reset(self) -> None:
        self._d = self.initial_s


class Handler(Protocol):
    def on_open(self, conn: str) -> None: ...
    def on_message(self, conn: str, msg: Any, recv_ms: int) -> None: ...
    def on_close(self, conn: str, last_msg_ms: int) -> None: ...


@dataclass
class ConnStats:
    connects: int = 0
    up: bool = False
    last_msg_ms: int = 0
    last_error: str = ""

    @property
    def reconnects(self) -> int:
        return max(self.connects - 1, 0)


def now_ms() -> int:
    return int(time.time() * 1000)


class WsConnection:
    def __init__(
        self,
        name: str,
        url: str,
        subscriptions: list[dict[str, Any]],
        handler: Handler,
        *,
        ping_s: float,
        stale_s: float,
        backoff: Backoff,
        stable_reset_s: float,
    ) -> None:
        self.name = name
        self.url = url
        self.subscriptions = subscriptions
        self.handler = handler
        self.ping_s = ping_s
        self.stale_s = stale_s
        self.backoff = backoff
        self.stable_reset_s = stable_reset_s
        self.stats = ConnStats()

    async def run(self) -> None:
        """Connect, subscribe, read; on any failure wait (backoff) and do it again. Runs until
        cancelled."""
        while True:
            opened_at: float | None = None
            try:
                async with connect(
                    self.url, open_timeout=10, close_timeout=5, max_size=2**23
                ) as ws:
                    opened_at = time.monotonic()
                    self.stats.connects += 1
                    self.stats.up = True
                    self.stats.last_msg_ms = now_ms()
                    log.info(
                        "[%s] connected (%d subscriptions)", self.name, len(self.subscriptions)
                    )
                    for sub in self.subscriptions:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
                    self.handler.on_open(self.name)
                    await self._read(ws)
            except (OSError, TimeoutError, WebSocketException, StaleConnectionError) as e:
                self.stats.last_error = f"{type(e).__name__}: {e}"
                log.warning("[%s] connection lost: %s", self.name, self.stats.last_error)
            finally:
                if opened_at is not None:
                    self.stats.up = False
                    self.handler.on_close(self.name, self.stats.last_msg_ms)
            if opened_at is not None and time.monotonic() - opened_at >= self.stable_reset_s:
                self.backoff.reset()
            delay = self.backoff.next_delay()
            log.info("[%s] reconnecting in %.1fs", self.name, delay)
            await asyncio.sleep(delay)

    async def _read(self, ws: ClientConnection) -> None:
        pinger = asyncio.create_task(self._ping(ws))
        try:
            while True:
                try:
                    async with asyncio.timeout(self.stale_s):
                        raw = await ws.recv()
                except TimeoutError:
                    raise StaleConnectionError(f"no message for {self.stale_s}s") from None
                recv = now_ms()
                self.stats.last_msg_ms = recv
                try:
                    msg = json.loads(raw)
                except ValueError:
                    log.debug("[%s] non-JSON message: %.80s", self.name, raw)
                    continue
                if isinstance(msg, dict):
                    self._dispatch(cast(dict[str, Any], msg), recv)
        finally:
            pinger.cancel()

    def _dispatch(self, msg: dict[str, Any], recv_ms: int) -> None:
        channel = msg.get("channel")
        if channel in ("pong", "subscriptionResponse"):
            return
        if channel == "error":
            log.error("[%s] server error: %s", self.name, msg.get("data"))
            return
        try:
            self.handler.on_message(self.name, msg, recv_ms)
        except Exception:
            # One malformed message must not take the connection (and all coins) down.
            log.exception("[%s] failed to handle message: %.200s", self.name, msg)

    async def _ping(self, ws: ClientConnection) -> None:
        # Hyperliquid drops clients that send nothing for 60s; this is its app-level ping.
        while True:
            await asyncio.sleep(self.ping_s)
            await ws.send(json.dumps({"method": "ping"}))
