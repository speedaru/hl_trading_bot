"""A local stand-in for the Hyperliquid websocket + REST info API, for recorder tests.

It runs a tiny "market" that prints a trade per coin every `trade_every_s`, whether or not
a client is connected, so disconnects really lose trades unless the reconnect snapshot
covers them. Like the real server, subscribing to trades sends a snapshot of the last
`snapshot_size` trades first.
"""

import asyncio
import contextlib
import copy
import json
import time
from pathlib import Path
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve

FIX = Path(__file__).parent / "fixtures" / "recorder"


def _fixture(name: str) -> Any:
    return json.loads((FIX / name).read_text())


def now_ms() -> int:
    return int(time.time() * 1000)


class FakeExchange:
    def __init__(
        self,
        coins: tuple[str, ...],
        *,
        snapshot_size: int = 30,
        trade_every_s: float = 0.05,
        push_every_s: float = 0.1,
    ) -> None:
        self.coins = coins
        self.snapshot_size = snapshot_size
        self.trade_every_s = trade_every_s
        self.push_every_s = push_every_s
        self.history: dict[str, list[dict[str, Any]]] = {c: [] for c in coins}
        self.subscriptions: list[dict[str, Any]] = []
        self.connections = 0
        self.pings = 0
        self.silent = False  # accept connections but never send anything
        self._live: set[ServerConnection] = set()
        self._tid = 1000
        self._server: Server | None = None
        self._market: asyncio.Task[None] | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        port = next(iter(self._server.sockets)).getsockname()[1]
        return f"ws://127.0.0.1:{port}"

    async def __aenter__(self) -> "FakeExchange":
        self._server = await serve(self._handle, "127.0.0.1", 0)
        self._market = asyncio.create_task(self._run_market())
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None and self._market is not None
        self._market.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._market
        self._server.close()
        await self._server.wait_closed()

    async def drop_all(self) -> None:
        """Server-side disconnect of every client, like an unannounced API restart."""
        for ws in list(self._live):
            await ws.close()

    # ---- market -------------------------------------------------------------------------

    async def _run_market(self) -> None:
        while True:
            await asyncio.sleep(self.trade_every_s)
            for coin in self.coins:
                self._tid += 7
                self.history[coin].append(
                    {
                        "coin": coin,
                        "side": "B" if self._tid % 2 else "A",
                        "px": "100.5",
                        "sz": "0.25",
                        "time": now_ms(),
                        "hash": "0xfeed",
                        "tid": self._tid,
                        "users": ["0xbuyer", "0xseller"],
                    }
                )

    # ---- websocket ------------------------------------------------------------------------

    async def _handle(self, ws: ServerConnection) -> None:
        self.connections += 1
        self._live.add(ws)
        feeds: list[asyncio.Task[None]] = []
        try:
            async for raw in ws:
                msg = json.loads(raw)
                if msg["method"] == "ping":
                    self.pings += 1
                    if not self.silent:
                        await ws.send(json.dumps({"channel": "pong"}))
                    continue
                sub = msg["subscription"]
                self.subscriptions.append(sub)
                if self.silent:
                    continue
                await ws.send(json.dumps({"channel": "subscriptionResponse", "data": msg}))
                feeds.append(asyncio.create_task(self._feed(ws, sub)))
        except Exception:
            pass
        finally:
            self._live.discard(ws)
            for f in feeds:
                f.cancel()

    async def _feed(self, ws: ServerConnection, sub: dict[str, Any]) -> None:
        coin = sub["coin"]
        if sub["type"] == "trades":
            sent = len(self.history[coin])
            snapshot = self.history[coin][max(0, sent - self.snapshot_size) :]
            await ws.send(json.dumps({"channel": "trades", "data": snapshot}))
            while True:
                await asyncio.sleep(self.push_every_s)
                new = self.history[coin][sent:]
                sent += len(new)
                if new:
                    await ws.send(json.dumps({"channel": "trades", "data": new}))
        while True:
            await ws.send(json.dumps(self._state_msg(sub)))
            await asyncio.sleep(self.push_every_s)

    def _state_msg(self, sub: dict[str, Any]) -> dict[str, Any]:
        coin = sub["coin"]
        if sub["type"] == "activeAssetCtx":
            msg = copy.deepcopy(_fixture("ws_activeAssetCtx.json"))
            msg["data"]["coin"] = coin
            return msg
        name = "ws_l2Book_coarse.json" if sub.get("nSigFigs") else "ws_l2Book.json"
        msg = copy.deepcopy(_fixture(name))
        msg["data"]["coin"] = coin
        msg["data"]["time"] = now_ms() - 50
        return msg


class FakeRest:
    """InfoClient returning captured fixtures; can be switched to fail."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail = False

    async def info(self, payload: dict[str, Any]) -> Any:
        self.calls.append(payload["type"])
        if self.fail:
            raise ConnectionError("fake REST down")
        if payload["type"] == "predictedFundings":
            resp = _fixture("rest_predictedFundings.json")
            return [[c, venues] for _, venues in resp[:1] for c in ("BTC", "SOL")]
        if payload["type"] == "metaAndAssetCtxs":
            return _fixture("rest_metaAndAssetCtxs.json")
        if payload["type"] == "l2Book":
            book = _fixture("rest_l2Book.json")
            book["coin"] = payload["coin"]
            return book
        raise ValueError(payload)
