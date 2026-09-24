"""The recorder: websocket feeds -> latest state -> samplers/trade tracker -> buffers -> Parquet.

Connections: "main" carries activeAssetCtx + trades for every coin; each configured book
stream has its own connection (l2Book messages don't say which nSigFigs they belong to).

Tasks (one asyncio loop): a reader per connection, one sampler per sampled stream, the
trade-bucket finalizer, the flusher, the compactor and the heartbeat.
"""

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from functools import partial
from typing import Any

from hlbot.data.recorder import schemas
from hlbot.data.recorder.config import BookStreamConfig, RecorderConfig
from hlbot.data.recorder.heartbeat import (
    HeartbeatInput,
    SkewTracker,
    StreamStat,
    format_heartbeat,
    heartbeat_json,
)
from hlbot.data.recorder.parse import (
    BookSnapshot,
    CtxSnapshot,
    Row,
    Trade,
    book_row,
    ctx_row,
    funding_pred_row,
    gap_row,
    parse_book,
    parse_meta_and_asset_ctxs,
    parse_predicted_fundings,
    parse_trades,
    parse_ws_ctx,
    trade_row,
)
from hlbot.data.recorder.rest import InfoClient
from hlbot.data.recorder.state import downtime_gaps, load_state, save_state
from hlbot.data.recorder.storage import (
    StreamBuffer,
    atomic_write_bytes,
    compact_ready,
    utc_date,
    write_parts,
)
from hlbot.data.recorder.trades import MINUTE_MS, TradeTracker
from hlbot.data.recorder.ws import Backoff, WsConnection, now_ms

log = logging.getLogger(__name__)

MAIN = "main"
# After (re)connecting, the trades snapshot should arrive within this long.
SNAPSHOT_TIMEOUT_MS = 30_000
# A sampled stream counts as missed when two written samples are further apart than this
# many intervals.
MISSED_SAMPLE_FACTOR = 1.5
COMPACT_EVERY_S = 600
SKEW_WARN_MS = 2_000


class Recorder:
    def __init__(
        self, cfg: RecorderConfig, rest: InfoClient, clock_ms: Callable[[], int] = now_ms
    ) -> None:
        self.cfg = cfg
        self.rest = rest
        self.clock_ms = clock_ms
        self.root = cfg.data_dir
        self.meta_dir = self.root / "_meta"
        self.state_path = self.meta_dir / "state.json"
        self.start_ms = clock_ms()
        self.state = load_state(self.state_path)

        self.books: dict[str, BookStreamConfig] = {b.stream: b for b in cfg.books}
        self.buffers: dict[tuple[str, str], StreamBuffer] = {}
        for coin in cfg.coins:
            for stream in self._streams():
                levels = self.books[stream].levels if stream in self.books else None
                self.buffers[coin, stream] = StreamBuffer(
                    coin,
                    stream,
                    schema=schemas.schema_for(stream, levels),
                    max_rows=cfg.flush_max_rows,
                    max_seconds=cfg.flush_max_seconds,
                )

        self.trackers = {c: self._make_tracker(c) for c in cfg.coins}
        self.latest_ctx: dict[str, CtxSnapshot] = {}
        self.latest_book: dict[tuple[str, str], BookSnapshot] = {}
        self.last_exch_ms: int | None = None  # newest exchange timestamp on any feed
        self.last_sample: dict[tuple[str, str], int] = {}
        self.snapshot_deadline: dict[str, int] = {}
        self.skew = SkewTracker()
        self.stats: dict[str, dict[str, StreamStat]] = {
            c: {s: StreamStat() for s in self._streams() if s != schemas.GAPS} for c in cfg.coins
        }
        self.gap_count = 0
        self.rest_errors = 0
        self.write_errors = 0

        backoff = lambda: Backoff(cfg.backoff_initial_s, cfg.backoff_max_s)  # noqa: E731
        conn_kw: dict[str, Any] = {
            "ping_s": cfg.ws_ping_s,
            "stale_s": cfg.ws_stale_s,
            "stable_reset_s": cfg.stable_reset_s,
        }
        main_subs = [
            {"type": t, "coin": c} for c in cfg.coins for t in ("activeAssetCtx", "trades")
        ]
        self.connections = [
            WsConnection(MAIN, cfg.ws_url, main_subs, self, backoff=backoff(), **conn_kw)
        ]
        for b in cfg.books:
            subs = [self._book_sub(c, b) for c in cfg.coins]
            self.connections.append(
                WsConnection(b.stream, cfg.ws_url, subs, self, backoff=backoff(), **conn_kw)
            )
        self._write_lock = asyncio.Lock()

    # ---- setup helpers ------------------------------------------------------------------------

    def _streams(self) -> list[str]:
        streams = [schemas.CTX, schemas.FUNDING_PRED, schemas.TRADES_1M, schemas.GAPS]
        if self.cfg.store_raw_trades:
            streams.append(schemas.TRADES)
        return streams + [b.stream for b in self.cfg.books]

    def _sampled_streams(self) -> list[str]:
        return [schemas.CTX, schemas.FUNDING_PRED, *self.books]

    def _make_tracker(self, coin: str) -> TradeTracker:
        written = self.state.last_row_ms.get(coin, {})
        last_trade = written.get(schemas.TRADES)
        if last_trade is None and schemas.TRADES_1M in written:
            last_trade = written[schemas.TRADES_1M] + MINUTE_MS  # raw trades not stored
        return TradeTracker(
            coin,
            start_ms=self.start_ms,
            grace_ms=int(self.cfg.bucket_grace_s * 1000),
            seed_keys=self.state.recent_trade_keys.get(coin, []),
            last_trade_ms=last_trade,
        )

    @staticmethod
    def _book_sub(coin: str, b: BookStreamConfig) -> dict[str, Any]:
        sub: dict[str, Any] = {"type": "l2Book", "coin": coin}
        if b.n_sig_figs is not None:
            sub["nSigFigs"] = b.n_sig_figs
        return sub

    # ---- websocket handler (called by WsConnection) -------------------------------------------

    def on_open(self, conn: str) -> None:
        if conn == MAIN:
            deadline = self.clock_ms() + SNAPSHOT_TIMEOUT_MS
            self.snapshot_deadline = dict.fromkeys(self.cfg.coins, deadline)

    def on_close(self, conn: str, last_msg_ms: int) -> None:
        if conn == MAIN:
            self.snapshot_deadline = {}
            for t in self.trackers.values():
                t.on_disconnect(last_msg_ms)

    def on_message(self, conn: str, msg: Any, recv_ms: int) -> None:
        channel = msg.get("channel")
        data = msg.get("data")
        if channel == "activeAssetCtx":
            s = parse_ws_ctx(data, recv_ms)
            if s.coin in self.trackers:
                self.latest_ctx[s.coin] = s
                self.stats[s.coin][schemas.CTX].last_msg_ms = recv_ms
        elif channel == "trades":
            self._on_trades(parse_trades(data, recv_ms), recv_ms)
        elif channel == "l2Book" and conn in self.books:
            b = parse_book(data, self.books[conn].levels, recv_ms)
            if b.coin in self.trackers:
                self.latest_book[conn, b.coin] = b
                self.stats[b.coin][conn].last_msg_ms = recv_ms
                self._saw_exch_time(b.exch_ms)
                self.skew.add(recv_ms, b.exch_ms)

    def _on_trades(self, trades: list[Trade], recv_ms: int) -> None:
        if not trades:
            return  # an empty message can't be attributed to a coin (and carries nothing)
        coin = trades[0].coin
        tracker = self.trackers.get(coin)
        if tracker is None:
            return
        new, gaps = tracker.on_trades(trades, recv_ms)
        self.snapshot_deadline.pop(coin, None)
        self.stats[coin][schemas.TRADES_1M].last_msg_ms = recv_ms
        if self.cfg.store_raw_trades:
            self.buffers[coin, schemas.TRADES].extend([trade_row(t) for t in new])
        for t in new:
            self._saw_exch_time(t.exch_ms)
        self._add_gaps(coin, gaps)

    def _saw_exch_time(self, exch_ms: int) -> None:
        if self.last_exch_ms is None or exch_ms > self.last_exch_ms:
            self.last_exch_ms = exch_ms

    def _add_gaps(self, coin: str, gaps: list[Row]) -> None:
        for g in gaps:
            log.warning("GAP %s %s", coin, g)
        self.gap_count += len(gaps)
        self.buffers[coin, schemas.GAPS].extend(gaps)

    def _add_sample(self, coin: str, stream: str, sample_ms: int, row: Row) -> None:
        """Buffer a sampled row; write a gap row if samples were missed since the last one."""
        interval_ms = self._interval_s(stream) * 1000
        last = self.last_sample.get((coin, stream))
        if last is not None and sample_ms - last > MISSED_SAMPLE_FACTOR * interval_ms:
            self._add_gaps(coin, [gap_row(stream, last, sample_ms, "missed_samples")])
        self.last_sample[coin, stream] = sample_ms
        self.buffers[coin, stream].add(row)

    def _interval_s(self, stream: str) -> int:
        if stream in self.books:
            return self.books[stream].interval_s
        if stream == schemas.CTX:
            return self.cfg.ctx_interval_s
        return self.cfg.funding_pred_interval_s

    # ---- samplers --------------------------------------------------------------------------

    async def _every(self, interval_s: int, fn: Callable[[int], Coroutine[Any, Any, None]]) -> None:
        """Call fn(sample_ms) at every wall-clock multiple of interval_s. Late ticks are
        skipped, never bunched up."""
        interval_ms = interval_s * 1000
        while True:
            now = self.clock_ms()
            tick = now - now % interval_ms + interval_ms
            await asyncio.sleep((tick - now) / 1000)
            try:
                await fn(tick)
            except Exception:
                log.exception("sampler failed at %d", tick)

    def _fresh(self, recv_ms: int | None, sample_ms: int) -> bool:
        return recv_ms is not None and sample_ms - recv_ms <= self.cfg.rest_stale_s * 1000

    async def _sample_ctx(self, sample_ms: int) -> None:
        stale: list[str] = []
        for coin in self.cfg.coins:
            s = self.latest_ctx.get(coin)
            if s is not None and self._fresh(s.recv_ms, sample_ms):
                row = ctx_row(s, sample_ms, "ws", self.last_exch_ms)
                self._add_sample(coin, schemas.CTX, sample_ms, row)
            else:
                stale.append(coin)
        if not stale:
            return
        resp = await self._rest({"type": "metaAndAssetCtxs"})
        if resp is None:
            return
        snaps = parse_meta_and_asset_ctxs(resp, tuple(stale), self.clock_ms())
        for coin, s in snaps.items():
            self._add_sample(coin, schemas.CTX, sample_ms, ctx_row(s, sample_ms, "rest", None))

    async def _sample_book(self, stream: str, sample_ms: int) -> None:
        cfg = self.books[stream]
        for coin in self.cfg.coins:
            b = self.latest_book.get((stream, coin))
            source = "ws"
            if b is None or not self._fresh(b.recv_ms, sample_ms):
                payload: dict[str, Any] = self._book_sub(coin, cfg)
                resp = await self._rest(payload)
                if resp is None:
                    continue
                b = parse_book(resp, cfg.levels, self.clock_ms())
                source = "rest"
            self._add_sample(coin, stream, sample_ms, book_row(b, cfg.levels, sample_ms, source))

    async def _sample_funding_pred(self, sample_ms: int) -> None:
        resp = await self._rest({"type": "predictedFundings"})
        if resp is None:
            return
        recv = self.clock_ms()
        preds = parse_predicted_fundings(resp, self.cfg.coins)
        for coin in self.cfg.coins:
            rows = [funding_pred_row(p, sample_ms, recv) for p in preds if p.coin == coin]
            if not rows:
                continue
            self._add_sample(coin, schemas.FUNDING_PRED, sample_ms, rows[0])
            self.buffers[coin, schemas.FUNDING_PRED].extend(rows[1:])

    async def _rest(self, payload: dict[str, Any]) -> Any | None:
        try:
            return await self.rest.info(payload)
        except Exception as e:
            self.rest_errors += 1
            log.warning("REST %s failed: %s: %s", payload.get("type"), type(e).__name__, e)
            return None

    # ---- periodic loops ----------------------------------------------------------------------

    async def _trades_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            now = self.clock_ms()
            for coin, tracker in self.trackers.items():
                deadline = self.snapshot_deadline.get(coin)
                if deadline is not None and now > deadline and tracker.awaiting_snapshot:
                    self.snapshot_deadline.pop(coin)
                    self._add_gaps(coin, tracker.resolve_without_snapshot(now))
                rows = tracker.finalize(now)
                self.buffers[coin, schemas.TRADES_1M].extend(rows)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            await self.flush(only_due=True)

    async def flush(self, *, only_due: bool) -> None:
        """Write due buffers (or all) as part files, then persist state."""
        wrote = False
        for (coin, stream), buf in self.buffers.items():
            if not len(buf) or (only_due and not buf.due()):
                continue
            rows = buf.take()
            try:
                async with self._write_lock:
                    await asyncio.to_thread(write_parts, self.root, coin, stream, buf.schema, rows)
            except Exception:
                self.write_errors += 1
                log.exception("write failed for %s/%s (%d rows kept)", coin, stream, len(rows))
                buf.restore(rows)
                continue
            wrote = True
            self.state.record_flush(coin, stream, rows)
            if stream in self.stats[coin]:
                self.stats[coin][stream].rows_since_hb += len(rows)
                self.stats[coin][stream].rows_total += len(rows)
        if wrote:
            await self._save_state()

    async def _save_state(self) -> None:
        async with self._write_lock:
            await asyncio.to_thread(save_state, self.state_path, self.state)

    async def _compact_loop(self) -> None:
        while True:
            today = utc_date(self.clock_ms())
            try:
                async with self._write_lock:
                    await asyncio.to_thread(compact_ready, self.root, today)
            except Exception:
                log.exception("compaction failed; parts are kept and retried later")
            await asyncio.sleep(COMPACT_EVERY_S)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.heartbeat_s)
            await self.heartbeat()

    async def heartbeat(self) -> None:
        hb = HeartbeatInput(
            now_ms=self.clock_ms(),
            start_ms=self.start_ms,
            connections={c.name: (c.stats.up, c.stats.reconnects) for c in self.connections},
            skew_ms=self.skew.median_ms(),
            gaps=self.gap_count,
            late_trades=sum(t.late_trades for t in self.trackers.values()),
            rest_errors=self.rest_errors,
            write_errors=self.write_errors,
            buffered_rows=sum(len(b) for b in self.buffers.values()),
            streams=self.stats,
        )
        log.info(format_heartbeat(hb))
        if hb.skew_ms is not None and abs(hb.skew_ms) > SKEW_WARN_MS:
            log.warning("clock skew %+d ms vs exchange: check NTP (timedatectl)", hb.skew_ms)
        payload = json.dumps(heartbeat_json(hb), indent=1).encode()
        for per_coin in self.stats.values():
            for st in per_coin.values():
                st.rows_since_hb = 0
        path = self.meta_dir / "heartbeat.json"
        try:
            await asyncio.to_thread(atomic_write_bytes, path, payload)
        except OSError:
            log.exception("could not write %s", path)

    # ---- lifecycle ---------------------------------------------------------------------------

    def _startup_gaps(self) -> None:
        gaps = downtime_gaps(self.state, self.cfg.coins, self._sampled_streams(), self.start_ms)
        for coin, rows in gaps.items():
            self._add_gaps(coin, rows)
        self.state.clean_shutdown = False  # a crash from here on must not look clean

    async def run(self, stop: asyncio.Event) -> None:
        started = datetime.fromtimestamp(self.start_ms / 1000, UTC).isoformat()
        log.info(
            "recorder starting at %s: coins=%s data_dir=%s", started, self.cfg.coins, self.root
        )
        self._startup_gaps()
        await self._save_state()
        jobs: list[Coroutine[Any, Any, None]] = [c.run() for c in self.connections]
        jobs += [
            self._every(self.cfg.ctx_interval_s, self._sample_ctx),
            self._every(self.cfg.funding_pred_interval_s, self._sample_funding_pred),
            self._trades_loop(),
            self._flush_loop(),
            self._compact_loop(),
            self._heartbeat_loop(),
        ]
        for b in self.cfg.books:
            jobs.append(self._every(b.interval_s, partial(self._sample_book, b.stream)))
        tasks = [asyncio.create_task(j) for j in jobs]
        try:
            await stop.wait()
        finally:
            log.info("stopping: cancelling tasks and flushing buffers")
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await t
            self.state.clean_shutdown = True
            await self.flush(only_due=False)
            await self._save_state()
            log.info("recorder stopped cleanly")
