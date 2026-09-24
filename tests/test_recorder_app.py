"""End-to-end: the Recorder against a local fake exchange, with 1-second cadences.

Slow-ish (a few seconds each) because they run real sockets and real sampling ticks.
"""

import asyncio
import json
from pathlib import Path

import polars as pl

from hlbot.data.recorder import schemas
from hlbot.data.recorder.app import Recorder
from hlbot.data.recorder.config import BookStreamConfig, RecorderConfig
from hlbot.data.recorder.storage import read_day, utc_date
from hlbot.data.recorder.ws import now_ms
from tests.recorder_fake_exchange import FakeExchange, FakeRest

COINS = ("BTC", "SOL")


def make_cfg(root: Path, url: str, *, store_raw: bool = True) -> RecorderConfig:
    return RecorderConfig(
        coins=COINS,
        data_dir=root,
        ws_url=url,
        ctx_interval_s=1,
        funding_pred_interval_s=1,
        heartbeat_s=1,
        books=(
            BookStreamConfig(stream="book", levels=10, interval_s=1),
            BookStreamConfig(stream="book_coarse", levels=10, interval_s=1, n_sig_figs=4),
        ),
        store_raw_trades=store_raw,
        flush_max_rows=10_000,
        flush_max_seconds=0.3,
        ws_ping_s=0.5,
        ws_stale_s=2,
        backoff_initial_s=0.2,
        backoff_max_s=0.4,
        rest_stale_s=2,
    )


async def run_recorder(
    cfg: RecorderConfig, ex: FakeExchange, rest: FakeRest, seconds: float, drop_at: float | None
) -> Recorder:
    rec = Recorder(cfg, rest)
    stop = asyncio.Event()
    task = asyncio.create_task(rec.run(stop))
    if drop_at is not None:
        await asyncio.sleep(drop_at)
        await ex.drop_all()
        seconds -= drop_at
    await asyncio.sleep(seconds)
    stop.set()
    await task
    return rec


def record(
    root: Path,
    seconds: float,
    *,
    drop_at: float | None = None,
    snapshot_size: int = 30,
    rest_fails: bool = False,
) -> tuple[FakeExchange, FakeRest, Recorder]:
    async def scenario() -> tuple[FakeExchange, FakeRest, Recorder]:
        rest = FakeRest()
        rest.fail = rest_fails
        async with FakeExchange(COINS, snapshot_size=snapshot_size) as ex:
            rec = await run_recorder(make_cfg(root, ex.url), ex, rest, seconds, drop_at)
        return ex, rest, rec

    return asyncio.run(scenario())


def maybe_day(root: Path, coin: str, stream: str) -> pl.DataFrame | None:
    """Today's rows, plus yesterday's in case the test ran across UTC midnight."""
    days = sorted({utc_date(now_ms() - 60_000), utc_date(now_ms())})
    frames = [df for d in days if (df := read_day(root, coin, stream, d)) is not None]
    return pl.concat(frames) if frames else None


def day(root: Path, coin: str, stream: str) -> pl.DataFrame:
    df = maybe_day(root, coin, stream)
    assert df is not None, f"no {coin}/{stream} data"
    return df


def assert_trades_contiguous(root: Path, ex: FakeExchange, coin: str) -> pl.DataFrame:
    """Recorded trades = every exchange trade between the first and last one recorded."""
    df = day(root, coin, schemas.TRADES)
    assert df.select(["exch_time", "tid"]).is_duplicated().sum() == 0
    got = set(df["tid"].to_list())
    lo, hi = min(got), max(got)
    expected = {t["tid"] for t in ex.history[coin] if lo <= t["tid"] <= hi}
    assert got == expected, f"missing tids: {sorted(expected - got)[:5]}"
    return df


def test_records_every_stream(tmp_path: Path) -> None:
    ex, rest, _ = record(tmp_path, 3.5)
    for coin in COINS:
        ctx = day(tmp_path, coin, schemas.CTX)
        assert ctx.height >= 2
        assert ctx.schema == schemas.CTX_SCHEMA
        assert ctx["source"].unique().to_list() == ["ws"]
        assert (ctx["age_ms"] < 2000).all()
        assert ctx["exch_time_ref"].null_count() == 0
        for stream in ("book", "book_coarse"):
            book = day(tmp_path, coin, stream)
            assert book.height >= 2 and (book["source"] == "ws").all()
        fp = day(tmp_path, coin, schemas.FUNDING_PRED)
        assert fp.height % 3 == 0 and set(fp["venue"]) == {"HlPerp", "BinPerp", "BybitPerp"}
        assert_trades_contiguous(tmp_path, ex, coin)
    # The coarse connection subscribed with nSigFigs; the full-precision one without.
    books = [s for s in ex.subscriptions if s["type"] == "l2Book"]
    assert {s.get("nSigFigs") for s in books} == {None, 4}
    assert "metaAndAssetCtxs" not in rest.calls  # websocket values were fresh
    state = json.loads((tmp_path / "_meta" / "state.json").read_text())
    assert state["clean_shutdown"] is True
    hb = json.loads((tmp_path / "_meta" / "heartbeat.json").read_text())
    assert hb["connections"]["main"]["up"] is True
    assert maybe_day(tmp_path, "BTC", schemas.GAPS) is None  # first run, no outage


def test_server_drop_covered_by_snapshot_loses_nothing(tmp_path: Path) -> None:
    # 30-trade snapshot at 20 trades/s = 1.5s of history > reconnect time (<= 0.4s backoff).
    ex, _, rec = record(tmp_path, 4.0, drop_at=1.5)
    assert rec.connections[0].stats.reconnects >= 1
    for coin in COINS:
        assert_trades_contiguous(tmp_path, ex, coin)
        gaps = maybe_day(tmp_path, coin, schemas.GAPS)
        assert gaps is None or gaps.filter(pl.col("stream") == "trades").height == 0


def test_server_drop_with_short_snapshot_writes_trades_gap(tmp_path: Path) -> None:
    # A 1-trade snapshot can't cover the outage: trades are really lost and must be flagged.
    ex, _, _ = record(tmp_path, 3.5, drop_at=1.5, snapshot_size=1)
    for coin in COINS:
        gaps = day(tmp_path, coin, schemas.GAPS).filter(pl.col("stream") == "trades")
        assert gaps.height >= 1
        assert gaps["reason"].to_list()[0] == "trades_lost"
        g = gaps.row(0, named=True)
        trades = day(tmp_path, coin, schemas.TRADES)
        recorded = set(trades["tid"].to_list())
        missing = [
            t
            for t in ex.history[coin]
            if min(recorded) < t["tid"] < max(recorded) and t["tid"] not in recorded
        ]
        assert missing, "the scenario should really lose trades"
        # every missing trade lies inside the gap window
        start = g["gap_start"].timestamp() * 1000
        end = g["gap_end"].timestamp() * 1000
        assert all(start <= t["time"] <= end for t in missing)


def test_restart_writes_downtime_gaps_and_no_duplicate_trades(tmp_path: Path) -> None:
    async def scenario() -> FakeExchange:
        async with FakeExchange(COINS) as ex:
            rest = FakeRest()
            await run_recorder(make_cfg(tmp_path, ex.url), ex, rest, 2.5, None)
            await asyncio.sleep(0.3)  # quick restart: the snapshot still overlaps
            await run_recorder(make_cfg(tmp_path, ex.url), ex, rest, 2.5, None)
        return ex

    ex = asyncio.run(scenario())
    for coin in COINS:
        assert_trades_contiguous(tmp_path, ex, coin)  # overlap found via state.json keys
        gaps = day(tmp_path, coin, schemas.GAPS)
        assert set(gaps["reason"]) == {"restart_clean"}
        assert set(gaps["stream"]) >= {"ctx", "book", "book_coarse", "funding_pred"}


def test_rest_fallback_and_failures_are_survivable(tmp_path: Path) -> None:
    # REST down the whole time: funding predictions are missing, everything else continues.
    _, rest, rec = record(tmp_path, 2.5, rest_fails=True)
    assert rec.rest_errors >= 1 and "predictedFundings" in rest.calls
    assert maybe_day(tmp_path, "BTC", schemas.FUNDING_PRED) is None
    assert day(tmp_path, "BTC", schemas.CTX).height >= 1
