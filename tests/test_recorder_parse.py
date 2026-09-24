"""Parsers, run against real messages captured from the live API (tests/fixtures/recorder/)."""

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hlbot.data.recorder import schemas
from hlbot.data.recorder.parse import (
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

FIX = Path(__file__).parent / "fixtures" / "recorder"
RECV = 1_790_291_900_000


def load(name: str) -> Any:
    return json.loads((FIX / name).read_text())


def fits(rows: list[dict[str, object]], schema: pl.Schema) -> pl.DataFrame:
    """Rows must build a frame with exactly the declared columns and dtypes."""
    df = pl.DataFrame(rows, schema=schema)
    assert df.schema == schema
    return df


def test_ws_ctx() -> None:
    msg = load("ws_activeAssetCtx.json")
    s = parse_ws_ctx(msg["data"], RECV)
    raw = msg["data"]["ctx"]
    assert s.coin == "SOL"
    assert s.funding == float(raw["funding"])
    assert s.open_interest == float(raw["openInterest"])
    assert s.mark_px == float(raw["markPx"])
    assert s.oracle_px == float(raw["oraclePx"])
    assert s.impact_bid_px == float(raw["impactPxs"][0])
    row = ctx_row(s, RECV + 400, "ws", RECV - 50)
    assert row["age_ms"] == 400
    df = fits([row], schemas.CTX_SCHEMA)
    assert df["sample_time"].dt.epoch("ms")[0] == RECV + 400


def test_rest_ctx_matches_universe_order() -> None:
    resp = load("rest_metaAndAssetCtxs.json")
    out = parse_meta_and_asset_ctxs(resp, ("BTC", "SOL"), RECV)
    assert set(out) == {"BTC", "SOL"}
    names = [a["name"] for a in resp[0]["universe"]]
    sol_raw = resp[1][names.index("SOL")]
    assert out["SOL"].mark_px == float(sol_raw["markPx"])
    # BTC and SOL prices differ by ~700x; a misaligned zip would swap them.
    assert out["BTC"].mark_px > 50 * out["SOL"].mark_px


def test_ws_book_top_levels() -> None:
    data = load("ws_l2Book.json")["data"]
    b = parse_book(data, 10, RECV)
    assert len(b.bids) == 10 and len(b.asks) == 10
    assert b.bids[0].px == float(data["levels"][0][0]["px"])
    assert b.asks[0].px > b.bids[0].px  # not crossed: bids are levels[0]
    assert all(b.bids[i].px > b.bids[i + 1].px for i in range(9))  # best first
    assert all(b.asks[i].px < b.asks[i + 1].px for i in range(9))
    row = book_row(b, 10, RECV + 1000, "ws")
    df = fits([row], schemas.book_schema(10))
    assert df["exch_time"].dt.epoch("ms")[0] == data["time"]


def test_coarse_book_has_wider_levels() -> None:
    full = parse_book(load("ws_l2Book.json")["data"], 10, RECV)
    coarse = parse_book(load("ws_l2Book_coarse.json")["data"], 10, RECV)
    span_full = full.asks[-1].px - full.bids[-1].px
    span_coarse = coarse.asks[-1].px - coarse.bids[-1].px
    assert span_coarse > span_full


def test_rest_book_same_shape_as_ws() -> None:
    b = parse_book(load("rest_l2Book.json"), 10, RECV)
    assert b.coin == "SOL" and len(b.bids) == 10


def test_thin_book_fills_missing_levels_with_null() -> None:
    data = {"coin": "X", "time": RECV, "levels": [[{"px": "1.0", "sz": "2", "n": 1}], []]}
    b = parse_book(data, 3, RECV)
    row = book_row(b, 3, RECV, "ws")
    assert row["bid_px_0"] == 1.0
    assert row["bid_px_1"] is None and row["ask_px_0"] is None
    fits([row], schemas.book_schema(3))


def test_trades() -> None:
    data = load("ws_trades.json")["data"]
    trades = parse_trades(data, RECV)
    assert len(trades) == len(data) == 3
    t = trades[0]
    assert t.side in ("A", "B")
    assert t.buyer == data[0]["users"][0] and t.seller == data[0]["users"][1]
    assert t.key == (data[0]["time"], data[0]["tid"])
    fits([trade_row(x) for x in trades], schemas.TRADES_SCHEMA)


def test_empty_trades_message() -> None:
    assert parse_trades([], RECV) == []


def test_predicted_fundings() -> None:
    preds = parse_predicted_fundings(load("rest_predictedFundings.json"), ("SOL",))
    assert {p.venue for p in preds} == {"HlPerp", "BinPerp", "BybitPerp"}
    hl = next(p for p in preds if p.venue == "HlPerp")
    assert hl.interval_hours == 1
    fits([funding_pred_row(p, RECV, RECV) for p in preds], schemas.FUNDING_PRED_SCHEMA)


def test_predicted_fundings_skips_null_venue() -> None:
    resp = [["NEW", [["HlPerp", None], ["BinPerp", None]]]]
    assert parse_predicted_fundings(resp, ("NEW",)) == []


def test_gap_row_fits_schema() -> None:
    fits([gap_row("ctx", RECV, RECV + 5000, "missed_samples")], schemas.GAPS_SCHEMA)


@pytest.mark.parametrize("stream", ["ctx", "trades", "book", "book_coarse", "gaps"])
def test_every_stream_has_partition_and_key(stream: str) -> None:
    schema = schemas.schema_for(stream, book_levels=10)
    assert schemas.partition_column(stream) in schema
    key = schemas.unique_key(stream)
    assert key is None or all(k in schema for k in key)
