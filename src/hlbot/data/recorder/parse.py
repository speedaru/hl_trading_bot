"""Pure functions turning Hyperliquid websocket/REST JSON into typed records and table rows.

Field names follow the exchange payloads as captured live on 2026-09-24 (fixtures in
tests/fixtures/recorder/). Numbers arrive as strings ("84218.0"); they are converted here
and nowhere else.
"""

from dataclasses import dataclass
from typing import Any

Row = dict[str, object]


@dataclass(frozen=True)
class CtxSnapshot:
    """One perp asset context (activeAssetCtx on WS, an element of metaAndAssetCtxs on REST)."""

    coin: str
    recv_ms: int
    funding: float
    open_interest: float
    mark_px: float
    oracle_px: float
    mid_px: float | None
    premium: float | None
    impact_bid_px: float | None
    impact_ask_px: float | None
    day_ntl_vlm: float
    day_base_vlm: float | None
    prev_day_px: float


@dataclass(frozen=True)
class Level:
    px: float
    sz: float
    n: int


@dataclass(frozen=True)
class BookSnapshot:
    coin: str
    exch_ms: int
    recv_ms: int
    bids: tuple[Level, ...]  # best first
    asks: tuple[Level, ...]


@dataclass(frozen=True)
class Trade:
    coin: str
    exch_ms: int
    recv_ms: int
    tid: int
    px: float
    sz: float
    side: str  # "B" taker buy, "A" taker sell
    hash: str
    buyer: str
    seller: str

    @property
    def key(self) -> tuple[int, int]:
        """Unique per coin, per the API docs: tid alone is a hash and can repeat over time."""
        return (self.exch_ms, self.tid)


@dataclass(frozen=True)
class FundingPrediction:
    coin: str
    venue: str
    funding_rate: float
    interval_hours: int
    next_funding_ms: int


def _opt_float(v: Any) -> float | None:
    return None if v is None else float(v)


def parse_ctx(coin: str, ctx: Any, recv_ms: int) -> CtxSnapshot:
    impact = ctx.get("impactPxs")
    return CtxSnapshot(
        coin=coin,
        recv_ms=recv_ms,
        funding=float(ctx["funding"]),
        open_interest=float(ctx["openInterest"]),
        mark_px=float(ctx["markPx"]),
        oracle_px=float(ctx["oraclePx"]),
        mid_px=_opt_float(ctx.get("midPx")),
        premium=_opt_float(ctx.get("premium")),
        impact_bid_px=float(impact[0]) if impact else None,
        impact_ask_px=float(impact[1]) if impact else None,
        day_ntl_vlm=float(ctx["dayNtlVlm"]),
        day_base_vlm=_opt_float(ctx.get("dayBaseVlm")),
        prev_day_px=float(ctx["prevDayPx"]),
    )


def parse_ws_ctx(data: Any, recv_ms: int) -> CtxSnapshot:
    """`data` of an activeAssetCtx message: {"coin": ..., "ctx": {...}}."""
    return parse_ctx(data["coin"], data["ctx"], recv_ms)


def parse_meta_and_asset_ctxs(
    resp: Any, coins: tuple[str, ...], recv_ms: int
) -> dict[str, CtxSnapshot]:
    """REST metaAndAssetCtxs: [meta, ctxs], where ctxs[i] belongs to meta.universe[i]."""
    universe = resp[0]["universe"]
    ctxs = resp[1]
    wanted = set(coins)
    out: dict[str, CtxSnapshot] = {}
    for asset, ctx in zip(universe, ctxs, strict=True):
        name = asset["name"]
        if name in wanted:
            out[name] = parse_ctx(name, ctx, recv_ms)
    return out


def _levels(raw: Any, n: int) -> tuple[Level, ...]:
    return tuple(Level(float(x["px"]), float(x["sz"]), int(x["n"])) for x in raw[:n])


def parse_book(data: Any, levels: int, recv_ms: int) -> BookSnapshot:
    """l2Book (WS `data` or the REST response, same shape). levels[0] = bids, [1] = asks."""
    return BookSnapshot(
        coin=data["coin"],
        exch_ms=int(data["time"]),
        recv_ms=recv_ms,
        bids=_levels(data["levels"][0], levels),
        asks=_levels(data["levels"][1], levels),
    )


def parse_trades(data: Any, recv_ms: int) -> list[Trade]:
    """`data` of a trades message: a list of trades, possibly empty."""
    out: list[Trade] = []
    for t in data:
        buyer, seller = t["users"]
        out.append(
            Trade(
                coin=t["coin"],
                exch_ms=int(t["time"]),
                recv_ms=recv_ms,
                tid=int(t["tid"]),
                px=float(t["px"]),
                sz=float(t["sz"]),
                side=t["side"],
                hash=t["hash"],
                buyer=buyer,
                seller=seller,
            )
        )
    return out


def parse_predicted_fundings(resp: Any, coins: tuple[str, ...]) -> list[FundingPrediction]:
    """REST predictedFundings: [[coin, [[venue, {fundingRate, nextFundingTime, ...}], ...]], ...].

    Venues can be null for a coin not listed there; those are skipped.
    """
    wanted = set(coins)
    out: list[FundingPrediction] = []
    for coin, venues in resp:
        if coin not in wanted:
            continue
        for venue, pred in venues:
            if pred is None:
                continue
            out.append(
                FundingPrediction(
                    coin=coin,
                    venue=venue,
                    funding_rate=float(pred["fundingRate"]),
                    interval_hours=int(pred["fundingIntervalHours"]),
                    next_funding_ms=int(pred["nextFundingTime"]),
                )
            )
    return out


# ---- table rows -------------------------------------------------------------------------


def ctx_row(s: CtxSnapshot, sample_ms: int, source: str, exch_time_ref: int | None) -> Row:
    return {
        "sample_time": sample_ms,
        "recv_time": s.recv_ms,
        "exch_time_ref": exch_time_ref,
        "age_ms": sample_ms - s.recv_ms,
        "source": source,
        "funding": s.funding,
        "open_interest": s.open_interest,
        "mark_px": s.mark_px,
        "oracle_px": s.oracle_px,
        "mid_px": s.mid_px,
        "premium": s.premium,
        "impact_bid_px": s.impact_bid_px,
        "impact_ask_px": s.impact_ask_px,
        "day_ntl_vlm": s.day_ntl_vlm,
        "day_base_vlm": s.day_base_vlm,
        "prev_day_px": s.prev_day_px,
    }


def book_row(b: BookSnapshot, levels: int, sample_ms: int, source: str) -> Row:
    """Wide row. A thin book with fewer levels than asked leaves the missing ones null."""
    row: Row = {
        "sample_time": sample_ms,
        "exch_time": b.exch_ms,
        "recv_time": b.recv_ms,
        "age_ms": sample_ms - b.recv_ms,
        "source": source,
    }
    for side, lv in (("bid", b.bids), ("ask", b.asks)):
        for i in range(levels):
            level = lv[i] if i < len(lv) else None
            row[f"{side}_px_{i}"] = level.px if level else None
            row[f"{side}_sz_{i}"] = level.sz if level else None
            row[f"{side}_n_{i}"] = level.n if level else None
    return row


def trade_row(t: Trade) -> Row:
    return {
        "exch_time": t.exch_ms,
        "recv_time": t.recv_ms,
        "tid": t.tid,
        "px": t.px,
        "sz": t.sz,
        "side": t.side,
        "hash": t.hash,
        "buyer": t.buyer,
        "seller": t.seller,
    }


def funding_pred_row(p: FundingPrediction, sample_ms: int, recv_ms: int) -> Row:
    return {
        "sample_time": sample_ms,
        "recv_time": recv_ms,
        "venue": p.venue,
        "funding_rate": p.funding_rate,
        "interval_hours": p.interval_hours,
        "next_funding_time": p.next_funding_ms,
    }


def gap_row(stream: str, start_ms: int, end_ms: int, reason: str, detail: str = "") -> Row:
    return {
        "stream": stream,
        "gap_start": start_ms,
        "gap_end": end_ms,
        "reason": reason,
        "detail": detail,
    }
