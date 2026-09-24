"""Smoke test: 100 synthetic bars, always-long strategy, full session written to disk."""

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.engine import BacktestEngine
from hlbot.backtest.exits import ExitRule
from hlbot.backtest.session import compare_sessions, load_session, run_session
from tests.conftest import TEST_COIN_MAX_LEVERAGE, AlwaysLong

RULE = ExitRule(stop_atr_mult=None, stop_pct=0.02, target_r=2.0)
COSTS = CostConfig()
RISK = RiskConfig(coin_max_leverage=TEST_COIN_MAX_LEVERAGE)
N_BASELINE = 20  # fewer than the default 200 to keep tests fast
SESSION_FILES = ("manifest.json", "trades.parquet", "equity.parquet", "metrics.json")


def _run(tmp_path: Path, data: pl.DataFrame) -> tuple[Path, AlwaysLong]:
    strategy = AlwaysLong(RULE)
    path = run_session(
        data,
        strategy,
        COSTS,
        RISK,
        symbol="SYNTH",
        timeframe="1h",
        sessions_root=tmp_path,
        n_baseline_runs=N_BASELINE,
    )
    return path, strategy


def test_session_files_created(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, _ = _run(tmp_path, ohlcv_100)
    for folder in (path, path / "random_baseline"):
        for name in SESSION_FILES:
            assert (folder / name).is_file(), f"missing {folder / name}"
    assert not list(tmp_path.glob("*.tmp")), "temporary folder left behind"

    manifest = json.loads((path / "manifest.json").read_text())
    assert manifest["strategy"] == "always_long"
    assert manifest["data"]["n_bars"] == 100
    assert manifest["costs"]["taker_fee"] == COSTS.taker_fee
    assert "commit" in manifest["git"]
    assert manifest["split"] is None  # not labelled train/test in this test

    session = load_session(path)
    assert session.trades.height > 0
    assert session.baseline is not None
    assert session.baseline.manifest["baseline_for"] == manifest["session_id"]
    assert "vs_baseline" in session.metrics
    assert compare_sessions(session, session.baseline).height > 0


def test_session_is_never_overwritten(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)

    def run_once() -> Path:
        return run_session(
            ohlcv_100,
            AlwaysLong(RULE),
            COSTS,
            RISK,
            symbol="SYNTH",
            timeframe="1h",
            sessions_root=tmp_path,
            n_baseline_runs=N_BASELINE,
            now=now,
        )

    first = run_once()
    before = (first / "metrics.json").read_text()
    with pytest.raises(FileExistsError):
        run_once()
    assert (first / "metrics.json").read_text() == before


def test_costs_are_deducted(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, _ = _run(tmp_path, ohlcv_100)
    trades = load_session(path).trades
    for t in trades.to_dicts():
        expected_fees = COSTS.taker_fee * t["qty"] * (t["entry_price"] + t["exit_price"])
        assert t["fees"] == pytest.approx(expected_fees)
        assert t["slippage"] > 0
        assert t["net_pnl"] < t["gross_pnl"]
        assert t["net_pnl"] == pytest.approx(
            t["gross_pnl"] - t["fees"] - t["slippage"] - t["funding"]
        )


def test_entries_fill_at_next_bar_open(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, _ = _run(tmp_path, ohlcv_100)
    opens = ohlcv_100["open"].to_list()
    for t in load_session(path).trades.to_dicts():
        assert t["entry_bar"] == t["signal_bar"] + 1
        assert t["entry_price"] == pytest.approx(opens[t["entry_bar"]] * (1 + COSTS.slippage_frac))


def test_strategy_only_sees_bars_up_to_t(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, strategy = _run(tmp_path, ohlcv_100)
    times = ohlcv_100["open_time"].to_list()
    assert strategy.seen, "strategy was never called"
    for height, last_time in strategy.seen:
        # history ends at bar t = height - 1, and its last row really is bar t
        assert last_time == times[height - 1]
    signal_bars = {h - 1 for h, _ in strategy.seen}
    assert set(load_session(path).trades["signal_bar"].to_list()) <= signal_bars


def test_appending_future_bars_does_not_change_past_trades(ohlcv_100: pl.DataFrame) -> None:
    cut = 60
    short = (
        BacktestEngine(ohlcv_100.head(cut), AlwaysLong(RULE), COSTS, RISK, timeframe="1h")
        .run()
        .trades
    )
    full = BacktestEngine(ohlcv_100, AlwaysLong(RULE), COSTS, RISK, timeframe="1h").run().trades
    # Trades that closed before the short run's last bar can't depend on bars >= cut.
    before = pl.col("exit_bar") < cut - 1
    assert short.filter(before).height > 0
    assert short.filter(before).equals(full.filter(before))


# ---------------------------------------------------------------- random baseline
def test_metrics_json_has_percentile_rank(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, _ = _run(tmp_path, ohlcv_100)
    metrics = json.loads((path / "metrics.json").read_text())
    pct = metrics["baseline_percentile"]
    assert isinstance(pct, float)
    assert 0 <= pct <= 100
    vs = metrics["vs_baseline"]
    assert vs["n_runs"] == N_BASELINE
    assert vs["expectancy_r"]["strategy_percentile"] == pct
    assert vs["expectancy_r"]["strategy"] == metrics["expectancy_r"]
    for key in ("baseline_mean", "baseline_std", "baseline_p5", "baseline_p50", "baseline_p95"):
        assert key in vs["expectancy_r"]


def test_baseline_distribution_written(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, _ = _run(tmp_path, ohlcv_100)
    session = load_session(path)
    assert session.baseline is not None
    dist = session.baseline_distribution
    assert dist is not None
    assert dist["seed"].to_list() == list(range(N_BASELINE))
    # the representative run in random_baseline/ is seed 0
    assert session.baseline.manifest["params"]["seed"] == 0
    assert dist.row(0, named=True)["n_trades"] == session.baseline.trades.height


def test_baseline_matches_long_only_strategy(tmp_path: Path, ohlcv_100: pl.DataFrame) -> None:
    path, _ = _run(tmp_path, ohlcv_100)
    session = load_session(path)
    assert session.metrics["vs_baseline"]["long_prob"] == 1.0
    assert session.baseline is not None
    assert set(session.baseline.trades["direction"].to_list()) == {"long"}


def test_baseline_seeds_do_not_depend_on_strategy_params(
    tmp_path: Path, ohlcv_100: pl.DataFrame
) -> None:
    # Same exits and behaviour, different params dict -> different session id, same baseline.
    class Renamed(AlwaysLong):
        @property
        def params(self) -> dict[str, object]:
            return {"irrelevant": 123}

    a, _ = _run(tmp_path, ohlcv_100)
    b = run_session(
        ohlcv_100,
        Renamed(RULE),
        COSTS,
        RISK,
        symbol="SYNTH",
        timeframe="1h",
        sessions_root=tmp_path,
        n_baseline_runs=N_BASELINE,
    )
    assert a != b
    dist_a = load_session(a).baseline_distribution
    dist_b = load_session(b).baseline_distribution
    assert dist_a is not None
    assert dist_b is not None
    assert dist_a.equals(dist_b)
