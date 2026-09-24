"""BacktestSession: every run is an isolated, immutable folder.

research/sessions/<session-id>/
    manifest.json      what was run (strategy, params, data range, configs, git commit)
    trades.parquet     one row per trade
    equity.parquet     mark-to-market equity at every bar close
    metrics.json       summary + where the strategy ranks among the random-baseline runs
    random_baseline/   manifest/trades/equity/metrics of one representative run (seed 0)
        distribution.parquet   one row of metrics per baseline seed

Files are written into `<id>.tmp/` and the folder is renamed only when complete, so a
half-written session never appears. An existing session is never overwritten.
"""

import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl

from hlbot.backtest.config import CostConfig, RiskConfig
from hlbot.backtest.engine import BacktestEngine, BacktestResult
from hlbot.backtest.metrics import baseline_comparison, compute_metrics
from hlbot.strategies.base import Strategy
from hlbot.strategies.random_baseline import RandomBaseline

DEFAULT_SESSIONS_ROOT = Path("research/sessions")
BASELINE_DIR = "random_baseline"
DEFAULT_BASELINE_RUNS = 200
DISTRIBUTION_SCHEMA: dict[str, type[pl.DataType]] = {
    "seed": pl.Int64,
    "n_trades": pl.Int64,
    "expectancy_r": pl.Float64,
    "win_rate": pl.Float64,
    "total_r": pl.Float64,
    "max_drawdown_r": pl.Float64,
}


@dataclass(frozen=True)
class Session:
    path: Path
    manifest: dict[str, Any]
    metrics: dict[str, Any]
    trades: pl.DataFrame
    baseline: "Session | None"
    baseline_distribution: pl.DataFrame | None


def params_hash(params: object) -> str:
    """Stable 8-hex-char hash of the params (key order doesn't matter)."""
    blob = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:8]


def make_session_id(strategy_name: str, params: object, now: datetime) -> str:
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}_{strategy_name}_{params_hash(params)}"


def git_info() -> dict[str, object]:
    """Commit hash and dirty flag of the repo containing this file (None if unavailable)."""
    cwd = Path(__file__).resolve().parent

    def git(*args: str) -> str | None:
        try:
            res = subprocess.run(
                ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        return res.stdout.strip()

    commit = git("rev-parse", "HEAD")
    status = git("status", "--porcelain")
    return {"commit": commit, "dirty": None if status is None else bool(status)}


def run_session(
    data: pl.DataFrame,
    strategy: Strategy,
    costs: CostConfig,
    risk: RiskConfig,
    *,
    symbol: str,
    timeframe: str,
    sessions_root: Path = DEFAULT_SESSIONS_ROOT,
    initial_equity: float = 10_000.0,
    n_baseline_runs: int = DEFAULT_BASELINE_RUNS,
    split: Literal["train", "test"] | None = None,
    now: datetime | None = None,
) -> Path:
    """Run the strategy and its random baselines, write the session folder, return its path.

    `split` records which part of the 70/30 time split `data` is, so a session run on the
    test set is visible in its manifest (the test set may be touched once).
    """
    if n_baseline_runs < 1:
        raise ValueError("n_baseline_runs must be >= 1")
    now = now or datetime.now(UTC)
    params = dict(strategy.params)
    session_id = make_session_id(strategy.name, params, now)
    final_dir = sessions_root / session_id
    tmp_dir = sessions_root / f"{session_id}.tmp"
    if final_dir.exists() or tmp_dir.exists():
        raise FileExistsError(f"session already exists: {final_dir}")

    def run(s: Strategy) -> BacktestResult:
        engine = BacktestEngine(
            data, s, costs, risk, timeframe=timeframe, initial_equity=initial_equity
        )
        return engine.run()

    result = run(strategy)
    metrics = _metrics(result)

    # Baseline: same exits/sizing/costs/warm-up, entering as often (per post-warm-up flat
    # bar) as the strategy did, with the same long/short mix. Many seeds, because one random
    # run is mostly noise. Seeds are 0..n-1 regardless of params, so tuning params can't
    # reshuffle the baseline.
    entry_prob = result.n_signals / result.n_strategy_calls if result.n_strategy_calls else 0.0
    long_prob = result.n_long_signals / result.n_signals if result.n_signals else 0.5
    baselines: list[RandomBaseline] = []
    base_results: list[BacktestResult] = []
    base_metrics: list[dict[str, object]] = []
    for seed in range(n_baseline_runs):
        baseline = RandomBaseline(
            entry_prob=entry_prob,
            long_prob=long_prob,
            seed=seed,
            exit_rule=strategy.exit_rule,
            warmup_bars=result.warmup_bars,
        )
        base_result = run(baseline)
        baselines.append(baseline)
        base_results.append(base_result)
        base_metrics.append(_metrics(base_result))

    comparison = baseline_comparison(metrics, base_metrics)
    comparison["entry_prob"] = entry_prob
    comparison["long_prob"] = long_prob
    comparison["seeds"] = f"0..{n_baseline_runs - 1}"
    metrics["vs_baseline"] = comparison
    # Headline number at top level (so compare_sessions() shows it): % of random runs whose
    # expectancy the strategy beat. ~50 = indistinguishable from random entries.
    metrics["baseline_percentile"] = comparison["expectancy_r_percentile"]

    distribution = pl.DataFrame(
        [
            {k: (b.seed if k == "seed" else m.get(k)) for k in DISTRIBUTION_SCHEMA}
            for b, m in zip(baselines, base_metrics, strict=True)
        ],
        schema=DISTRIBUTION_SCHEMA,
        orient="row",
        strict=False,
    )

    common: dict[str, object] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "split": split,
        "costs": asdict(costs),
        "risk": asdict(risk),
        "exit_rule": asdict(strategy.exit_rule),
        "initial_equity": initial_equity,
        "git": git_info(),
        "created_at": now.isoformat(),
    }
    manifest = {
        "session_id": session_id,
        "strategy": strategy.name,
        "params": params,
        **common,
        **_data_info(result),
    }
    base_manifest = {
        "session_id": f"{session_id}/{BASELINE_DIR}",
        "strategy": baselines[0].name,
        "params": dict(baselines[0].params),
        "baseline_for": session_id,
        "representative_of": f"{n_baseline_runs} runs, seeds 0..{n_baseline_runs - 1}",
        **common,
        **_data_info(base_results[0]),
    }

    tmp_dir.mkdir(parents=True)
    _write_run(tmp_dir, manifest, metrics, result)
    _write_run(tmp_dir / BASELINE_DIR, base_manifest, base_metrics[0], base_results[0])
    distribution.write_parquet(tmp_dir / BASELINE_DIR / "distribution.parquet")
    tmp_dir.rename(final_dir)
    return final_dir


def load_session(path: Path) -> Session:
    baseline_path = path / BASELINE_DIR
    # Only the top-level session has one (the baseline sub-session has no random_baseline/).
    distribution_path = baseline_path / "distribution.parquet"
    return Session(
        path=path,
        manifest=json.loads((path / "manifest.json").read_text()),
        metrics=json.loads((path / "metrics.json").read_text()),
        trades=pl.read_parquet(path / "trades.parquet"),
        baseline=load_session(baseline_path) if baseline_path.is_dir() else None,
        baseline_distribution=(
            pl.read_parquet(distribution_path) if distribution_path.is_file() else None
        ),
    )


def compare_sessions(a: Session, b: Session) -> pl.DataFrame:
    """Numeric metrics of two sessions side by side."""
    keys = [
        k for k, v in a.metrics.items() if isinstance(v, int | float) and not isinstance(v, bool)
    ]
    return pl.DataFrame(
        {
            "metric": keys,
            a.manifest["session_id"]: [_as_float(a.metrics.get(k)) for k in keys],
            b.manifest["session_id"]: [_as_float(b.metrics.get(k)) for k in keys],
        }
    )


def _as_float(v: object) -> float | None:
    return float(v) if isinstance(v, int | float) else None


def _metrics(result: BacktestResult) -> dict[str, object]:
    return compute_metrics(result.trades, result.initial_equity, result.skips, result.equity_curve)


def _data_info(result: BacktestResult) -> dict[str, object]:
    return {
        "data": {
            "start": result.data_start.isoformat(),
            "end": result.data_end.isoformat(),
            "n_bars": result.n_bars,
            "bar_hours": result.bar_hours,
        },
        "warmup_bars": result.warmup_bars,
        "n_strategy_calls": result.n_strategy_calls,
        "n_signals": result.n_signals,
        "n_long_signals": result.n_long_signals,
        "final_equity": result.final_equity,
    }


def _write_run(
    folder: Path, manifest: dict[str, object], metrics: dict[str, object], result: BacktestResult
) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    (folder / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str))
    result.trades.write_parquet(folder / "trades.parquet")
    result.equity_curve.write_parquet(folder / "equity.parquet")
