"""Summary metrics for a finished backtest."""

import statistics

import polars as pl


def max_drawdown_pct(equity: list[float]) -> float:
    """Largest peak-to-trough fall, as a fraction of the peak (0.25 = -25%)."""
    peak = equity[0] if equity else 0.0
    worst = 0.0
    for e in equity:
        peak = max(peak, e)
        worst = max(worst, 1 - e / peak)
    return worst


def compute_metrics(
    trades: pl.DataFrame,
    initial_equity: float,
    skips: dict[str, int],
    equity_curve: pl.DataFrame | None = None,
) -> dict[str, object]:
    """Per-run summary. All R values are net of costs.

    - expectancy_r: mean R per trade (the number that has to be > 0 after costs).
    - max_drawdown_pct: largest peak-to-trough fall of mark-to-market equity (bar closes,
      open positions included). Falls back to closed-trade equity if no curve is given.
    - max_drawdown_closed_pct: same on closed-trade equity only (understates risk).
    - max_drawdown_r: drawdown of cumulative R (independent of position sizing).
    - end_of_data trades are artificial exits; they count in expectancy_r but are also
      reported separately so they can be judged on their own.
    """
    n = trades.height
    r: list[float] = trades["r_multiple"].to_list() if n else []
    wins = [x for x in r if x > 0]
    losses = [x for x in r if x <= 0]
    reasons: list[str] = trades["exit_reason"].to_list() if n else []
    r_real_exits = [x for x, why in zip(r, reasons, strict=True) if why != "end_of_data"]

    closed_equity: list[float] = [initial_equity, *trades["equity_after"].to_list()]
    mtm_equity: list[float] = (
        [initial_equity, *equity_curve["equity"].to_list()]
        if equity_curve is not None
        else closed_equity
    )

    cum = 0.0
    peak_r = 0.0
    max_dd_r = 0.0
    for x in r:
        cum += x
        peak_r = max(peak_r, cum)
        max_dd_r = max(max_dd_r, peak_r - cum)

    return {
        "n_trades": n,
        "win_rate": len(wins) / n if n else None,
        "expectancy_r": sum(r) / n if n else None,
        "avg_win_r": sum(wins) / len(wins) if wins else None,
        "avg_loss_r": sum(losses) / len(losses) if losses else None,
        "total_r": sum(r),
        "n_end_of_data_exits": n - len(r_real_exits),
        "expectancy_r_excl_end_of_data": (
            sum(r_real_exits) / len(r_real_exits) if r_real_exits else None
        ),
        "total_return_pct": closed_equity[-1] / initial_equity - 1,
        "max_drawdown_pct": max_drawdown_pct(mtm_equity),
        "max_drawdown_closed_pct": max_drawdown_pct(closed_equity),
        "max_drawdown_r": max_dd_r,
        "total_fees": float(trades["fees"].sum()) if n else 0.0,
        "total_slippage": float(trades["slippage"].sum()) if n else 0.0,
        "total_funding": float(trades["funding"].sum()) if n else 0.0,
        "skips": dict(skips),
    }


def percentile_rank(value: float, samples: list[float]) -> float:
    """Percent of samples below `value` (ties count half). 50 = no better than random.

    Example: 97.5 means the strategy beat 97.5% of the random-entry runs.
    """
    if not samples:
        raise ValueError("no samples")
    below = sum(s < value for s in samples)
    ties = sum(s == value for s in samples)
    return 100 * (below + 0.5 * ties) / len(samples)


def _summary(samples: list[float]) -> dict[str, float | None]:
    if len(samples) < 2:
        only = samples[0] if samples else None
        return {"mean": only, "std": None, "p5": only, "p50": only, "p95": only}
    cuts = statistics.quantiles(samples, n=20, method="inclusive")  # 5%, 10%, ..., 95%
    return {
        "mean": statistics.fmean(samples),
        "std": statistics.stdev(samples),
        "p5": cuts[0],
        "p50": cuts[9],
        "p95": cuts[18],
    }


def baseline_comparison(
    strategy: dict[str, object], baseline_runs: list[dict[str, object]]
) -> dict[str, object]:
    """Where the strategy sits in the distribution of random-entry runs.

    A single random run is too noisy to compare against, so we compare against many
    seeds. Runs with zero trades have no expectancy and are left out of that metric.
    """
    out: dict[str, object] = {"n_runs": len(baseline_runs)}
    for key in ("expectancy_r", "total_r"):
        samples = [float(v) for run in baseline_runs if isinstance(v := run.get(key), int | float)]
        value = strategy.get(key)
        percentile = (
            percentile_rank(float(value), samples)
            if isinstance(value, int | float) and samples
            else None
        )
        out[key] = {
            "strategy": value,
            "strategy_percentile": percentile,
            "n_valid_runs": len(samples),
            **{f"baseline_{k}": v for k, v in _summary(samples).items()},
        }
        out[f"{key}_percentile"] = percentile  # flat copy of the headline numbers
    return out
