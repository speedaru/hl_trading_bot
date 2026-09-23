import pytest

from hlbot.backtest.metrics import baseline_comparison, percentile_rank


def test_percentile_rank_hand_checked() -> None:
    samples = [-1.0, 0.0, 0.5, 1.0]
    assert percentile_rank(2.0, samples) == 100
    assert percentile_rank(-5.0, samples) == 0
    assert percentile_rank(0.75, samples) == 75  # beats 3 of 4
    assert percentile_rank(0.5, samples) == 62.5  # beats 2, ties 1 (counts half)


def test_percentile_rank_needs_samples() -> None:
    with pytest.raises(ValueError):
        percentile_rank(1.0, [])


def test_baseline_comparison_skips_runs_without_trades() -> None:
    strategy: dict[str, object] = {"expectancy_r": 0.3, "total_r": 3.0}
    runs: list[dict[str, object]] = [
        {"expectancy_r": 0.1, "total_r": 1.0},
        {"expectancy_r": None, "total_r": 0},  # no trades
        {"expectancy_r": 0.5, "total_r": 5.0},
    ]
    out = baseline_comparison(strategy, runs)
    exp = out["expectancy_r"]
    assert isinstance(exp, dict)
    assert exp["n_valid_runs"] == 2
    assert exp["strategy_percentile"] == 50
    assert out["expectancy_r_percentile"] == 50
    # total_r keeps the zero-trade run (total_r = 0): 3.0 beats 1.0 and 0 out of 3 runs
    assert out["total_r_percentile"] == pytest.approx(100 * 2 / 3)


def test_baseline_comparison_without_strategy_trades() -> None:
    out = baseline_comparison({"expectancy_r": None, "total_r": 0}, [{"expectancy_r": 0.1}])
    assert out["expectancy_r_percentile"] is None
