---
name: log-experiment
description: Append a backtest or research experiment to docs/research_log.md. Use after running any backtest, parameter sweep, or strategy test, including failures.
---

Append one entry to `docs/research_log.md` for the experiment just run (or the one the user
describes in $ARGUMENTS). Never edit or delete earlier entries.

Use this exact template, filling every field. Write "unknown" rather than guessing.

```
## EXP-<next number, zero-padded to 3> — <short name> (<YYYY-MM-DD>)
- **Hypothesis:** what we expected and why (the mechanism, not just "it should make money")
- **Strategy / variant:** file + parameters
- **Data:** coins, timeframe(s), date range, train/test split used
- **Set touched:** train only / test (test should be touched once per strategy!)
- **Costs assumed:** fees, slippage, funding
- **Results:** trades, win rate, avg win/loss in R, expectancy (R/trade) after costs,
  max drawdown, vs random_baseline
- **Variants tried so far for this idea:** running count
- **Verdict:** keep / discard / needs more data — one sentence why
- **Next step:**
```

After writing, tell the user the entry number and the running variant count for this idea.
If the test set was used more than once for the same strategy, warn them clearly.
