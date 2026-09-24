---
paths:
  - "src/hlbot/backtest/**"
  - "src/hlbot/strategies/**"
  - "src/hlbot/indicators/**"
---

# Rules for backtest, strategy and indicator code

- Every function that produces a signal or indicator value must document which bars it
  reads. Write a test that proves value at bar t is unchanged when future bars are
  appended (the "append future data" lookahead test).
- Indicators are implemented from their formulas and tested against a small hand-computed
  example in the test file.
- Fills: market entries at next bar open + slippage. When one bar touches both stop and
  target, assume the stop was hit.
- Costs are passed in as config, never hard-coded inside strategy files.
- Strategies only emit intents (direction, stop, target, time stop). Sizing and risk rails
  live in the engine, not in strategies.
- No randomness without an explicit seed.
