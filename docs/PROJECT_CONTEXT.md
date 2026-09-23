# Project Context & Design Decisions

This document records what was decided during planning and why. Update it when a
decision changes; don't let it drift from reality.

## Goal
Build a systematic trading research pipeline and, if an edge survives honest testing,
a small live bot on Hyperliquid perpetuals.

Success is defined as: working infrastructure + an honest research process + a clear
answer to "is there an edge?". Profit is a bonus. Expectations: a good systematic
strategy makes roughly 20–40%/year, not per week.

## Capital & risk budget
- Live capital (later): ~$250–300 in a separate Hyperliquid subaccount/wallet,
  fully isolated from manual positions (manual trading happens on dYdX).
- Demo/paper trading first; live only after a strategy passes out-of-sample tests.
- Hard floor: if the bot pot drops 30–40%, stop and reassess the strategy (not the params).

## Key concepts (glossary)
- **R**: the amount lost if a trade hits its stop. Targets are expressed as multiples (2R = win twice the risk).
- **Expectancy**: (win% × avg win) − (loss% × avg loss) − costs, measured in R per trade.
- **ATR(14)**: average true range of the last 14 bars; used to set stops (1.5–2× ATR) so they sit outside noise.
- **Leverage is not return**: fees are charged on notional; leverage only changes margin locked up.
- **Kelly**: optimal fraction = W − (1−W)/payoff. We use ~¼ Kelly because estimated edges are overstated.

## Risk rules
- Risk per trade: 2% default (≈¼ Kelly for typical setups), up to 3–4% only after
  ~100 live trades confirm win rate and payoff. Hard cap 4%.
- Size: `notional = equity × risk% / stop_distance%`. Leverage falls out of that, capped 10x.
- Liquidation must be ≥3× further than the stop. Stops live on the exchange as trigger
  orders, placed immediately after entry fills. The bot's kill switch is a second layer.
- Fee filter: skip trades where round-trip cost > ~0.1R. Targets come from the strategy
  and volatility; never stretch them to pass the filter.
- Rails: daily loss ~3R, weekly ~8R, max ~5 trades/day, max 2 open positions,
  no re-entry on a coin right after a stop-out, correlated coins share risk budget.
- Losing streaks are normal: at a 30% win rate, a 10-loss streak within 200 trades is ~80% likely.

## Market & timeframe
- Signals on **4h** (1h as a variant), entries timed on **1h** (or 15m).
- Reason: costs are small relative to moves; trend effects are best documented at
  slower horizons; less noise to overfit. 5–15m signal strategies are kept only as a
  comparison, not the main bet.
- Universe: basket of ~8–10 most liquid Hyperliquid perps (BTC, ETH, SOL, HYPE +
  large alts by volume; check current volume). Avoid thin memecoins/small caps.

## Strategy plan (in order)
1. **Random-entry baseline** with the same exits and sizing. Benchmark + engine sanity check.
2. **Core: 4h trend / breakout**, entries on 1h. Simple, few parameters.
   - Trend filter e.g. EMA50 vs EMA200, price vs EMA50.
   - Entry variants: pullback-and-resume, range breakout, S/R bounce/break in trend direction.
   - Stop: structure or ≥1.5× ATR. Target 1.5–3R or trailing stop. Time stop.
3. **Niche layer (Hyperliquid on-chain data)** as filters, added one at a time, kept only if
   they improve out-of-sample results: funding/open-interest crowding, liquidation clusters,
   aggregate large-account positioning. (Not naive whale copying — already crowded.)
4. **Side project: funding-carry monitor**: Hyperliquid vs Binance funding spreads.
   Structural edge (paid for providing leverage), low turnover.

Edges we believe in come from: risk premia (trend, carry), persistent behavior (herding),
niches too small for big funds, and data others don't use. Not from clever formulas on OHLC.
Stay on the right side of edge vs. manipulation: never game liquidation/vault mechanics.

## Data
- Historical candles: Binance public data archive (years of 15m/1h/4h klines).
- Hyperliquid funding history via API.
- **Start the Hyperliquid logger early**: funding, open interest, positions/liquidation
  snapshots for the basket, stored as Parquet. History is the bottleneck for on-chain research.

## Research protocol
- No lookahead; next-bar fills; realistic fees, slippage, funding.
- 70/30 time split; test set touched once. Walk-forward later if needed.
- Check across coins, across bull/bear/sideways periods, and parameter robustness
  (neighbors of the chosen params should also work).
- Log every experiment (including failures) in `docs/research_log.md`. Many variants tested
  means the best result is partly luck.
- Decision gate: if nothing beats the baseline out-of-sample after costs, change the
  approach (timeframe, funding strategy), don't pile on indicators.

## Roadmap
1. Hyperliquid data logger (runs 24/7 on a cheap VPS)
2. Historical data loader
3. Minimal backtest engine + tests (hand-checked cases, lookahead tests)
4. Random baseline → trend strategy → S/R variants → on-chain filters
5. Paper trading on live mainnet data with simulated fills
6. Small live run (~100 trades at 2% risk), compare with backtest
7. Scale only if live matches backtest

## Using AI
- Great for: writing code, reviewing for lookahead/overfitting, summarizing research,
  interpreting results, explaining exchange mechanics.
- Bad for: generating edges, finding patterns in raw price data. AI-suggested strategies are
  hypotheses like any other and count toward the number of tests run.
- Rule: don't run code you can't explain line by line — especially sizing, orders, stops.
