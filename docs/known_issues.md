# Known issues, approximations and open items

Things the backtester knowingly simplifies or doesn't do yet. Check this list before
trusting a result, and update it when an item is fixed or a new one is found.
Last reviewed: 2026-09-25 (fifth backtest-reviewer pass: first engine version judged fit to commit).

## Data / loader (fix when writing the loaders)
- **Zero-volume bars are accepted.** A fill at the open of a bar where nothing traded isn't
  realistic. Rare on Binance; the loader should flag or report them.
- **Timezone must be exactly `"UTC"`.** `Etc/UTC` or other aliases are rejected. Safe, just
  strict; the loader should convert to `"UTC"`.
- **Funding timestamp alignment is a loader responsibility.** The engine assumes
  `funding_rate` row t = the hourly rate in force *during* bar t. If funding is stamped at
  settlement time, shift it by one bar before joining, and add an append-future test for
  every joined column (funding, OI, ...).
- **Price source mismatch.** Backtests use Binance candles, but live HL stops trigger on HL's
  mark price. Stop fills can differ, especially on alts. Compare HL vs Binance candles for
  the basket before trusting stop-heavy results.
- **`coin_max_leverage` must come from the exchange**, per coin: `maxLeverage` in the SDK's
  `Info.meta()` universe (3–40x on Hyperliquid). Never guess it; it drives the leverage cap
  and the maintenance margin.

## Modeling approximations (acceptable for now)
- **Funding on bars longer than 1h** uses bar_hours × that row's hourly rate, not the actual
  hourly settlements inside the bar.
- **Intrabar exits pay funding for half the exit bar** and get `exit_time` = mid-bar (exact
  time unknown). Gap exits are timed at the bar's open, time stops and end-of-data at its
  close. The daily/weekly rails book R on `exit_time`.
- **Exit-bar funding is priced at that bar's close**, even if the trade left at the open or
  mid-bar. A cost approximation (not lookahead); tiny, and errs both ways.
- **The fee filter always assumes taker on both legs**, even with `target_is_maker=True`.
  Conservative: it skips slightly more trades than strictly needed.
- **Stop tighter than entry slippage**: the fill already lies beyond the stop, so the trade is
  stopped at the open with no funding (pessimistic). The default fee filter blocks these.
- **Liquidation is only checked at entry**, using Hyperliquid's cross-margin formula for a
  single position (docs: trading/liquidations). A gap through the liquidation price during a
  trade is not simulated. With HL margin rules and risk ≤ 4% this can't realistically happen
  before the stop (see below), but it would matter for isolated margin or multiple positions.
- **Slippage is a fixed bps per side**, not dependent on order size or liquidity.
- **Fees assume tier 0** (taker 0.045%, maker 0.015%, verified 2026-09-25). All exits pay
  taker unless `target_is_maker=True`.
- **Mark-to-market equity excludes the not-yet-paid exit fee and slippage.** They are
  realized when the trade closes.
- **Drawdown is sampled at bar closes.** The worst intrabar dip isn't captured.
- **No minimum order size or size rounding.** Hyperliquid has a minimum order value and
  per-coin size decimals; at $250-300 of capital some trades may be impossible or rounded.
  Model this before the paper/live phase.
- **Margin tiers ignored.** HL raises maintenance margin for very large positions. Irrelevant
  at this account size.

## Interpretation notes
- **`baseline_percentile` does not correct for multiple testing.** 95 means little if 20
  variants were tried: about one in 20 gets there by luck. Log the variant count in
  `docs/research_log.md` and read the percentile with that in mind.
- **The baseline matches entry rate, long/short mix and warm-up, not timing.** Compare
  `n_trades` next to the percentile; clustering of real signals isn't reproduced.
- **The liquidation check effectively never binds at risk ≤ 4%.** With maintenance margin =
  1/(2 × coin max leverage), liquidation at leverage L is ≥ 1/(2L+1) away (worst case: a
  short at the coin's max leverage). The stop at leverage L is ≤ risk/L ≤ 0.04/L, so
  3× the stop ≤ 0.12/L < 1/(2L+1) for every L ≥ 1. For L < 1 (stop wider than risk%),
  m ≤ 1/2 gives a distance ≥ (2−L)/(3L), which is also > 0.12/L. A reviewer sweep (coin max
  1–50x, risk 0.5–4%, stops 0.05–60%) found the smallest liquidation-to-stop ratio was 8.3x.
  The check stays as a guard (e.g. for isolated margin later).
- **Placeholder rail values**: `cooldown_bars_after_stop = 3` and `max_trades_per_day = 5`
  are not tuned. Changing them counts as a variant.
- **`end_of_data` exits are artificial.** They count in `expectancy_r`; compare with
  `expectancy_r_excl_end_of_data`.

## Engine limitations (by design for now)
- **Strategies are only called while flat.** A stateful strategy (counters, crossover
  memory) misses bars while a trade is open; recompute from `history` instead.
- **Strategies can't exit a trade** (e.g. on a trend flip). Exits come only from the
  ExitRule: stop, target, time stop.
- **Single coin, one position at a time.**

## Not implemented yet (needed before multi-coin or live)
- Portfolio rails: max 2 open positions, correlated coins (BTC/ETH/SOL...) share one risk
  budget. The engine is single-coin, one position.
- The 30–40% bot-pot hard floor (stop and reassess). Belongs in live/paper monitoring.
- Walk-forward testing. The 70/30 split is done by hand for now; pass `split="train"` or
  `split="test"` to `run_session` so it's recorded in the manifest.

## Recorder data (`data/live/`, see docs/recorder.md)
- **Check every day before use:** `uv run python -m hlbot.data.recorder.quality`. Treat BAD
  days as missing; for WARN days, read the reasons (usually a gap to exclude).
- **`ctx` has no exchange timestamp.** Its time is `sample_time` (local clock, NTP-synced);
  `exch_time_ref` is only a proxy. Clock skew per day is in the quality report.
- **`ctx.funding` is the live estimate for the hour in progress**, not the settled rate. It
  moves during the hour. Realized rates come from REST `fundingHistory`. When joining funding
  into a backtest, use the settled series, not the last ctx sample.
- **HlPerp `next_funding_time` in `funding_pred` is stale by at least one interval** (API issue).
  Use the next top of the hour instead.
- **Sampled book rows are up to ~5 s old** (`age_ms`): the websocket pushes a book only about
  every 5 s. Don't treat a book row as the book at exactly `sample_time`.
- **Trades are exact only inside covered windows.** Use raw trades or `trades_1m` rows with
  `complete = true`; `trades_lost` gap rows mark windows where trades may be missing.
- **Late trades** (arriving after their minute closed, heartbeat `late=`) are in the raw
  `trades` stream but not in `trades_1m`. Rebuild buckets from raw trades if exact volume matters.
