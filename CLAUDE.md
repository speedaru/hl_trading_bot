# Hyperliquid Trading Bot

Systematic trading research + bot for Hyperliquid perps. Solo project by a CS student
(strong C++/low-level background, comfortable in Python, new to quant methods).
Full design decisions and rationale: @docs/PROJECT_CONTEXT.md

## Current phase
Phase 1: data logger + historical data + minimal backtester. NO live trading code yet.
Do not write order-placing code unless I explicitly say we are starting the live phase.

## Stack & commands
- Python 3.11+, managed with `uv`
- Install deps: `uv sync`
- Tests: `uv run pytest`
- Lint/format: `uv run ruff check . --fix` and `uv run ruff format .`
- Types: `uv run pyright`
- Data: polars (not pandas) + Parquet files under `data/` (gitignored)
- Exchange: `hyperliquid-python-sdk`, asyncio for anything network-bound

## Layout
- `src/hlbot/data/` – loaders (Binance archive, Hyperliquid API) and the live logger
- `src/hlbot/indicators/` – indicators written from their formulas (EMA, ATR, RSI...)
- `src/hlbot/backtest/` – event loop, fills, costs, sizing, metrics
- `src/hlbot/strategies/` – one file per strategy, plus `random_baseline.py`
- `src/hlbot/live/` – (later) execution, risk rails, reconciliation
- `research/` – notebooks/scripts for experiments; `docs/research_log.md` logs every test

## How to work with me
- I am learning. When you introduce a trading/stats concept or a non-obvious
  implementation choice, explain it in 1–3 sentences. Keep code readable over clever.
- Explore → plan → code. For anything touching backtest logic, sizing, or execution,
  propose a plan first and wait for my OK.
- Small, reviewable changes. Every new module gets unit tests in the same change.
- Never invent API fields or SDK methods. If unsure about the Hyperliquid API/SDK,
  check the SDK source or docs, or ask me.
- Don't add dependencies without asking.
- After changing anything in `src/hlbot/backtest/` or `src/hlbot/strategies/`, or when a
  result looks surprisingly good, run the `backtest-reviewer` subagent before I trust it.
- Log experiments with `/log-experiment`.

## Non-negotiable rules (research integrity)
- No lookahead: a signal at bar t may only use data with close time <= t.
  Entries fill at the NEXT bar's open (or later), never the signal bar's close.
- Always model costs: taker 0.045%, maker 0.015% (verify current HL fees),
  slippage (configurable, default 1–2 bps per side), and hourly funding for held positions.
- Every strategy is compared against `random_baseline` using identical exits/sizing.
- Train/test split by time: tune on the first ~70%, test once on the last ~30%.
  Never tune on the test set. Log every experiment in `docs/research_log.md`.
- Treat surprisingly good results as a bug until proven otherwise. Look for lookahead first.

## Risk model (applies to backtests and later live code)
- Size from risk: `notional = equity * risk_pct / stop_distance_pct`
- risk_pct default 2%, hard cap 4%. Leverage is an output, capped at 10x.
- Liquidation price must be >= 3x further than the stop.
- Skip trades where round-trip cost > ~0.1R (fee filter). Never stretch targets to pass it.
- Portfolio rails: daily loss limit ~3R, weekly ~8R, max 2 open positions,
  correlated coins (BTC/ETH/SOL...) share one risk budget.

## Security
- Never read, print, log, or commit `.env`, private keys, or wallet files.
- Live trading will use a Hyperliquid agent (API) wallet with no withdrawal rights.
  Secrets come from environment variables only.

## Git workflow

After completing any meaningful unit of work, stage and commit:
- A new subsystem scaffold: "scaffold: add backtest session isolation"  
- A passing test suite: "test: smoke test for engine bar loop"
- A completed experiment: "research: EXP-003 funding carry baseline comparison"

Commit format: `<type>: <what>` where type is one of:
  scaffold / feat / fix / test / research / docs / refactor

Before committing:
1. Run `uv run ruff check src/` — fix any errors
2. Run `uv run pytest` — all tests must pass
3. Run `git status` to confirm only intended files are staged
4. Never commit: data/, research/sessions/, .env, .venv/

Ask before committing if you are unsure what should be staged.
