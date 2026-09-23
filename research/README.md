# research/

One-off analysis scripts and notebooks. Nothing in `src/hlbot` imports from here.

- `sessions/` – backtest session outputs written by `hlbot.backtest.session.run_session`.
  Each session is immutable once written (gitignored; regenerable from code + manifest).
- `scratch/` – throwaway checks (gitignored).

Log every experiment in `docs/research_log.md` with `/log-experiment`.
