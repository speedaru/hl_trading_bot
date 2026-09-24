# Market-data recorder

Records Hyperliquid **public** market data 24/7 on a VPS: data that can't be fetched later
(or only partly, from a monthly, possibly incomplete S3 archive). No wallet, no keys, no
credentials anywhere in this code path.

```
uv run python -m hlbot.data.recorder --config config/recorder.toml   # record
uv run python -m hlbot.data.recorder.quality                         # check pulled data
```

## What is recorded

Everything goes to `data/live/<coin>/<stream>/<YYYY-MM-DD>.parquet` (UTC days). All time
columns are UTC datetimes (ms). `exch_time` = exchange timestamp, `recv_time` = local
receive time, `sample_time` = the wall-clock slot a sampled row belongs to.

| stream | cadence | source | contents |
|---|---|---|---|
| `ctx` | 1 min | WS `activeAssetCtx`, REST `metaAndAssetCtxs` fallback | funding (live estimate for the current hour), open interest (coins), mark/oracle/mid, premium, impact prices, day volume |
| `funding_pred` | 1 min | REST `predictedFundings` (not on WS) | predicted funding for HlPerp, BinPerp, BybitPerp: rate per interval, interval hours, next funding time |
| `book` | 30 s | WS `l2Book`, REST fallback | top 10 levels per side at full precision (px, sz, order count) |
| `book_coarse` | 30 s | WS `l2Book` with `nSigFigs=4`, REST fallback | top 10 aggregated levels (BTC: $10 buckets, about ±12 bps) |
| `trades` | every trade | WS `trades` | px, sz, side (`B` = taker bought), tid, hash, buyer and seller address |
| `trades_1m` | 1 min | built from trades | count, base/notional volume, taker buy/sell split, VWAP, OHLC, `complete` flag |
| `gaps` | on event | recorder | windows where data is known to be missing (see below) |

`source` (`ws`/`rest`) and `age_ms` (how old the value was at sample time) are stored on every
sampled row.

### API facts this relies on (verified 2026-09-24)
- Asset contexts carry **no exchange timestamp** (WS or REST). `ctx.exch_time_ref` is the
  newest exchange timestamp seen on any feed at sample time: a proxy, not the ctx's own time.
- `predictedFundings` HlPerp `nextFundingTime` **lags by at least one interval** (it read 23:00 at
  23:44 UTC). Stored raw; HL funding settles every hour on the hour, so compute it from that.
- `tid` is a 50-bit hash of the two order ids. Unique trade key = `(exch_time, coin, tid)`.
  Several fills of one taker order share a `hash`.
- The trades subscription sends a snapshot of the **last 30 trades** first. For BTC at quiet
  times that is about 15 s of history, so an outage longer than that loses BTC trades (flagged).
- `l2Book` messages don't say which `nSigFigs` they were subscribed with, so each book
  stream uses its own websocket connection (3 connections total; the limit is 10).
- The server drops clients that send nothing for 60 s; the recorder pings every 20 s.
- The public trade feed has **no liquidation flag**. Liquidation research needs another source.

## How it stays alive and honest

- **Reconnects:** each connection reconnects forever with exponential backoff (1 s → 60 s,
  jittered), and also forces a reconnect when no message at all arrives for 15 s (open but
  dead socket).
- **REST fallback:** at each sample time, a websocket value older than 15 s is replaced
  by a REST request (`source = "rest"`), so sampled streams keep going through outages.
- **Gap rows** (`gaps` stream, `reason`):
  - `trades_lost`: after a reconnect, the 30-trade snapshot didn't join up with trades
    already seen. `[gap_start, gap_end]` is the window where trades may be missing.
    `trades_unknown`: no snapshot arrived, so coverage can't be proven.
  - `missed_samples`: two consecutive samples of a stream more than 1.5 intervals apart
    (websocket stale *and* REST failed).
  - `process_down` / `restart_clean`: the recorder wasn't running (crash, reboot, deploy).
    Written at startup from `_meta/state.json`.
- **`trades_1m.complete = false`** when a minute touches a window where trades may be missing.
  A minute entirely inside such a window gets no row. The first minute after every start is
  incomplete (its earlier trades were counted by the previous process).
- **Crash safety:** buffers are flushed every 60 s (or 2000 rows) as small, complete part files
  (`<date>.parts/part-*.parquet`, written via temp file + fsync + rename). After UTC midnight
  they are compacted into `<date>.parquet`. A crash loses at most one flush interval.
  Compaction is idempotent and merges late parts into an existing day file.
- **Restarts:** `_meta/state.json` records what was durably written, including the last 200 trade
  keys. After a quick restart, the reconnect snapshot overlaps them, so no duplicates and no
  false gap.

### Heartbeat
Every minute, to stdout/journald and `data/live/_meta/recorder.log`:
```
HB up=3h12m main=up/r2 book=up/r0 book_coarse=up/r0 skew=+410ms gaps=1 late=0 rest_err=0 write_err=0 buf=412 | BTC ctx=1/0.4s trades=150/0.2s book=2/3.1s ... | ETH ...
```
- `r2` means 2 reconnects on that connection.
- `skew` is the median of `recv − exch` on book messages.
- `late` counts trades that arrived after their minute had closed (kept raw, not bucketed).
- Per stream: `rows written since the last heartbeat / age of the newest websocket message`.

**What to worry about:**
- a connection `DOWN` for minutes
- a message age of tens of seconds
- `write_err > 0`
- `skew` beyond ±2 s. The normal value is roughly +0.4 s: Hyperliquid's block → publish time plus network latency. A Windows dev box with an unsynced clock showed +0.9 s.

The same data is written to `data/live/_meta/heartbeat.json` for scripted checks.

## Deploy on a VPS

Sizing: 1 vCPU, 1 GB RAM, 25–40 GB SSD (see disk estimate below). Ubuntu 24.04 assumed.

```bash
# 1. Clock: the data relies on it. Must say "System clock synchronized: yes".
sudo timedatectl set-ntp true && timedatectl

# 2. Service user whose home is /opt/hlbot (keeps uv's Python outside /home; see the unit)
sudo useradd --system --create-home --home-dir /opt/hlbot --shell /bin/bash hlbot
sudo -iu hlbot

# 3. As hlbot: uv, code, dependencies
curl -LsSf https://astral.sh/uv/install.sh | sh && source ~/.local/bin/env
git clone <your repo url> /opt/hlbot/app     # or rsync the repo from your machine
cd /opt/hlbot/app && uv sync --frozen --no-dev

# 4. Foreground test: wait for two "HB" lines with rows > 0, then Ctrl+C
.venv/bin/python -m hlbot.data.recorder --config config/recorder.toml
exit

# 5. Service
sudo cp /opt/hlbot/app/deploy/hlbot-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now hlbot-recorder
journalctl -u hlbot-recorder -f                 # live log; `| grep HB` for heartbeats
```

Cap journald so logs can't fill the disk: set `SystemMaxUse=500M` in
`/etc/systemd/journald.conf`, then `sudo systemctl restart systemd-journald`.

**Update:** `sudo -iu hlbot`, `cd app && git pull && uv sync --frozen --no-dev`, then
`sudo systemctl restart hlbot-recorder`. A restart costs a few seconds of samples,
recorded as `restart_clean` gap rows.

**Is it alive?** `systemctl status hlbot-recorder`,
`journalctl -u hlbot-recorder --since "10 min ago" | grep HB`, or check that
`data/live/_meta/heartbeat.json` is less than 3 minutes old:
`find /opt/hlbot/app/data/live/_meta/heartbeat.json -mmin -3`.

## Getting the data home

Pull only the compacted day files (`YYYY-MM-DD.parquet`). They are final once written,
which happens within ~10 minutes after UTC midnight. The VPS holds the only copy until you
pull, so pull daily (Windows Task Scheduler works).

**With WSL (recommended, incremental):**
```bash
rsync -av --prune-empty-dirs --include='*/' --include='20??-??-??.parquet' --exclude='*' \
  hlbot@<vps>:/opt/hlbot/app/data/live/ /mnt/f/coding/python/trading_bot/data/live/
```

**With Git Bash (no rsync):** stream day files changed in the last 3 days through tar:
```bash
ssh hlbot@<vps> 'cd /opt/hlbot/app/data/live && find . -name "20??-??-??.parquet" -mtime -3 -print0 | tar --null -cf - -T -' \
  | tar -xf - -C data/live
```

Then check the data before building research on it:
```bash
uv run python -m hlbot.data.recorder.quality                                   # all complete days
uv run python -m hlbot.data.recorder.quality --start 2026-10-01 --end 2026-10-07 --csv q.csv
```

Per coin per day it prints:
- coverage for each sampled stream
- share of complete trade minutes
- longest gap, and minutes of lost trades
- clock skew and REST share
- a verdict (**OK**, **WARN** or **BAD**) with reasons

The exit code is 1 if any day is BAD. Days with no data inside `--start/--end` count as BAD.
Thresholds are constants at the top of `quality.py`.

Reading data in research code:
```python
pl.scan_parquet("data/live/BTC/ctx/*.parquet")        # compacted days only
from hlbot.data.recorder.storage import read_day      # one day incl. uncompacted parts
```

## Disk estimate

These are per day for BTC/ETH/SOL. They're extrapolated from a quiet-night sample, so the real figures depend on market activity. Check with `du -sh data/live/*` after the first days.

| stream | per day |
|---|---|
| ctx + funding_pred + trades_1m | < 1 MB |
| book + book_coarse (30 s, 10 levels) | ~9 MB |
| raw trades with addresses | ~20 MB quiet, 100–200 MB volatile |

- **Typical total:** ~30 MB/day, about 11 GB/year.
- **Worst case:** ~200 MB/day.
- **Before compaction:** the current day's part files take 2–3× more space until they're compacted.
