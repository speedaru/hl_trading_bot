"""Recorder configuration, loaded from a single TOML file (see config/recorder.toml)."""

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# The websocket l2Book feed carries 20 levels per side; more can't be recorded.
MAX_BOOK_LEVELS = 20
VALID_N_SIG_FIGS = (2, 3, 4, 5)


def _check_interval(name: str, seconds: int) -> None:
    # Samples are aligned to wall-clock multiples of the interval, so it must divide a day.
    if seconds <= 0 or 86_400 % seconds != 0:
        raise ValueError(f"{name} must be a positive divisor of 86400 seconds, got {seconds}")


@dataclass(frozen=True, kw_only=True)
class BookStreamConfig:
    """One sampled order-book stream. Each gets its own websocket connection."""

    stream: str
    levels: int = 10
    interval_s: int = 30
    n_sig_figs: int | None = None  # None = full precision

    def __post_init__(self) -> None:
        if not self.stream.startswith("book"):
            raise ValueError(f"book stream names must start with 'book', got {self.stream!r}")
        if not 1 <= self.levels <= MAX_BOOK_LEVELS:
            raise ValueError(f"levels must be in [1, {MAX_BOOK_LEVELS}], got {self.levels}")
        _check_interval("book interval_s", self.interval_s)
        if self.n_sig_figs is not None and self.n_sig_figs not in VALID_N_SIG_FIGS:
            raise ValueError(f"n_sig_figs must be one of {VALID_N_SIG_FIGS} or omitted")


@dataclass(frozen=True, kw_only=True)
class RecorderConfig:
    coins: tuple[str, ...]
    data_dir: Path
    log_file: Path | None = None
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    rest_url: str = "https://api.hyperliquid.xyz"
    ctx_interval_s: int = 60
    funding_pred_interval_s: int = 60
    heartbeat_s: float = 60
    books: tuple[BookStreamConfig, ...] = (BookStreamConfig(stream="book"),)
    store_raw_trades: bool = True
    bucket_grace_s: float = 10
    flush_max_rows: int = 2000
    flush_max_seconds: float = 60
    ws_ping_s: float = 20
    ws_stale_s: float = 15
    backoff_initial_s: float = 1
    backoff_max_s: float = 60
    stable_reset_s: float = 60
    rest_stale_s: float = 15
    rest_timeout_s: float = 10

    def __post_init__(self) -> None:
        if not self.coins:
            raise ValueError("coins must not be empty")
        if len(set(self.coins)) != len(self.coins):
            raise ValueError(f"duplicate coins in {self.coins}")
        names = [b.stream for b in self.books]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate book stream names in {names}")
        _check_interval("ctx_s", self.ctx_interval_s)
        _check_interval("funding_pred_s", self.funding_pred_interval_s)
        if self.flush_max_rows < 1 or self.flush_max_seconds <= 0:
            raise ValueError("flush.max_rows must be >= 1 and flush.max_seconds > 0")
        # The server closes connections that stay silent for 60s; pings must come sooner.
        if not 0 < self.ws_ping_s < 60:
            raise ValueError(f"ws.ping_s must be in (0, 60), got {self.ws_ping_s}")
        if self.backoff_initial_s <= 0 or self.backoff_max_s < self.backoff_initial_s:
            raise ValueError("need 0 < backoff_initial_s <= backoff_max_s")


def load_config(path: Path) -> RecorderConfig:
    with path.open("rb") as f:
        raw = tomllib.load(f)
    return config_from_dict(raw)


def config_from_dict(raw: dict[str, Any]) -> RecorderConfig:
    """Map the TOML layout onto RecorderConfig. Unknown keys are errors, not silently ignored."""
    known_top = {"coins", "data_dir", "log_file", "endpoints", "cadence", "books", "trades"}
    known_top |= {"flush", "ws"}
    _reject_unknown("top level", raw, known_top)
    endpoints = _section(raw, "endpoints", {"ws_url", "rest_url"})
    cadence = _section(raw, "cadence", {"ctx_s", "funding_pred_s", "heartbeat_s"})
    trades = _section(raw, "trades", {"store_raw", "bucket_grace_s"})
    flush = _section(raw, "flush", {"max_rows", "max_seconds"})
    ws_keys = {"ping_s", "stale_s", "backoff_initial_s", "backoff_max_s", "stable_reset_s"}
    ws = _section(raw, "ws", ws_keys | {"rest_stale_s", "rest_timeout_s"})

    books: list[BookStreamConfig] = []
    for b in raw.get("books", []):
        _reject_unknown("[[books]]", b, {"stream", "levels", "interval_s", "n_sig_figs"})
        books.append(BookStreamConfig(**b))

    kwargs: dict[str, Any] = {
        "coins": tuple(raw["coins"]),
        "data_dir": Path(raw["data_dir"]),
        "log_file": Path(raw["log_file"]) if "log_file" in raw else None,
        **endpoints,
        **_renamed(
            cadence, {"ctx_s": "ctx_interval_s", "funding_pred_s": "funding_pred_interval_s"}
        ),
        **_renamed(trades, {"store_raw": "store_raw_trades"}),
        **_renamed(flush, {"max_rows": "flush_max_rows", "max_seconds": "flush_max_seconds"}),
        **_renamed(ws, {"ping_s": "ws_ping_s", "stale_s": "ws_stale_s"}),
    }
    if books:
        kwargs["books"] = tuple(books)
    return RecorderConfig(**kwargs)


def _section(raw: dict[str, Any], name: str, allowed: set[str]) -> dict[str, Any]:
    section: dict[str, Any] = raw.get(name, {})
    _reject_unknown(f"[{name}]", section, allowed)
    return section


def _reject_unknown(where: str, d: dict[str, Any], allowed: set[str]) -> None:
    unknown = set(d) - allowed
    if unknown:
        raise ValueError(f"unknown config keys in {where}: {sorted(unknown)}")


def _renamed(d: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    return {mapping.get(k, k): v for k, v in d.items()}
