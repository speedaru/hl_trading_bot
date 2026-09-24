from pathlib import Path

import pytest

from hlbot.data.recorder.config import config_from_dict, load_config

REPO_CONFIG = Path(__file__).parent.parent / "config" / "recorder.toml"


def test_repo_config_loads() -> None:
    cfg = load_config(REPO_CONFIG)
    assert cfg.coins == ("BTC", "ETH", "SOL")
    assert [b.stream for b in cfg.books] == ["book", "book_coarse"]
    assert cfg.books[0].n_sig_figs is None
    assert cfg.books[1].n_sig_figs == 4
    assert cfg.books[1].interval_s == 30
    assert cfg.store_raw_trades
    assert cfg.ws_ping_s == 20
    assert cfg.flush_max_seconds == 60


def test_minimal_config_uses_defaults() -> None:
    cfg = config_from_dict({"coins": ["BTC"], "data_dir": "x"})
    assert cfg.data_dir == Path("x")
    assert [b.stream for b in cfg.books] == ["book"]
    assert cfg.log_file is None


def test_unknown_key_is_rejected() -> None:
    # A typo like "stale_secs" must not silently fall back to a default.
    with pytest.raises(ValueError, match="stale_secs"):
        config_from_dict({"coins": ["BTC"], "data_dir": "x", "ws": {"stale_secs": 5}})


@pytest.mark.parametrize(
    "raw",
    [
        {"coins": [], "data_dir": "x"},
        {"coins": ["BTC", "BTC"], "data_dir": "x"},
        {"coins": ["BTC"], "data_dir": "x", "cadence": {"ctx_s": 7}},  # doesn't divide a day
        {"coins": ["BTC"], "data_dir": "x", "ws": {"ping_s": 60}},  # server idles out at 60s
        {"coins": ["BTC"], "data_dir": "x", "books": [{"stream": "book", "levels": 21}]},
        {"coins": ["BTC"], "data_dir": "x", "books": [{"stream": "b", "levels": 5}]},
        {"coins": ["BTC"], "data_dir": "x", "books": [{"stream": "book", "n_sig_figs": 6}]},
        {"coins": ["BTC"], "data_dir": "x", "books": [{"stream": "book"}, {"stream": "book"}]},
    ],
)
def test_invalid_configs(raw: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        config_from_dict(raw)
