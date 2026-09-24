"""Entry point: uv run python -m hlbot.data.recorder --config config/recorder.toml"""

import argparse
import asyncio
import logging
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from hlbot.data.recorder.app import Recorder
from hlbot.data.recorder.config import RecorderConfig, load_config
from hlbot.data.recorder.rest import RestInfoClient


def setup_logging(log_file: Path | None) -> None:
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s: %(message)s")
    fmt.converter = time.gmtime  # log timestamps in UTC, like the data
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in handlers:
        h.setFormatter(fmt)
        root.addHandler(h)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def run(cfg: RecorderConfig) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def on_signal(_sig: int, _frame: object) -> None:
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Windows (local development): no loop signal handlers; Ctrl+C still works.
            signal.signal(sig, on_signal)
    rest = RestInfoClient(cfg.rest_url, cfg.rest_timeout_s)
    await Recorder(cfg, rest).run(stop)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Record Hyperliquid public market data.")
    parser.add_argument("--config", type=Path, default=Path("config/recorder.toml"))
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    setup_logging(cfg.log_file)
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
