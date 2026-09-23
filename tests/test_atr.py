import pytest

from hlbot.indicators.atr import atr, true_range
from tests.conftest import bars_from_prices, make_ohlcv

# (open, high, low, close). Hand computation, period 3:
#   TR = [2, 2, 3, 1, 3.5]
#   ATR[2] = (2+2+3)/3            = 7/3
#   ATR[3] = (7/3*2 + 1)/3        = 17/9
#   ATR[4] = (17/9*2 + 3.5)/3     = 65.5/27
BARS = [
    (9.0, 10.0, 8.0, 9.0),
    (9.0, 11.0, 9.0, 10.0),
    (10.0, 12.0, 9.0, 11.0),
    (11.0, 11.0, 10.0, 10.5),
    (10.5, 14.0, 10.5, 13.0),
]


def test_true_range_hand_computed() -> None:
    df = bars_from_prices(BARS)
    tr = true_range(df["high"].to_list(), df["low"].to_list(), df["close"].to_list())
    assert tr == pytest.approx([2, 2, 3, 1, 3.5])


def test_atr_hand_computed() -> None:
    values = atr(bars_from_prices(BARS), period=3).to_list()
    assert values[:2] == [None, None]
    assert values[2:] == pytest.approx([7 / 3, 17 / 9, 65.5 / 27])


def test_atr_append_future_data_does_not_change_past() -> None:
    full = make_ohlcv(200)
    short = full.head(120)
    assert atr(short, 14).to_list() == atr(full, 14).head(120).to_list()
