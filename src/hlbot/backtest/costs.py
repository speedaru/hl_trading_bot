"""Fee, slippage and funding helpers. All rates come from CostConfig."""

from hlbot.backtest.types import Direction


def slipped_price(price: float, direction: Direction, *, is_entry: bool, slippage: float) -> float:
    """Move a reference price against us by `slippage` (fraction).

    Buying (long entry, short exit) pays more; selling (long exit, short entry) receives less.
    """
    buying = (direction is Direction.LONG) == is_entry
    return price * (1 + slippage) if buying else price * (1 - slippage)


def fee(notional: float, rate: float) -> float:
    """Exchange fee: charged on notional (not on margin), so leverage doesn't reduce it."""
    return abs(notional) * rate


def funding_cost(notional: float, direction: Direction, rate_hourly: float, hours: float) -> float:
    """Funding paid (positive) or received (negative) for holding `notional` for `hours`.

    With a positive rate, longs pay shorts.
    """
    return direction.value * notional * rate_hourly * hours
