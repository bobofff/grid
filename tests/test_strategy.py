import unittest

from grid_bot.config import GridConfig
from grid_bot.models import Purpose, Side
from grid_bot.strategy import StaticNeutralGrid


def config() -> GridConfig:
    return GridConfig("BTCUSDT", 90, 110, 4, "arithmetic", 10, 2, 100, 1000,
                      -0.20, 0.15, 0.10, 0.30, 0.08, "runtime/state.json", 1, True,
                      "paper", "", "", True, 5000, True, False)


class StaticNeutralGridTests(unittest.TestCase):
    def test_seeds_buys_below_and_sells_above_reference(self) -> None:
        grid = StaticNeutralGrid(config())
        orders = grid.initial_orders()
        self.assertTrue(all(o.side is Side.BUY for o in orders if o.price < 100))
        self.assertTrue(all(o.side is Side.SELL for o in orders if o.price > 100))

    def test_long_cycle_replaces_with_reduce_only_close_then_entry(self) -> None:
        grid = StaticNeutralGrid(config())
        entry = next(o for o in grid.initial_orders() if o.purpose is Purpose.OPEN_LONG)
        close = grid.on_fill(entry)
        self.assertEqual(close.purpose, Purpose.CLOSE_LONG)
        self.assertTrue(close.reduce_only)
        restored = grid.on_fill(close)
        self.assertEqual(restored, entry)
