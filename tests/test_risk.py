import unittest

from grid_bot.config import GridConfig
from grid_bot.models import EquitySnapshot
from grid_bot.risk import ExitReason, RoiRiskManager


def config() -> GridConfig:
    return GridConfig("BTCUSDT", 90, 110, 10, "arithmetic", 10, 2, 100, 1000,
                      -0.20, 0.15, 0.10, 0.30, 0.08, "runtime/state.json", 1, True,
                      "paper", "", "", True, 5000, True, False)


def snapshot(roi: float) -> EquitySnapshot:
    return EquitySnapshot(realized_pnl=roi * 1000, unrealized_pnl=0, fees=0, funding=0)


class RoiRiskManagerTests(unittest.TestCase):
    def test_hard_stop(self) -> None:
        _, reason = RoiRiskManager(config()).evaluate(snapshot(-0.20))
        self.assertEqual(reason, ExitReason.HARD_STOP_LOSS)

    def test_trailing_profit_uses_peak(self) -> None:
        manager = RoiRiskManager(config())
        self.assertEqual(manager.evaluate(snapshot(0.42))[1], ExitReason.NONE)
        self.assertEqual(manager.evaluate(snapshot(0.35))[1], ExitReason.NONE)
        self.assertEqual(manager.evaluate(snapshot(0.34))[1], ExitReason.TRAILING_TAKE_PROFIT)

    def test_breakeven_floor_before_trailing_is_armed(self) -> None:
        manager = RoiRiskManager(config())
        manager.evaluate(snapshot(0.16))
        self.assertEqual(manager.evaluate(snapshot(0.10))[1], ExitReason.BREAKEVEN_PROTECTION)
