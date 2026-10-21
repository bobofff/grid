from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from grid_bot.config import GridConfig


class DotenvPriorityTests(unittest.TestCase):
    def test_durable_dotenv_overrides_stale_process_environment(self) -> None:
        values = {
            "SYMBOL": "BTCUSDT", "GRID_LOWER_PRICE": "80", "GRID_UPPER_PRICE": "120",
            "GRID_COUNT": "4", "GRID_SPACING": "arithmetic", "ORDER_NOTIONAL_USDT": "10",
            "LEVERAGE": "2", "START_REFERENCE_PRICE": "100", "INITIAL_CAPITAL_USDT": "1000",
            "HARD_STOP_LOSS_ROI": "-0.2", "BREAKEVEN_ARM_ROI": "0.15",
            "BREAKEVEN_FLOOR_ROI": "0.1", "TRAILING_ARM_ROI": "0.3",
            "TRAILING_DRAWDOWN_ROI": "0.08", "STATE_FILE": "runtime/state.json",
            "LOOP_INTERVAL_SECONDS": "2", "DRY_RUN": "false", "EXECUTION_MODE": "binance_testnet",
            "BINANCE_API_KEY": "key", "BINANCE_API_SECRET": "secret", "BINANCE_TESTNET": "true",
            "BINANCE_RECV_WINDOW": "5000", "BINANCE_HEDGE_MODE_REQUIRED": "true",
            "RUNNER_ENABLED": "true",
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            **values, "GRID_LOWER_PRICE": "81000", "GRID_UPPER_PRICE": "83000", "GRID_COUNT": "60",
        }, clear=True):
            path = Path(directory) / ".env"
            path.write_text("\n".join(f"{key}={value}" for key, value in values.items()))
            config = GridConfig.from_env(str(path))

        self.assertEqual((config.lower_price, config.upper_price, config.grid_count), (80.0, 120.0, 4))

