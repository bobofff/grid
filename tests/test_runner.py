import tempfile
import unittest
from pathlib import Path
from queue import Queue
from types import SimpleNamespace

from grid_bot.config import GridConfig
from grid_bot.models import EquitySnapshot
from grid_bot.runner import BinanceUserStream, GridRunner
from grid_bot.state import RunnerState, StateStore, load_runner_state, save_runner_state


def config(state_file: str) -> GridConfig:
    return GridConfig("BTCUSDT", 80, 120, 4, "arithmetic", 10, 2, 100, 1000,
                      -0.20, 0.15, 0.10, 0.30, 0.08, state_file, 1, True,
                      "binance_testnet", "key", "secret", True, 5000, True, True, "")


class _Exchange:
    def __init__(self):
        self.actions = []

    def verify_account(self):
        self.actions.append("verify")

    def cancel_all(self, symbol):
        self.actions.append(f"cancel:{symbol}")

    def close_all_positions(self, symbol):
        self.actions.append(f"close:{symbol}")

    def assert_clean_symbol(self):
        self.actions.append("clean")

    def assert_starting_capital_available(self):
        self.actions.append("capital")

    def mark_price(self):
        return 100.0

    def set_configured_leverage(self):
        self.actions.append("leverage")

    def equity_snapshot(self):
        return EquitySnapshot(0, 0, 0, 0, equity_override=1000)

    def submit_grid_order(self, _order):
        return "order-id"

    def create_listen_key(self):
        return "listen-key"


class _Stream:
    def start(self):
        pass

    def wait_connected(self, timeout):
        pass

    def stop(self):
        pass


class GridRunnerRecoveryTests(unittest.TestCase):
    def test_user_stream_selects_mainnet_websocket_endpoint(self):
        stream = BinanceUserStream(SimpleNamespace(is_testnet=False), Queue())

        self.assertEqual(stream.url_template, "wss://fstream.binance.com/ws/{}")

    def test_terminal_risk_run_refuses_automatic_reentry(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = str(Path(directory) / "state.json")
            store = StateStore(state_file)
            save_runner_state(store, RunnerState(phase="STOPPED:HARD_STOP_LOSS"))
            exchange = _Exchange()
            runner = GridRunner(config(state_file), exchange)
            runner.stream = _Stream()

            with self.assertRaisesRegex(RuntimeError, "manual reset required"):
                runner.start()

        self.assertEqual(exchange.actions, ["verify"])
        self.assertEqual(runner.state.phase, "STOPPED:HARD_STOP_LOSS")

    def test_new_grid_gets_a_durable_grid_run_id(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = str(Path(directory) / "state.json")
            exchange = _Exchange()
            runner = GridRunner(config(state_file), exchange)
            runner.stream = _Stream()

            runner.start()

            persisted = StateStore(state_file)
            self.assertIsNotNone(runner.state.grid_run_id)
            self.assertEqual(len(runner.state.grid_run_id), 12)
            self.assertEqual(runner.state.grid_run_id, load_runner_state(persisted).grid_run_id)
