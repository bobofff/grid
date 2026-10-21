import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from grid_bot.dashboard import exit_grid, performance_snapshot, strategy_health_snapshot
from grid_bot.state import RunnerState, StateStore, load_runner_state, save_runner_state


class PerformanceSnapshotTests(unittest.TestCase):
    def _snapshot(self, started_at: datetime):
        with tempfile.TemporaryDirectory() as directory:
            state_file = str(Path(directory) / "state.json")
            save_runner_state(StateStore(state_file), RunnerState(
                baseline_account_equity=1_000.0,
                baseline_started_at=started_at.isoformat(),
                phase="RUNNING",
            ))
            config = SimpleNamespace(state_file=state_file, initial_capital_usdt=1_000.0)
            return performance_snapshot(config, 999.0, [], [])

    def test_hides_annualization_for_a_grid_younger_than_one_day(self):
        snapshot = self._snapshot(datetime.now(timezone.utc) - timedelta(hours=1))

        self.assertIsNone(snapshot["annualized_roi"])

    def test_keeps_annualization_after_a_full_day(self):
        snapshot = self._snapshot(datetime.now(timezone.utc) - timedelta(days=2))

        self.assertIsNotNone(snapshot["annualized_roi"])


class ExitGridTests(unittest.TestCase):
    def test_stops_then_cancels_flattens_and_locks_the_grid(self):
        class FakeExchange:
            def __init__(self):
                self.calls = []

            def verify_account(self):
                self.calls.append("verify")

            def cancel_all(self, symbol):
                self.calls.append(("cancel", symbol))

            def close_all_positions(self, symbol):
                self.calls.append(("close", symbol))

            def assert_clean_symbol(self):
                self.calls.append("clean")

        with tempfile.TemporaryDirectory() as directory:
            state_file = str(Path(directory) / "state.json")
            save_runner_state(StateStore(state_file), RunnerState(
                phase="RUNNING", orders={"old-order": {"price": "82000"}},
                deferred_orders=[{"price": "81600"}], approved_expired_order_ids=["expired-order"],
            ))
            config = SimpleNamespace(
                execution_mode="binance_testnet", dry_run=False,
                state_file=state_file, symbol="BTCUSDT",
            )
            exchange = FakeExchange()
            with patch("grid_bot.dashboard.GridConfig.from_env", return_value=config), \
                 patch("grid_bot.dashboard.pm2_status", side_effect=[
                     {"known": True, "status": "online"},
                     {"known": True, "status": "stopped"},
                 ]), \
                 patch("grid_bot.dashboard.pm2_action", return_value={"status": "stopped"}) as stop, \
                 patch("grid_bot.dashboard.BinanceUsdmExchange", return_value=exchange):
                result = exit_grid()

            self.assertIn("已撤销全部挂单并平仓", result["message"])
            stop.assert_called_once_with("stop")
            self.assertEqual(exchange.calls, [
                "verify", ("cancel", "BTCUSDT"), ("close", "BTCUSDT"), "clean",
            ])
            state = load_runner_state(StateStore(state_file))
            self.assertEqual(state.phase, "STOPPED:MANUAL_CLOSE")
            self.assertEqual(state.orders, {})
            self.assertEqual(state.deferred_orders, [])
            self.assertEqual(state.approved_expired_order_ids, [])


class StrategyHealthTests(unittest.TestCase):
    def test_flags_a_deferred_close_for_an_open_short(self):
        state = RunnerState(phase="RUNNING", orders={"entry": {}}, deferred_orders=[{
            "price": 106.0, "side": "BUY", "purpose": "CLOSE_SHORT", "reduce_only": True,
        }])
        snapshot = strategy_health_snapshot(
            SimpleNamespace(), state,
            [{"positionSide": "SHORT", "positionAmt": "-0.92"}], [],
            {"status": "online"},
        )

        self.assertEqual(snapshot["level"], "warning")
        self.assertEqual(snapshot["deferred_order_count"], 1)
        self.assertIn("计划中的平空单尚未提交", [item["title"] for item in snapshot["checks"]])

    def test_flags_orders_left_unattended_when_pm2_is_down(self):
        snapshot = strategy_health_snapshot(
            SimpleNamespace(), RunnerState(phase="RUNNING", orders={"entry": {}}), [], [],
            {"status": "stopped"},
        )

        self.assertEqual(snapshot["level"], "critical")
        self.assertIn("PM2 策略守护未运行", [item["title"] for item in snapshot["checks"]])

    def test_recognises_a_hedge_mode_closing_leg_from_runner_purpose(self):
        snapshot = strategy_health_snapshot(
            SimpleNamespace(), RunnerState(phase="RUNNING", orders={
                "close": {"purpose": "CLOSE_SHORT", "side": "BUY"},
            }), [{"positionSide": "SHORT", "positionAmt": "-0.92"}],
            [{"position_side": "SHORT", "side": "BUY", "reduce_only": False}],
            {"status": "online"},
        )

        self.assertEqual(snapshot["level"], "ok")

