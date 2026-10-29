import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import grid_bot.dashboard as dashboard
from grid_bot.dashboard import exit_grid, performance_snapshot, strategy_health_snapshot
from grid_bot.journal import TradeJournal
from grid_bot.models import GridOrder, Purpose, Side
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


class FundingPlanTests(unittest.TestCase):
    def test_minimum_margin_accounts_for_equal_order_allocation(self):
        class Strategy:
            def initial_orders(self):
                return [SimpleNamespace(price=100.0), SimpleNamespace(price=90.0)]

        rules = {"step": Decimal("0.1"), "min_qty": Decimal("0.1"),
                 "min_notional": Decimal("10"), "tick": Decimal("0.1")}
        insufficient = dashboard.funding_plan(
            SimpleNamespace(initial_capital_usdt=6.0, leverage=5), Strategy(), rules)
        sufficient = dashboard.funding_plan(
            SimpleNamespace(initial_capital_usdt=7.2, leverage=5), Strategy(), rules)

        # The lower-priced leg needs 0.2 units (18 USDT), so each of both legs
        # needs an 18 USDT allocation: 18 * 2 / 5 = 7.2 USDT collateral.
        self.assertEqual(insufficient["minimum_margin_usdt"], 7.2)
        self.assertFalse(insufficient["sufficient"])
        self.assertTrue(sufficient["sufficient"])


class GridRunTimelineTests(unittest.TestCase):
    def test_submission_confirmation_is_presented_as_new(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal(str(Path(directory) / "events.sqlite3"))
            order = GridOrder(0, 80.0, Side.BUY, Purpose.OPEN_LONG, False, 0,
                              client_order_id="grid-order")
            journal.record_order("SUBMIT_INTENT", order, grid_run_id="run")
            journal.record_order("SUBMIT_CONFIRMED", order, binance_order_id="123",
                                 grid_run_id="run")

            timeline = journal.grid_run_orders("run")

        self.assertEqual(timeline[0]["status"], "NEW")
        self.assertEqual(timeline[0]["binance_order_id"], "123")


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

            def assert_starting_capital_available(self):
                self.calls.append("capital")

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
    def test_flags_missing_local_strategy_order_from_binance_book(self):
        state = RunnerState(phase="RUNNING", orders={
            "g-run-one": {"purpose": "OPEN_LONG", "side": "BUY"},
            "g-run-two": {"purpose": "OPEN_LONG", "side": "BUY"},
        })
        snapshot = strategy_health_snapshot(
            SimpleNamespace(), state, [],
            [{"client_id": "g-run-one", "position_side": "LONG", "side": "BUY"}],
            {"status": "online"},
        )

        self.assertEqual(snapshot["level"], "critical")
        self.assertEqual(snapshot["live_strategy_order_count"], 1)
        self.assertIn("策略订单与 Binance 挂单不一致", [item["title"] for item in snapshot["checks"]])

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


class StatusCacheTests(unittest.TestCase):
    def setUp(self):
        dashboard.invalidate_status_cache()
        self.addCleanup(dashboard.invalidate_status_cache)

    def test_shares_a_paper_status_snapshot_until_invalidated(self):
        config = SimpleNamespace(execution_mode="paper", dry_run=True)
        with patch("grid_bot.dashboard.GridConfig.from_env", return_value=config) as from_env:
            first = dashboard.status_snapshot()
            second = dashboard.status_snapshot()

        self.assertIs(first, second)
        from_env.assert_called_once()

        dashboard.invalidate_status_cache()
        with patch("grid_bot.dashboard.GridConfig.from_env", return_value=config) as from_env:
            dashboard.status_snapshot()
        from_env.assert_called_once()

