import sqlite3
import tempfile
import unittest
from pathlib import Path

from grid_bot.journal import TradeJournal
from grid_bot.models import GridOrder, Purpose, Side
from grid_bot.user_stream import OrderUpdate


class TradeJournalTests(unittest.TestCase):
    def test_records_order_lifecycle_and_exit_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal.for_state_file(str(Path(directory) / "state.json"))
            order = GridOrder(2, 84000.0, Side.BUY, Purpose.OPEN_LONG, False, 2, "grid-123")
            journal.record_order("SUBMIT_INTENT", order, symbol="BTCUSDT")
            journal.record_order("SUBMIT_CONFIRMED", order, binance_order_id="456", symbol="BTCUSDT")
            update = OrderUpdate("456", "FILLED", "TRADE", "BTCUSDT", "grid-123")
            journal.record_order_update(update, {"e": "ORDER_TRADE_UPDATE", "o": {"i": 456}})
            journal.record_runner_event("PROCESS_EXIT", reason="UNHANDLED_EXCEPTION", detail="network error")

            with sqlite3.connect(journal.path) as connection:
                order_events = connection.execute(
                    "SELECT event_type, binance_order_id, symbol FROM order_events ORDER BY id"
                ).fetchall()
                exits = connection.execute(
                    "SELECT event_type, reason, detail FROM runner_events"
                ).fetchall()

        self.assertEqual(order_events, [("SUBMIT_INTENT", None, "BTCUSDT"),
                                        ("SUBMIT_CONFIRMED", "456", "BTCUSDT"),
                                        ("ORDER_FILLED", "456", "BTCUSDT")])
        self.assertEqual(exits, [("PROCESS_EXIT", "UNHANDLED_EXCEPTION", "network error")])

    def test_latest_running_at_uses_the_most_recent_grid_start(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal.for_state_file(str(Path(directory) / "state.json"))
            with sqlite3.connect(journal.path) as connection:
                connection.executemany(
                    "INSERT INTO runner_events (occurred_at, event_type) VALUES (?, ?)",
                    [("2026-09-30T14:20:15+00:00", "RUNNING"),
                     ("2026-10-20T01:52:34+00:00", "RUNNING")],
                )

            latest = journal.latest_running_at()

        self.assertEqual(latest.isoformat(), "2026-10-20T01:52:34+00:00")

    def test_grid_run_keeps_its_orders_and_lifecycle_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal.for_state_file(str(Path(directory) / "state.json"))
            journal.create_grid_run("a1b2c3d4e5f6", {"symbol": "BTCUSDT", "grid_count": 10})
            journal.start_grid_run("a1b2c3d4e5f6", "2026-10-20T01:52:34+00:00", 1_000.0)
            order = GridOrder(2, 84_000.0, Side.BUY, Purpose.OPEN_LONG, False, 2,
                              "g-a1b2c3d4e5f6-01234567890123456789")
            journal.record_order("SUBMIT_INTENT", order, symbol="BTCUSDT", grid_run_id="a1b2c3d4e5f6")
            journal.record_order("SUBMIT_CONFIRMED", order, binance_order_id="456", symbol="BTCUSDT",
                                 grid_run_id="a1b2c3d4e5f6")
            journal.record_order_update(OrderUpdate("456", "FILLED", "TRADE", "BTCUSDT",
                                                    order.client_order_id), {"e": "ORDER_TRADE_UPDATE"})

            runs = journal.grid_runs()
            orders = journal.grid_run_orders("a1b2c3d4e5f6")

        self.assertEqual(runs[0]["run_id"], "a1b2c3d4e5f6")
        self.assertEqual(runs[0]["status"], "RUNNING")
        self.assertEqual(orders, [{"client_order_id": order.client_order_id,
                                   "submitted_at": orders[0]["submitted_at"], "binance_order_id": "456",
                                   "symbol": "BTCUSDT", "level_index": 2, "price": 84_000.0,
                                   "side": "BUY", "purpose": "OPEN_LONG", "status": "FILLED"}])
