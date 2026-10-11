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
