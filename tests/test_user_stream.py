import unittest

from grid_bot.user_stream import parse_order_update


class UserStreamTests(unittest.TestCase):
    def test_extracts_order_trade_update(self) -> None:
        event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": 123, "X": "FILLED", "x": "TRADE", "s": "BTCUSDT", "c": "grid-1"}}
        update = parse_order_update(event)
        assert update is not None
        self.assertEqual(update.order_id, "123")
        self.assertEqual(update.status, "FILLED")

    def test_ignores_non_order_event(self) -> None:
        self.assertIsNone(parse_order_update({"e": "ACCOUNT_UPDATE"}))
