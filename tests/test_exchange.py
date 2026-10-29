import json
import ssl
import unittest
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

from grid_bot.exchange import (
    BinanceApiError,
    BinanceRateLimitError,
    BinanceTimestampError,
    BinanceUsdmExchange,
)
from grid_bot.models import GridOrder, Purpose, Side


class _Response:
    def __init__(self, payload=None):
        self.payload = payload or {"markPrice": "84000"}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


class ExchangeRequestTests(unittest.TestCase):
    def test_mainnet_uses_the_production_rest_endpoint(self):
        config = SimpleNamespace(execution_mode="binance_mainnet", binance_testnet=False,
                                 binance_api_key="key", binance_api_secret="secret",
                                 binance_recv_window=5000, symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=lambda *_args, **_kwargs: _Response())

        request = exchange._build_request("GET", "/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"}, False)

        self.assertTrue(request.full_url.startswith("https://fapi.binance.com/fapi/v1/premiumIndex"))
        self.assertFalse(exchange.is_testnet)

    def test_rejects_mode_and_testnet_flag_mismatch(self):
        config = SimpleNamespace(execution_mode="binance_mainnet", binance_testnet=True,
                                 binance_api_key="key", binance_api_secret="secret",
                                 binance_recv_window=5000, symbol="BTCUSDT")

        with self.assertRaisesRegex(ValueError, "BINANCE_TESTNET=false"):
            BinanceUsdmExchange(config)

    def test_rejects_new_grid_when_available_margin_is_too_small(self):
        config = SimpleNamespace(execution_mode="binance_mainnet", binance_testnet=False,
                                 binance_api_key="key", binance_api_secret="secret",
                                 binance_recv_window=5000, symbol="BTCUSDT",
                                 initial_capital_usdt=1000)
        exchange = BinanceUsdmExchange(config, opener=lambda *_args, **_kwargs: _Response({
            "availableBalance": "99.89",
        }))

        with self.assertRaisesRegex(BinanceApiError, "Available margin 99.89 USDT"):
            exchange.assert_starting_capital_available()

    def test_retries_transient_read_error(self):
        calls = 0

        def opener(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise URLError("TLS connection closed")
            return _Response()

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=opener)
        self.assertEqual(exchange.mark_price(), 84000.0)
        self.assertEqual(calls, 3)

    def test_retries_ssl_eof_on_read(self):
        calls = 0

        def opener(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol")
            return _Response()

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=opener)
        with patch("grid_bot.exchange.time.sleep"):
            self.assertEqual(exchange.mark_price(), 84000.0)
        self.assertEqual(calls, 2)

    def test_retries_idempotent_cancel_on_ssl_eof(self):
        calls = 0

        def opener(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol")
            return _Response({})

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=opener)
        with patch("grid_bot.exchange.time.sleep"):
            exchange.cancel_all("BTCUSDT")
        self.assertEqual(calls, 2)

    def test_rejects_notional_that_falls_below_minimum_after_rounding(self):
        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT", order_notional_usdt=50)
        exchange = BinanceUsdmExchange(config, opener=lambda *_args, **_kwargs: _Response())
        exchange._symbol_filters = {"tick": Decimal("0.1"), "step": Decimal("0.0001"),
                                    "min_qty": Decimal("0.0001"), "min_notional": Decimal("50")}
        order = GridOrder(0, 84000.09, Side.BUY, Purpose.OPEN_LONG, False, 0)

        with self.assertRaisesRegex(BinanceApiError, "minimum notional"):
            exchange.submit_grid_order(order)

    def test_sets_and_confirms_configured_leverage(self):
        requests = []

        def opener(request, **_kwargs):
            requests.append(request)
            return _Response({"leverage": 5})

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT", leverage=5)
        exchange = BinanceUsdmExchange(config, opener=opener)
        exchange.set_configured_leverage()

        request = requests[0]
        self.assertEqual(request.method, "POST")
        values = parse_qs(urlparse(request.full_url).query)
        self.assertEqual(values["symbol"], ["BTCUSDT"])
        self.assertEqual(values["leverage"], ["5"])

    def test_rejects_a_leverage_response_that_does_not_match_configuration(self):
        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT", leverage=5)
        exchange = BinanceUsdmExchange(config, opener=lambda *_args, **_kwargs: _Response({"leverage": 20}))

        with self.assertRaisesRegex(BinanceApiError, "20x leverage instead of configured 5x"):
            exchange.set_configured_leverage()

    def test_timestamp_rejection_syncs_clock_and_replays_signed_request(self):
        requests = []

        def opener(request, **_kwargs):
            requests.append(request)
            path = urlparse(request.full_url).path
            if path == "/fapi/v1/time":
                return _Response({"serverTime": 102000})
            if len([item for item in requests if urlparse(item.full_url).path == "/fapi/v2/account"]) == 1:
                raise HTTPError(request.full_url, 400, "Bad Request", None,
                                BytesIO(b'{"code":-1021,"msg":"Timestamp outside recvWindow"}'))
            return _Response({"totalMarginBalance": "123.45"})

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=opener)
        with patch("grid_bot.exchange.time.time", return_value=100.0):
            self.assertEqual(exchange.equity_snapshot().equity(0), 123.45)

        account_requests = [item for item in requests if urlparse(item.full_url).path == "/fapi/v2/account"]
        self.assertEqual(len(account_requests), 2)
        self.assertEqual(parse_qs(urlparse(account_requests[0].full_url).query)["timestamp"], ["99000"])
        self.assertEqual(parse_qs(urlparse(account_requests[1].full_url).query)["timestamp"], ["101000"])

    def test_clock_sync_uses_response_receive_time_to_avoid_future_timestamp(self):
        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=10000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=lambda *_args, **_kwargs: _Response({
            "serverTime": 100050,
        }))

        with patch("grid_bot.exchange.time.time", side_effect=[100.000, 100.120]):
            exchange.sync_server_time()

        self.assertEqual(exchange._server_time_offset_ms, -70)

    def test_rate_limit_captures_binance_ban_deadline(self):
        def opener(request, **_kwargs):
            raise HTTPError(request.full_url, 418, "I'm a teapot", None, BytesIO(
                b'{"code":-1003,"msg":"Way too many requests; IP banned until 1234567890123."}'
            ))

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=10000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=opener)

        with self.assertRaises(BinanceRateLimitError) as caught:
            exchange.equity_snapshot()

        self.assertEqual(caught.exception.code, -1003)
        self.assertEqual(caught.exception.retry_at_ms, 1234567890123)

    def test_stops_timestamp_replays_after_one_clock_sync(self):
        requests = []

        def opener(request, **_kwargs):
            requests.append(request)
            path = urlparse(request.full_url).path
            if path == "/fapi/v1/time":
                return _Response({"serverTime": 103000})
            if len([item for item in requests if urlparse(item.full_url).path == "/fapi/v2/account"]) < 3:
                raise HTTPError(request.full_url, 400, "Bad Request", None,
                                BytesIO(b'{"code":-1021,"msg":"Timestamp outside recvWindow"}'))
            return _Response({"totalMarginBalance": "123.45"})

        config = SimpleNamespace(binance_testnet=True, binance_api_key="key",
                                 binance_api_secret="secret", binance_recv_window=5000,
                                 symbol="BTCUSDT")
        exchange = BinanceUsdmExchange(config, opener=opener)
        with patch("grid_bot.exchange.time.time", return_value=100.0):
            with self.assertRaisesRegex(BinanceTimestampError, "Timestamp outside recvWindow"):
                exchange.equity_snapshot()

        account_requests = [item for item in requests if urlparse(item.full_url).path == "/fapi/v2/account"]
        self.assertEqual(len(account_requests), 2)
