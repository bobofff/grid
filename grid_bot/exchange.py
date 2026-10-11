from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal, ROUND_DOWN
import hashlib
import hmac
import json
import logging
import ssl
import time
from typing import Any
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import EquitySnapshot, GridOrder


class Exchange(ABC):
    """Implement this interface with Binance Futures testnet before enabling live trading."""

    @abstractmethod
    def submit_grid_order(self, order: GridOrder) -> str: ...

    @abstractmethod
    def cancel_all(self, symbol: str) -> None: ...

    @abstractmethod
    def close_all_positions(self, symbol: str) -> None: ...

    @abstractmethod
    def equity_snapshot(self) -> EquitySnapshot: ...


class PaperExchange(Exchange):
    def __init__(self) -> None:
        self.orders: list[GridOrder] = []
        self.exit_actions: list[str] = []

    def submit_grid_order(self, order: GridOrder) -> str:
        self.orders.append(order)
        return f"paper-{len(self.orders)}"

    def cancel_all(self, symbol: str) -> None:
        self.orders.clear()
        self.exit_actions.append(f"cancel:{symbol}")

    def close_all_positions(self, symbol: str) -> None:
        self.exit_actions.append(f"close:{symbol}")

    def equity_snapshot(self) -> EquitySnapshot:
        return EquitySnapshot(0, 0, 0, 0)


class BinanceApiError(RuntimeError):
    pass


class BinanceTransportError(BinanceApiError):
    """A request did not reach Binance reliably and may be retried by the caller."""


class BinanceUsdmExchange(Exchange):
    """USDⓈ-M Testnet adapter. It deliberately cannot use a production base URL."""

    BASE_URL = "https://testnet.binancefuture.com"
    # Testnet occasionally serves /time and signed endpoints from nodes whose
    # clocks disagree, or delays the /time response for seconds.  Binance rejects
    # a timestamp even slightly in the future, while a 5-second recvWindow permits
    # this deliberately older timestamp.
    SIGNED_TIMESTAMP_SAFETY_MARGIN_MS = 3_000

    def __init__(self, config, opener=urlopen) -> None:
        if not config.binance_testnet:
            raise ValueError("BinanceUsdmExchange is Testnet-only in this version")
        if not config.binance_api_key or not config.binance_api_secret:
            raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET are required for Testnet")
        self.config = config
        self._opener = opener
        self._symbol_filters: dict[str, Decimal] | None = None
        self._server_time_offset_ms = 0

    def sync_server_time(self) -> None:
        """Align signed-request timestamps with Binance Testnet's clock."""
        started_at_ms = int(time.time() * 1000)
        data = self._request("GET", "/fapi/v1/time")
        finished_at_ms = int(time.time() * 1000)
        try:
            server_time_ms = int(data["serverTime"])
        except (KeyError, TypeError, ValueError) as error:
            raise BinanceApiError("Binance time endpoint returned an invalid serverTime") from error
        self._server_time_offset_ms = server_time_ms - ((started_at_ms + finished_at_ms) // 2)
        logging.info("Binance Testnet clock synchronized (offset=%d ms)", self._server_time_offset_ms)

    def _build_request(self, method: str, path: str, params: dict[str, Any] | None,
                       signed: bool) -> Request:
        values = dict(params or {})
        if signed:
            values["timestamp"] = (int(time.time() * 1000) + self._server_time_offset_ms
                                   - self.SIGNED_TIMESTAMP_SAFETY_MARGIN_MS)
            values["recvWindow"] = self.config.binance_recv_window
            query = urlencode(values)
            values["signature"] = hmac.new(
                self.config.binance_api_secret.encode(), query.encode(), hashlib.sha256
            ).hexdigest()
        query = urlencode(values)
        url = f"{self.BASE_URL}{path}" + (f"?{query}" if query else "")
        return Request(url, method=method, headers={"X-MBX-APIKEY": self.config.binance_api_key})

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None,
                 signed: bool = False) -> Any:
        # Retrying a POST could duplicate an order.  GET, PUT and DELETE endpoints
        # used here are safe to repeat, so give transient Testnet TLS/socket faults
        # time to clear without turning a momentary outage into a stopped bot.
        attempts = 5 if method in {"GET", "PUT", "DELETE"} else 1
        for timestamp_attempt in range(3 if signed else 1):
            retry_after_clock_sync = False
            for attempt in range(attempts):
                request = self._build_request(method, path, params, signed)
                try:
                    with self._opener(request, timeout=10) as response:
                        payload = json.loads(response.read().decode())
                    if isinstance(payload, dict) and payload.get("code", 0) not in (0, 200):
                        raise BinanceApiError(f"Binance API error {payload.get('code')}: {payload.get('msg')}")
                    return payload
                except HTTPError as error:
                    # A -1021 request was rejected before Binance acted on it, so it is
                    # safe to synchronize the clock and replay it, even for writes.
                    try:
                        detail = error.read().decode("utf-8", errors="replace").strip()
                        code = json.loads(detail).get("code")
                    except (OSError, ValueError):
                        detail, code = "<response body unavailable>", None
                    if signed and code == -1021 and timestamp_attempt < 2:
                        logging.warning("Binance rejected request timestamp; synchronizing clock and retrying")
                        self.sync_server_time()
                        retry_after_clock_sync = True
                        break
                    raise BinanceApiError(
                        f"Binance API error: {method} {path}: HTTP {error.code} {error.reason}; {detail}"
                    ) from error
                except (URLError, ssl.SSLError, TimeoutError) as error:
                    if attempt + 1 < attempts:
                        time.sleep(0.25 * (attempt + 1))
                        continue
                    raise BinanceTransportError(
                        f"Binance transport failed: {method} {path}: {error}"
                    ) from error
                except BinanceApiError:
                    raise
                except Exception as error:  # urllib exposes several transport-specific exceptions
                    raise BinanceTransportError(
                        f"Binance transport failed: {method} {path}: {error}"
                    ) from error
            if retry_after_clock_sync:
                continue
        raise AssertionError("timestamp retry loop completed without returning or raising")

    def verify_account(self, load_symbol_filters: bool = True) -> None:
        self.sync_server_time()
        mode = self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        if self.config.binance_hedge_mode_required and not mode.get("dualSidePosition", False):
            raise BinanceApiError("Neutral grid requires Binance Hedge Mode; enable it manually before running")
        if load_symbol_filters:
            self._load_symbol_filters()

    def set_configured_leverage(self) -> None:
        """Set the symbol's Testnet leverage and confirm Binance accepted it."""
        payload = self._request("POST", "/fapi/v1/leverage", {
            "symbol": self.config.symbol,
            "leverage": self.config.leverage,
        }, signed=True)
        try:
            actual_leverage = int(payload["leverage"])
        except (KeyError, TypeError, ValueError) as error:
            raise BinanceApiError("Binance leverage endpoint returned an invalid leverage") from error
        if actual_leverage != self.config.leverage:
            raise BinanceApiError(
                f"Binance applied {actual_leverage}x leverage instead of configured {self.config.leverage}x"
            )
        logging.info("Binance Testnet leverage set to %dx for %s", actual_leverage, self.config.symbol)

    def _load_symbol_filters(self) -> None:
        data = self._request("GET", "/fapi/v1/exchangeInfo")
        symbol = next((item for item in data["symbols"] if item["symbol"] == self.config.symbol), None)
        if not symbol or symbol["status"] != "TRADING":
            raise BinanceApiError(f"Symbol {self.config.symbol} is unavailable for trading")
        filters = {item["filterType"]: item for item in symbol["filters"]}
        notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL")
        if not notional_filter:
            raise BinanceApiError(f"Symbol {self.config.symbol} does not expose a minimum-notional filter")
        minimum_notional = notional_filter.get("notional", notional_filter.get("minNotional"))
        if minimum_notional is None:
            raise BinanceApiError(f"Symbol {self.config.symbol} has an invalid minimum-notional filter")
        self._symbol_filters = {
            "tick": Decimal(filters["PRICE_FILTER"]["tickSize"]),
            "step": Decimal(filters["LOT_SIZE"]["stepSize"]),
            "min_qty": Decimal(filters["LOT_SIZE"]["minQty"]),
            "min_notional": Decimal(minimum_notional),
        }

    def symbol_order_rules(self) -> dict[str, Decimal]:
        """Return active Testnet symbol constraints used to size a grid."""
        if self._symbol_filters is None:
            self._load_symbol_filters()
        assert self._symbol_filters is not None
        return dict(self._symbol_filters)

    @staticmethod
    def _round_down(value: Decimal, quantum: Decimal) -> Decimal:
        return (value / quantum).to_integral_value(rounding=ROUND_DOWN) * quantum

    def _position_side(self, order: GridOrder) -> str:
        return "LONG" if order.purpose.value.endswith("LONG") else "SHORT"

    def submit_grid_order(self, order: GridOrder) -> str:
        if self._symbol_filters is None:
            self._load_symbol_filters()
        assert self._symbol_filters is not None
        price = self._round_down(Decimal(str(order.price)), self._symbol_filters["tick"])
        quantity = self._round_down(Decimal(str(self.config.order_notional_usdt)) / price, self._symbol_filters["step"])
        if quantity < self._symbol_filters["min_qty"]:
            raise BinanceApiError("ORDER_NOTIONAL_USDT is below the symbol minimum quantity")
        notional = price * quantity
        if notional < self._symbol_filters["min_notional"]:
            raise BinanceApiError(
                "ORDER_NOTIONAL_USDT becomes smaller than the symbol minimum notional after price/quantity rounding"
            )
        # In Hedge Mode Binance uses positionSide for the separate legs; reduceOnly is
        # intentionally omitted because Binance rejects it in Hedge Mode.
        payload = self._request("POST", "/fapi/v1/order", {
            "symbol": self.config.symbol, "side": order.side.value, "positionSide": self._position_side(order),
            "type": "LIMIT", "timeInForce": "GTC", "quantity": format(quantity, "f"),
            "price": format(price, "f"), "newOrderRespType": "RESULT",
            **({"newClientOrderId": order.client_order_id} if order.client_order_id else {}),
        }, signed=True)
        return str(payload["orderId"])

    def cancel_all(self, symbol: str) -> None:
        self._request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol}, signed=True)

    def close_all_positions(self, symbol: str) -> None:
        positions = self._request("GET", "/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
        for position in positions:
            amount = Decimal(position["positionAmt"])
            if not amount:
                continue
            side = "SELL" if position["positionSide"] == "LONG" else "BUY"
            self._request("POST", "/fapi/v1/order", {
                "symbol": symbol, "side": side, "positionSide": position["positionSide"], "type": "MARKET",
                "quantity": format(abs(amount), "f"), "newOrderRespType": "RESULT",
            }, signed=True)

    def equity_snapshot(self) -> EquitySnapshot:
        account = self._request("GET", "/fapi/v2/account", signed=True)
        # totalMarginBalance is wallet balance plus unrealized PnL, and therefore
        # naturally reflects settled PnL, commissions, and funding already applied.
        return EquitySnapshot(0, 0, 0, 0, equity_override=float(account["totalMarginBalance"]))

    def mark_price(self) -> float:
        data = self._request("GET", "/fapi/v1/premiumIndex", {"symbol": self.config.symbol})
        return float(data["markPrice"])

    def positions(self) -> list[dict[str, Any]]:
        """Read non-zero positions for the configured symbol without mutating it."""
        data = self._request("GET", "/fapi/v2/positionRisk", {"symbol": self.config.symbol}, signed=True)
        return [item for item in data if Decimal(item["positionAmt"]) != 0]

    def open_orders(self) -> list[dict[str, Any]]:
        """Read outstanding orders for the configured symbol without mutating them."""
        return self._request("GET", "/fapi/v1/openOrders", {"symbol": self.config.symbol}, signed=True)

    def assert_clean_symbol(self) -> None:
        """Prevent a grid from being started on top of manual/old bot exposure."""
        open_orders = self.open_orders()
        if open_orders:
            raise BinanceApiError(f"{self.config.symbol} has existing open orders; cancel/reconcile them before starting")
        if self.positions():
            raise BinanceApiError(f"{self.config.symbol} has an existing position; close it before starting")

    def create_listen_key(self) -> str:
        return self._request("POST", "/fapi/v1/listenKey")["listenKey"]

    def keepalive_listen_key(self) -> None:
        self._request("PUT", "/fapi/v1/listenKey")
