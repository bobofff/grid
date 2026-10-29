from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal, ROUND_DOWN
import hashlib
import hmac
import json
import logging
import re
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

    @abstractmethod
    def open_client_order_ids(self) -> set[str]:
        """Client IDs of the orders this account currently has working."""

    @abstractmethod
    def order_status(self, client_order_id: str) -> str | None:
        """Binance status for one client ID, or None when the order never existed."""

    @abstractmethod
    def can_submit_reduce_only(self, order: GridOrder) -> bool:
        """Whether a close leg fits the current exchange-side close capacity."""


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

    def open_client_order_ids(self) -> set[str]:
        return {order.client_order_id for order in self.orders}

    def order_status(self, client_order_id: str) -> str | None:
        return "NEW" if client_order_id in self.open_client_order_ids() else None

    def can_submit_reduce_only(self, order: GridOrder) -> bool:
        return True


class BinanceApiError(RuntimeError):
    """A Binance request reached the exchange but was rejected."""

    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class BinanceTransportError(BinanceApiError):
    """A request did not reach Binance reliably and may be retried by the caller."""


class BinanceRateLimitError(BinanceApiError):
    """Binance rejected the request because this IP has exhausted its quota."""

    def __init__(self, message: str, code: int | None = None,
                 retry_at_ms: int | None = None) -> None:
        super().__init__(message, code)
        self.retry_at_ms = retry_at_ms


class BinanceTimestampError(BinanceApiError):
    """A signed request still failed after its single clock-sync replay."""


# Binance returns this when a client order ID has never been seen for the symbol.
ORDER_NOT_FOUND_CODE = -2013


class BinanceUsdmExchange(Exchange):
    """USDⓈ-M Futures adapter for an explicitly selected Testnet or Mainnet account."""

    TESTNET_BASE_URL = "https://testnet.binancefuture.com"
    MAINNET_BASE_URL = "https://fapi.binance.com"
    # Binance rejects timestamps that are even slightly ahead of its clock.  The
    # clock offset is anchored to when the /time response is received, so it is
    # already conservative with respect to response latency; retain only a small
    # additional margin for local scheduling jitter.
    SIGNED_TIMESTAMP_SAFETY_MARGIN_MS = 1_000

    def __init__(self, config, opener=urlopen) -> None:
        execution_mode = getattr(config, "execution_mode", "binance_testnet")
        if execution_mode not in {"binance_testnet", "binance_mainnet"}:
            raise ValueError("BinanceUsdmExchange requires a Binance execution mode")
        expected_testnet = execution_mode == "binance_testnet"
        if config.binance_testnet != expected_testnet:
            expected = "true" if expected_testnet else "false"
            raise ValueError(f"{execution_mode} requires BINANCE_TESTNET={expected}")
        if not config.binance_api_key or not config.binance_api_secret:
            raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET are required")
        self.config = config
        self.execution_mode = execution_mode
        self.is_testnet = expected_testnet
        self.base_url = self.TESTNET_BASE_URL if self.is_testnet else self.MAINNET_BASE_URL
        self._opener = opener
        self._symbol_filters: dict[str, Decimal] | None = None
        self._server_time_offset_ms = 0

    def sync_server_time(self) -> None:
        """Align signed-request timestamps with the selected Binance environment."""
        started_at_ms = int(time.time() * 1000)
        data = self._request("GET", "/fapi/v1/time")
        finished_at_ms = int(time.time() * 1000)
        try:
            server_time_ms = int(data["serverTime"])
        except (KeyError, TypeError, ValueError) as error:
            raise BinanceApiError("Binance time endpoint returned an invalid serverTime") from error
        # Do not use the midpoint of a request with an asymmetric or delayed
        # response: it can produce a positive offset and a timestamp in Binance's
        # future.  Referencing the receive time instead makes the next timestamp
        # no newer than the server time observed by this response.
        self._server_time_offset_ms = server_time_ms - finished_at_ms
        logging.info("Binance %s clock synchronized (offset=%d ms, rtt=%d ms)",
                     "Testnet" if self.is_testnet else "Mainnet", self._server_time_offset_ms,
                     finished_at_ms - started_at_ms)

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
        url = f"{self.base_url}{path}" + (f"?{query}" if query else "")
        return Request(url, method=method, headers={"X-MBX-APIKEY": self.config.binance_api_key})

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None,
                 signed: bool = False) -> Any:
        # Retrying a POST could duplicate an order.  GET, PUT and DELETE endpoints
        # used here are safe to repeat, so give transient Testnet TLS/socket faults
        # time to clear without turning a momentary outage into a stopped bot.
        attempts = 5 if method in {"GET", "PUT", "DELETE"} else 1
        # One clock sync/replay is enough. More retries only amplify request volume
        # when a Testnet node is unhealthy or the IP is already being throttled.
        for timestamp_attempt in range(2 if signed else 1):
            retry_after_clock_sync = False
            for attempt in range(attempts):
                request = self._build_request(method, path, params, signed)
                try:
                    with self._opener(request, timeout=10) as response:
                        payload = json.loads(response.read().decode())
                    if isinstance(payload, dict) and payload.get("code", 0) not in (0, 200):
                        raise BinanceApiError(
                            f"Binance API error {payload.get('code')}: {payload.get('msg')}",
                            code=payload.get("code"),
                        )
                    return payload
                except HTTPError as error:
                    # A -1021 request was rejected before Binance acted on it, so it is
                    # safe to synchronize the clock and replay it, even for writes.
                    try:
                        detail = error.read().decode("utf-8", errors="replace").strip()
                        payload = json.loads(detail)
                        code = payload.get("code")
                    except (OSError, ValueError):
                        detail, payload, code = "<response body unavailable>", {}, None
                    if code == -1003:
                        match = re.search(r"banned until (\d+)", str(payload.get("msg", "")))
                        retry_at_ms = int(match.group(1)) if match else None
                        raise BinanceRateLimitError(
                            f"Binance API rate limit: {method} {path}: HTTP {error.code}; {detail}",
                            code=code, retry_at_ms=retry_at_ms,
                        ) from error
                    if signed and code == -1021 and timestamp_attempt < 1:
                        logging.warning("Binance rejected request timestamp; synchronizing clock and retrying")
                        self.sync_server_time()
                        retry_after_clock_sync = True
                        break
                    if signed and code == -1021:
                        raise BinanceTimestampError(
                            f"Binance clock synchronization failed: {method} {path}; {detail}",
                            code=code,
                        ) from error
                    raise BinanceApiError(
                        f"Binance API error: {method} {path}: HTTP {error.code} {error.reason}; {detail}",
                        code=code,
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
        """Set the symbol leverage and confirm Binance accepted it."""
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
        logging.info("Binance %s leverage set to %dx for %s",
                     "Testnet" if self.is_testnet else "Mainnet", actual_leverage, self.config.symbol)

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
        """Return active symbol constraints used to size a grid."""
        if self._symbol_filters is None:
            self._load_symbol_filters()
        assert self._symbol_filters is not None
        return dict(self._symbol_filters)

    @staticmethod
    def _round_down(value: Decimal, quantum: Decimal) -> Decimal:
        return (value / quantum).to_integral_value(rounding=ROUND_DOWN) * quantum

    def _position_side(self, order: GridOrder) -> str:
        return "LONG" if order.purpose.value.endswith("LONG") else "SHORT"

    def _order_quantity(self, order: GridOrder) -> Decimal:
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
        return quantity

    def can_submit_reduce_only(self, order: GridOrder) -> bool:
        """Prevent Binance from expiring a close that exceeds hedge-side capacity."""
        if not order.reduce_only:
            return True
        position_side = self._position_side(order)
        positions = self._request("GET", "/fapi/v2/positionRisk", {"symbol": self.config.symbol}, signed=True)
        position = next((item for item in positions if item.get("positionSide") == position_side), None)
        available = abs(Decimal(str((position or {}).get("positionAmt", "0"))))
        for pending in self.open_orders():
            pending_side = str(pending.get("side", ""))
            closes_side = ((position_side == "LONG" and pending_side == "SELL")
                           or (position_side == "SHORT" and pending_side == "BUY"))
            if pending.get("positionSide") != position_side or not closes_side:
                continue
            available -= max(Decimal("0"), Decimal(str(pending.get("origQty", "0")))
                             - Decimal(str(pending.get("executedQty", "0"))))
        return available >= self._order_quantity(order)

    def submit_grid_order(self, order: GridOrder) -> str:
        if self._symbol_filters is None:
            self._load_symbol_filters()
        assert self._symbol_filters is not None
        price = self._round_down(Decimal(str(order.price)), self._symbol_filters["tick"])
        quantity = self._order_quantity(order)
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
            position_side = position["positionSide"]
            # Hedge-mode amounts are naturally positive for LONG and negative for
            # SHORT.  In one-way mode Binance uses BOTH, so its sign alone tells
            # us which market side closes rather than increases the position.
            side = "SELL" if (position_side == "LONG" or amount > 0) else "BUY"
            params = {
                "symbol": symbol, "side": side, "positionSide": position_side, "type": "MARKET",
                "quantity": format(abs(amount), "f"), "newOrderRespType": "RESULT",
            }
            if position_side == "BOTH":
                params["reduceOnly"] = "true"
            self._request("POST", "/fapi/v1/order", params, signed=True)

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

    def income_history(self, start_time_ms: int) -> list[dict[str, Any]]:
        """Read grid-period realised PnL, funding and commission income entries."""
        return self._request("GET", "/fapi/v1/income", {
            "symbol": self.config.symbol, "startTime": start_time_ms, "limit": 1000,
        }, signed=True)

    def open_client_order_ids(self) -> set[str]:
        """Client IDs of the orders this account currently has working on the symbol."""
        return {str(item["clientOrderId"]) for item in self.open_orders() if item.get("clientOrderId")}

    def order_status(self, client_order_id: str) -> str | None:
        """Resolve one of our client IDs, including orders that already finished.

        Returns None only when Binance has never seen the ID, which is what makes a
        lost submit response distinguishable from a genuinely rejected order.
        """
        try:
            payload = self._request("GET", "/fapi/v1/order",
                                    {"symbol": self.config.symbol,
                                     "origClientOrderId": client_order_id}, signed=True)
        except BinanceApiError as error:
            if error.code == ORDER_NOT_FOUND_CODE:
                return None
            raise
        return str(payload["status"])

    def order_details(self, client_order_id: str) -> dict[str, Any] | None:
        """Return Binance's terminal-order record for an operator recovery check.

        This is read-only and deliberately returns the raw order fields needed to
        prove that an expired leg has not traded before it can be approved.
        """
        try:
            return self._request("GET", "/fapi/v1/order",
                                 {"symbol": self.config.symbol,
                                  "origClientOrderId": client_order_id}, signed=True)
        except BinanceApiError as error:
            if error.code == ORDER_NOT_FOUND_CODE:
                return None
            raise

    def assert_clean_symbol(self) -> None:
        """Prevent a grid from being started on top of manual/old bot exposure."""
        open_orders = self.open_orders()
        if open_orders:
            raise BinanceApiError(f"{self.config.symbol} has existing open orders; cancel/reconcile them before starting")
        if self.positions():
            raise BinanceApiError(f"{self.config.symbol} has an existing position; close it before starting")

    def assert_starting_capital_available(self) -> None:
        """Refuse a new grid when its declared capital exceeds available USDT margin."""
        account = self._request("GET", "/fapi/v2/account", signed=True)
        try:
            available = Decimal(str(account["availableBalance"]))
        except (KeyError, TypeError, ValueError, ArithmeticError) as error:
            raise BinanceApiError("Binance account endpoint returned an invalid availableBalance") from error
        required = Decimal(str(self.config.initial_capital_usdt))
        if available < required:
            raise BinanceApiError(
                f"Available margin {available} USDT is below INITIAL_CAPITAL_USDT {required}; "
                "reduce INITIAL_CAPITAL_USDT or transfer funds before starting"
            )

    def create_listen_key(self) -> str:
        return self._request("POST", "/fapi/v1/listenKey")["listenKey"]

    def keepalive_listen_key(self) -> None:
        self._request("PUT", "/fapi/v1/listenKey")
