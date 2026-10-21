"""Shared doubles for the failure-path tests.

`ScriptedExchange` is an in-memory Binance stand-in. It keeps a miniature order
book so crash-recovery paths can be exercised without a network, and it can be
scripted to raise a chosen exception on a chosen call.

The tests build their scenarios by actually running the runner once and then
"restarting" it, so the persistence format is exercised rather than mocked.
"""
from __future__ import annotations

from decimal import Decimal
from queue import Queue
import sqlite3
from pathlib import Path
from typing import Any

from grid_bot.config import GridConfig
from grid_bot.exchange import BinanceApiError, Exchange
from grid_bot.models import EquitySnapshot, GridOrder
from grid_bot.runner import GridRunner


# Levels 80/90/100/110/120 with the reference price on the middle level, so the
# grid seeds two buy entries and two sell entries.
CONFIG_DEFAULTS: dict[str, Any] = {
    "symbol": "BTCUSDT", "lower_price": 80.0, "upper_price": 120.0, "grid_count": 4,
    "spacing": "arithmetic", "order_notional_usdt": 10.0, "leverage": 2,
    "reference_price": 100.0, "initial_capital_usdt": 1000.0,
    "hard_stop_loss_roi": -0.20, "breakeven_arm_roi": 0.15, "breakeven_floor_roi": 0.10,
    "trailing_arm_roi": 0.30, "trailing_drawdown_roi": 0.08,
    "loop_interval_seconds": 0.01, "dry_run": False, "execution_mode": "binance_testnet",
    "binance_api_key": "key", "binance_api_secret": "secret", "binance_testnet": True,
    "binance_recv_window": 5000, "binance_hedge_mode_required": True, "runner_enabled": True,
}


def make_config(state_file: str, **overrides: Any) -> GridConfig:
    return GridConfig(**{**CONFIG_DEFAULTS, "state_file": state_file, **overrides})


def make_runner(state_file: str, exchange: "ScriptedExchange", **overrides: Any) -> GridRunner:
    """Build a runner whose user stream never opens a socket."""
    runner = GridRunner(make_config(state_file, **overrides), exchange)
    runner.stream = StreamStub()
    return runner


def order_update_event(client_order_id: str, status: str = "FILLED",
                       symbol: str = "BTCUSDT", order_id: str = "1", *,
                       expiry_reason: str = "", executed_quantity: str = "0") -> dict[str, Any]:
    return {"e": "ORDER_TRADE_UPDATE",
            "o": {"i": order_id, "X": status, "x": "TRADE", "s": symbol, "c": client_order_id,
                  "er": expiry_reason, "z": executed_quantity}}


def journal_event_types(state_file: str, table: str = "runner_events") -> list[str]:
    path = Path(state_file).with_suffix(".sqlite3")
    if not path.exists():
        return []
    with sqlite3.connect(path) as connection:
        return [row[0] for row in connection.execute(
            f"SELECT event_type FROM {table} ORDER BY id")]


class StreamStub:
    """Replaces `BinanceUserStream` so tests never open a WebSocket."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.connected = False
        self.connect_calls: list[float] = []

    def start(self) -> None:
        self.started = True

    def wait_connected(self, timeout: float) -> None:
        """Record the wait so a test can assert orders are not placed before it."""
        self.connect_calls.append(timeout)
        self.connected = True

    def stop(self) -> None:
        self.stopped = True


class OrderingProbeStream(StreamStub):
    """Records when the stream became observable relative to exchange traffic."""

    def __init__(self, exchange: "ScriptedExchange") -> None:
        super().__init__()
        self.exchange = exchange

    def wait_connected(self, timeout: float) -> None:
        super().wait_connected(timeout)
        self.exchange.calls.append("stream_connected")


class ScriptedExchange(Exchange):
    """A programmable exchange double with an inspectable order book."""

    def __init__(self, *, mark_price: float = 100.0, equity: float = 1000.0) -> None:
        self.calls: list[str] = []
        self.arguments: list[tuple[str, tuple[Any, ...]]] = []
        self.faults: dict[str, list[BaseException | None]] = {}
        self.live_orders: dict[str, str] = {}
        self.archived_orders: dict[str, str] = {}
        self.submitted: list[GridOrder] = []
        self.position = Decimal(0)
        self._mark_price = mark_price
        self._equity = equity
        self._sequence = 0
        self._lost_responses = 0
        self.reduce_only_allowed = True

    # --- scripting helpers -------------------------------------------------
    def fail_next(self, method: str, error: BaseException) -> "ScriptedExchange":
        self.faults.setdefault(method, []).append(error)
        return self

    def fail_always(self, method: str, error: BaseException) -> "ScriptedExchange":
        self.faults[method] = [error] * 10_000
        return self

    def lose_next_submit_response(self) -> "ScriptedExchange":
        """Accept the next order, then fail to answer - a lost POST response."""
        self._lost_responses += 1
        return self

    def count(self, method: str) -> int:
        return self.calls.count(method)

    def restart(self) -> "ScriptedExchange":
        """Keep the order book but forget the call log, to simulate a new process."""
        self.calls.clear()
        self.arguments.clear()
        self.submitted.clear()
        return self

    def _record(self, method: str, *args: Any) -> None:
        self.calls.append(method)
        self.arguments.append((method, args))
        queued = self.faults.get(method)
        if queued:
            error = queued.pop(0)
            if error is not None:
                raise error

    # --- Exchange interface ------------------------------------------------
    def submit_grid_order(self, order: GridOrder) -> str:
        self._record("submit_grid_order", order)
        self.submitted.append(order)
        self._sequence += 1
        if order.client_order_id:
            self.live_orders[order.client_order_id] = "NEW"
        if self._lost_responses:
            self._lost_responses -= 1
            raise BinanceTransportError("connection reset after the order was accepted")
        return f"binance-{self._sequence}"

    def cancel_all(self, symbol: str) -> None:
        self._record("cancel_all", symbol)
        for client_order_id in self.live_orders:
            self.archived_orders[client_order_id] = "CANCELED"
        self.live_orders.clear()

    def close_all_positions(self, symbol: str) -> None:
        self._record("close_all_positions", symbol)
        self.position = Decimal(0)

    def equity_snapshot(self) -> EquitySnapshot:
        self._record("equity_snapshot")
        return EquitySnapshot(0, 0, 0, 0, equity_override=self._equity)

    def open_client_order_ids(self) -> set[str]:
        self._record("open_client_order_ids")
        return set(self.live_orders)

    def order_status(self, client_order_id: str) -> str | None:
        self._record("order_status", client_order_id)
        if client_order_id in self.live_orders:
            return self.live_orders[client_order_id]
        return self.archived_orders.get(client_order_id)

    def can_submit_reduce_only(self, order: GridOrder) -> bool:
        self._record("can_submit_reduce_only", order)
        return self.reduce_only_allowed

    # --- methods GridRunner.start() also uses ------------------------------
    def verify_account(self) -> None:
        self._record("verify_account")

    def mark_price(self) -> float:
        self._record("mark_price")
        return self._mark_price

    def set_configured_leverage(self) -> None:
        self._record("set_configured_leverage")

    def assert_clean_symbol(self) -> None:
        self._record("assert_clean_symbol")
        if self.live_orders:
            raise BinanceApiError(f"{len(self.live_orders)} existing open order(s)")
        if self.position:
            raise BinanceApiError("existing position")

    def create_listen_key(self) -> str:
        self._record("create_listen_key")
        return "listen-key"

    def keepalive_listen_key(self) -> None:
        self._record("keepalive_listen_key")

    # --- book helpers ------------------------------------------------------
    def fill(self, client_order_id: str) -> None:
        """Move a live order out of the book and archive it as filled."""
        self.live_orders.pop(client_order_id, None)
        self.archived_orders[client_order_id] = "FILLED"

    def vanish(self, client_order_id: str, status: str = "CANCELED") -> None:
        self.live_orders.pop(client_order_id, None)
        self.archived_orders[client_order_id] = status


class FillingExchange(ScriptedExchange):
    """Fills a leg the instant it is accepted, exactly as Binance would.

    The fill event is pushed through `events`, which a test wires to the runner's
    own queue, so the fill arrives while `start()` is still seeding the grid - the
    race that lost the level-26 leg on 2026-10-15.
    """

    def __init__(self, fill_after: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fill_after = fill_after
        self.events: Queue[dict[str, Any]] | None = None
        self.accepted = 0

    def submit_grid_order(self, order: GridOrder) -> str:
        binance_order_id = super().submit_grid_order(order)
        self.accepted += 1
        if self.accepted == self.fill_after and self.events is not None:
            self.fill(order.client_order_id)
            self.events.put(order_update_event(order.client_order_id))
        return binance_order_id
