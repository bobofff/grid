"""Long-running Binance Futures Testnet grid executor.

It is deliberately gated by both DRY_RUN=false and RUNNER_ENABLED=true. The user
stream is the source of truth for filled orders; state is persisted before every
replacement order so a restart cannot silently duplicate a grid leg.
"""
from __future__ import annotations

from dataclasses import replace
import json
import logging
from queue import Empty, Queue
from threading import Event, Thread
import time
import traceback
from typing import Any
from uuid import uuid4

import websocket

from .config import GridConfig
from .exchange import BinanceTransportError, BinanceUsdmExchange
from .journal import TradeJournal
from .models import EquitySnapshot, GridOrder
from .risk import ExitReason, RoiRiskManager
from .state import RunnerState, StateStore, load_runner_state, save_runner_state
from .strategy import StaticNeutralGrid
from .user_stream import parse_order_update


class BinanceUserStream:
    URL = "wss://stream.binancefuture.com/ws/{}"

    def __init__(self, exchange: BinanceUsdmExchange, events: Queue[dict[str, Any]]) -> None:
        self.exchange, self.events, self.stop_event = exchange, events, Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        listen_key = self.exchange.create_listen_key()
        self.thread = Thread(target=self._run, args=(listen_key,), daemon=True, name="binance-user-stream")
        self.thread.start()

    def _run(self, listen_key: str) -> None:
        def on_message(_: Any, message: str) -> None:
            self.events.put(json.loads(message))
        def on_error(_: Any, error: Any) -> None:
            logging.warning("User stream error: %s", error)
        app = websocket.WebSocketApp(self.URL.format(listen_key), on_message=on_message, on_error=on_error)
        while not self.stop_event.is_set():
            app.run_forever(ping_interval=30, ping_timeout=10)
            if not self.stop_event.is_set():
                time.sleep(2)

    def stop(self) -> None:
        self.stop_event.set()


class GridRunner:
    def __init__(self, config: GridConfig, exchange: BinanceUsdmExchange,
                 journal: TradeJournal | None = None) -> None:
        self.config, self.exchange = config, exchange
        self.strategy = StaticNeutralGrid(config)
        self.store = StateStore(config.state_file)
        self.journal = journal or TradeJournal.for_state_file(config.state_file)
        self.state = load_runner_state(self.store)
        self.events: Queue[dict[str, Any]] = Queue()
        self.stream = BinanceUserStream(exchange, self.events)
        self.stopped = False

    def _save(self) -> None:
        save_runner_state(self.store, self.state)

    def _encode_order(self, order: GridOrder) -> dict[str, Any]:
        return {"level_index": order.level_index, "price": order.price, "side": order.side.value,
                "purpose": order.purpose.value, "reduce_only": order.reduce_only,
                "origin_level": order.origin_level, "client_order_id": order.client_order_id}

    def _decode_order(self, payload: dict[str, Any]) -> GridOrder:
        from .models import Purpose, Side
        return GridOrder(payload["level_index"], payload["price"], Side(payload["side"]),
                         Purpose(payload["purpose"]), payload["reduce_only"], payload["origin_level"],
                         payload["client_order_id"])

    def _with_id(self, order: GridOrder) -> GridOrder:
        # Binance client IDs have a 36-character cap; UUID hex keeps retries unambiguous.
        return replace(order, client_order_id=f"grid-{uuid4().hex[:27]}")

    def _submit(self, order: GridOrder) -> None:
        order = self._with_id(order)
        # Persist intent before submitting. A restart will reconcile client IDs instead
        # of blindly submitting a duplicate order.
        self.journal.record_order("SUBMIT_INTENT", order, symbol=self.config.symbol)
        self.state.orders[order.client_order_id] = self._encode_order(order)
        self._save()
        try:
            binance_order_id = self.exchange.submit_grid_order(order)
        except Exception as error:
            self.journal.record_order("SUBMIT_FAILED", order, symbol=self.config.symbol,
                                      detail={"error": repr(error)})
            raise
        self.journal.record_order("SUBMIT_CONFIRMED", order, binance_order_id=binance_order_id,
                                  symbol=self.config.symbol)

    def _strategy_equity(self) -> EquitySnapshot:
        account = self.exchange.equity_snapshot().equity(self.config.initial_capital_usdt)
        assert self.state.baseline_account_equity is not None
        strategy_equity = self.config.initial_capital_usdt + account - self.state.baseline_account_equity
        return EquitySnapshot(0, 0, 0, 0, equity_override=strategy_equity)

    def _exit(self, reason: ExitReason) -> None:
        if self.stopped:
            return
        self.stopped = True
        self.state.phase = f"EXITING:{reason.value}"
        self._save()
        self.journal.record_runner_event("EXIT_STARTED", reason=reason.value)
        logging.warning("Risk exit %s: cancelling orders then closing positions", reason.value)
        failures: list[Exception] = []
        for action, operation in (("CANCEL_ALL", self.exchange.cancel_all),
                                  ("CLOSE_ALL_POSITIONS", self.exchange.close_all_positions)):
            try:
                operation(self.config.symbol)
            except Exception as error:
                failures.append(error)
                self.journal.record_runner_event("EXIT_STEP_FAILED", reason=reason.value,
                                                 detail=f"{action}: {traceback.format_exc()}")
                logging.exception("Risk-exit step %s failed", action)
        if failures:
            self.state.phase = f"STOPPED:{reason.value}:INCOMPLETE"
            self._save()
            self.journal.record_runner_event("EXIT_INCOMPLETE", reason=reason.value,
                                             detail=repr(failures[0]))
            raise failures[0]
        self.state.orders.clear()
        self.state.phase = f"STOPPED:{reason.value}"
        self._save()
        self.journal.record_runner_event("EXIT_COMPLETED", reason=reason.value)

    def _handle_event(self, event: dict[str, Any]) -> None:
        update = parse_order_update(event)
        if not update or update.symbol != self.config.symbol:
            return
        self.journal.record_order_update(update, event)
        encoded = self.state.orders.get(update.client_order_id)
        if not encoded:
            return
        if update.status == "FILLED":
            order = self._decode_order(encoded)
            del self.state.orders[update.client_order_id]
            self._save()
            self._submit(self.strategy.on_fill(order))
        elif update.status in {"CANCELED", "EXPIRED", "REJECTED"}:
            self._exit(ExitReason.OPERATIONAL_FAILURE)  # fail closed; never continue with a missing leg

    def start(self) -> None:
        self.journal.record_runner_event("RUN_STARTING")
        self.exchange.verify_account()
        if self.state.phase.startswith("STOPPED:"):
            # A prior failed safety exit can leave stale orders or exposure on the
            # exchange.  Finish that fail-closed cleanup before discarding local
            # state or placing a new grid; otherwise the clean-symbol preflight
            # merely reports the problem and every subsequent start fails too.
            previous_phase = self.state.phase
            logging.warning("Previous run ended in %s; reconciling it before starting a new grid", previous_phase)
            self.journal.record_runner_event("RECOVERY_STARTED", reason=previous_phase)
            try:
                self.exchange.cancel_all(self.config.symbol)
                self.exchange.close_all_positions(self.config.symbol)
                self.exchange.assert_clean_symbol()
            except Exception:
                self.journal.record_runner_event("RECOVERY_INCOMPLETE", reason=previous_phase,
                                                 detail=traceback.format_exc())
                raise
            self.journal.record_runner_event("RECOVERY_COMPLETED", reason=previous_phase)
            self.state = RunnerState()
        if self.state.phase == "NEW":
            mark_price = self.exchange.mark_price()
            if not self.config.lower_price < mark_price < self.config.upper_price:
                raise RuntimeError("Current mark price is outside GRID_LOWER_PRICE/GRID_UPPER_PRICE; update the range first")
            smallest_step = min(b - a for a, b in zip(self.strategy.levels, self.strategy.levels[1:]))
            if abs(mark_price - self.config.reference_price) > smallest_step / 2:
                raise RuntimeError("START_REFERENCE_PRICE is too far from the current mark price; refresh the grid preview first")
            self.exchange.assert_clean_symbol()
            # Leverage belongs to the Binance symbol configuration, not an order.
            # Apply it only after confirming there is no old exposure to modify.
            self.exchange.set_configured_leverage()
            self.state.baseline_account_equity = self.exchange.equity_snapshot().equity(self.config.initial_capital_usdt)
            self.state.phase = "RUNNING"
            self._save()
            for order in self.strategy.initial_orders():
                self._submit(order)
            logging.info("Submitted %d initial grid orders", len(self.state.orders))
        elif self.state.phase != "RUNNING":
            raise RuntimeError(f"Refusing to resume runner in terminal state {self.state.phase}")
        self.stream.start()
        self.journal.record_runner_event("RUNNING")

    def run_forever(self) -> None:
        self.start()
        manager = RoiRiskManager(self.config, self.state.risk_state())
        try:
            while not self.stopped:
                try:
                    self._handle_event(self.events.get(timeout=self.config.loop_interval_seconds))
                except Empty:
                    pass
                try:
                    equity = self._strategy_equity()
                except BinanceTransportError as error:
                    # The exchange request already used bounded retries.  Keep the
                    # runner and user stream alive after a transient TLS outage so
                    # a single failed account poll cannot abandon open grid orders.
                    self.journal.record_runner_event("ACCOUNT_SNAPSHOT_RETRY",
                                                     reason="TRANSPORT_ERROR", detail=repr(error))
                    logging.warning("Account snapshot unavailable; will retry: %s", error)
                    continue
                roi, reason = manager.evaluate(equity)
                self.state.peak_roi = manager.state.peak_roi
                self._save()
                if reason is not ExitReason.NONE:
                    logging.warning("ROI %.2f%% triggered %s", roi * 100, reason.value)
                    self._exit(reason)
        except KeyboardInterrupt:
            logging.warning("Runner interrupted; failing closed")
            self.journal.record_runner_event("INTERRUPTED", reason="KEYBOARD_INTERRUPT")
            self._exit(ExitReason.OPERATIONAL_FAILURE)
        except Exception:
            self.journal.record_runner_event("RUNNER_EXCEPTION", reason="UNHANDLED_EXCEPTION",
                                             detail=traceback.format_exc())
            logging.exception("Runner failed; failing closed")
            self._exit(ExitReason.OPERATIONAL_FAILURE)
            raise
        finally:
            self.stream.stop()
