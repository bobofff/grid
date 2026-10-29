"""Long-running Binance USDⓈ-M Futures grid executor.

It is deliberately gated by both DRY_RUN=false and RUNNER_ENABLED=true. The user
stream is the source of truth for filled orders; state is persisted before every
replacement order so a restart cannot silently duplicate a grid leg.

Failure policy: ROI risk exits cancel and flatten.  Operational faults pause the
runner without changing Binance positions or orders; a later process reconciles
the exchange book by client order ID before it can trade again.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
import json
import logging
from queue import Empty, Queue
import signal
from threading import Event, Thread
import time
import traceback
from typing import Any
from uuid import uuid4

import websocket

from .config import GridConfig
from .exchange import (
    BinanceApiError,
    BinanceRateLimitError,
    BinanceTimestampError,
    BinanceTransportError,
    BinanceUsdmExchange,
)
from .journal import TradeJournal
from .models import EquitySnapshot, GridOrder, Purpose, Side
from .risk import ExitReason, ROI_EXIT_REASONS, RoiRiskManager
from .state import RunnerState, StateStore, load_runner_state, save_runner_state
from .strategy import StaticNeutralGrid
from .user_stream import MalformedOrderEvent, parse_order_update


def encode_order(order: GridOrder) -> dict[str, Any]:
    """Serialise a grid leg for the restart state file."""
    return {"level_index": order.level_index, "price": order.price, "side": order.side.value,
            "purpose": order.purpose.value, "reduce_only": order.reduce_only,
            "origin_level": order.origin_level, "client_order_id": order.client_order_id}


def decode_order(payload: dict[str, Any]) -> GridOrder:
    return GridOrder(payload["level_index"], payload["price"], Side(payload["side"]),
                     Purpose(payload["purpose"]), payload["reduce_only"], payload["origin_level"],
                     payload["client_order_id"])


class BinanceUserStream:
    """User-data WebSocket with reconnect and listen-key upkeep.

    Binance expires a listen key 60 minutes after creation and 30 minutes after the
    last keepalive, so the key is refreshed on a timer; without that the stream goes
    quiet while the REST loop keeps trading on stale knowledge.
    """

    TESTNET_URL = "wss://stream.binancefuture.com/ws/{}"
    MAINNET_URL = "wss://fstream.binance.com/ws/{}"
    LISTEN_KEY_REFRESH_SECONDS = 20 * 60
    RECONNECT_DELAY_SECONDS = 2

    def __init__(self, exchange: BinanceUsdmExchange, events: Queue[dict[str, Any]]) -> None:
        self.exchange, self.events = exchange, events
        self.url_template = self.TESTNET_URL if getattr(exchange, "is_testnet", True) else self.MAINNET_URL
        self.stop_event = Event()
        self.thread: Thread | None = None
        self.keepalive_thread: Thread | None = None
        self.listen_key: str | None = None
        self.refresh_seconds = self.LISTEN_KEY_REFRESH_SECONDS
        # Set once the socket is live and cleared whenever it drops. Orders are only
        # placed after this fires, so a fill can never land in a window the runner
        # is unable to observe.
        self.connected = Event()
        self._app: Any = None

    def start(self) -> None:
        self.listen_key = self.exchange.create_listen_key()
        self.thread = Thread(target=self._run, daemon=True, name="binance-user-stream")
        self.thread.start()
        self.keepalive_thread = Thread(target=self._refresh_listen_key, daemon=True,
                                       name="binance-listen-key")
        self.keepalive_thread.start()

    def wait_connected(self, timeout: float) -> None:
        """Block until the socket is live, or refuse to trade blind.

        Binance only pushes an order event to a stream that is already subscribed,
        so a fill that lands before this returns is never delivered.  Seeding the
        grid first and subscribing afterwards therefore loses the fill entirely:
        the runner keeps a leg that no longer exists and never places its
        replacement.  Callers must place orders only after this returns.
        """
        if not self.connected.wait(timeout):
            raise RuntimeError(
                "The Binance user stream did not connect within "
                f"{timeout:.0f}s; refusing to place orders that cannot be observed"
            )

    def _refresh_listen_key(self) -> None:
        while not self.stop_event.wait(self.refresh_seconds):
            try:
                self.exchange.keepalive_listen_key()
            except Exception:
                logging.exception("Failed to refresh the Binance listen key; requesting a new one")
                try:
                    self.listen_key = self.exchange.create_listen_key()
                except Exception:
                    logging.exception("Failed to obtain a replacement Binance listen key")

    def _run(self) -> None:
        def on_message(_: Any, message: str) -> None:
            try:
                payload = json.loads(message)
            except (TypeError, ValueError):
                logging.warning("Ignoring an unparseable user-stream frame")
                return
            if isinstance(payload, dict):
                self.events.put(payload)

        def on_open(_: Any) -> None:
            logging.info("User stream connected")
            self.connected.set()

        def on_error(_: Any, error: Any) -> None:
            logging.warning("User stream error: %s", error)

        while not self.stop_event.is_set():
            listen_key = self.listen_key
            if not listen_key:
                self.stop_event.wait(self.RECONNECT_DELAY_SECONDS)
                continue
            app = websocket.WebSocketApp(self.url_template.format(listen_key),
                                         on_message=on_message, on_error=on_error,
                                         on_open=on_open)
            self._app = app
            app.run_forever(ping_interval=30, ping_timeout=10)
            # The socket is gone, so nothing is observable until it comes back.
            self.connected.clear()
            self._app = None
            if not self.stop_event.is_set():
                self.stop_event.wait(self.RECONNECT_DELAY_SECONDS)

    def stop(self) -> None:
        self.stop_event.set()
        self.connected.clear()
        app, self._app = self._app, None
        if app is not None:
            try:
                app.close()
            except Exception:
                logging.debug("Closing the user stream raised", exc_info=True)


SHUTDOWN_SIGNALS: tuple[int, ...] = (signal.SIGTERM, signal.SIGHUP)


def install_signal_handlers(runner: "GridRunner",
                            signals: tuple[int, ...] = SHUTDOWN_SIGNALS) -> dict[int, Any]:
    """Route shutdown signals into a recoverable, journalled shutdown.

    `docker stop` and `systemctl stop` send SIGTERM; closing the terminal that owns
    the process sends SIGHUP.  Unhandled, either one kills the process mid-grid and
    leaves every order and the exposure in place - and leaves no journal entry
    either, because a signal death never reaches the handler in `main()`.

    A signal that is already ignored stays ignored, so `nohup` still detaches.
    """
    previous: dict[int, Any] = {}

    def handler(signum: int, _frame: Any) -> None:
        name = signal.Signals(signum).name
        logging.warning("Received %s; requesting a recoverable shutdown", name)
        runner.stop_signal = name
        runner.stop_requested = True

    for number in signals:
        current = signal.getsignal(number)
        if current is signal.SIG_IGN:
            previous[number] = current
            continue
        previous[number] = signal.signal(number, handler)
    return previous


def restore_signal_handlers(previous: dict[int, Any]) -> None:
    for number, handler in previous.items():
        signal.signal(number, handler)


class GridRunner:
    # How long to wait for the user stream before refusing to trade blind.
    STREAM_CONNECT_TIMEOUT_SECONDS = 30.0
    # A short WebSocket outage can reconnect between two loop iterations.  Do not
    # rely solely on observing its disconnected flag: periodically reconcile the
    # exchange book so a missed fill is repaired even when that transition was
    # missed locally.
    PERIODIC_RECONCILE_INTERVAL_SECONDS = 30.0

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
        self.stop_requested = False
        self.stop_signal: str | None = None
        self._stream_reconcile_needed = False
        self._next_periodic_reconcile_at = time.monotonic() + self.PERIODIC_RECONCILE_INTERVAL_SECONDS

    def _save(self) -> None:
        save_runner_state(self.store, self.state)

    def _run_config_snapshot(self) -> dict[str, Any]:
        """Persist only display-safe configuration alongside an audit run."""
        return {"symbol": self.config.symbol, "lower_price": self.config.lower_price,
                "upper_price": self.config.upper_price, "grid_count": self.config.grid_count,
                "spacing": self.config.spacing, "leverage": self.config.leverage,
                "reference_price": self.config.reference_price,
                "initial_capital_usdt": self.config.initial_capital_usdt}

    def _with_id(self, order: GridOrder) -> GridOrder:
        # Binance client IDs have a 36-character cap.  Keep the grid-run prefix
        # on every leg so exchange/audit records can be attributed unambiguously.
        run_id = self.state.grid_run_id or "legacy"
        return replace(order, client_order_id=f"g-{run_id[:12]}-{uuid4().hex[:20]}")

    def _submit(self, order: GridOrder) -> bool:
        if not self._stream_is_connected():
            raise RuntimeError("User stream is disconnected; refusing to submit an unobservable order")
        if order.reduce_only and not self.exchange.can_submit_reduce_only(order):
            self._defer_replacement(order, reason="REDUCE_ONLY_CAPACITY")
            return False
        order = self._with_id(order)
        # Persist intent before submitting. A restart will reconcile client IDs instead
        # of blindly submitting a duplicate order.
        self.journal.record_order("SUBMIT_INTENT", order, symbol=self.config.symbol,
                                  grid_run_id=self.state.grid_run_id)
        self.state.orders[order.client_order_id] = encode_order(order)
        self._save()
        try:
            binance_order_id = self.exchange.submit_grid_order(order)
        except Exception as error:
            # The POST may have been accepted even though its response never arrived.
            # Ask by client ID before declaring the leg unplaced, or the grid would be
            # left short while an orphan order sits on the exchange.
            landed = self._confirm_after_failed_submit(order)
            self.journal.record_order("SUBMIT_FAILED", order, symbol=self.config.symbol,
                                      grid_run_id=self.state.grid_run_id,
                                      detail={"error": repr(error), "reached_binance": landed})
            if not landed:
                raise
            logging.warning("Order %s was accepted despite a failed response; keeping the leg",
                            order.client_order_id)
            return True
        self.journal.record_order("SUBMIT_CONFIRMED", order, binance_order_id=binance_order_id,
                                  symbol=self.config.symbol, grid_run_id=self.state.grid_run_id)
        return True

    def _confirm_after_failed_submit(self, order: GridOrder) -> bool:
        try:
            return self.exchange.order_status(order.client_order_id) is not None
        except Exception:
            logging.warning("Could not confirm whether %s reached Binance",
                            order.client_order_id, exc_info=True)
            return False

    @staticmethod
    def _is_close(order: GridOrder) -> bool:
        return order.purpose in {Purpose.CLOSE_LONG, Purpose.CLOSE_SHORT}

    def _would_self_match(self, order: GridOrder) -> bool:
        """Whether this leg would cross a live leg belonging to this runner."""
        return any(
            existing.side is not order.side and existing.price == order.price
            for payload in self.state.orders.values()
            for existing in (decode_order(payload),)
        )

    def _defer_replacement(self, order: GridOrder, *, reason: str = "SELF_TRADE_COLLISION") -> None:
        """Persist a replacement until it can safely trade against this account."""
        payload = encode_order(order)
        if payload not in self.state.deferred_orders:
            self.state.deferred_orders.append(payload)
            self._save()
            self.journal.record_order("REPLACEMENT_DEFERRED", order, symbol=self.config.symbol,
                                      grid_run_id=self.state.grid_run_id,
                                      detail={"reason": reason})

    def _release_deferred_orders(self) -> None:
        """Submit non-crossing replacements, always giving closes first priority.

        A fast reversal can fill an entry and a different close in the same stream
        burst. Their natural replacements can be opposing orders at one level.
        Submit the close, retain the entry durably, and release it only after the
        close is no longer a live opposing order.
        """
        while self.state.deferred_orders and not self.stopped:
            candidates = sorted((decode_order(payload) for payload in self.state.deferred_orders),
                                key=lambda order: not self._is_close(order))
            submitted = False
            for order in candidates:
                if self._would_self_match(order):
                    continue
                self.state.deferred_orders.remove(encode_order(order))
                self._save()
                submitted = self._submit(order)
                break
            if not submitted:
                return

    @staticmethod
    def _is_zero_fill_capacity_expiry(order: GridOrder, expiry_reason: str,
                                      executed_quantity: str) -> bool:
        if not order.reduce_only or expiry_reason != "4":
            return False
        try:
            return Decimal(executed_quantity) == 0
        except InvalidOperation:
            return False

    def _journaled_zero_fill_capacity_expiry(self, order: GridOrder) -> bool:
        event = self.journal.latest_order_event(order.client_order_id)
        payload = (event or {}).get("o", {})
        return isinstance(payload, dict) and self._is_zero_fill_capacity_expiry(
            order, str(payload.get("er", "")), str(payload.get("z", "")))

    def _strategy_equity(self) -> EquitySnapshot:
        account = self.exchange.equity_snapshot().equity(self.config.initial_capital_usdt)
        baseline = self.state.baseline_account_equity
        if baseline is None:
            raise RuntimeError("Runner state has no account baseline; refusing to compute ROI")
        strategy_equity = self.config.initial_capital_usdt + account - baseline
        return EquitySnapshot(0, 0, 0, 0, equity_override=strategy_equity)

    def _risk_exit(self, reason: ExitReason) -> None:
        """Perform the only exit path that is permitted to flatten a position."""
        if reason not in ROI_EXIT_REASONS:
            raise ValueError(f"{reason.value} is not an ROI risk exit")
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
        self.state.deferred_orders.clear()
        self.state.phase = f"STOPPED:{reason.value}"
        self._save()
        self.journal.record_runner_event("EXIT_COMPLETED", reason=reason.value)

    def _fault_exit(self, reason: str, detail: str | None = None) -> None:
        """Stop submitting new work without cancelling orders or flattening exposure."""
        if self.stopped:
            return
        self.stopped = True
        self.state.phase = f"PAUSED:{reason}"
        self._save()
        self.journal.record_runner_event("FAULT_PAUSED", reason=reason, detail=detail)
        logging.warning("Runner paused for recoverable fault %s; preserving Binance orders and positions",
                        reason)

    def _complete_interrupted_risk_exit(self) -> None:
        """Finish a risk exit interrupted after it was durably marked EXITING."""
        reason_text = self.state.phase.split(":", 1)[1]
        try:
            reason = ExitReason(reason_text)
        except ValueError as error:
            raise RuntimeError(f"Unknown risk exit state {self.state.phase}") from error
        self._risk_exit(reason)

    def _reconcile(self) -> None:
        """Match persisted legs against Binance before trusting local state.

        The process can die between a fill and its replacement, so the exchange book
        decides what happened; `state.orders` alone would silently keep a leg that no
        longer exists.
        """
        live = self.exchange.open_client_order_ids()
        # A non-NEW runner must always have a durable grid leg locally or on the
        # exchange.  Treating an empty book as a clean resume starts a process
        # that looks healthy (stream connected, phase RUNNING) but can never
        # trade, because only NEW seeds initial orders.  Do not silently seed
        # here: the operator must first confirm that creating a fresh grid is
        # intentional.
        if not self.state.orders and not live:
            raise RuntimeError(
                "Runner has no persisted or Binance grid orders; refusing to run an empty grid. "
                "Verify the account, then reset the runner state to create a new grid"
            )
        foreign = live - set(self.state.orders)
        if foreign:
            raise BinanceApiError(
                f"{self.config.symbol} has open order(s) this runner does not own "
                f"({', '.join(sorted(foreign)[:3])}); reconcile the account before restarting"
            )
        unresolved = [client_order_id for client_order_id in self.state.orders
                      if client_order_id not in live]
        if not unresolved:
            self.journal.record_runner_event("RECONCILE_CLEAN", detail=f"{len(live)} live legs")
            return
        for client_order_id in unresolved:
            order = decode_order(self.state.orders[client_order_id])
            status = self.exchange.order_status(client_order_id)
            if status is None:
                # The persisted intent never reached Binance, so resending is safe.
                self.journal.record_runner_event("RECONCILE_RESUBMIT", reason="NEVER_ACCEPTED",
                                                 detail=client_order_id)
                del self.state.orders[client_order_id]
                self._save()
                self._defer_replacement(order)
                continue
            if status == "FILLED":
                self.journal.record_runner_event("RECONCILE_FILL", reason="FILLED_WHILE_DOWN",
                                                 detail=client_order_id)
                # A fill found through REST reconciliation has no user-stream frame
                # to journal.  Record the same terminal order event the stream
                # would have produced so the dashboard timeline does not claim
                # that this grid has no fills after a restart.  The preceding
                # SUBMIT_CONFIRMED entry already carries Binance's order ID.
                self.journal.record_order("ORDER_FILLED", order, symbol=self.config.symbol,
                                          grid_run_id=self.state.grid_run_id,
                                          detail={"source": "reconciliation"})
                del self.state.orders[client_order_id]
                self._save()
                self._defer_replacement(self.strategy.on_fill(order))
                continue
            if status == "EXPIRED_IN_MATCH":
                # Binance STP cancelled this exact leg before it traded.  Keep its
                # original intent, but defer it until no opposing local leg is live
                # at the same price.
                self.journal.record_runner_event("RECONCILE_STP_RETRY",
                                                 reason="EXPIRED_IN_MATCH", detail=client_order_id)
                del self.state.orders[client_order_id]
                self._save()
                self._defer_replacement(order)
                continue
            if status == "EXPIRED" and self._journaled_zero_fill_capacity_expiry(order):
                self.journal.record_runner_event("RECONCILE_REDUCE_ONLY_DEFER",
                                                 reason="REDUCE_ONLY_CAPACITY", detail=client_order_id)
                del self.state.orders[client_order_id]
                self._save()
                self._defer_replacement(order, reason="REDUCE_ONLY_CAPACITY")
                continue
            if status == "EXPIRED" and client_order_id in self.state.approved_expired_order_ids:
                # A generic expiry can mean a reduce-only capacity conflict.  It
                # is retried only after the separate recovery command has checked
                # zero execution and the operator has approved this exact ID.
                self.journal.record_runner_event("RECONCILE_APPROVED_EXPIRED_RETRY",
                                                 reason="OPERATOR_APPROVED", detail=client_order_id)
                self.state.approved_expired_order_ids.remove(client_order_id)
                del self.state.orders[client_order_id]
                self._save()
                self._defer_replacement(order)
                continue
            raise BinanceApiError(
                f"Grid leg {client_order_id} is {status} on {self.config.symbol} without a fill; "
                "refusing to resume an incomplete grid; inspect it with grid_bot.recover before approval"
            )
        self.journal.record_runner_event("RECONCILE_COMPLETED",
                                         detail=f"{len(unresolved)} legs resolved")
        self._release_deferred_orders()

    def _handle_event(self, event: dict[str, Any]) -> None:
        try:
            update = parse_order_update(event)
        except MalformedOrderEvent:
            # A damaged frame is not a reason to flatten the account. Journal it and
            # let the next restart's reconciliation repair any leg we did not observe.
            self.journal.record_runner_event("ORDER_EVENT_UNPARSEABLE", reason="MALFORMED_EVENT",
                                             detail=traceback.format_exc())
            logging.exception("Ignoring a malformed user-stream order event")
            return
        if not update or update.symbol != self.config.symbol:
            return
        self.journal.record_order_update(update, event)
        encoded = self.state.orders.get(update.client_order_id)
        if not encoded:
            return
        if update.status == "FILLED":
            order = decode_order(encoded)
            del self.state.orders[update.client_order_id]
            self._save()
            self._defer_replacement(self.strategy.on_fill(order))
            self._release_deferred_orders()
        elif update.status == "EXPIRED_IN_MATCH":
            # Self-trade prevention removed an unfilled leg.  It is safe to retry
            # the same intent once the collision manager sees no opposing leg.
            order = decode_order(encoded)
            del self.state.orders[update.client_order_id]
            self._save()
            self._defer_replacement(order)
            self._release_deferred_orders()
        elif update.status == "EXPIRED" and self._is_zero_fill_capacity_expiry(
                decode_order(encoded), update.expiry_reason, update.executed_quantity):
            order = decode_order(encoded)
            del self.state.orders[update.client_order_id]
            self._save()
            self._defer_replacement(order, reason="REDUCE_ONLY_CAPACITY")
            self._release_deferred_orders()
        elif update.status in {"CANCELED", "EXPIRED", "REJECTED"}:
            self._fault_exit("ORDER_TERMINAL", f"{update.client_order_id} is {update.status}")

    def start(self) -> None:
        self.journal.record_runner_event("RUN_STARTING")
        self.exchange.verify_account()
        if self.state.phase.startswith("EXITING:"):
            # This marker is written only by _risk_exit, so completing it is still
            # an ROI-risk action, not a generic restart cleanup.
            previous_phase = self.state.phase
            self.stopped = False
            self._complete_interrupted_risk_exit()
            raise RuntimeError(f"Completed interrupted ROI exit {previous_phase}; manual reset required")
        if self.state.phase.startswith("STOPPED:"):
            raise RuntimeError(f"Runner is in terminal ROI state {self.state.phase}; manual reset required")
        if self.state.phase == "NEW":
            mark_price = self.exchange.mark_price()
            if not self.config.lower_price < mark_price < self.config.upper_price:
                raise RuntimeError("Current mark price is outside GRID_LOWER_PRICE/GRID_UPPER_PRICE; update the range first")
            smallest_step = min(b - a for a, b in zip(self.strategy.levels, self.strategy.levels[1:]))
            if abs(mark_price - self.config.reference_price) > smallest_step / 2:
                raise RuntimeError("START_REFERENCE_PRICE is too far from the current mark price; refresh the grid preview first")
            self.exchange.assert_clean_symbol()
            self.exchange.assert_starting_capital_available()
            # Leverage belongs to the Binance symbol configuration, not an order.
            # Apply it only after confirming there is no old exposure to modify.
            self.exchange.set_configured_leverage()
        elif self.state.phase not in {"RUNNING"} and not self.state.phase.startswith("PAUSED:"):
            raise RuntimeError(f"Refusing to resume runner in terminal state {self.state.phase}")
        # Subscribe before a single order exists.  A fill that lands while the grid
        # is being seeded or reconciled is never pushed to a stream that is not up
        # yet, so the runner would keep a leg that is already gone and never place
        # its replacement - which is exactly how the level-26 fill was lost.
        self.stream.start()
        self.stream.wait_connected(self.STREAM_CONNECT_TIMEOUT_SECONDS)
        if self.state.phase == "NEW":
            if not self.state.grid_run_id:
                self.state.grid_run_id = uuid4().hex[:12]
                self.journal.create_grid_run(self.state.grid_run_id, self._run_config_snapshot())
            self.state.baseline_account_equity = self.exchange.equity_snapshot().equity(self.config.initial_capital_usdt)
            self.state.baseline_started_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            self.state.phase = "RUNNING"
            self._save()
            if self.state.grid_run_id:
                self.journal.start_grid_run(self.state.grid_run_id, self.state.baseline_started_at,
                                            self.state.baseline_account_equity)
            for order in self.strategy.initial_orders():
                self._submit(order)
            logging.info("Submitted %d initial grid orders", len(self.state.orders))
        else:
            self._reconcile()
            self.state.phase = "RUNNING"
            self._save()
        # Apply anything the stream delivered while the grid was being built, before
        # the account poll can observe a book that is one leg short.
        self._drain_events()
        self._release_deferred_orders()
        self.journal.record_runner_event("RUNNING")

    def _drain_events(self) -> int:
        """Consume every queued event before doing any blocking account work.

        A burst of fills must be replaced as a batch: draining one event per account
        poll leaves the grid short of legs for as long as the burst lasts.
        """
        handled = 0
        while not self.stopped:
            try:
                event = self.events.get_nowait()
            except Empty:
                break
            self._handle_event(event)
            handled += 1
        return handled

    def _stream_is_connected(self) -> bool:
        connected = getattr(self.stream, "connected", None)
        if connected is None:  # compatibility with minimal test/dummy streams
            return True
        return connected.is_set() if hasattr(connected, "is_set") else bool(connected)

    def _reconcile_if_due(self) -> None:
        """Reconcile periodically, including after an unobserved short disconnect."""
        now = time.monotonic()
        if now < self._next_periodic_reconcile_at:
            return
        self._next_periodic_reconcile_at = now + self.PERIODIC_RECONCILE_INTERVAL_SECONDS
        try:
            self._reconcile()
        except (BinanceTransportError, BinanceTimestampError) as error:
            # Preserve the live runner after a temporary REST failure; the next
            # bounded periodic check will retry without request amplification.
            self.journal.record_runner_event("PERIODIC_RECONCILE_RETRY",
                                             reason=type(error).__name__, detail=repr(error))
            logging.warning("Periodic Binance reconciliation unavailable; will retry: %s", error)

    def run_forever(self) -> None:
        # `start()` is inside the guard on purpose: a failure to resume (a leg that
        # vanished without a fill, an order this runner does not own) must run the
        # same fail-closed cleanup as a failure mid-loop.
        try:
            self.start()
            manager = RoiRiskManager(self.config, self.state.risk_state())
            while not self.stopped and not self.stop_requested:
                try:
                    first = self.events.get(timeout=self.config.loop_interval_seconds)
                except Empty:
                    pass
                else:
                    self._handle_event(first)
                    self._drain_events()
                    self._release_deferred_orders()
                try:
                    equity = self._strategy_equity()
                except (BinanceTransportError, BinanceTimestampError) as error:
                    # The exchange request already used bounded retries.  Keep the
                    # runner and user stream alive after a transient TLS outage so
                    # a single failed account poll cannot abandon open grid orders.
                    reason = "CLOCK_SYNC_ERROR" if isinstance(error, BinanceTimestampError) else "TRANSPORT_ERROR"
                    self.journal.record_runner_event("ACCOUNT_SNAPSHOT_RETRY",
                                                     reason=reason, detail=repr(error))
                    logging.warning("Account snapshot unavailable; will retry: %s", error)
                    continue
                roi, reason = manager.evaluate(equity)
                self.state.peak_roi = manager.state.peak_roi
                self._save()
                if reason is not ExitReason.NONE:
                    logging.warning("ROI %.2f%% triggered %s", roi * 100, reason.value)
                    self._risk_exit(reason)
                if not self._stream_is_connected():
                    if not self._stream_reconcile_needed:
                        self.journal.record_runner_event("STREAM_DISCONNECTED",
                                                         reason="USER_STREAM_UNAVAILABLE")
                        self._stream_reconcile_needed = True
                    continue
                if self._stream_reconcile_needed:
                    self.journal.record_runner_event("STREAM_RECONNECTED")
                    self._reconcile()
                    self._stream_reconcile_needed = False
                    self._next_periodic_reconcile_at = (
                        time.monotonic() + self.PERIODIC_RECONCILE_INTERVAL_SECONDS
                    )
                else:
                    self._reconcile_if_due()
            if self.stop_requested and not self.stopped:
                reason = self.stop_signal or "SIGTERM"
                logging.warning("Shutdown requested by %s; preserving exchange state", reason)
                self.journal.record_runner_event("INTERRUPTED", reason=reason)
                self._fault_exit(reason)
        except KeyboardInterrupt:
            logging.warning("Runner interrupted; preserving exchange state")
            self.journal.record_runner_event("INTERRUPTED", reason="KEYBOARD_INTERRUPT")
            self._fault_exit("KEYBOARD_INTERRUPT")
        except BinanceRateLimitError as error:
            detail = repr(error)
            if error.retry_at_ms is not None:
                detail = f"{detail}; retry_at_ms={error.retry_at_ms}"
            logging.error("Binance rate limit reached; pausing instead of restarting: %s", detail)
            self._fault_exit("BINANCE_RATE_LIMIT", detail=detail)
            # Return normally: PM2's stop_exit_codes prevents an automatic restart
            # from turning a rate limit into a request storm.
            return
        except BinanceTimestampError as error:
            logging.error("Binance clock synchronization failed during startup; pausing: %s", error)
            self._fault_exit("CLOCK_SYNC_FAILED", detail=repr(error))
            return
        except Exception:
            self.journal.record_runner_event("RUNNER_EXCEPTION", reason="UNHANDLED_EXCEPTION",
                                             detail=traceback.format_exc())
            logging.exception("Runner failed; preserving exchange state for reconciliation")
            self._fault_exit("UNHANDLED_EXCEPTION")
            raise
        finally:
            self.stream.stop()
