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
from datetime import datetime, timedelta, timezone
import json
import logging
import re
from queue import Empty, Queue
import signal
from threading import Event, Lock, Thread
import time
import traceback
from typing import Any
from uuid import uuid4

import websocket

from .config import GridConfig
from .email_notifier import EmailNotifier
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
        self._error_reported = False

    def _report_error(self, source: str, error: Any) -> None:
        if self._error_reported:
            return
        self._error_reported = True
        self.events.put({RUNNER_ERROR_EVENT_KEY: {
            "source": source, "detail": str(error),
        }})

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
            except Exception as error:
                logging.exception("Failed to refresh the Binance listen key; requesting a new one")
                self._report_error("LISTEN_KEY", error)
                try:
                    self.listen_key = self.exchange.create_listen_key()
                except Exception as replacement_error:
                    logging.exception("Failed to obtain a replacement Binance listen key")
                    self._report_error("LISTEN_KEY", replacement_error)

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
            self._error_reported = False
            self.connected.set()

        def on_error(_: Any, error: Any) -> None:
            logging.warning("User stream error: %s", error)
            self._report_error("WEBSOCKET", error)

        while not self.stop_event.is_set():
            listen_key = self.listen_key
            if not listen_key:
                self.stop_event.wait(self.RECONNECT_DELAY_SECONDS)
                continue
            app = websocket.WebSocketApp(self.url_template.format(listen_key),
                                         on_message=on_message, on_error=on_error,
                                         on_open=on_open)
            self._app = app
            try:
                app.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as error:
                logging.exception("Binance user stream worker failed")
                self._report_error("WEBSOCKET", error)
            # The socket is gone, so nothing is observable until it comes back.
            self.connected.clear()
            self._app = None
            if not self.stop_event.is_set():
                self._report_error("WEBSOCKET", "User-data WebSocket disconnected")
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
RUNNER_ERROR_EVENT_KEY = "_grid_runner_error"


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
        self.notifier = EmailNotifier(config, journal=self.journal)
        self._summary_lock = Lock()
        self._summary_pending = False
        self._summary_thread: Thread | None = None
        self._summary_due_at = self._load_summary_due()

    def _save(self) -> None:
        save_runner_state(self.store, self.state)

    @staticmethod
    def _safe_error_detail(detail: Any) -> str:
        text = re.sub(r"(?i)(signature=)[^&\s]+", r"\1<redacted>", str(detail))
        return text[:1000]

    def _notify(self, subject: str, body: str, *, dedupe_key: str | None = None,
                cooldown_seconds: float = 0) -> bool:
        return self.notifier.notify(subject, body, dedupe_key=dedupe_key,
                                   cooldown_seconds=cooldown_seconds)

    def _notification_run_key(self) -> str:
        if self.state.grid_run_id:
            return self.state.grid_run_id
        if self.state.baseline_started_at:
            return f"legacy-{self.state.baseline_started_at}"
        return f"legacy-{self.config.symbol}"

    def _notify_first_fill(self, order: GridOrder, *, source: str) -> None:
        if not self.notifier.enabled:
            return
        run_id = self._notification_run_key()
        self._notify(
            f"Grid 首次真实成交 - {self.config.symbol}",
            f"首次成交已由 Binance 确认。\n交易对：{self.config.symbol}\n"
            f"方向/用途：{order.side.value} / {order.purpose.value}\n"
            f"价格：{order.price}\n订单编号：{order.client_order_id}\n"
            f"确认来源：{source}\n运行编号：{run_id}",
            dedupe_key=f"first-fill:{run_id}",
        )

    def _load_summary_due(self) -> datetime | None:
        run_key = self._notification_run_key()
        previous = self.journal.latest_runner_event_at("EMAIL_SUMMARY_QUEUED", reason=run_key)
        if previous is None and self.state.baseline_started_at:
            try:
                previous = datetime.fromisoformat(self.state.baseline_started_at)
            except ValueError:
                previous = None
        previous = previous or self.journal.latest_runner_event_at("RUNNING")
        return previous + timedelta(hours=6) if previous is not None else None

    def _maybe_send_summary(self) -> None:
        if not self.notifier.enabled or self.state.phase != "RUNNING":
            return
        now = datetime.now(timezone.utc)
        run_key = self._notification_run_key()
        with self._summary_lock:
            due_at = self._summary_due_at
            if due_at is None or now < due_at or self._summary_pending:
                return
            self._summary_pending = True
        self._summary_thread = Thread(
            target=self._send_summary, args=(now, run_key, due_at),
            daemon=True, name="grid-email-summary",
        )
        self._summary_thread.start()

    def _send_summary(self, now: datetime, run_key: str, due_at: datetime) -> None:
        summary_queued = False
        try:
            if self.state.phase != "RUNNING":
                return
            equity_text, profit_text = "暂不可用", "暂不可用"
            positions_text = "暂不可用"
            try:
                equity = self.exchange.equity_snapshot().equity(self.config.initial_capital_usdt)
                baseline = self.state.baseline_account_equity
                equity_text = f"{equity:.8f} USDT"
                profit_text = f"{equity - baseline:+.8f} USDT" if baseline is not None else "基线缺失"
                positions = self.exchange.positions()
                positions_text = "无持仓" if not positions else "\n".join(
                    f"{item.get('positionSide', '?')}: 数量 {item.get('positionAmt', '?')}，"
                    f"入场价 {item.get('entryPrice', '?')}，未实现盈亏 {item.get('unRealizedProfit', '?')} USDT"
                    for item in positions
                )
            except Exception as error:
                detail = self._safe_error_detail(error)
                self.journal.record_runner_event("SUMMARY_SNAPSHOT_FAILED", reason=type(error).__name__,
                                                 detail=detail)
                self._notify("Grid 汇总数据读取异常", f"{self.config.symbol}：{detail}",
                             dedupe_key=f"summary-snapshot:{type(error).__name__}",
                             cooldown_seconds=900)
            if self.state.phase != "RUNNING":
                return
            status = "运行中" if not self.stopped else self.state.phase
            error_count = self.journal.runner_error_count()
            body = (
                f"交易对：{self.config.symbol}\n运行状态：{status}（{self.state.phase}）\n"
                f"账户权益：{equity_text}\n策略累计收益：{profit_text}\n"
                f"当前持仓：{positions_text}\n异常次数：{error_count}\n"
                f"运行编号：{run_key}\n统计时间：{now.isoformat(timespec='seconds')}"
            )
            period_key = int(due_at.timestamp())
            notification_key = f"summary:{run_key}:{period_key}"
            enqueued = self._notify(f"Grid 6 小时运行汇总 - {self.config.symbol}", body,
                                    dedupe_key=notification_key)
            if enqueued or self.journal.email_notification_exists(notification_key):
                self.journal.record_runner_event("EMAIL_SUMMARY_QUEUED", reason=run_key,
                                                 detail=now.isoformat())
                summary_queued = True
        except Exception:
            logging.exception("Failed to build the six-hour email summary")
        finally:
            with self._summary_lock:
                if summary_queued:
                    self._summary_due_at = now + timedelta(hours=6)
                self._summary_pending = False

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
            confirmed_status = self._confirm_after_failed_submit(order)
            landed = confirmed_status in {"NEW", "PARTIALLY_FILLED", "FILLED"}
            self.journal.record_order("SUBMIT_FAILED", order, symbol=self.config.symbol,
                                      grid_run_id=self.state.grid_run_id,
                                      detail={"error": self._safe_error_detail(error),
                                              "reached_binance": confirmed_status is not None,
                                              "exchange_status": confirmed_status})
            if not landed:
                detail = self._safe_error_detail(error)
                self.journal.record_runner_event("ORDER_SUBMIT_FAILED", reason=type(error).__name__,
                                                 detail=detail)
                self._notify(
                    f"Grid 下单失败 - {self.config.symbol}",
                    f"订单未能由 Binance 确认成功。\n订单：{order.client_order_id}\n"
                    f"方向/用途：{order.side.value} / {order.purpose.value}\n"
                    f"交易所查询状态：{confirmed_status or '未查询到'}\n错误：{detail}",
                    dedupe_key=f"submit-failed:{order.client_order_id}",
                )
                raise
            logging.warning("Order %s was accepted despite a failed response; keeping the leg",
                            order.client_order_id)
            return True
        self.journal.record_order("SUBMIT_CONFIRMED", order, binance_order_id=binance_order_id,
                                  symbol=self.config.symbol, grid_run_id=self.state.grid_run_id)
        return True

    def _confirm_after_failed_submit(self, order: GridOrder) -> str | None:
        try:
            return self.exchange.order_status(order.client_order_id)
        except Exception:
            logging.warning("Could not confirm whether %s reached Binance",
                            order.client_order_id, exc_info=True)
            return None

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
        self._notify(
            f"Grid 风控退出触发 - {self.config.symbol}",
            f"触发条件：{reason.value}\n策略已开始撤单和平仓流程。\n运行编号：{self.state.grid_run_id or '未知'}",
            dedupe_key=f"risk-exit:{self.state.grid_run_id}:{reason.value}",
        )
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
            detail = self._safe_error_detail(failures[0])
            self.journal.record_runner_event("EXIT_INCOMPLETE", reason=reason.value, detail=detail)
            self._notify(f"Grid 风控退出未完成 - {self.config.symbol}",
                         f"原因：{reason.value}\n错误：{detail}\n需要人工检查撤单/平仓状态。",
                         dedupe_key=f"risk-exit-incomplete:{self.state.grid_run_id}:{reason.value}")
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
        safe_detail = self._safe_error_detail(detail) if detail else ""
        self.journal.record_runner_event("FAULT_PAUSED", reason=reason, detail=safe_detail)
        self._notify(
            f"Grid 机器人已停止/暂停 - {self.config.symbol}",
            f"原因：{reason}\n详情：{safe_detail or '无'}\n当前状态：{self.state.phase}\n"
            f"交易所挂单和仓位已保留，未自动撤单或平仓。\n运行编号：{self.state.grid_run_id or '未知'}",
        )
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
                self._notify_first_fill(order, source="Binance REST 对账")
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
        stream_error = event.get(RUNNER_ERROR_EVENT_KEY)
        if isinstance(stream_error, dict):
            source = str(stream_error.get("source", "WEBSOCKET"))
            source_label = {"WEBSOCKET": "WebSocket", "LISTEN_KEY": "监听密钥"}.get(source, source)
            detail = self._safe_error_detail(stream_error.get("detail", "Unknown stream error"))
            self.journal.record_runner_event("WEBSOCKET_ERROR", reason=source, detail=detail)
            self._notify(f"Grid Binance {source_label} 异常 - {self.config.symbol}",
                         f"来源：{source}\n详情：{detail}\n策略状态：{self.state.phase}",
                         dedupe_key=f"stream-error:{source}", cooldown_seconds=900)
            return
        try:
            update = parse_order_update(event)
        except MalformedOrderEvent:
            # A damaged frame is not a reason to flatten the account. Journal it and
            # let the next restart's reconciliation repair any leg we did not observe.
            detail = self._safe_error_detail(traceback.format_exc())
            self.journal.record_runner_event("ORDER_EVENT_UNPARSEABLE", reason="MALFORMED_EVENT",
                                             detail=detail)
            self._notify(f"Grid Binance WebSocket 订单事件异常 - {self.config.symbol}",
                         f"无法解析交易所订单事件：{detail}",
                         dedupe_key="malformed-order-event", cooldown_seconds=900)
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
            self._notify_first_fill(order, source="Binance ORDER_TRADE_UPDATE")
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
        self._summary_due_at = self._load_summary_due()
        if self.state.phase == "RUNNING" and not self.stopped:
            self._notify(
                f"Grid 策略启动成功 - {self.config.symbol}",
                f"策略已通过 Binance 账户校验并进入运行状态。\n"
                f"执行模式：{self.config.execution_mode}\n运行编号：{self.state.grid_run_id or '未知'}\n"
                f"当前追踪订单：{len(self.state.orders)}",
            )

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
            detail = self._safe_error_detail(error)
            self.journal.record_runner_event("PERIODIC_RECONCILE_RETRY",
                                             reason=type(error).__name__, detail=detail)
            self._notify(f"Grid Binance 对账异常 - {self.config.symbol}", detail,
                         dedupe_key=f"binance-reconcile:{type(error).__name__}",
                         cooldown_seconds=900)
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
                    detail = self._safe_error_detail(error)
                    self.journal.record_runner_event("ACCOUNT_SNAPSHOT_RETRY",
                                                     reason=reason, detail=detail)
                    self._notify(f"Grid Binance 账户快照异常 - {self.config.symbol}", detail,
                                 dedupe_key=f"account-snapshot:{reason}", cooldown_seconds=900)
                    logging.warning("Account snapshot unavailable; will retry: %s", error)
                    continue
                roi, reason = manager.evaluate(equity)
                self.state.peak_roi = manager.state.peak_roi
                self._save()
                self._maybe_send_summary()
                if reason is not ExitReason.NONE:
                    logging.warning("ROI %.2f%% triggered %s", roi * 100, reason.value)
                    self._risk_exit(reason)
                if not self._stream_is_connected():
                    if not self._stream_reconcile_needed:
                        self.journal.record_runner_event("STREAM_DISCONNECTED",
                                                         reason="USER_STREAM_UNAVAILABLE")
                        self._notify(f"Grid Binance WebSocket 已断开 - {self.config.symbol}",
                                     "用户数据流已断开；策略暂停提交新订单，等待重连后对账。",
                                     dedupe_key="stream-error:WEBSOCKET", cooldown_seconds=900)
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
            detail = self._safe_error_detail(error)
            if error.retry_at_ms is not None:
                detail = f"{detail}; retry_at_ms={error.retry_at_ms}"
            logging.error("Binance rate limit reached; pausing instead of restarting: %s", detail)
            self._fault_exit("BINANCE_RATE_LIMIT", detail=detail)
            # Return normally: PM2's stop_exit_codes prevents an automatic restart
            # from turning a rate limit into a request storm.
            return
        except BinanceTimestampError as error:
            logging.error("Binance clock synchronization failed during startup; pausing: %s", error)
            self._fault_exit("CLOCK_SYNC_FAILED", detail=self._safe_error_detail(error))
            return
        except Exception as error:
            trace = self._safe_error_detail(traceback.format_exc())
            self.journal.record_runner_event("RUNNER_EXCEPTION", reason=type(error).__name__,
                                             detail=trace)
            logging.exception("Runner failed; preserving exchange state for reconciliation")
            self._fault_exit("UNHANDLED_EXCEPTION", detail=self._safe_error_detail(error))
            raise
        finally:
            self.stream.stop()
            self.notifier.flush(timeout=12)
