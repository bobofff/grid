"""Failure-path tests for the grid runner.

These pin the behaviour the runner must have when things go wrong: a restart, a
dropped or malformed user-stream frame, a burst of fills, an expiring listen key,
and an operator shutdown. Nothing here tests grid profitability.

Scenarios are built by running the runner once and then "restarting" it against
the same state file, so the persistence format is exercised rather than mocked.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from queue import Queue
import signal
import tempfile
from threading import Thread
import time
import unittest
from unittest.mock import patch

from grid_bot.exchange import BinanceApiError, BinanceRateLimitError, BinanceTransportError
from grid_bot.models import Purpose
from grid_bot.journal import TradeJournal
from grid_bot.runner import (
    BinanceUserStream,
    install_signal_handlers,
    restore_signal_handlers,
)
from grid_bot.risk import ExitReason
from grid_bot.state import StateStore, load_runner_state, save_runner_state
from grid_bot.user_stream import MalformedOrderEvent, parse_order_update

from support import (
    FillingExchange,
    OrderingProbeStream,
    ScriptedExchange,
    journal_event_types,
    make_runner,
    order_update_event,
)


class RunnerTestCase(unittest.TestCase):
    """Common fixture: a temporary state file and a fresh exchange double."""

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state_file = str(Path(directory.name) / "state.json")
        self.exchange = ScriptedExchange()

    def started_runner(self):
        """A runner that has already seeded its grid, plus its four live legs."""
        runner = make_runner(self.state_file, self.exchange)
        runner.start()
        return runner, list(self.exchange.submitted)

    def open_long_leg(self):
        return next(order for order in self.exchange.submitted
                    if order.purpose is Purpose.OPEN_LONG)

    def run_until_stopped(self, runner, timeout: float = 5.0):
        """Run the runner on a worker thread and return whatever it raised.

        `run_forever` is written to loop until a risk exit stops it, so a test that
        expects it to abort must not call it on the test thread: a regression there
        would hang the whole suite instead of failing one case.
        """
        outcome: dict[str, BaseException] = {}

        def target() -> None:
            try:
                runner.run_forever()
            except BaseException as error:  # noqa: BLE001 - the test asserts on this
                outcome["error"] = error

        worker = Thread(target=target, daemon=True)
        worker.start()
        worker.join(timeout=timeout)
        if worker.is_alive():
            runner.stopped = True
            worker.join(timeout=timeout)
            self.fail(f"run_forever did not return within {timeout}s")
        return outcome.get("error")


class ResumeReconciliationTests(RunnerTestCase):
    """A restart must reconcile persisted legs against the exchange.

    The process can die between a fill and its replacement order, so the exchange
    book - not the in-memory state - decides what actually happened.
    """

    def test_a_leg_filled_while_the_process_was_down_is_replaced(self):
        _, _ = self.started_runner()
        # Fill the highest opening buy so its close target has no adjacent opening
        # buy at the same price.
        filled = max((order for order in self.exchange.submitted
                      if order.purpose is Purpose.OPEN_LONG), key=lambda order: order.level_index)
        self.exchange.fill(filled.client_order_id)
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        resumed.start()

        self.assertIn("order_status", self.exchange.calls)
        self.assertEqual(self.exchange.count("submit_grid_order"), 1)
        self.assertEqual(self.exchange.submitted[0].purpose, Purpose.CLOSE_LONG)
        self.assertNotIn(filled.client_order_id, resumed.state.orders)
        self.assertEqual(len(resumed.state.orders), 4)
        self.assertEqual(resumed.state.phase, "RUNNING")
        self.assertIn("RECONCILE_FILL", journal_event_types(self.state_file))
        timeline = TradeJournal.for_state_file(self.state_file).grid_run_orders(
            resumed.state.grid_run_id
        )
        recovered = next(item for item in timeline
                         if item["client_order_id"] == filled.client_order_id)
        self.assertEqual(recovered["status"], "FILLED")

    def test_periodic_reconciliation_repairs_a_fill_after_a_short_unobserved_disconnect(self):
        runner, _ = self.started_runner()
        filled = max((order for order in self.exchange.submitted
                      if order.purpose is Purpose.OPEN_LONG), key=lambda order: order.level_index)
        self.exchange.fill(filled.client_order_id)
        submitted_before = self.exchange.count("submit_grid_order")
        runner._next_periodic_reconcile_at = 0

        runner._reconcile_if_due()

        self.assertNotIn(filled.client_order_id, runner.state.orders)
        self.assertEqual(self.exchange.count("submit_grid_order"), submitted_before + 1)
        self.assertIn("RECONCILE_FILL", journal_event_types(self.state_file))

    def test_an_empty_resume_is_rejected_instead_of_running_a_blank_grid(self):
        _, _ = self.started_runner()
        state_store = StateStore(self.state_file)
        state = load_runner_state(state_store)
        state.orders.clear()
        save_runner_state(state_store, state)
        self.exchange.cancel_all("BTCUSDT")
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        with self.assertRaisesRegex(RuntimeError, "refusing to run an empty grid"):
            resumed.start()

        self.assertEqual(self.exchange.count("submit_grid_order"), 0)
        self.assertEqual(resumed.state.phase, "RUNNING")

    def test_a_leg_that_vanished_without_a_fill_pauses_without_flattening(self):
        _, _ = self.started_runner()
        lost = self.open_long_leg()
        self.exchange.vanish(lost.client_order_id, "CANCELED")
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        error = self.run_until_stopped(resumed)

        self.assertIsInstance(error, BinanceApiError)
        self.assertNotIn("cancel_all", self.exchange.calls)
        self.assertNotIn("close_all_positions", self.exchange.calls)
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase,
                         "PAUSED:UNHANDLED_EXCEPTION")

    def test_stp_expired_leg_is_retried_after_reconciliation(self):
        _, _ = self.started_runner()
        expired = max((order for order in self.exchange.submitted
                       if order.purpose is Purpose.OPEN_LONG), key=lambda order: order.level_index)
        self.exchange.vanish(expired.client_order_id, "EXPIRED_IN_MATCH")
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        resumed.start()

        self.assertEqual(self.exchange.count("submit_grid_order"), 1)
        self.assertEqual(self.exchange.submitted[0].price, expired.price)
        self.assertIn("RECONCILE_STP_RETRY", journal_event_types(self.state_file))

    def test_an_operator_approved_generic_expiry_is_retried_once(self):
        _, _ = self.started_runner()
        expired = self.open_long_leg()
        self.exchange.vanish(expired.client_order_id, "EXPIRED")
        store = StateStore(self.state_file)
        state = load_runner_state(store)
        state.approved_expired_order_ids.append(expired.client_order_id)
        save_runner_state(store, state)
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        resumed.start()

        self.assertEqual(self.exchange.count("submit_grid_order"), 1)
        self.assertEqual(self.exchange.submitted[0].price, expired.price)
        self.assertNotIn(expired.client_order_id, resumed.state.approved_expired_order_ids)
        self.assertIn("RECONCILE_APPROVED_EXPIRED_RETRY", journal_event_types(self.state_file))

    def test_an_order_the_runner_does_not_own_pauses_without_flattening(self):
        _, _ = self.started_runner()
        self.exchange.live_orders["grid-somebody-else"] = "NEW"
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        error = self.run_until_stopped(resumed)

        self.assertIsInstance(error, BinanceApiError)
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase,
                         "PAUSED:UNHANDLED_EXCEPTION")
        self.assertNotIn("close_all_positions", self.exchange.calls)

    def test_a_leg_that_never_reached_binance_is_resubmitted(self):
        _, _ = self.started_runner()
        unsent = self.open_long_leg()
        self.exchange.live_orders.pop(unsent.client_order_id, None)
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        resumed.start()

        self.assertEqual(self.exchange.count("submit_grid_order"), 1)
        self.assertEqual(self.exchange.submitted[0].purpose, Purpose.OPEN_LONG)
        self.assertEqual(self.exchange.submitted[0].price, unsent.price)
        self.assertEqual(len(resumed.state.orders), 4)

    def test_a_submit_whose_response_was_lost_is_confirmed_by_client_id(self):
        self.exchange.lose_next_submit_response()

        runner = make_runner(self.state_file, self.exchange)
        runner.start()

        self.assertEqual(self.exchange.count("order_status"), 1)
        self.assertEqual(len(runner.state.orders), 4)
        self.assertEqual(len(self.exchange.live_orders), 4)
        self.assertIn("SUBMIT_FAILED", journal_event_types(self.state_file, "order_events"))

    def test_resume_does_not_demand_a_flat_symbol(self):
        # A static neutral grid holds exposure whenever a close leg is working.
        _, _ = self.started_runner()
        self.exchange.position = Decimal("0.001")
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        resumed.start()

        self.assertNotIn("assert_clean_symbol", self.exchange.calls)
        self.assertEqual(resumed.state.phase, "RUNNING")

    def test_restart_after_recoverable_shutdown_preserves_existing_position(self):
        runner, _ = self.started_runner()
        self.exchange.position = Decimal("0.001")
        runner.stop_requested = True
        self.run_until_stopped(runner)
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        resumed.start()

        self.assertEqual(resumed.state.phase, "RUNNING")
        self.assertEqual(self.exchange.position, Decimal("0.001"))
        self.assertEqual(self.exchange.count("close_all_positions"), 0)
        self.assertEqual(self.exchange.count("submit_grid_order"), 0)

    def test_corrupt_state_never_triggers_exchange_cleanup(self):
        Path(self.state_file).write_text("{not json", encoding="utf-8")

        with self.assertRaisesRegex(RuntimeError, "unreadable"):
            make_runner(self.state_file, self.exchange)

        self.assertEqual(self.exchange.count("cancel_all"), 0)
        self.assertEqual(self.exchange.count("close_all_positions"), 0)

    def test_an_interrupted_risk_exit_is_completed_but_never_reenters(self):
        _, _ = self.started_runner()
        store = StateStore(self.state_file)
        state = load_runner_state(store)
        state.phase = "EXITING:HARD_STOP_LOSS"
        save_runner_state(store, state)
        self.exchange.restart()

        resumed = make_runner(self.state_file, self.exchange)
        with self.assertRaisesRegex(RuntimeError, "manual reset required"):
            resumed.start()

        self.assertIn("cancel_all", self.exchange.calls)
        self.assertIn("close_all_positions", self.exchange.calls)
        self.assertEqual(self.exchange.count("submit_grid_order"), 0)
        self.assertEqual(resumed.state.phase, "STOPPED:HARD_STOP_LOSS")


class StartupOrderingTests(RunnerTestCase):
    """Orders may only be placed once the user stream can observe them.

    Binance pushes an order event only to a stream that is already subscribed, so a
    fill landing before the socket is live is never delivered: the runner keeps a
    leg that is already gone and never places its replacement.  On 2026-10-15 the
    level-26 leg filled at 18:11:18 while the socket only came up at 18:11:28, and
    the grid lost that leg with no journal entry to show for it.
    """

    def probe_runner(self, exchange):
        runner = make_runner(self.state_file, exchange)
        runner.stream = OrderingProbeStream(exchange)
        return runner

    def test_a_new_grid_is_subscribed_before_the_first_order(self):
        exchange = ScriptedExchange()
        runner = self.probe_runner(exchange)

        runner.start()

        self.assertIn("stream_connected", exchange.calls)
        self.assertLess(exchange.calls.index("stream_connected"),
                        exchange.calls.index("submit_grid_order"))
        self.assertEqual(len(runner.state.orders), 4)

    def test_a_resume_is_subscribed_before_it_reconciles(self):
        _, _ = self.started_runner()
        self.exchange.fill(self.open_long_leg().client_order_id)
        self.exchange.restart()

        resumed = self.probe_runner(self.exchange)
        resumed.start()

        self.assertLess(self.exchange.calls.index("stream_connected"),
                        self.exchange.calls.index("open_client_order_ids"))

    def test_a_fill_landing_during_seeding_is_replaced_immediately(self):
        exchange = FillingExchange(fill_after=2)
        runner = make_runner(self.state_file, exchange)
        exchange.events = runner.events

        runner.start()

        # Four legs, one of which filled and was replaced before start() returned.
        self.assertEqual(len(runner.state.orders), 4)
        self.assertEqual(exchange.count("submit_grid_order"), 5)
        self.assertEqual(exchange.submitted[-1].purpose, Purpose.CLOSE_LONG)

    def test_orders_are_not_placed_when_the_stream_never_connects(self):
        exchange = ScriptedExchange()
        runner = make_runner(self.state_file, exchange)

        def dead_stream(timeout):
            raise RuntimeError("The Binance user stream did not connect within 30s")

        runner.stream = OrderingProbeStream(exchange)
        runner.stream.wait_connected = dead_stream

        with self.assertRaises(RuntimeError):
            runner.start()

        self.assertEqual(exchange.count("submit_grid_order"), 0)
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase, "NEW")


class UserStreamParsingTests(RunnerTestCase):
    """A single malformed frame must never escalate into a fail-closed exit."""

    def test_malformed_order_events_raise_a_typed_error(self):
        for payload in ({"e": "ORDER_TRADE_UPDATE"},
                        {"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}},
                        {"e": "ORDER_TRADE_UPDATE",
                         "o": {"i": 1, "X": "FILLED", "x": "TRADE", "s": "BTCUSDT"}}):
            with self.subTest(payload=payload):
                with self.assertRaises(MalformedOrderEvent):
                    parse_order_update(payload)

    def test_non_order_payloads_are_ignored(self):
        for payload in ({"e": "ACCOUNT_UPDATE"}, {}, "not-a-mapping", None):
            with self.subTest(payload=payload):
                self.assertIsNone(parse_order_update(payload))

    def test_a_malformed_event_does_not_trigger_a_fail_closed_exit(self):
        runner, _ = self.started_runner()
        self.exchange.restart()

        runner.events.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}})
        runner._drain_events()

        self.assertFalse(runner.stopped)
        self.assertEqual(self.exchange.count("cancel_all"), 0)
        self.assertEqual(self.exchange.count("close_all_positions"), 0)
        self.assertIn("ORDER_EVENT_UNPARSEABLE", journal_event_types(self.state_file))

    def test_a_malformed_event_does_not_block_a_real_fill(self):
        runner, _ = self.started_runner()
        filled = max((order for order in self.exchange.submitted
                      if order.purpose is Purpose.OPEN_LONG), key=lambda order: order.level_index)
        self.exchange.restart()

        runner.events.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}})
        runner.events.put(order_update_event(filled.client_order_id))
        runner._drain_events()

        self.assertEqual(self.exchange.count("submit_grid_order"), 1)
        self.assertEqual(self.exchange.submitted[0].purpose, Purpose.CLOSE_LONG)


class EventLoopTests(RunnerTestCase):
    """A burst of fills must not be processed one per account poll."""

    def test_a_burst_of_fills_is_replaced_before_the_next_account_poll(self):
        runner, legs = self.started_runner()
        self.exchange.restart()

        snapshots: list[object] = []
        original = runner._strategy_equity

        def counting_snapshot():
            snapshot = original()
            snapshots.append(snapshot)
            if len(snapshots) >= 2:
                raise SystemExit
            return snapshot

        runner._strategy_equity = counting_snapshot
        for index, leg in enumerate(legs[:3]):
            runner.events.put(order_update_event(leg.client_order_id, order_id=str(index)))

        with self.assertRaises(SystemExit):
            runner.run_forever()

        # The last replacement would be a BUY at the same price as the close
        # created by the preceding fill, so it is held back instead of crossing
        # the runner's own order.
        self.assertEqual(self.exchange.count("submit_grid_order"), 2)
        self.assertEqual(len(runner.state.deferred_orders), 1)

    def test_conflicting_replacement_waits_until_the_adjacent_leg_is_resolved(self):
        runner, _ = self.started_runner()
        lower, higher = sorted(
            (order for order in self.exchange.submitted if order.purpose is Purpose.OPEN_LONG),
            key=lambda order: order.level_index,
        )
        self.exchange.restart()

        runner._handle_event(order_update_event(lower.client_order_id))

        self.assertEqual(self.exchange.count("submit_grid_order"), 0)
        self.assertEqual(len(runner.state.deferred_orders), 1)

        runner._handle_event(order_update_event(higher.client_order_id))

        submitted_prices = {order.price for order in self.exchange.submitted}
        self.assertEqual(submitted_prices, {90.0, 100.0})
        self.assertFalse(runner.state.deferred_orders)

    def test_zero_fill_reduce_only_expiry_is_deferred_without_pausing(self):
        runner, _ = self.started_runner()
        opening = max((order for order in self.exchange.submitted
                       if order.purpose is Purpose.OPEN_LONG), key=lambda order: order.level_index)
        self.exchange.restart()
        self.exchange.fill(opening.client_order_id)
        runner._handle_event(order_update_event(opening.client_order_id))
        close = self.exchange.submitted[-1]
        self.assertEqual(close.purpose, Purpose.CLOSE_LONG)

        self.exchange.vanish(close.client_order_id, "EXPIRED")
        self.exchange.reduce_only_allowed = False
        runner._handle_event(order_update_event(close.client_order_id, status="EXPIRED",
                                                expiry_reason="4", executed_quantity="0"))

        self.assertFalse(runner.stopped)
        self.assertNotIn(close.client_order_id, runner.state.orders)
        self.assertEqual(len(runner.state.deferred_orders), 1)
        self.assertEqual(self.exchange.count("submit_grid_order"), 1)

        self.exchange.reduce_only_allowed = True
        runner._release_deferred_orders()
        self.assertEqual(self.exchange.count("submit_grid_order"), 2)
        self.assertFalse(runner.state.deferred_orders)

    def test_draining_stops_once_an_order_is_terminal_without_flattening(self):
        runner, legs = self.started_runner()
        self.exchange.restart()

        runner.events.put(order_update_event(legs[0].client_order_id, status="CANCELED"))
        runner.events.put(order_update_event(legs[1].client_order_id, status="FILLED"))
        runner._drain_events()

        self.assertTrue(runner.stopped)
        self.assertEqual(self.exchange.count("submit_grid_order"), 0)
        self.assertEqual(self.exchange.count("close_all_positions"), 0)
        self.assertEqual(runner.events.qsize(), 1)

    def test_keyboard_interrupt_pauses_without_flattening(self):
        runner, _ = self.started_runner()
        self.exchange.restart()

        def interrupted():
            raise KeyboardInterrupt

        runner._strategy_equity = interrupted
        self.assertIsNone(self.run_until_stopped(runner))
        self.assertEqual(self.exchange.count("close_all_positions"), 0)
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase,
                         "PAUSED:KEYBOARD_INTERRUPT")

    def test_network_error_pauses_without_flattening(self):
        runner, _ = self.started_runner()
        self.exchange.restart()

        def unavailable():
            runner.stop_requested = True
            raise BinanceTransportError("TLS reset")

        runner._strategy_equity = unavailable
        self.assertIsNone(self.run_until_stopped(runner))
        self.assertEqual(self.exchange.count("close_all_positions"), 0)
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase,
                         "PAUSED:SIGTERM")

    def test_rate_limit_pauses_without_pm2_restart_exit(self):
        runner, _ = self.started_runner()
        self.exchange.restart()

        def limited():
            raise BinanceRateLimitError("too many requests", code=-1003,
                                        retry_at_ms=1_234_567_890_123)

        runner._strategy_equity = limited
        self.assertIsNone(self.run_until_stopped(runner))
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase,
                         "PAUSED:BINANCE_RATE_LIMIT")
        self.assertNotIn("close_all_positions", self.exchange.calls)

    def test_websocket_disconnect_never_flattens_or_submits_blindly(self):
        runner, _ = self.started_runner()
        leg = self.open_long_leg()
        self.exchange.restart()
        runner.stream.connected = False

        with self.assertRaisesRegex(RuntimeError, "disconnected"):
            runner._submit(leg)

        self.assertEqual(self.exchange.count("submit_grid_order"), 0)
        self.assertEqual(self.exchange.count("close_all_positions"), 0)


class RoiExitTests(RunnerTestCase):
    """Only the three configured ROI exits are permitted to flatten."""

    def _assert_risk_exit(self, reason: ExitReason) -> None:
        runner, _ = self.started_runner()
        self.exchange.restart()
        runner._risk_exit(reason)
        self.assertEqual(self.exchange.count("cancel_all"), 1)
        self.assertEqual(self.exchange.count("close_all_positions"), 1)
        self.assertEqual(runner.state.phase, f"STOPPED:{reason.value}")

    def test_hard_stop_cancels_and_flattens(self):
        self._assert_risk_exit(ExitReason.HARD_STOP_LOSS)

    def test_breakeven_take_profit_cancels_and_flattens(self):
        self._assert_risk_exit(ExitReason.BREAKEVEN_PROTECTION)

    def test_trailing_take_profit_cancels_and_flattens(self):
        self._assert_risk_exit(ExitReason.TRAILING_TAKE_PROFIT)


class ListenKeyTests(RunnerTestCase):
    """Binance expires a listen key 60 minutes after creation."""

    def test_the_refresh_interval_is_inside_the_expiry_window(self):
        self.assertLess(BinanceUserStream.LISTEN_KEY_REFRESH_SECONDS, 60 * 60)
        self.assertGreater(BinanceUserStream.LISTEN_KEY_REFRESH_SECONDS, 0)

    def test_start_launches_a_refresh_worker_and_stop_closes_the_socket(self):
        stream = BinanceUserStream(self.exchange, Queue())
        stream.refresh_seconds = 0.01

        with patch("grid_bot.runner.websocket.WebSocketApp") as app_class:
            app_class.return_value.run_forever.side_effect = lambda **_kwargs: stream.stop_event.wait(5)
            stream.start()
            deadline = time.time() + 2
            while self.exchange.count("keepalive_listen_key") < 2 and time.time() < deadline:
                time.sleep(0.01)
            stream.stop()
            for worker in (getattr(stream, "keepalive_thread", None), getattr(stream, "thread", None)):
                if worker is not None:
                    worker.join(timeout=2)

        self.assertGreaterEqual(self.exchange.count("keepalive_listen_key"), 2)
        self.assertTrue(stream.stop_event.is_set())
        self.assertTrue(app_class.return_value.close.called)

    def test_a_failed_refresh_requests_a_new_key(self):
        self.exchange.fail_always("keepalive_listen_key", BinanceTransportError("tls reset"))
        stream = BinanceUserStream(self.exchange, Queue())
        stream.refresh_seconds = 0.01
        stream.listen_key = "stale-key"

        target = getattr(stream, "_refresh_listen_key", None)
        self.assertIsNotNone(target, "the user stream has no listen-key refresh worker")
        if target is None:
            return

        worker = Thread(target=target, daemon=True)
        worker.start()
        time.sleep(0.1)
        stream.stop_event.set()
        worker.join(timeout=2)

        self.assertGreaterEqual(self.exchange.count("create_listen_key"), 1)


class ShutdownTests(RunnerTestCase):
    """A shutdown signal must pause without changing exchange exposure.

    SIGTERM arrives from `docker stop`/`systemctl stop`; SIGHUP arrives when the
    terminal that owns the process closes.  Either one used to kill the runner
    outright - no traceback, no journal entry, and a live grid left behind.
    """

    def test_sigterm_is_routed_to_a_recoverable_shutdown(self):
        runner = make_runner(self.state_file, self.exchange)
        before = signal.getsignal(signal.SIGTERM)
        previous = install_signal_handlers(runner)
        self.addCleanup(restore_signal_handlers, previous)

        installed = signal.getsignal(signal.SIGTERM)
        self.assertIsNot(installed, before)
        installed(signal.SIGTERM, None)  # deliver the signal without killing the test run

        self.assertTrue(runner.stop_requested)
        self.assertFalse(runner.stopped)

    def test_restoring_signal_handlers_puts_the_previous_handler_back(self):
        runner = make_runner(self.state_file, self.exchange)
        before = signal.getsignal(signal.SIGTERM)

        previous = install_signal_handlers(runner)
        self.assertIsNot(signal.getsignal(signal.SIGTERM), before)

        restore_signal_handlers(previous)
        self.assertIs(signal.getsignal(signal.SIGTERM), before)

    def test_a_shutdown_request_preserves_orders_and_positions(self):
        runner, _ = self.started_runner()
        self.exchange.restart()
        runner.stop_requested = True

        error = self.run_until_stopped(runner)

        self.assertIsNone(error)
        self.assertNotIn("cancel_all", self.exchange.calls)
        self.assertNotIn("close_all_positions", self.exchange.calls)
        self.assertEqual(load_runner_state(StateStore(self.state_file)).phase,
                         "PAUSED:SIGTERM")
        self.assertIn("INTERRUPTED", journal_event_types(self.state_file))

    def test_sighup_is_routed_to_a_recoverable_shutdown(self):
        runner = make_runner(self.state_file, self.exchange)
        previous = install_signal_handlers(runner)
        self.addCleanup(restore_signal_handlers, previous)

        installed = signal.getsignal(signal.SIGHUP)
        installed(signal.SIGHUP, None)

        self.assertTrue(runner.stop_requested)
        self.assertEqual(runner.stop_signal, "SIGHUP")

    def test_a_signal_that_is_already_ignored_stays_ignored(self):
        # `nohup` detaches by ignoring SIGHUP; installing a handler over it would
        # make closing the terminal tear the grid down instead of detaching.
        before = signal.getsignal(signal.SIGHUP)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        self.addCleanup(signal.signal, signal.SIGHUP, before)

        runner = make_runner(self.state_file, self.exchange)
        previous = install_signal_handlers(runner)
        self.addCleanup(restore_signal_handlers, previous)

        self.assertIs(signal.getsignal(signal.SIGHUP), signal.SIG_IGN)
        self.assertFalse(runner.stop_requested)

    def test_the_journal_records_which_signal_asked_for_the_shutdown(self):
        runner, _ = self.started_runner()
        self.exchange.restart()
        runner.stop_signal = "SIGHUP"
        runner.stop_requested = True

        self.run_until_stopped(runner)

        self.assertIn("INTERRUPTED", journal_event_types(self.state_file))
        self.assertEqual(self.exchange.count("close_all_positions"), 0)


class StreamLivenessTests(RunnerTestCase):
    """The runner must refuse to place orders it would be unable to observe."""

    def test_wait_connected_times_out_when_the_socket_never_opens(self):
        stream = BinanceUserStream(self.exchange, Queue())
        with patch("grid_bot.runner.websocket.WebSocketApp") as app_class:
            app_class.return_value.run_forever.side_effect = lambda **_kwargs: stream.stop_event.wait(5)
            stream.start()
            self.addCleanup(stream.stop)

            with self.assertRaises(RuntimeError) as raised:
                stream.wait_connected(0.05)

        self.assertIn("did not connect", str(raised.exception))

    def test_wait_connected_returns_once_the_socket_opens(self):
        stream = BinanceUserStream(self.exchange, Queue())
        stream.connected.set()

        stream.wait_connected(0.05)  # must not raise

    def test_stopping_the_stream_clears_the_connected_flag(self):
        stream = BinanceUserStream(self.exchange, Queue())
        stream.connected.set()

        stream.stop()

        self.assertFalse(stream.connected.is_set())


if __name__ == "__main__":
    unittest.main()
