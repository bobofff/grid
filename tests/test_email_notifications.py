from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from grid_bot.email_notifier import EmailNotifier
from grid_bot.journal import TradeJournal
import grid_bot.main as main
from grid_bot.exchange import BinanceApiError
from grid_bot.models import Purpose
from grid_bot.risk import ExitReason
from grid_bot.runner import RUNNER_ERROR_EVENT_KEY

from support import make_config, make_runner, order_update_event, ScriptedExchange


class EmailNotifierTests(unittest.TestCase):
    def config(self):
        return SimpleNamespace(
            email_enabled=True, smtp_host="smtp.gmail.com", smtp_port=587,
            smtp_username="sender@gmail.com", smtp_app_password="abcd efgh ijkl mnop",
            email_to="recipient@example.com",
        )

    def test_smtp_uses_starttls_and_the_app_password(self):
        with patch("grid_bot.email_notifier.smtplib.SMTP") as smtp_class:
            notifier = EmailNotifier(self.config())
            notifier._send_smtp("subject", "body")

        smtp_class.assert_called_once_with("smtp.gmail.com", 587, timeout=10)
        server = smtp_class.return_value.__enter__.return_value
        server.starttls.assert_called_once()
        server.login.assert_called_once_with("sender@gmail.com", "abcdefghijklmnop")
        message = server.send_message.call_args.args[0]
        self.assertEqual(message["To"], "recipient@example.com")
        self.assertEqual(message["Subject"], "subject")

    def test_dispatcher_queues_without_blocking_and_deduplicates_alerts(self):
        delivered = []
        notifier = EmailNotifier(self.config(), sender=lambda subject, body: delivered.append((subject, body)))

        self.assertTrue(notifier.notify("alert", "body", dedupe_key="ws", cooldown_seconds=60))
        self.assertFalse(notifier.notify("alert again", "body", dedupe_key="ws", cooldown_seconds=60))
        self.assertTrue(notifier.flush(timeout=2))

        self.assertEqual(delivered, [("alert", "body")])

    def test_once_only_alert_is_not_downgraded_when_outbox_persistence_fails(self):
        class BrokenJournal:
            def pending_email_notifications(self, _limit):
                return []

            def enqueue_email_notification(self, *_args):
                raise sqlite3.OperationalError("database unavailable")

        delivered = []
        notifier = EmailNotifier(
            self.config(), sender=lambda subject, body: delivered.append((subject, body)),
            journal=BrokenJournal(),
        )

        self.assertFalse(notifier.notify("first fill", "body", dedupe_key="first-fill:run"))
        self.assertEqual(delivered, [])

    def test_persistent_outbox_recovers_pending_mail_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal.for_state_file(str(Path(directory) / "state.json"))
            notification_id = journal.enqueue_email_notification("once", "subject", "body")
            delivered = []
            notifier = EmailNotifier(
                self.config(), sender=lambda subject, body: delivered.append((subject, body)),
                journal=journal,
            )
            self.assertTrue(notifier.flush(timeout=2))

            self.assertEqual(delivered, [("subject", "body")])
            record = journal.get_email_notification(notification_id)
            self.assertIsNone(record)
            self.assertEqual(journal.pending_email_notifications(), [])
            self.assertIsNone(journal.enqueue_email_notification("once", "duplicate", "body"))

    def test_cooldown_is_preserved_across_notifier_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal.for_state_file(str(Path(directory) / "state.json"))
            first_delivery = []
            first = EmailNotifier(
                self.config(), sender=lambda subject, body: first_delivery.append((subject, body)),
                journal=journal,
            )
            self.assertTrue(first.notify("alert", "body", dedupe_key="transport", cooldown_seconds=900))
            self.assertTrue(first.flush(timeout=2))
            self.assertEqual(first_delivery, [("alert", "body")])

            after_restart = EmailNotifier(self.config(), sender=lambda *_: self.fail("should not send"),
                                          journal=journal)
            self.assertFalse(after_restart.notify(
                "duplicate", "body", dedupe_key="transport", cooldown_seconds=900,
            ))

    def test_outbox_stops_retrying_after_max_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal.for_state_file(str(Path(directory) / "state.json"))
            notification_id = journal.enqueue_email_notification("bad-smtp", "subject", "body")
            disabled_config = SimpleNamespace(email_enabled=False)
            notifier = EmailNotifier(disabled_config, sender=lambda *_: (_ for _ in ()).throw(OSError("offline")),
                                     journal=journal)
            notifier.DELIVERY_ATTEMPTS = 1
            notifier.MAX_OUTBOX_ATTEMPTS = 2

            notifier._deliver_outbox(notification_id)
            with sqlite3.connect(journal.path) as connection:
                connection.execute(
                    "UPDATE email_outbox SET lease_until = ? WHERE id = ?",
                    ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), notification_id),
                )
            notifier._deliver_outbox(notification_id)

            self.assertEqual(journal.pending_email_notifications(), [])
            with sqlite3.connect(journal.path) as connection:
                attempts, failed_at = connection.execute(
                    "SELECT attempts, failed_at FROM email_outbox WHERE id = ?", (notification_id,),
                ).fetchone()
            self.assertEqual(attempts, 2)
            self.assertIsNotNone(failed_at)

    def test_email_configuration_is_optional_but_required_when_enabled(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_config(str(Path(directory) / "state.json"))
            config.validate()
            with self.assertRaisesRegex(ValueError, "SMTP_USERNAME"):
                make_config(str(Path(directory) / "enabled-state.json"), email_enabled=True).validate()


class RunnerEmailNotificationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state_file = str(Path(self.directory.name) / "state.json")
        self.exchange = ScriptedExchange()

    def make_notifier(self, journal=None):
        class Recorder:
            enabled = True

            def __init__(self, journal):
                self.journal = journal
                self.messages = []
                self.sent = Event()

            def notify(self, subject, body, **kwargs):
                dedupe_key = kwargs.get("dedupe_key")
                if self.journal is not None and dedupe_key:
                    if self.journal.enqueue_email_notification(dedupe_key, subject, body) is None:
                        return False
                self.messages.append((subject, body, kwargs))
                self.sent.set()
                return True

            def flush(self, timeout=12):
                return True

        return Recorder(journal)

    def test_start_and_only_the_first_exchange_confirmed_fill_are_notified(self):
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier
        runner.start()
        self.assertTrue(any("策略启动成功" in subject for subject, _, _ in notifier.messages))

        opening_orders = [order for order in self.exchange.submitted if order.purpose is Purpose.OPEN_LONG]
        self.exchange.restart()
        runner._handle_event(order_update_event(opening_orders[0].client_order_id))
        runner._handle_event(order_update_event(opening_orders[1].client_order_id, order_id="2"))

        fill_messages = [item for item in notifier.messages if "首次真实成交" in item[0]]
        self.assertEqual(len(fill_messages), 1)
        self.assertIn("Binance ORDER_TRADE_UPDATE", fill_messages[0][1])
        self.assertEqual(sum("策略启动成功" in item[0] for item in notifier.messages), 1)

        resumed = make_runner(self.state_file, self.exchange)
        after_restart = self.make_notifier(resumed.journal)
        resumed.notifier = after_restart
        resumed._notify_first_fill(opening_orders[0], source="reconciliation")
        self.assertFalse(any("首次真实成交" in item[0] for item in after_restart.messages))

    def test_binance_startup_api_error_is_alerted_before_runner_creation(self):
        self.exchange.fail_next("verify_account", BinanceApiError("account unavailable"))
        notifier = self.make_notifier()
        config = make_config(self.state_file, email_enabled=True)

        with patch("grid_bot.main.BinanceUsdmExchange", return_value=self.exchange), patch(
            "grid_bot.main.EmailNotifier", return_value=notifier,
        ):
            with self.assertRaisesRegex(BinanceApiError, "account unavailable"):
                main.run(config)

        self.assertTrue(any("启动校验异常" in subject for subject, _, _ in notifier.messages))

    def test_failed_unconfirmed_order_is_alerted_but_not_reported_as_success(self):
        self.exchange.fail_next("submit_grid_order", BinanceApiError("order rejected"))
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier

        with self.assertRaises(BinanceApiError):
            runner.start()

        subjects = [subject for subject, _, _ in notifier.messages]
        self.assertTrue(any("下单失败" in subject for subject in subjects))
        self.assertFalse(any("策略启动成功" in subject for subject in subjects))

    def test_a_lost_submit_response_confirmed_by_binance_is_not_alerted_as_failure(self):
        self.exchange.lose_next_submit_response()
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier

        runner.start()

        subjects = [subject for subject, _, _ in notifier.messages]
        self.assertFalse(any("下单失败" in subject for subject in subjects))
        self.assertTrue(any("策略启动成功" in subject for subject in subjects))

    def test_terminal_exchange_order_is_not_misreported_as_success(self):
        self.exchange.fail_next("submit_grid_order", BinanceApiError("response lost"))
        self.exchange.order_status = lambda _client_order_id: "CANCELED"
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier

        with self.assertRaises(BinanceApiError):
            runner.start()

        failed = next(body for subject, body, _ in notifier.messages if "下单失败" in subject)
        self.assertIn("交易所查询状态：CANCELED", failed)
        self.assertFalse(any("策略启动成功" in subject for subject, _, _ in notifier.messages))

    def test_risk_exit_trigger_sends_immediate_reason_notification(self):
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier
        runner.start()

        runner._risk_exit(ExitReason.BREAKEVEN_PROTECTION)

        exit_messages = [item for item in notifier.messages if "风控退出触发" in item[0]]
        self.assertEqual(len(exit_messages), 1)
        self.assertIn("BREAKEVEN_PROTECTION", exit_messages[0][1])

    def test_operational_stop_and_websocket_error_are_alerted(self):
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier
        runner.start()

        runner._handle_event({RUNNER_ERROR_EVENT_KEY: {
            "source": "WEBSOCKET", "detail": "socket reset",
        }})
        runner._fault_exit("SIGTERM")

        subjects = [subject for subject, _, _ in notifier.messages]
        self.assertTrue(any("WebSocket 异常" in subject for subject in subjects))
        stopped = next(body for subject, body, _ in notifier.messages if "已停止/暂停" in subject)
        self.assertIn("SIGTERM", stopped)
        self.assertIn("交易所挂单和仓位已保留", stopped)

    def test_summary_recovers_marker_when_outbox_was_written_before_restart(self):
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier
        runner.start()
        old_time = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
        runner.state.baseline_started_at = old_time
        runner._save()
        with sqlite3.connect(Path(self.state_file).with_suffix(".sqlite3")) as connection:
            connection.execute(
                "UPDATE runner_events SET occurred_at = ? WHERE id = "
                "(SELECT MAX(id) FROM runner_events WHERE event_type = 'RUNNING')",
                (old_time,),
            )
        runner._summary_due_at = runner._load_summary_due()
        run_key = runner._notification_run_key()
        period_key = int(runner._summary_due_at.timestamp())
        notification_key = f"summary:{run_key}:{period_key}"
        runner.journal.enqueue_email_notification(notification_key, "already queued", "body")
        self.exchange.positions = lambda: []

        runner._maybe_send_summary()
        self.assertIsNotNone(runner._summary_thread)
        runner._summary_thread.join(timeout=2)
        self.assertFalse(runner._summary_thread.is_alive())

        self.assertIsNotNone(runner.journal.latest_runner_event_at(
            "EMAIL_SUMMARY_QUEUED", reason=run_key,
        ))

    def test_six_hour_summary_includes_equity_profit_positions_and_error_count(self):
        runner = make_runner(self.state_file, self.exchange)
        notifier = self.make_notifier(runner.journal)
        runner.notifier = notifier
        runner.start()
        self.exchange.positions = lambda: []
        old_time = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
        runner.state.baseline_started_at = old_time
        runner._save()
        with sqlite3.connect(Path(self.state_file).with_suffix(".sqlite3")) as connection:
            connection.execute(
                "UPDATE runner_events SET occurred_at = ? WHERE id = "
                "(SELECT MAX(id) FROM runner_events WHERE event_type = 'RUNNING')",
                (old_time,),
            )
        runner.journal.record_runner_event("EMAIL_SUMMARY_QUEUED", reason="previous-grid")
        runner.journal.record_runner_event("WEBSOCKET_ERROR", reason="WEBSOCKET", detail="test")
        runner._summary_due_at = runner._load_summary_due()
        notifier.sent.clear()

        runner._maybe_send_summary()
        self.assertTrue(notifier.sent.wait(2))
        if runner._summary_thread is not None:
            runner._summary_thread.join(timeout=2)
            self.assertFalse(runner._summary_thread.is_alive())
        runner._maybe_send_summary()

        summaries = [body for subject, body, _ in notifier.messages if "6 小时运行汇总" in subject]
        self.assertEqual(len(summaries), 1)
        self.assertIn("账户权益：1000.00000000 USDT", summaries[0])
        self.assertIn("策略累计收益：+0.00000000 USDT", summaries[0])
        self.assertIn("当前持仓：无持仓", summaries[0])
        self.assertIn("异常次数：1", summaries[0])
        self.assertIn("运行状态：运行中", summaries[0])


if __name__ == "__main__":
    unittest.main()
