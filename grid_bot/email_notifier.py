"""Gmail-compatible SMTP notifications with a persistent, fail-safe dispatcher."""
from __future__ import annotations

from email.message import EmailMessage
import logging
import queue
import smtplib
import ssl
from threading import Lock, Thread
import time
from typing import Callable
from uuid import uuid4


class EmailNotifier:
    """Queue SMTP work away from trading; an SQLite outbox recovers after restarts."""

    SMTP_TIMEOUT_SECONDS = 10
    MAX_QUEUE_SIZE = 100
    DELIVERY_ATTEMPTS = 3
    MAX_OUTBOX_ATTEMPTS = 5
    OUTBOX_POLL_SECONDS = 30

    def __init__(self, config, *, sender: Callable[[str, str], None] | None = None,
                 journal=None) -> None:
        self.enabled = bool(getattr(config, "email_enabled", False))
        self.host = getattr(config, "smtp_host", "smtp.gmail.com")
        self.port = int(getattr(config, "smtp_port", 587))
        self.username = getattr(config, "smtp_username", "")
        self.app_password = getattr(config, "smtp_app_password", "")
        self.recipient = getattr(config, "email_to", "")
        self.journal = journal
        self._sender = sender or self._send_smtp
        self._queue: queue.Queue[int | tuple[str, str]] = queue.Queue(self.MAX_QUEUE_SIZE)
        self._lock = Lock()
        self._worker: Thread | None = None
        self._queued_ids: set[int] = set()
        self._last_sent_by_key: dict[str, float] = {}
        if self.enabled and self.journal is not None:
            self._recover_pending()

    def _ensure_worker_locked(self) -> None:
        if self._worker is None or not self._worker.is_alive():
            self._worker = Thread(target=self._dispatch, daemon=True, name="grid-email-notifier")
            self._worker.start()

    def _recover_pending(self) -> None:
        if not self.enabled or self.journal is None:
            return
        try:
            pending = self.journal.pending_email_notifications(self.MAX_QUEUE_SIZE)
        except Exception:
            logging.exception("Could not read pending email notifications")
            return
        with self._lock:
            queued_any = False
            for notification_id in pending:
                if notification_id in self._queued_ids:
                    continue
                try:
                    self._queue.put_nowait(notification_id)
                except queue.Full:
                    break
                self._queued_ids.add(notification_id)
                queued_any = True
            if queued_any:
                self._ensure_worker_locked()

    def notify(self, subject: str, body: str, *, dedupe_key: str | None = None,
               cooldown_seconds: float = 0) -> bool:
        """Persist/enqueue one message; return False if disabled or deduplicated."""
        if not self.enabled:
            return False
        now = time.monotonic()
        with self._lock:
            if dedupe_key and now - self._last_sent_by_key.get(dedupe_key, float("-inf")) < cooldown_seconds:
                return False
            item: int | tuple[str, str] = (subject, body)
            if self.journal is not None:
                if dedupe_key and cooldown_seconds > 0:
                    try:
                        if not self.journal.claim_email_cooldown(dedupe_key, cooldown_seconds):
                            return False
                    except Exception:
                        # Preserve alert delivery if the optional cooldown record cannot be read.
                        logging.exception("Could not persist email notification cooldown")
                # Exact keys are durable once-only claims. Cooldown alerts instead use
                # unique outbox keys so they can be delivered again after the durable cooldown.
                persistent_key = (dedupe_key if dedupe_key and cooldown_seconds <= 0
                                  else uuid4().hex)
                try:
                    notification_id = self.journal.enqueue_email_notification(
                        persistent_key, subject, body,
                    )
                except Exception:
                    logging.exception("Could not persist email notification")
                    if dedupe_key and cooldown_seconds <= 0:
                        # Do not bypass durable once-only semantics for first fills,
                        # risk exits, or scheduled summaries when SQLite is unavailable.
                        return False
                else:
                    if notification_id is None:
                        return False
                    item = notification_id
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                if isinstance(item, tuple):
                    logging.error("Email notification queue is full; dropping subject=%s", subject)
                    return False
                # The outbox is durable; the worker will recover this item on its next poll.
            else:
                if isinstance(item, int):
                    self._queued_ids.add(item)
            if dedupe_key:
                self._last_sent_by_key[dedupe_key] = now
            self._ensure_worker_locked()
        return True

    def _send_smtp(self, subject: str, body: str) -> None:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.username
        message["To"] = self.recipient
        message.set_content(body)
        with smtplib.SMTP(self.host, self.port, timeout=self.SMTP_TIMEOUT_SECONDS) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            # Google displays app passwords in groups; whitespace is not part of the credential.
            server.login(self.username, "".join(self.app_password.split()))
            server.send_message(message)

    def _deliver(self, subject: str, body: str) -> bool:
        for attempt in range(self.DELIVERY_ATTEMPTS):
            try:
                self._sender(subject, body)
                return True
            except Exception:
                logging.exception("Email notification delivery failed (attempt %d/%d): %s",
                                  attempt + 1, self.DELIVERY_ATTEMPTS, subject)
                if attempt + 1 < self.DELIVERY_ATTEMPTS:
                    time.sleep(attempt + 1)
        return False

    def _deliver_outbox(self, notification_id: int) -> None:
        if self.journal is None:
            return
        try:
            if not self.journal.claim_email_notification(notification_id):
                return
            notification = self.journal.get_email_notification(notification_id)
            if notification is None:
                return
            if self._deliver(notification["subject"], notification["body"]):
                self.journal.mark_email_notification_delivered(notification_id)
            else:
                exhausted = self.journal.release_email_notification(
                    notification_id, "SMTP delivery failed after retries",
                    max_attempts=self.MAX_OUTBOX_ATTEMPTS,
                )
                if exhausted:
                    logging.error("Email notification permanently failed after %d deliveries: id=%s",
                                  self.MAX_OUTBOX_ATTEMPTS, notification_id)
        except Exception:
            logging.exception("Email outbox delivery failed for id=%s", notification_id)
            try:
                exhausted = self.journal.release_email_notification(
                    notification_id, "Outbox dispatch error", max_attempts=self.MAX_OUTBOX_ATTEMPTS,
                )
                if exhausted:
                    logging.error("Email notification permanently failed after %d deliveries: id=%s",
                                  self.MAX_OUTBOX_ATTEMPTS, notification_id)
            except Exception:
                logging.exception("Could not release email outbox lease for id=%s", notification_id)

    def _dispatch(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=self.OUTBOX_POLL_SECONDS)
            except queue.Empty:
                self._recover_pending()
                continue
            try:
                if isinstance(item, int):
                    self._deliver_outbox(item)
                else:
                    self._deliver(*item)
            finally:
                if isinstance(item, int):
                    with self._lock:
                        self._queued_ids.discard(item)
                self._queue.task_done()

    def flush(self, timeout: float = 12) -> bool:
        """Wait briefly for queued mail; durable pending items survive process exit."""
        deadline = time.monotonic() + max(0, timeout)
        while self._queue.unfinished_tasks:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.02)
        return True
