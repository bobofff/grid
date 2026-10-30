"""Append-only SQLite audit journal for grid-runner activity.

The journal intentionally stores no credentials, request signatures, or response
headers. It is separate from the compact restart state so filled/cancelled orders
remain available for later review.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from .models import GridOrder
from .user_stream import OrderUpdate


class TradeJournal:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS order_events (
                    id INTEGER PRIMARY KEY,
                    occurred_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    client_order_id TEXT,
                    binance_order_id TEXT,
                    symbol TEXT,
                    level_index INTEGER,
                    price REAL,
                    side TEXT,
                    purpose TEXT,
                    detail_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS order_events_client_id
                    ON order_events(client_order_id);
                CREATE TABLE IF NOT EXISTS runner_events (
                    id INTEGER PRIMARY KEY,
                    occurred_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    reason TEXT,
                    detail TEXT
                );
                CREATE TABLE IF NOT EXISTS grid_runs (
                    run_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    baseline_equity REAL,
                    config_json TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS grid_runs_created_at ON grid_runs(created_at DESC);
                CREATE TABLE IF NOT EXISTS strategy_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS email_outbox (
                    id INTEGER PRIMARY KEY,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    subject TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    delivered_at TEXT,
                    lease_until TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    failed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS email_notification_cooldowns (
                    dedupe_key TEXT PRIMARY KEY,
                    blocked_until TEXT NOT NULL
                );
            """)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(order_events)")}
            if "grid_run_id" not in columns:
                connection.execute("ALTER TABLE order_events ADD COLUMN grid_run_id TEXT")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS order_events_grid_run_id ON order_events(grid_run_id, id)"
            )
            outbox_columns = {row[1] for row in connection.execute("PRAGMA table_info(email_outbox)")}
            if "failed_at" not in outbox_columns:
                connection.execute("ALTER TABLE email_outbox ADD COLUMN failed_at TEXT")

    @classmethod
    def for_state_file(cls, state_file: str) -> "TradeJournal":
        return cls(Path(state_file).with_suffix(".sqlite3"))

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=10)

    def strategy_config(self) -> dict[str, str]:
        """Return editable strategy settings. Credentials never belong in this table."""
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM strategy_config").fetchall()
        return {str(key): str(value) for key, value in rows}

    def save_strategy_config(self, values: dict[str, str]) -> None:
        """Atomically upsert non-secret strategy settings."""
        if not values:
            return
        now = self._now()
        with self._connect() as connection:
            connection.executemany(
                """INSERT INTO strategy_config (key, value, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
                [(key, value, now) for key, value in values.items()],
            )

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def record_order(self, event_type: str, order: GridOrder, *,
                     binance_order_id: str | None = None,
                     symbol: str | None = None,
                     grid_run_id: str | None = None,
                     detail: dict[str, Any] | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO order_events (
                    occurred_at, event_type, client_order_id, binance_order_id, symbol,
                    level_index, price, side, purpose, grid_run_id, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (self._now(), event_type, order.client_order_id, binance_order_id, symbol,
                 order.level_index, order.price, order.side.value, order.purpose.value,
                 grid_run_id, json.dumps(detail or {}, separators=(",", ":"), sort_keys=True)),
            )

    def record_order_update(self, update: OrderUpdate, event: dict[str, Any]) -> None:
        with self._connect() as connection:
            run_row = connection.execute(
                "SELECT grid_run_id FROM order_events WHERE client_order_id = ? "
                "AND grid_run_id IS NOT NULL ORDER BY id DESC LIMIT 1", (update.client_order_id,)
            ).fetchone()
            connection.execute(
                """INSERT INTO order_events (
                    occurred_at, event_type, client_order_id, binance_order_id, symbol,
                    grid_run_id, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (self._now(), f"ORDER_{update.status}", update.client_order_id,
                 update.order_id, update.symbol,
                 run_row[0] if run_row else None,
                 json.dumps(event, separators=(",", ":"), sort_keys=True)),
            )

    def create_grid_run(self, run_id: str, config: dict[str, Any]) -> None:
        """Create the immutable audit entry before its runner can place an order."""
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO grid_runs (run_id, created_at, config_json, status) VALUES (?, ?, ?, ?)",
                (run_id, self._now(), json.dumps(config, separators=(",", ":"), sort_keys=True), "PENDING"),
            )

    def start_grid_run(self, run_id: str, started_at: str, baseline_equity: float) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE grid_runs SET started_at = ?, baseline_equity = ?, status = 'RUNNING' WHERE run_id = ?",
                (started_at, baseline_equity, run_id),
            )

    def grid_runs(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT run_id, created_at, started_at, baseline_equity, config_json, status "
                "FROM grid_runs ORDER BY created_at DESC"
            ).fetchall()
        runs = []
        for run_id, created_at, started_at, baseline_equity, config_json, status in rows:
            try:
                config = json.loads(config_json)
            except (TypeError, json.JSONDecodeError):
                config = {}
            runs.append({"run_id": run_id, "created_at": created_at, "started_at": started_at,
                         "baseline_equity": baseline_equity, "config": config, "status": status})
        return runs

    def grid_run_orders(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT occurred_at, event_type, client_order_id, binance_order_id, symbol,
                          level_index, price, side, purpose, detail_json
                   FROM order_events WHERE grid_run_id = ? ORDER BY id""", (run_id,)
            ).fetchall()
            # Older runners logged a fill discovered during restart reconciliation
            # only in runner_events.  Client IDs include the grid-run prefix, so
            # these records can be safely attributed while retaining history made
            # before ORDER_FILLED was journaled directly.
            reconciled_fills = {
                row[0] for row in connection.execute(
                    """SELECT detail FROM runner_events
                       WHERE event_type = 'RECONCILE_FILL'
                         AND reason = 'FILLED_WHILE_DOWN'
                         AND detail LIKE ?""",
                    (f"g-{run_id[:12]}-%",),
                )
            }
        orders: dict[str, dict[str, Any]] = {}
        for occurred_at, event_type, client_order_id, binance_order_id, symbol, level_index, price, side, purpose, detail_json in rows:
            if not client_order_id:
                continue
            order = orders.setdefault(client_order_id, {"client_order_id": client_order_id,
                "submitted_at": occurred_at, "binance_order_id": None, "symbol": symbol,
                "level_index": level_index, "price": price, "side": side, "purpose": purpose,
                "status": event_type.removeprefix("ORDER_") if event_type.startswith("ORDER_") else event_type})
            if binance_order_id:
                order["binance_order_id"] = binance_order_id
            if event_type.startswith("ORDER_"):
                order["status"] = event_type.removeprefix("ORDER_")
            elif event_type == "SUBMIT_CONFIRMED":
                # The REST acknowledgement means the exchange accepted the
                # order. A later user-stream ORDER_NEW/FILLED event, when it
                # arrives, will still take precedence.
                order["status"] = "NEW"
            elif event_type == "SUBMIT_FAILED":
                order["status"] = "SUBMIT_FAILED"
        for client_order_id in reconciled_fills:
            if client_order_id in orders:
                orders[client_order_id]["status"] = "FILLED"
        return list(orders.values())

    def record_runner_event(self, event_type: str, *, reason: str | None = None,
                            detail: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO runner_events (occurred_at, event_type, reason, detail) VALUES (?, ?, ?, ?)",
                (self._now(), event_type, reason, detail),
            )

    def enqueue_email_notification(self, dedupe_key: str, subject: str, body: str) -> int | None:
        """Persist a notification before dispatch so process restarts can recover it."""
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO email_outbox (dedupe_key, subject, body, created_at) "
                "VALUES (?, ?, ?, ?)", (dedupe_key, subject, body, self._now()),
            )
            if cursor.rowcount != 1:
                return None
            return int(cursor.lastrowid)

    def email_notification_exists(self, dedupe_key: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM email_outbox WHERE dedupe_key = ? LIMIT 1", (dedupe_key,),
            ).fetchone()
        return row is not None

    def pending_email_notifications(self, limit: int = 100) -> list[int]:
        now = self._now()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM email_outbox WHERE delivered_at IS NULL AND failed_at IS NULL "
                "AND (lease_until IS NULL OR lease_until <= ?) ORDER BY id LIMIT ?",
                (now, limit),
            ).fetchall()
        return [int(row[0]) for row in rows]

    def claim_email_notification(self, notification_id: int, lease_seconds: int = 120) -> bool:
        now = datetime.now(timezone.utc)
        now_text = now.isoformat(timespec="milliseconds")
        lease_text = (now + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds")
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE email_outbox SET lease_until = ? WHERE id = ? AND delivered_at IS NULL "
                "AND (lease_until IS NULL OR lease_until <= ?)",
                (lease_text, notification_id, now_text),
            )
        return cursor.rowcount == 1

    def get_email_notification(self, notification_id: int) -> dict[str, str] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT subject, body FROM email_outbox WHERE id = ? AND delivered_at IS NULL",
                (notification_id,),
            ).fetchone()
        return {"subject": str(row[0]), "body": str(row[1])} if row else None

    def mark_email_notification_delivered(self, notification_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE email_outbox SET delivered_at = ?, lease_until = NULL, last_error = NULL "
                "WHERE id = ?", (self._now(), notification_id),
            )

    def release_email_notification(self, notification_id: int, error: str,
                                   retry_after_seconds: int = 60,
                                   max_attempts: int = 5) -> bool:
        """Release a failed delivery and return whether it became permanently failed."""
        now = datetime.now(timezone.utc)
        now_text = now.isoformat(timespec="milliseconds")
        retry_at = (now + timedelta(seconds=retry_after_seconds)).isoformat(timespec="milliseconds")
        with self._connect() as connection:
            connection.execute(
                "UPDATE email_outbox SET attempts = attempts + 1, last_error = ?, "
                "lease_until = CASE WHEN attempts + 1 >= ? THEN NULL ELSE ? END, "
                "failed_at = CASE WHEN attempts + 1 >= ? THEN ? ELSE failed_at END "
                "WHERE id = ? AND delivered_at IS NULL",
                (error[:1000], max_attempts, retry_at, max_attempts, now_text, notification_id),
            )
            row = connection.execute(
                "SELECT failed_at IS NOT NULL FROM email_outbox WHERE id = ?", (notification_id,),
            ).fetchone()
        return bool(row and row[0])

    def claim_email_cooldown(self, dedupe_key: str, cooldown_seconds: float) -> bool:
        """Atomically claim a notification key until its cooldown expires."""
        now = datetime.now(timezone.utc)
        now_text = now.isoformat(timespec="milliseconds")
        blocked_until = (now + timedelta(seconds=cooldown_seconds)).isoformat(timespec="milliseconds")
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT INTO email_notification_cooldowns (dedupe_key, blocked_until) VALUES (?, ?)
                   ON CONFLICT(dedupe_key) DO UPDATE SET blocked_until = excluded.blocked_until
                   WHERE email_notification_cooldowns.blocked_until <= ?""",
                (dedupe_key, blocked_until, now_text),
            )
        return cursor.rowcount == 1

    def latest_runner_event_at(self, event_type: str, *, reason: str | None = None) -> datetime | None:
        with self._connect() as connection:
            if reason is None:
                row = connection.execute(
                    "SELECT occurred_at FROM runner_events WHERE event_type = ? ORDER BY id DESC LIMIT 1",
                    (event_type,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT occurred_at FROM runner_events WHERE event_type = ? AND reason = ? "
                    "ORDER BY id DESC LIMIT 1", (event_type, reason),
                ).fetchone()
        return datetime.fromisoformat(row[0]) if row else None

    def runner_error_count(self) -> int:
        """Count operational faults/retries recorded by the runner, not ordinary fills."""
        event_types = (
            "EXIT_STEP_FAILED", "ORDER_EVENT_UNPARSEABLE", "PERIODIC_RECONCILE_RETRY",
            "ACCOUNT_SNAPSHOT_RETRY", "WEBSOCKET_ERROR", "STREAM_DISCONNECTED",
            "SUMMARY_SNAPSHOT_FAILED", "ORDER_SUBMIT_FAILED",
        )
        placeholders = ",".join("?" for _ in event_types)
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT COUNT(*) FROM runner_events WHERE event_type IN ({placeholders})",
                event_types,
            ).fetchone()
            stopped = connection.execute(
                "SELECT COUNT(*) FROM runner_events WHERE event_type = 'FAULT_PAUSED' "
                "AND reason NOT IN ('SIGTERM', 'SIGHUP', 'KEYBOARD_INTERRUPT')"
            ).fetchone()
        return int((row[0] if row else 0) + (stopped[0] if stopped else 0))

    def latest_order_event(self, client_order_id: str) -> dict[str, Any] | None:
        """Return the most recent raw user-stream order event for one grid leg."""
        with self._connect() as connection:
            row = connection.execute(
                """SELECT detail_json FROM order_events
                   WHERE client_order_id = ? AND event_type LIKE 'ORDER_%'
                   ORDER BY id DESC LIMIT 1""", (client_order_id,)
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def first_running_at(self) -> datetime | None:
        """Return the original grid activation time used to scope PnL reporting."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT occurred_at FROM runner_events WHERE event_type = 'RUNNING' ORDER BY id LIMIT 1"
            ).fetchone()
        return datetime.fromisoformat(row[0]) if row else None

    def latest_running_at(self) -> datetime | None:
        """Return the most recent successful start for legacy states without a baseline time."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT occurred_at FROM runner_events WHERE event_type = 'RUNNING' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return datetime.fromisoformat(row[0]) if row else None
