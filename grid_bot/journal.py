"""Append-only SQLite audit journal for grid-runner activity.

The journal intentionally stores no credentials, request signatures, or response
headers. It is separate from the compact restart state so filled/cancelled orders
remain available for later review.
"""
from __future__ import annotations

from datetime import datetime, timezone
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
            """)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(order_events)")}
            if "grid_run_id" not in columns:
                connection.execute("ALTER TABLE order_events ADD COLUMN grid_run_id TEXT")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS order_events_grid_run_id ON order_events(grid_run_id, id)"
            )

    @classmethod
    def for_state_file(cls, state_file: str) -> "TradeJournal":
        return cls(Path(state_file).with_suffix(".sqlite3"))

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=10)

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
        return list(orders.values())

    def record_runner_event(self, event_type: str, *, reason: str | None = None,
                            detail: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO runner_events (occurred_at, event_type, reason, detail) VALUES (?, ?, ?, ?)",
                (self._now(), event_type, reason, detail),
            )

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
