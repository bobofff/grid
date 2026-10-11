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
            """)

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
                     detail: dict[str, Any] | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO order_events (
                    occurred_at, event_type, client_order_id, binance_order_id, symbol,
                    level_index, price, side, purpose, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (self._now(), event_type, order.client_order_id, binance_order_id, symbol,
                 order.level_index, order.price, order.side.value, order.purpose.value,
                 json.dumps(detail or {}, separators=(",", ":"), sort_keys=True)),
            )

    def record_order_update(self, update: OrderUpdate, event: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO order_events (
                    occurred_at, event_type, client_order_id, binance_order_id, symbol,
                    detail_json
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (self._now(), f"ORDER_{update.status}", update.client_order_id,
                 update.order_id, update.symbol,
                 json.dumps(event, separators=(",", ":"), sort_keys=True)),
            )

    def record_runner_event(self, event_type: str, *, reason: str | None = None,
                            detail: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO runner_events (occurred_at, event_type, reason, detail) VALUES (?, ?, ?, ?)",
                (self._now(), event_type, reason, detail),
            )
