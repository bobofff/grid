"""Parsing helpers for Binance USDⓈ-M user-data WebSocket events.

Connection/reconnect scheduling belongs in the long-running runner. Keeping parser
logic pure lets it be tested and prevents REST polling from becoming the source of
truth for fills.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OrderUpdate:
    order_id: str
    status: str
    execution_type: str
    symbol: str
    client_order_id: str


def parse_order_update(event: dict[str, Any]) -> OrderUpdate | None:
    if event.get("e") != "ORDER_TRADE_UPDATE":
        return None
    order = event["o"]
    return OrderUpdate(str(order["i"]), order["X"], order["x"], order["s"], order["c"])
