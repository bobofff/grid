"""Parsing helpers for Binance USDⓈ-M user-data WebSocket events.

Connection/reconnect scheduling belongs in the long-running runner. Keeping parser
logic pure lets it be tested and prevents REST polling from becoming the source of
truth for fills.

The parser distinguishes three cases so the runner can react appropriately:

* not an order event          -> returns ``None`` (ignore)
* an order event we cannot read -> raises :class:`MalformedOrderEvent` (journal and skip)
* a well-formed order event   -> returns an :class:`OrderUpdate`

A malformed frame must never look like a reason to stop trading: reconciliation on
the next start is what repairs a leg the runner failed to observe.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ORDER_EVENT_TYPE = "ORDER_TRADE_UPDATE"

# Every field the runner depends on. Binance always sends them on a real
# ORDER_TRADE_UPDATE, so a missing one means the frame is damaged or truncated.
REQUIRED_ORDER_FIELDS = ("i", "X", "x", "s", "c")


class MalformedOrderEvent(ValueError):
    """An ORDER_TRADE_UPDATE that is missing fields the runner depends on."""


@dataclass(frozen=True)
class OrderUpdate:
    order_id: str
    status: str
    execution_type: str
    symbol: str
    client_order_id: str
    expiry_reason: str = ""
    executed_quantity: str = "0"


def parse_order_update(event: Any) -> OrderUpdate | None:
    if not isinstance(event, dict) or event.get("e") != ORDER_EVENT_TYPE:
        return None
    order = event.get("o")
    if not isinstance(order, dict):
        raise MalformedOrderEvent("ORDER_TRADE_UPDATE without an 'o' payload")
    missing = [name for name in REQUIRED_ORDER_FIELDS if order.get(name) in (None, "")]
    if missing:
        raise MalformedOrderEvent(f"ORDER_TRADE_UPDATE is missing {', '.join(missing)}")
    return OrderUpdate(str(order["i"]), str(order["X"]), str(order["x"]),
                       str(order["s"]), str(order["c"]), str(order.get("er", "")),
                       str(order.get("z", "0")))
