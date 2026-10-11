from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Purpose(str, Enum):
    OPEN_LONG = "OPEN_LONG"
    CLOSE_LONG = "CLOSE_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    CLOSE_SHORT = "CLOSE_SHORT"


@dataclass(frozen=True)
class GridOrder:
    level_index: int
    price: float
    side: Side
    purpose: Purpose
    reduce_only: bool
    origin_level: int
    client_order_id: str = ""


@dataclass(frozen=True)
class EquitySnapshot:
    realized_pnl: float
    unrealized_pnl: float
    fees: float
    funding: float
    equity_override: float | None = None

    def equity(self, initial_capital: float) -> float:
        if self.equity_override is not None:
            return self.equity_override
        return initial_capital + self.realized_pnl + self.unrealized_pnl - self.fees - self.funding
