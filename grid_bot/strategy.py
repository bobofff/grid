from __future__ import annotations

from math import pow

from .config import GridConfig
from .models import GridOrder, Purpose, Side


class StaticNeutralGrid:
    def __init__(self, config: GridConfig) -> None:
        self.config = config
        self.levels = self._make_levels()

    def _make_levels(self) -> list[float]:
        c = self.config
        if c.spacing == "arithmetic":
            step = (c.upper_price - c.lower_price) / c.grid_count
            return [c.lower_price + step * i for i in range(c.grid_count + 1)]
        ratio = pow(c.upper_price / c.lower_price, 1 / c.grid_count)
        return [c.lower_price * pow(ratio, i) for i in range(c.grid_count + 1)]

    def initial_orders(self) -> list[GridOrder]:
        orders: list[GridOrder] = []
        for index, price in enumerate(self.levels):
            if price < self.config.reference_price:
                orders.append(GridOrder(index, price, Side.BUY, Purpose.OPEN_LONG, False, index))
            elif price > self.config.reference_price:
                orders.append(GridOrder(index, price, Side.SELL, Purpose.OPEN_SHORT, False, index))
        return orders

    def on_fill(self, order: GridOrder) -> GridOrder:
        """Return exactly one replacement order; caller submits it after persisting the fill."""
        if order.purpose is Purpose.OPEN_LONG:
            return GridOrder(order.level_index + 1, self.levels[order.level_index + 1], Side.SELL,
                             Purpose.CLOSE_LONG, True, order.origin_level)
        if order.purpose is Purpose.CLOSE_LONG:
            i = order.origin_level
            return GridOrder(i, self.levels[i], Side.BUY, Purpose.OPEN_LONG, False, i)
        if order.purpose is Purpose.OPEN_SHORT:
            return GridOrder(order.level_index - 1, self.levels[order.level_index - 1], Side.BUY,
                             Purpose.CLOSE_SHORT, True, order.origin_level)
        i = order.origin_level
        return GridOrder(i, self.levels[i], Side.SELL, Purpose.OPEN_SHORT, False, i)
