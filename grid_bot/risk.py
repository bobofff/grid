from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .config import GridConfig
from .models import EquitySnapshot


class ExitReason(str, Enum):
    NONE = "NONE"
    HARD_STOP_LOSS = "HARD_STOP_LOSS"
    BREAKEVEN_PROTECTION = "BREAKEVEN_PROTECTION"
    TRAILING_TAKE_PROFIT = "TRAILING_TAKE_PROFIT"
    OPERATIONAL_FAILURE = "OPERATIONAL_FAILURE"


@dataclass
class RiskState:
    peak_roi: float = float("-inf")


class RoiRiskManager:
    def __init__(self, config: GridConfig, state: RiskState | None = None) -> None:
        self.config = config
        self.state = state or RiskState()

    def evaluate(self, snapshot: EquitySnapshot) -> tuple[float, ExitReason]:
        # ROI is derived from floats returned by an exchange; permit only microscopic
        # rounding error at a configured boundary, never a meaningful extra loss.
        epsilon = 1e-12
        equity = snapshot.equity(self.config.initial_capital_usdt)
        roi = (equity - self.config.initial_capital_usdt) / self.config.initial_capital_usdt
        self.state.peak_roi = max(self.state.peak_roi, roi)

        if roi <= self.config.hard_stop_loss_roi + epsilon:
            return roi, ExitReason.HARD_STOP_LOSS
        if self.state.peak_roi >= self.config.trailing_arm_roi:
            if roi <= self.state.peak_roi - self.config.trailing_drawdown_roi + epsilon:
                return roi, ExitReason.TRAILING_TAKE_PROFIT
        if self.state.peak_roi >= self.config.breakeven_arm_roi and roi <= self.config.breakeven_floor_roi + epsilon:
            return roi, ExitReason.BREAKEVEN_PROTECTION
        return roi, ExitReason.NONE
