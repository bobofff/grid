from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def load_dotenv(path: str = ".env") -> None:
    """Small dependency-free .env loader; existing environment values win."""
    file = Path(path)
    if not file.exists():
        return
    for raw in file.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError(f"Invalid .env line: {raw!r}")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class GridConfig:
    symbol: str
    lower_price: float
    upper_price: float
    grid_count: int
    spacing: str
    order_notional_usdt: float
    leverage: int
    reference_price: float
    initial_capital_usdt: float
    hard_stop_loss_roi: float
    breakeven_arm_roi: float
    breakeven_floor_roi: float
    trailing_arm_roi: float
    trailing_drawdown_roi: float
    state_file: str
    loop_interval_seconds: float
    dry_run: bool
    execution_mode: str
    binance_api_key: str
    binance_api_secret: str
    binance_testnet: bool
    binance_recv_window: int
    binance_hedge_mode_required: bool
    runner_enabled: bool

    @classmethod
    def from_env(cls, path: str = ".env") -> "GridConfig":
        load_dotenv(path)
        config = cls(
            symbol=_required("SYMBOL"),
            lower_price=float(_required("GRID_LOWER_PRICE")),
            upper_price=float(_required("GRID_UPPER_PRICE")),
            grid_count=int(_required("GRID_COUNT")),
            spacing=_required("GRID_SPACING").lower(),
            order_notional_usdt=float(_required("ORDER_NOTIONAL_USDT")),
            leverage=int(_required("LEVERAGE")),
            reference_price=float(_required("START_REFERENCE_PRICE")),
            initial_capital_usdt=float(_required("INITIAL_CAPITAL_USDT")),
            hard_stop_loss_roi=float(_required("HARD_STOP_LOSS_ROI")),
            breakeven_arm_roi=float(_required("BREAKEVEN_ARM_ROI")),
            breakeven_floor_roi=float(_required("BREAKEVEN_FLOOR_ROI")),
            trailing_arm_roi=float(_required("TRAILING_ARM_ROI")),
            trailing_drawdown_roi=float(_required("TRAILING_DRAWDOWN_ROI")),
            state_file=_required("STATE_FILE"),
            loop_interval_seconds=float(_required("LOOP_INTERVAL_SECONDS")),
            dry_run=_required("DRY_RUN").lower() == "true",
            execution_mode=_required("EXECUTION_MODE").lower(),
            binance_api_key=os.getenv("BINANCE_API_KEY", ""),
            binance_api_secret=os.getenv("BINANCE_API_SECRET", ""),
            binance_testnet=_required("BINANCE_TESTNET").lower() == "true",
            binance_recv_window=int(_required("BINANCE_RECV_WINDOW")),
            binance_hedge_mode_required=_required("BINANCE_HEDGE_MODE_REQUIRED").lower() == "true",
            runner_enabled=_required("RUNNER_ENABLED").lower() == "true",
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.lower_price < self.reference_price < self.upper_price:
            raise ValueError("START_REFERENCE_PRICE must be strictly inside the grid range")
        if self.grid_count < 2:
            raise ValueError("GRID_COUNT must be at least 2")
        if self.spacing not in {"arithmetic", "geometric"}:
            raise ValueError("GRID_SPACING must be arithmetic or geometric")
        if self.order_notional_usdt <= 0 or self.initial_capital_usdt <= 0:
            raise ValueError("capital and order notional must be positive")
        if self.hard_stop_loss_roi >= 0:
            raise ValueError("HARD_STOP_LOSS_ROI must be negative")
        if not 0 < self.breakeven_floor_roi < self.breakeven_arm_roi < self.trailing_arm_roi:
            raise ValueError("ROI thresholds must satisfy 0 < floor < breakeven arm < trailing arm")
        if self.trailing_drawdown_roi <= 0:
            raise ValueError("TRAILING_DRAWDOWN_ROI must be positive")
        if self.execution_mode not in {"paper", "binance_testnet"}:
            raise ValueError("EXECUTION_MODE must be paper or binance_testnet")
        if self.execution_mode == "binance_testnet" and not self.binance_testnet:
            raise ValueError("The first version only supports Binance Testnet")
