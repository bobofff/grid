from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from .journal import TradeJournal


# These govern strategy behaviour and are deliberately stored in SQLite, not .env.
# Credentials and execution switches remain process-local environment secrets.
STRATEGY_CONFIG_KEYS = frozenset({
    "SYMBOL", "GRID_LOWER_PRICE", "GRID_UPPER_PRICE", "GRID_COUNT", "GRID_SPACING",
    "ORDER_NOTIONAL_USDT", "LEVERAGE", "START_REFERENCE_PRICE", "INITIAL_CAPITAL_USDT",
    "HARD_STOP_LOSS_ROI", "BREAKEVEN_ARM_ROI", "BREAKEVEN_FLOOR_ROI", "TRAILING_ARM_ROI",
    "TRAILING_DRAWDOWN_ROI", "LOOP_INTERVAL_SECONDS",
})


class StrategyConfigNotInitialized(ValueError):
    """The database has no strategy yet; the dashboard must collect one first."""


def load_dotenv(path: str = ".env", *, override: bool = False) -> None:
    """Small dependency-free .env loader."""
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
        name, parsed = key.strip(), value.strip().strip('"').strip("'")
        if override or name not in os.environ:
            os.environ[name] = parsed


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
    live_trading_confirmation: str

    @classmethod
    def from_env(cls, path: str = ".env", *, prefer_file: bool = True,
                 strategy_overrides: dict[str, str] | None = None) -> "GridConfig":
        """Load secrets/execution switches from .env and strategy values from SQLite.

        On first run only, strategy values are migrated from the legacy .env into
        the state sidecar database. Every later load ignores their .env copies.
        """
        load_dotenv(path, override=prefer_file)
        state_file = _required("STATE_FILE")
        journal = TradeJournal.for_state_file(state_file)
        strategy_values = journal.strategy_config()
        if not strategy_values:
            legacy_values = {key: os.getenv(key, "") for key in STRATEGY_CONFIG_KEYS}
            if all(legacy_values.values()):
                strategy_values = legacy_values
                journal.save_strategy_config(strategy_values)
            elif strategy_overrides:
                strategy_values = dict(strategy_overrides)
            else:
                raise StrategyConfigNotInitialized(
                    "Strategy configuration has not been created; save all parameters in the dashboard first"
                )
        missing = STRATEGY_CONFIG_KEYS - set(strategy_values)
        if missing:
            raise ValueError(f"Strategy configuration table is missing: {', '.join(sorted(missing))}")
        if strategy_overrides:
            invalid = set(strategy_overrides) - STRATEGY_CONFIG_KEYS
            if invalid:
                raise ValueError(f"Invalid strategy configuration keys: {', '.join(sorted(invalid))}")
            strategy_values = {**strategy_values, **strategy_overrides}
        setting = strategy_values.__getitem__
        config = cls(
            symbol=setting("SYMBOL"),
            lower_price=float(setting("GRID_LOWER_PRICE")),
            upper_price=float(setting("GRID_UPPER_PRICE")),
            grid_count=int(setting("GRID_COUNT")),
            spacing=setting("GRID_SPACING").lower(),
            order_notional_usdt=float(setting("ORDER_NOTIONAL_USDT")),
            leverage=int(setting("LEVERAGE")),
            reference_price=float(setting("START_REFERENCE_PRICE")),
            initial_capital_usdt=float(setting("INITIAL_CAPITAL_USDT")),
            hard_stop_loss_roi=float(setting("HARD_STOP_LOSS_ROI")),
            breakeven_arm_roi=float(setting("BREAKEVEN_ARM_ROI")),
            breakeven_floor_roi=float(setting("BREAKEVEN_FLOOR_ROI")),
            trailing_arm_roi=float(setting("TRAILING_ARM_ROI")),
            trailing_drawdown_roi=float(setting("TRAILING_DRAWDOWN_ROI")),
            state_file=state_file,
            loop_interval_seconds=float(setting("LOOP_INTERVAL_SECONDS")),
            dry_run=_required("DRY_RUN").lower() == "true",
            execution_mode=_required("EXECUTION_MODE").lower(),
            binance_api_key=os.getenv("BINANCE_API_KEY", ""),
            binance_api_secret=os.getenv("BINANCE_API_SECRET", ""),
            binance_testnet=_required("BINANCE_TESTNET").lower() == "true",
            binance_recv_window=int(_required("BINANCE_RECV_WINDOW")),
            binance_hedge_mode_required=_required("BINANCE_HEDGE_MODE_REQUIRED").lower() == "true",
            runner_enabled=_required("RUNNER_ENABLED").lower() == "true",
            live_trading_confirmation=os.getenv("LIVE_TRADING_CONFIRMATION", ""),
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
        if self.execution_mode not in {"paper", "binance_testnet", "binance_mainnet"}:
            raise ValueError("EXECUTION_MODE must be paper, binance_testnet, or binance_mainnet")
        if self.execution_mode == "binance_testnet" and not self.binance_testnet:
            raise ValueError("EXECUTION_MODE=binance_testnet requires BINANCE_TESTNET=true")
        if self.execution_mode == "binance_mainnet" and self.binance_testnet:
            raise ValueError("EXECUTION_MODE=binance_mainnet requires BINANCE_TESTNET=false")
        if (self.execution_mode == "binance_mainnet" and not self.dry_run
                and self.live_trading_confirmation != "I_UNDERSTAND_LIVE_TRADING"):
            raise ValueError(
                "Mainnet orders require LIVE_TRADING_CONFIRMATION=I_UNDERSTAND_LIVE_TRADING"
            )
