from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
import traceback
from zoneinfo import ZoneInfo

from .config import GridConfig
from .exchange import PaperExchange
from .exchange import BinanceUsdmExchange
from .journal import TradeJournal
from .runner import GridRunner, install_signal_handlers, restore_signal_handlers
from .risk import ROI_EXIT_REASONS, RoiRiskManager
from .strategy import StaticNeutralGrid
from .state import StateStore


LOG_DIRECTORY = Path("runtime")
LOG_TIMEZONE = ZoneInfo("Asia/Shanghai")


class DailyFileHandler(logging.FileHandler):
    """Append to one application log per local calendar day.

    PM2 keeps the process alive across midnight, so choosing a dated path only at
    startup would still let a single file grow indefinitely.  Reopen lazily on
    the first record of each new day instead.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.current_day: date | None = None
        super().__init__(self._path_for_today(), encoding="utf-8", delay=True)

    def _path_for_today(self) -> Path:
        return self.directory / f"grid_bot-{self._today():%Y-%m-%d}.log"

    @staticmethod
    def _today() -> date:
        return datetime.now(LOG_TIMEZONE).date()

    def emit(self, record: logging.LogRecord) -> None:
        today = self._today()
        if self.current_day != today:
            self.acquire()
            try:
                if self.current_day != today:
                    if self.stream is not None:
                        self.stream.flush()
                        self.stream.close()
                        self.stream = None
                    self.baseFilename = str(self._path_for_today())
                    self.current_day = today
            finally:
                self.release()
        super().emit(record)


def configure_logging() -> None:
    """Write operational and error logs to the terminal and one dated local file."""
    LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), DailyFileHandler(LOG_DIRECTORY)],
    )


def run(config: GridConfig | None = None, journal: TradeJournal | None = None) -> None:
    config = config or GridConfig.from_env()
    exchange = PaperExchange() if config.execution_mode == "paper" else BinanceUsdmExchange(config)
    if config.execution_mode in {"binance_testnet", "binance_mainnet"}:
        exchange.verify_account()
        environment = "Testnet" if config.execution_mode == "binance_testnet" else "Mainnet"
        logging.info("Binance %s verified; mark price=%.8f", environment, exchange.mark_price())
        if config.dry_run:
            logging.info("DRY_RUN=true: Binance %s verification complete; no orders submitted", environment)
            return
        if not config.runner_enabled:
            raise RuntimeError("Set RUNNER_ENABLED=true only when you explicitly want orders submitted")
        runner = GridRunner(config, exchange, journal)
        # `docker stop` and `systemctl stop` send SIGTERM; without a handler the
        # process dies mid-grid leaving orders and exposure in place.
        previous_handlers = install_signal_handlers(runner)
        try:
            runner.run_forever()
        finally:
            restore_signal_handlers(previous_handlers)
        return
    strategy = StaticNeutralGrid(config)
    store = StateStore(config.state_file)
    risk = RoiRiskManager(config, store.load_risk_state())
    for order in strategy.initial_orders():
        exchange.submit_grid_order(order)
    roi, reason = risk.evaluate(exchange.equity_snapshot())
    store.save_risk_state(risk.state)
    logging.info("seeded %d static grid orders; ROI=%.2f%%; risk=%s", len(exchange.orders), roi * 100, reason)
    # Keep this second execution mode under the same strict flattening policy as
    # GridRunner: only a configured ROI trigger may close an existing position.
    if reason in ROI_EXIT_REASONS:
        exchange.cancel_all(config.symbol)
        exchange.close_all_positions(config.symbol)


def main() -> None:
    configure_logging()
    journal: TradeJournal | None = None
    try:
        config = GridConfig.from_env()
        journal = TradeJournal.for_state_file(config.state_file)
        run(config, journal)
    except Exception:
        # This also captures failures before GridRunner has started, such as API
        # verification or configuration errors.
        logging.exception("Grid bot stopped with an unhandled error")
        if journal is not None:
            journal.record_runner_event("PROCESS_EXIT", reason="UNHANDLED_EXCEPTION",
                                        detail=traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
