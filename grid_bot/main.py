from __future__ import annotations

import logging
from pathlib import Path
import traceback

from .config import GridConfig
from .exchange import PaperExchange
from .exchange import BinanceUsdmExchange
from .journal import TradeJournal
from .runner import GridRunner
from .risk import ExitReason, RoiRiskManager
from .strategy import StaticNeutralGrid
from .state import StateStore


LOG_FILE = Path("runtime/grid_bot.log")


def configure_logging() -> None:
    """Write the same operational and error logs to the terminal and a local file."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
    )


def run(config: GridConfig | None = None, journal: TradeJournal | None = None) -> None:
    config = config or GridConfig.from_env()
    exchange = PaperExchange() if config.execution_mode == "paper" else BinanceUsdmExchange(config)
    if config.execution_mode == "binance_testnet":
        exchange.verify_account()
        logging.info("Binance Testnet verified; mark price=%.8f", exchange.mark_price())
        if config.dry_run:
            logging.info("DRY_RUN=true: Testnet verification complete; no orders submitted")
            return
        if not config.runner_enabled:
            raise RuntimeError("Set RUNNER_ENABLED=true only when you explicitly want Testnet orders submitted")
        GridRunner(config, exchange, journal).run_forever()
        return
    strategy = StaticNeutralGrid(config)
    store = StateStore(config.state_file)
    risk = RoiRiskManager(config, store.load_risk_state())
    for order in strategy.initial_orders():
        exchange.submit_grid_order(order)
    roi, reason = risk.evaluate(exchange.equity_snapshot())
    store.save_risk_state(risk.state)
    logging.info("seeded %d static grid orders; ROI=%.2f%%; risk=%s", len(exchange.orders), roi * 100, reason)
    if reason is not ExitReason.NONE:
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
