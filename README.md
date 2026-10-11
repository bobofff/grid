# Neutral Futures Grid — MVP

This is a deliberately safe first framework for a **static neutral futures grid**.
It includes a Binance USDⓈ-M REST adapter for testnet validation. `DRY_RUN=true` and
`EXECUTION_MODE=paper` are the defaults; do not change them until recovery behaviour
has been verified on Testnet.

## Quick start

```bash
cp .env.example .env
python -m unittest discover -s tests -v
python -m grid_bot.main
```

All trading and risk parameters live in `.env`; the committed `.env.example` lists
every supported option. Amounts are examples, not recommendations.

To verify testnet credentials without placing an order, set
`EXECUTION_MODE=binance_testnet` while keeping `DRY_RUN=true`, then run the command
above. This validates account access, Hedge Mode, exchange filters, and mark-price
valuation only. Set `DRY_RUN=false` only when you explicitly want to seed Testnet
orders. The adapter intentionally refuses `BINANCE_TESTNET=false`.

## Risk semantics

- ROI is `(strategy_equity - initial_capital) / initial_capital` and includes realized
  PnL, unrealized PnL, fees, and funding. The live adapter must supply all four values.
- At `BREAKEVEN_ARM_ROI`, a fall to `BREAKEVEN_FLOOR_ROI` ends the run.
- At `TRAILING_ARM_ROI`, a fall of `TRAILING_DRAWDOWN_ROI` from the peak ROI ends it.
- At `HARD_STOP_LOSS_ROI`, the run ends immediately.
- Ending a run is a stateful sequence: freeze new grid orders → cancel outstanding
  orders → close exposure → confirm zero position. The supplied paper executor only
  records that sequence; a production adapter must make every operation idempotent.

## Static neutral grid model

The strategy seeds non-reduce-only BUY entry orders below the reference price and SELL
entry orders above it. A filled long entry is replaced by a reduce-only sell at the
next level; when it closes, the original long entry is restored. Short cycles mirror
this behaviour. The price range never moves.

`grid_bot.exchange.Exchange` is the execution boundary. `BinanceUsdmExchange` uses
signed REST endpoints; `BinanceUserStream` parses the user WebSocket events that must
drive a production runner. Keep credentials outside source control and give API keys
trading permission only—never withdrawal permission.

## Local audit trail

The live runner writes an append-only SQLite journal next to `STATE_FILE` (for
example, `runtime/state.sqlite3`). `order_events` records each submission intent,
Binance acknowledgement, and user-stream status update; `runner_events` records
startup, risk exits, failed exit steps, and unhandled exceptions. It never stores API
keys or request signatures. Inspect it with `sqlite3 runtime/state.sqlite3`.
