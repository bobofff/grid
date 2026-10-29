# Neutral Futures Grid — MVP

This is a deliberately safe first framework for a **static neutral futures grid**.
It includes a Binance USDⓈ-M REST adapter for Testnet validation and explicitly
configured Mainnet execution. `DRY_RUN=true` and
`EXECUTION_MODE=paper` are the defaults; do not change them until recovery behaviour
has been verified on Testnet.

## Quick start

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Everything below assumes the virtualenv is active, so `python` is the `.venv`
interpreter. See [Start it](#start-it) to launch the bot.

`tests/test_robustness.py` covers the failure paths rather than the strategy: crash
recovery, malformed user-stream frames, fill bursts, an expiring listen key, and
operator shutdown. It uses an in-memory exchange double, so it needs no credentials
and never touches Testnet.

API credentials and execution switches live in `.env`; strategy and risk settings
are migrated on first run into the `strategy_config` table in the SQLite database
beside `STATE_FILE` (for example, `runtime/state.sqlite3`). The dashboard reads and
writes that table, so later edits to strategy numbers in `.env` do not take effect.
Amounts are examples, not recommendations.

To verify Binance credentials without placing an order, set either
`EXECUTION_MODE=binance_testnet` (`BINANCE_TESTNET=true`) or
`EXECUTION_MODE=binance_mainnet` (`BINANCE_TESTNET=false`) while keeping `DRY_RUN=true`, then run
`python -m grid_bot.main` in the foreground. This validates account access, Hedge
Mode, exchange filters, and mark-price valuation only. Set `DRY_RUN=false` only when
you explicitly want to submit orders. Mainnet order submission additionally requires
`LIVE_TRADING_CONFIRMATION=I_UNDERSTAND_LIVE_TRADING`; keep this only in the
server-local `.env`, never in source control. The execution mode and `BINANCE_TESTNET`
must match, so a Testnet key cannot accidentally be sent to Mainnet or vice versa.

## Start it

`python -m grid_bot.daemon` is the supported way to run the bot. A background job
shares the process group of the shell that started it, so when that shell goes away
— a closed terminal, a torn-down agent session — the whole group is killed, and a
killed runner leaves every order and the exposure in place with no journal entry to
explain it. The daemon calls `os.setsid()` instead, so the bot lands in its own
session where only an explicit signal can reach it.

```bash
python -m grid_bot.daemon            # start detached, print the PID
python -m grid_bot.daemon --status   # running (pid N) / not running
python -m grid_bot.daemon --stop     # SIGTERM, then wait for the fail-closed exit
```

The PID is tracked in `runtime/runner.pid`; stdout and stderr are appended to
`runtime/runner_stdout.log`. `--stop` sends `SIGTERM`, never `SIGKILL`, so the runner
gets to cancel its orders and flatten before it exits.

`python -m grid_bot.main` runs the same runner in the foreground instead, which is
what you want while watching it start. Do not launch that one with a bare `&` and
walk away: it is killed together with its shell, silently, and the grid is left
resting with nobody left to replace a fill.

`python -m grid_bot.dashboard` serves the local web dashboard as its own process.

### PM2 dashboard service

When using PM2, start both the strategy and dashboard definitions together:

```bash
pm2 start ecosystem.config.js
pm2 logs grid-dashboard
```

The dashboard is supervised independently as `grid-dashboard`, so restarting its
web server does not interrupt a running strategy. Its logs are written to
`runtime/dashboard-pm2-out.log` and `runtime/dashboard-pm2-error.log`.

### Daily log rotation

Run this once on the machine that runs PM2 (after activating the project is not
required):

```bash
./scripts/configure-pm2-logrotate.sh
```

It installs/configures PM2's official rotation module. PM2 output and error logs
rotate at midnight in `Asia/Shanghai`, receive a `YYYY-MM-DD` suffix, are
compressed after rotation, and keep 14 days. A noisy process is also rotated at
20 MB instead of waiting for midnight. The application's own logs are written as
`runtime/grid_bot-YYYY-MM-DD.log` and switch files at midnight in `Asia/Shanghai`.

`grid-bot` uses PM2 exponential restart backoff: it waits at least five seconds
after an early failure, increases the wait for consecutive failures, and stops
after ten short-lived failures. Inspect and reconcile the exchange before manually
restarting a stopped strategy.

After confirming both processes work, persist the PM2 process list for reboot:

```bash
pm2 save
pm2 startup
```

## Inspecting a run

`python -m grid_bot.preflight` is a strictly read-only diagnostic: it prints the
configuration, the live exchange state, the three `start()` gate checks, and a
per-leg preview of what reconciliation would decide. Run it before a restart when
you want to know what the runner is about to do.

## Risk semantics

- ROI is `(strategy_equity - initial_capital) / initial_capital` and includes realized
  PnL, unrealized PnL, fees, and funding. The live adapter must supply all four values.
- At `BREAKEVEN_ARM_ROI`, a fall to `BREAKEVEN_FLOOR_ROI` ends the run.
- At `TRAILING_ARM_ROI`, a fall of `TRAILING_DRAWDOWN_ROI` from the peak ROI ends it.
- At `HARD_STOP_LOSS_ROI`, the run ends immediately.
- Ending a run is a stateful sequence: freeze new grid orders → cancel outstanding
  orders → close exposure → confirm zero position. The supplied paper executor only
  records that sequence; a production adapter must make every operation idempotent.

## Restart and recovery

A restart never trusts the persisted state file on its own. `GridRunner.start()`
compares the legs it believes are live against the exchange before it resumes:

- a leg that filled while the process was down is replaced, exactly as if the fill
  had arrived over the user stream;
- a leg that vanished without a fill (cancelled, expired, rejected) ends the run
  fail-closed instead of resuming an incomplete grid;
- a leg that never reached Binance is resubmitted;
- an open order the runner does not own stops the restart and asks the operator to
  reconcile the account by hand.

A phase of `STOPPED:...` or `EXITING:...` means a previous exit did not finish, so
the runner completes that cleanup before it will place a new grid.

A submit whose response is lost is resolved by client order ID rather than retried
blindly, so a dropped POST cannot leave an orphan order working unsupervised. A
single malformed user-stream frame is journalled and skipped; reconciliation on the
next start is what repairs a leg the runner failed to observe.

`start()` subscribes to the user stream *before* it places or reconciles a single
order, and waits for the socket to come up. Binance only pushes an order event to a
stream that is already subscribed, so a fill landing in the gap between seeding the
grid and subscribing is never delivered: the runner would keep a leg that is already
gone and never place its replacement. If the socket does not connect within
`GridRunner.STREAM_CONNECT_TIMEOUT_SECONDS`, the runner refuses to trade at all
rather than build a grid it cannot observe.

Shutdown is fail-closed. `SIGTERM` - what `docker stop` and `systemctl stop` send -
and `SIGHUP` - what a closing terminal sends - are both routed into the same
cancel-then-flatten sequence as a risk exit, and the journal records which signal
asked for it. A signal that is already ignored stays ignored, so `nohup` still
detaches instead of tearing the grid down.

## Static neutral grid model

The strategy seeds non-reduce-only BUY entry orders below the reference price and SELL
entry orders above it. A filled long entry is replaced by a reduce-only sell at the
next level; when it closes, the original long entry is restored. Short cycles mirror
this behaviour. The price range never moves.

`grid_bot.exchange.Exchange` is the execution boundary. `BinanceUsdmExchange` uses
signed REST endpoints; `BinanceUserStream` keeps the listen key refreshed well
inside Binance's 60-minute expiry, reconnects with a fresh key when a refresh fails,
and parses the user WebSocket events that must drive a production runner. Keep
credentials outside source control and give API keys trading permission only—never
withdrawal permission.

## Local audit trail

The live runner writes an append-only SQLite journal next to `STATE_FILE` (for
example, `runtime/state.sqlite3`). `order_events` records each submission intent,
Binance acknowledgement, and user-stream status update; `runner_events` records
startup, restart reconciliation (`RECONCILE_*`), risk exits, failed exit steps,
unparseable user-stream frames, operator shutdowns, and unhandled exceptions. It
never stores API keys or request signatures. Inspect it with
`sqlite3 runtime/state.sqlite3`.
