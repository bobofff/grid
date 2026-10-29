"""Read-only preflight: what would `GridRunner.start()` see right now?

Every exchange call in this module is a GET. It never submits, cancels, or
modifies anything, so it is safe to run against a live account while deciding
whether to start, resume, or reconcile the runner.

Run from the repository root:

    python -m grid_bot.preflight
"""
from __future__ import annotations

import sys
from decimal import Decimal, InvalidOperation

from .config import GridConfig
from .exchange import BinanceApiError, BinanceUsdmExchange
from .journal import TradeJournal
from .runner import decode_order
from .state import StateStore, load_runner_state
from .strategy import StaticNeutralGrid


def _rule(title: str) -> None:
    print()
    print(f"--- {title} " + "-" * max(0, 60 - len(title)))


def _verdict(label: str, ok: bool, detail: str) -> None:
    print(f"  [{'ok' if ok else '!!'}] {label:<38} {detail}")


def _is_zero_fill_capacity_expiry(order, event: object) -> bool:
    payload = event.get("o", {}) if isinstance(event, dict) else {}
    if not order.reduce_only or not isinstance(payload, dict) or str(payload.get("er", "")) != "4":
        return False
    try:
        return Decimal(str(payload.get("z", ""))) == 0
    except InvalidOperation:
        return False


def main() -> int:
    config = GridConfig.from_env()
    strategy = StaticNeutralGrid(config)
    store = StateStore(config.state_file)

    _rule("configuration")
    print(f"  symbol            : {config.symbol}")
    print(f"  execution mode    : {config.execution_mode} (dry_run={config.dry_run}, "
          f"runner_enabled={config.runner_enabled})")
    print(f"  grid              : {config.lower_price} .. {config.upper_price} "
          f"x{config.grid_count} ({config.spacing})")
    print(f"  reference price   : {config.reference_price}")
    print(f"  leverage          : {config.leverage}x")
    print(f"  state file        : {config.state_file}")

    exchange = BinanceUsdmExchange(config)
    try:
        exchange.sync_server_time()
        exchange.verify_account()
        mark_price = exchange.mark_price()
        positions = exchange.positions()
        open_orders = exchange.open_orders()
    except BinanceApiError as error:
        print(f"\n  [!!] Binance rejected the read-only preflight: {error}")
        return 1

    _rule("exchange")
    print(f"  mark price        : {mark_price}")
    print(f"  open orders       : {len(open_orders)}")
    print(f"  non-zero positions: {len(positions)}")
    for position in positions:
        print(f"      {position['positionSide']:<6} qty={position['positionAmt']:<14} "
              f"entry={position['entryPrice']:<14} uPnL={position['unRealizedProfit']}")

    _rule("start() gate checks")
    smallest_step = min(b - a for a, b in zip(strategy.levels, strategy.levels[1:]))
    in_range = config.lower_price < mark_price < config.upper_price
    _verdict("mark price inside the grid range", in_range,
             f"{mark_price} vs ({config.lower_price}, {config.upper_price})")
    drift = abs(mark_price - config.reference_price)
    _verdict("reference price close to mark", drift <= smallest_step / 2,
             f"drift={drift:.4f}, tolerance={smallest_step / 2:.4f}")
    _verdict("symbol is flat", not positions,
             "no position" if not positions else f"{len(positions)} position(s) open")

    _rule("persisted runner state")
    if not store.path.exists():
        print("  no state file: start() would seed a brand new grid (phase NEW)")
        return 0 if in_range else 1
    state = load_runner_state(store)
    print(f"  phase             : {state.phase}")
    print(f"  baseline equity   : {state.baseline_account_equity}")
    print(f"  peak roi          : {state.peak_roi}")
    print(f"  tracked legs      : {len(state.orders)}")

    if state.phase.startswith("STOPPED:"):
        print()
        print("  ROI risk exit is terminal: start() will not automatically re-enter.")
        return 0
    if state.phase.startswith("EXITING:"):
        print()
        print("  start() will only finish this interrupted ROI exit, then remain stopped.")
        return 0

    if state.phase.startswith("PAUSED:"):
        print("  paused recovery     : reconciliation will classify and resolve terminal legs")
    elif state.phase != "RUNNING":
        print(f"\n  [!!] start() would refuse: phase {state.phase} is terminal")
        return 1

    _rule("reconciliation preview (this is what start() would decide)")
    live = {str(item["clientOrderId"]) for item in open_orders if item.get("clientOrderId")}
    foreign = live - set(state.orders)
    unresolved = [cid for cid in state.orders if cid not in live]

    print(f"  live on exchange      : {len(live)}")
    print(f"  tracked locally       : {len(state.orders)}")
    print(f"  matching legs         : {len(set(state.orders) & live)}")
    print(f"  live but not ours     : {len(foreign)}")
    print(f"  tracked but not live  : {len(unresolved)}")

    if foreign:
        print()
        print("  VERDICT: FAIL CLOSED - cancels every order on the symbol, then flattens.")
        print("           orders the runner does not own:")
        for client_order_id in sorted(foreign)[:5]:
            print(f"             {client_order_id}")
        return 0

    if not unresolved:
        print()
        print("  VERDICT: CLEAN - the runner resumes the existing grid without touching orders.")
        return 0

    print()
    print("  VERDICT: per-leg resolution")
    journal = TradeJournal.for_state_file(config.state_file)
    verdicts = {"FILLED": 0, "resubmit": 0, "stp-retry": 0, "capacity-defer": 0,
                "approved": 0, "fail-closed": 0}
    for client_order_id in unresolved:
        order = decode_order(state.orders[client_order_id])
        status = exchange.order_status(client_order_id)
        if status is None:
            outcome, note = "resubmit", "never reached Binance -> safe to resend"
            verdicts["resubmit"] += 1
        elif status == "FILLED":
            outcome, note = "FILLED", "replace it as a normal fill"
            verdicts["FILLED"] += 1
        elif status == "EXPIRED" and client_order_id in state.approved_expired_order_ids:
            outcome, note = "approved", "operator-approved zero-fill retry"
            verdicts["approved"] += 1
        elif status == "EXPIRED_IN_MATCH":
            outcome, note = "stp-retry", "self-trade prevention expiry -> safe retry"
            verdicts["stp-retry"] += 1
        elif status == "EXPIRED" and _is_zero_fill_capacity_expiry(
                order, journal.latest_order_event(client_order_id)):
            outcome, note = "capacity-defer", "zero-fill reduce-only expiry -> defer until capacity exists"
            verdicts["capacity-defer"] += 1
        else:
            outcome, note = "fail-closed", f"status {status} with no fill"
            verdicts["fail-closed"] += 1
        print(f"    {client_order_id:<36} {outcome:<12} {note}")
    print()
    print(f"  totals: {verdicts['FILLED']} to replace, {verdicts['resubmit']} to resend, "
          f"{verdicts['stp-retry']} STP retries, {verdicts['capacity-defer']} capacity-deferred, "
          f"{verdicts['approved']} operator-approved, "
          f"{verdicts['fail-closed']} causing a fail-closed exit")
    return 0


if __name__ == "__main__":
    sys.exit(main())
