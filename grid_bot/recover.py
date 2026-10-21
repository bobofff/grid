"""Approve one verified, zero-fill expired grid leg for the next resume.

This command never submits, cancels, or modifies a Binance order.  It checks the
immutable Testnet order record and the journaled WebSocket reason, then stores a
single-use local approval.  Starting the PM2 strategy remains a separate,
operator-controlled action.
"""
from __future__ import annotations

import argparse
from decimal import Decimal

from .config import GridConfig
from .exchange import BinanceUsdmExchange
from .journal import TradeJournal
from .runner import decode_order
from .state import StateStore, load_runner_state, save_runner_state


def approve_expired_order(client_order_id: str) -> None:
    config = GridConfig.from_env()
    store = StateStore(config.state_file)
    state = load_runner_state(store)
    encoded = state.orders.get(client_order_id)
    if encoded is None:
        raise ValueError("订单不在当前本地网格状态中；拒绝批准")
    order = decode_order(encoded)
    if not order.reduce_only:
        raise ValueError("仅允许审批 reduce-only 平仓腿；拒绝批准")

    exchange = BinanceUsdmExchange(config)
    exchange.sync_server_time()
    details = exchange.order_details(client_order_id)
    if details is None or details.get("status") != "EXPIRED":
        raise ValueError("Binance 订单当前不是 EXPIRED；拒绝批准")
    if Decimal(str(details.get("executedQty", "0"))) != 0:
        raise ValueError("该 EXPIRED 订单存在成交量；不能按未成交腿重挂")

    event = TradeJournal.for_state_file(config.state_file).latest_order_event(client_order_id)
    expiry_reason = str((event or {}).get("o", {}).get("er", ""))
    if expiry_reason != "4":
        raise ValueError("未找到经记录的 er=4 reduce-only 过期原因；拒绝批准")

    if client_order_id not in state.approved_expired_order_ids:
        state.approved_expired_order_ids.append(client_order_id)
        save_runner_state(store, state)
    print("已写入一次性本地恢复批准；未向 Binance 提交任何订单。")
    print(f"  client id : {client_order_id}")
    print(f"  grid leg  : {order.purpose.value} {order.side.value} @ {order.price}")
    print("下一步请先运行 python -m grid_bot.preflight；确认无误后再手动启动 PM2。")


def main() -> None:
    parser = argparse.ArgumentParser(description="Approve one verified expired grid leg for next resume")
    parser.add_argument("--approve-expired", metavar="CLIENT_ORDER_ID", required=True)
    args = parser.parse_args()
    approve_expired_order(args.approve_expired)


if __name__ == "__main__":
    main()
