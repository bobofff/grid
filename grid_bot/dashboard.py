"""Local-only control console for the grid strategy.

This small standard-library server deliberately binds to 127.0.0.1. It exposes only
non-secret configuration and read-only Testnet account snapshots to the browser.
"""
from __future__ import annotations

from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any
from urllib.parse import urlparse

from .config import GridConfig
from .exchange import BinanceUsdmExchange, BinanceApiError
from .strategy import StaticNeutralGrid


ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "dashboard_static"
ENV_FILE = ROOT / ".env"
EDITABLE_FIELDS = {
    "SYMBOL", "GRID_LOWER_PRICE", "GRID_UPPER_PRICE", "GRID_COUNT", "GRID_SPACING",
    "LEVERAGE", "START_REFERENCE_PRICE", "INITIAL_CAPITAL_USDT",
    "HARD_STOP_LOSS_ROI", "BREAKEVEN_ARM_ROI", "BREAKEVEN_FLOOR_ROI", "TRAILING_ARM_ROI",
    "TRAILING_DRAWDOWN_ROI", "LOOP_INTERVAL_SECONDS",
}


def public_config(config: GridConfig) -> dict[str, Any]:
    return {"symbol": config.symbol, "lower_price": config.lower_price, "upper_price": config.upper_price,
            "grid_count": config.grid_count, "spacing": config.spacing,
            "order_notional_usdt": config.order_notional_usdt, "leverage": config.leverage,
            "reference_price": config.reference_price, "total_investment_usdt": config.initial_capital_usdt,
            "hard_stop_loss_roi": config.hard_stop_loss_roi, "breakeven_arm_roi": config.breakeven_arm_roi,
            "breakeven_floor_roi": config.breakeven_floor_roi, "trailing_arm_roi": config.trailing_arm_roi,
            "trailing_drawdown_roi": config.trailing_drawdown_roi, "dry_run": config.dry_run,
            "execution_mode": config.execution_mode, "testnet": config.binance_testnet}


def _round_down(value: Decimal, quantum: Decimal) -> Decimal:
    return (value / quantum).to_integral_value(rounding=ROUND_DOWN) * quantum


def _round_up(value: Decimal, quantum: Decimal) -> Decimal:
    return (value / quantum).to_integral_value(rounding=ROUND_CEILING) * quantum


def funding_plan(config: GridConfig, strategy: StaticNeutralGrid,
                 rules: dict[str, Decimal]) -> dict[str, Any]:
    """Size the grid from total collateral while respecting every opening-order rule."""
    orders = strategy.initial_orders()
    if not orders:
        raise ValueError("Reference price leaves no initial opening orders")
    step, min_qty, min_notional = rules["step"], rules["min_qty"], rules["min_notional"]
    requested_per_order = Decimal(str(config.initial_capital_usdt * config.leverage)) / len(orders)
    minimum_notionals: list[Decimal] = []
    actual_notionals: list[Decimal] = []
    for order in orders:
        price = _round_down(Decimal(str(order.price)), rules["tick"])
        minimum_quantity = _round_up(max(min_qty, min_notional / price), step)
        minimum_notionals.append(minimum_quantity * price)
        quantity = _round_down(requested_per_order / price, step)
        actual_notionals.append(quantity * price)
    required_margin = sum(minimum_notionals) / Decimal(config.leverage)
    sufficient = all(actual >= required for actual, required in zip(actual_notionals, minimum_notionals))
    return {
        "initial_order_count": len(orders),
        "minimum_margin_usdt": float(required_margin),
        "recommended_margin_usdt": float(required_margin * Decimal("1.10")),
        "minimum_order_notional_usdt": float(max(minimum_notionals)),
        "per_order_notional_usdt": float(requested_per_order),
        "total_opening_notional_usdt": float(sum(actual_notionals)),
        "sufficient": sufficient,
    }


def config_from_values(values: dict[str, str]) -> GridConfig:
    """Validate unsaved form values without persisting them to .env."""
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        return GridConfig.from_env(str(ENV_FILE))
    finally:
        for key, old in previous.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


def public_position(position: dict[str, Any]) -> dict[str, Any]:
    return {"side": position["positionSide"], "quantity": position["positionAmt"],
            "entry_price": position["entryPrice"], "mark_price": position["markPrice"],
            "unrealized_pnl": position["unRealizedProfit"], "leverage": position["leverage"],
            "liquidation_price": position["liquidationPrice"]}


def public_order(order: dict[str, Any]) -> dict[str, Any]:
    return {"id": order["orderId"], "client_id": order.get("clientOrderId", "—"),
            "side": order["side"], "position_side": order["positionSide"], "price": order["price"],
            "quantity": order["origQty"], "reduce_only": order["reduceOnly"], "status": order["status"]}


def write_env(values: dict[str, str]) -> None:
    if not ENV_FILE.exists():
        raise ValueError("Create .env from .env.example before saving configuration")
    lines = ENV_FILE.read_text().splitlines()
    remaining = dict(values)
    rewritten: list[str] = []
    for line in lines:
        key, marker, _ = line.partition("=")
        if marker and key.strip() in remaining:
            name = key.strip()
            rewritten.append(f"{name}={remaining.pop(name)}")
        else:
            rewritten.append(line)
    rewritten.extend(f"{key}={value}" for key, value in remaining.items())
    temporary = ENV_FILE.with_suffix(".tmp")
    temporary.write_text("\n".join(rewritten) + "\n")
    temporary.replace(ENV_FILE)
    os.environ.update(values)


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def _json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def end_headers(self) -> None:
        # The console is edited and restarted locally during strategy setup. Never
        # let a browser keep an old HTML/JavaScript bundle that targets stale APIs.
        self.send_header("Cache-Control", "no-store, max-age=0")
        super().end_headers()

    def _config_and_preview(self) -> tuple[GridConfig, dict[str, Any]]:
        config = GridConfig.from_env(str(ENV_FILE))
        strategy = StaticNeutralGrid(config)
        orders = strategy.initial_orders()
        return config, {"config": public_config(config), "levels": strategy.levels,
                        "orders": [{"price": o.price, "side": o.side.value, "purpose": o.purpose.value,
                                    "reduce_only": o.reduce_only} for o in orders], "funding": None}

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/api/preview":
                _, response = self._config_and_preview()
                self._json(response)
                return
            if path == "/api/status":
                config, _ = self._config_and_preview()
                result: dict[str, Any] = {"mode": config.execution_mode, "dry_run": config.dry_run,
                                           "connected": False, "message": "Paper mode: no exchange connection.",
                                           "positions": [], "open_orders": []}
                if config.execution_mode == "binance_testnet" and config.binance_api_key:
                    exchange = BinanceUsdmExchange(config)
                    # The dashboard only reads status. Exchange filters are needed on
                    # order submission, not here, and the large exchangeInfo response
                    # made ordinary page refreshes unnecessarily slow.
                    exchange.verify_account(load_symbol_filters=False)
                    with ThreadPoolExecutor(max_workers=4) as pool:
                        equity_future = pool.submit(exchange.equity_snapshot)
                        mark_future = pool.submit(exchange.mark_price)
                        positions_future = pool.submit(exchange.positions)
                        orders_future = pool.submit(exchange.open_orders)
                        equity = equity_future.result().equity(config.initial_capital_usdt)
                        mark_price = mark_future.result()
                        positions = [public_position(item) for item in positions_future.result()]
                        open_orders = [public_order(item) for item in orders_future.result()]
                    result = {"mode": "binance_testnet", "dry_run": config.dry_run, "connected": True,
                              "mark_price": mark_price, "equity": equity, "roi": None,
                              "positions": positions, "open_orders": open_orders,
                              "message": "Testnet connected. No strategy runner is active yet; ROI starts when a strategy baseline is recorded."}
                self._json(result)
                return
            return super().do_GET()
        except (ValueError, BinanceApiError) as error:
            self._json({"error": str(error)}, HTTPStatus.BAD_REQUEST)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in {"/api/config", "/api/order-rules"}:
            self._json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            data = json.loads(self.rfile.read(length))
            if not isinstance(data, dict) or not data:
                raise ValueError("Expected a JSON object")
            if set(data) - EDITABLE_FIELDS:
                raise ValueError("Attempt to modify a protected setting")
            values = {key: str(value) for key, value in data.items()}
            if path == "/api/order-rules":
                config = config_from_values(values)
                if config.execution_mode != "binance_testnet":
                    raise ValueError("请先切换到 Binance Testnet 模式后读取下单规则")
                plan = funding_plan(config, StaticNeutralGrid(config),
                                    BinanceUsdmExchange(config).symbol_order_rules())
                self._json({"funding": plan})
                return
            old_environment = {key: os.environ.get(key) for key in (*values, "ORDER_NOTIONAL_USDT")}
            os.environ.update(values)
            try:
                provisional = GridConfig.from_env(str(ENV_FILE))
                if provisional.execution_mode == "binance_testnet":
                    plan = funding_plan(provisional, StaticNeutralGrid(provisional),
                                        BinanceUsdmExchange(provisional).symbol_order_rules())
                    if not plan["sufficient"]:
                        raise ValueError(
                            f"本次网格投入不足。至少需要 {plan['minimum_margin_usdt']:.2f} USDT 保证金；"
                            f"建议投入 {plan['recommended_margin_usdt']:.2f} USDT。"
                        )
                    values["ORDER_NOTIONAL_USDT"] = str(plan["per_order_notional_usdt"])
                    os.environ.update(values)
                    GridConfig.from_env(str(ENV_FILE))
            finally:
                for key, old in old_environment.items():
                    if old is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = old
            write_env(values)
            _, response = self._config_and_preview()
            response["funding"] = plan if provisional.execution_mode == "binance_testnet" else None
            self._json(response)
        except (ValueError, json.JSONDecodeError) as error:
            self._json({"error": str(error)}, HTTPStatus.BAD_REQUEST)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8765), DashboardHandler)
    print("Grid console listening on http://127.0.0.1:8765")
    server.serve_forever()


if __name__ == "__main__":
    main()
