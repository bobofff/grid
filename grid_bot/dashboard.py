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
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
import shutil
import subprocess
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

from .config import GridConfig
from .exchange import BinanceUsdmExchange, BinanceApiError
from .journal import TradeJournal
from .state import RunnerState, StateStore, load_runner_state, save_runner_state
from .strategy import StaticNeutralGrid


ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "dashboard_static"
ENV_FILE = ROOT / ".env"
BINANCE_TESTNET_PUBLIC_URL = "https://testnet.binancefuture.com"
EDITABLE_FIELDS = {
    "SYMBOL", "GRID_LOWER_PRICE", "GRID_UPPER_PRICE", "GRID_COUNT", "GRID_SPACING",
    "LEVERAGE", "START_REFERENCE_PRICE", "INITIAL_CAPITAL_USDT",
    "HARD_STOP_LOSS_ROI", "BREAKEVEN_ARM_ROI", "BREAKEVEN_FLOOR_ROI", "TRAILING_ARM_ROI",
    "TRAILING_DRAWDOWN_ROI", "LOOP_INTERVAL_SECONDS",
}


def launch_runner() -> str:
    """Start the strategy through PM2 so operational faults are supervised."""
    status = pm2_status()
    if status["known"] and status["status"] == "online":
        raise ValueError("策略已经在运行；请先停止后再创建或恢复网格")
    started = pm2_action("start")
    return f"PM2 {started['status']}（异常时将按 PM2 策略自动重启）"


def start_new_grid() -> dict[str, Any]:
    """Start a fresh grid after proving it cannot overwrite exchange work."""
    config = GridConfig.from_env(str(ENV_FILE))
    if config.execution_mode != "binance_testnet" or config.dry_run or not config.runner_enabled:
        raise ValueError("请启用 Binance Testnet、DRY_RUN=false 和 RUNNER_ENABLED=true 后再启动")

    exchange = BinanceUsdmExchange(config)
    exchange.verify_account()
    exchange.assert_clean_symbol()

    # A faulted PM2 instance can still be inside an exponential-restart window.
    # Stop it before replacing state, otherwise its stale in-memory runner can
    # overwrite the new NEW phase and turn a fresh grid into an empty resume.
    status = pm2_status()
    if status["known"] and status["status"] == "online":
        pm2_action("stop")

    store = StateStore(config.state_file)
    if store.path.exists():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = store.path.with_name(f"{store.path.stem}.before-new-grid-{timestamp}{store.path.suffix}")
        shutil.copy2(store.path, backup)

    # Refresh the reference immediately before spawning so page-refresh latency
    # cannot make a freshly saved grid fail its startup drift check.
    mark_price = exchange.mark_price()
    write_env({"START_REFERENCE_PRICE": str(mark_price)})
    run_id = uuid4().hex[:12]
    journal = TradeJournal.for_state_file(config.state_file)
    journal.create_grid_run(run_id, public_config(config))
    save_runner_state(store, RunnerState(grid_run_id=run_id))
    return {"mark_price": mark_price, "run_id": run_id, "message": launch_runner()}


def resume_grid() -> dict[str, Any]:
    """Restart a paused runner without changing its persisted grid legs."""
    status = pm2_status()
    if status["known"] and status["status"] == "online":
        raise ValueError("策略已经在运行；无需重启")
    config = GridConfig.from_env(str(ENV_FILE))
    store = StateStore(config.state_file)
    if not store.path.exists():
        raise ValueError("没有可恢复的网格状态；请使用“保存并启动网格”创建新网格")
    state = load_runner_state(store)
    if not state.orders:
        raise ValueError("当前状态没有可恢复的网格订单；请使用“保存并启动网格”创建新网格")
    return {"message": launch_runner(), "tracked_orders": len(state.orders)}


def exit_grid() -> dict[str, Any]:
    """Stop the runner, cancel this symbol's orders, flatten it, and lock the state.

    Each exchange mutation is deliberately followed by a clean-symbol check.  If a
    request fails halfway through, the runner stays stopped and the operator can
    inspect/retry instead of a background process placing another replacement leg.
    """
    config = GridConfig.from_env(str(ENV_FILE))
    if config.execution_mode != "binance_testnet" or config.dry_run:
        raise ValueError("一键平仓仅适用于已启用的 Binance Testnet 策略")
    status = pm2_status()
    if not status["known"]:
        raise ValueError("未找到 PM2 管理的 grid-bot；为避免与未知进程竞争，已拒绝平仓")
    if status["status"] == "online":
        pm2_action("stop")
    stopped = pm2_status()
    if stopped["status"] == "online":
        raise ValueError("策略尚未停止；为避免平仓时重新挂单，操作已中止")

    exchange = BinanceUsdmExchange(config)
    exchange.verify_account()
    exchange.cancel_all(config.symbol)
    exchange.close_all_positions(config.symbol)
    exchange.assert_clean_symbol()

    store = StateStore(config.state_file)
    state = load_runner_state(store)
    state.orders.clear()
    state.deferred_orders.clear()
    state.approved_expired_order_ids.clear()
    state.phase = "STOPPED:MANUAL_CLOSE"
    save_runner_state(store, state)
    TradeJournal.for_state_file(config.state_file).record_runner_event(
        "MANUAL_EXIT_COMPLETED", reason="OPERATOR_REQUEST",
        detail=f"{config.symbol}: orders cancelled and positions flattened",
    )
    return {"message": f"{config.symbol} 已撤销全部挂单并平仓；策略已停止", "status": stopped}


PM2_OUTPUT_LOG = ROOT / "runtime" / "pm2-out.log"
PM2_ERROR_LOG = ROOT / "runtime" / "pm2-error.log"


def _log_tail(path: Path) -> str:
    """Read a bounded UTF-8 tail without making dashboard refreshes expensive."""
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - 32_768))
            return handle.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return "（暂无日志）"


def runner_console() -> dict[str, Any]:
    """Return PM2-managed strategy logs rather than the legacy daemon log."""
    stdout, stderr = _log_tail(PM2_OUTPUT_LOG), _log_tail(PM2_ERROR_LOG)
    output = f"[ PM2 标准输出 · {PM2_OUTPUT_LOG.name} ]\n{stdout}\n\n[ PM2 错误输出 · {PM2_ERROR_LOG.name} ]\n{stderr}"
    try:
        status = pm2_status()
        return {"running": status["status"] == "online", "pid": status["pid"],
                "status": status["status"], "output": output}
    except ValueError as error:
        return {"running": False, "pid": None, "status": "PM2 不可用", "output": output,
                "error": str(error)}


PM2_GRID_PROCESS = "grid-bot"


def pm2_status() -> dict[str, Any]:
    """Return the PM2 state for the strategy without exposing arbitrary commands."""
    try:
        result = subprocess.run(["pm2", "jlist"], cwd=ROOT, text=True, capture_output=True,
                                timeout=10, check=False)
    except FileNotFoundError as error:
        raise ValueError("未找到 PM2；请先安装 Node.js 和 pm2") from error
    except subprocess.TimeoutExpired as error:
        raise ValueError("读取 PM2 状态超时") from error
    if result.returncode:
        raise ValueError(result.stderr.strip() or "PM2 状态读取失败")
    try:
        process = next((item for item in json.loads(result.stdout) if item.get("name") == PM2_GRID_PROCESS), None)
    except json.JSONDecodeError as error:
        raise ValueError("PM2 返回了无效状态") from error
    if not process:
        return {"known": False, "status": "未注册", "pid": None, "restarts": 0}
    environment = process.get("pm2_env", {})
    return {"known": True, "status": environment.get("status", "unknown"),
            "pid": process.get("pid") or None, "restarts": environment.get("restart_time", 0)}


def pm2_action(action: str) -> dict[str, Any]:
    """Execute one allow-listed PM2 lifecycle action for the strategy only."""
    commands = {
        "start": ["pm2", "start", "ecosystem.config.js", "--only", PM2_GRID_PROCESS],
        "restart": ["pm2", "restart", PM2_GRID_PROCESS],
        "stop": ["pm2", "stop", PM2_GRID_PROCESS],
    }
    if action not in commands:
        raise ValueError("Unsupported PM2 action")
    try:
        result = subprocess.run(commands[action], cwd=ROOT, text=True, capture_output=True,
                                timeout=30, check=False)
    except FileNotFoundError as error:
        raise ValueError("未找到 PM2；请先安装 Node.js 和 pm2") from error
    except subprocess.TimeoutExpired as error:
        raise ValueError("PM2 操作超时") from error
    if result.returncode:
        raise ValueError(result.stderr.strip() or result.stdout.strip() or "PM2 操作失败")
    return pm2_status()


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


def strategy_health_snapshot(config: GridConfig, state: RunnerState,
                             positions: list[dict[str, Any]], open_orders: list[dict[str, Any]],
                             pm2: dict[str, Any]) -> dict[str, Any]:
    """Translate exchange facts into operator-facing strategy risks.

    Binance already presents raw orders and positions well.  This view answers the
    distinct question: does the strategy have every leg it intends to have, and is
    a PM2-managed runner available to repair it after the next fill?
    """
    checks: list[dict[str, str]] = []
    if state.phase.startswith(("STOPPED", "EXITING")):
        checks.append({"level": "critical", "title": "策略处于退出状态",
                       "detail": f"当前阶段：{state.phase}。不会建立新的网格腿。"})
    if state.phase == "RUNNING" and pm2.get("status") != "online":
        checks.append({"level": "critical", "title": "PM2 策略守护未运行",
                       "detail": "交易所上的挂单仍可能成交，但没有运行中的策略处理成交后的替换或平仓。"})

    deferred = state.deferred_orders
    for payload in deferred:
        purpose = str(payload.get("purpose", "未知订单"))
        side = str(payload.get("side", ""))
        price = payload.get("price", "—")
        readable = {"CLOSE_SHORT": "平空", "CLOSE_LONG": "平多",
                    "OPEN_SHORT": "开空", "OPEN_LONG": "开多"}.get(purpose, purpose)
        checks.append({"level": "warning", "title": f"计划中的{readable}单尚未提交",
                       "detail": f"计划在 {price} 以 {side} 提交；策略正等待冲突订单解除或交易所恢复减仓容量。"})

    # Binance Hedge Mode rejects ``reduceOnly`` on its open-order payloads.  The
    # runner's persisted purpose is therefore the authoritative way to recognise
    # a protective closing leg; the exchange list alone would flag every healthy
    # hedge-mode position as unprotected.
    closing_sides = {
        "SHORT": str(order.get("side")) for order in state.orders.values()
        if order.get("purpose") == "CLOSE_SHORT"
    }
    closing_sides.update({
        "LONG": str(order.get("side")) for order in state.orders.values()
        if order.get("purpose") == "CLOSE_LONG"
    })
    for position in positions:
        side = str(position.get("positionSide", ""))
        required_side = "SELL" if side == "LONG" else "BUY"
        if closing_sides.get(side) != required_side:
            label = "多" if side == "LONG" else "空"
            checks.append({"level": "warning", "title": f"{label}仓没有已挂出的平仓腿",
                           "detail": "请检查下方执行时间线；可能正在等待一个延后订单，或需要人工处置。"})

    if not checks:
        checks.append({"level": "ok", "title": "策略执行健康",
                       "detail": f"PM2 在线；{len(state.orders)} 个策略订单与当前状态一致，未发现延后腿。"})
    level = "critical" if any(item["level"] == "critical" for item in checks) else (
        "warning" if any(item["level"] == "warning" for item in checks) else "ok"
    )
    return {"level": level, "checks": checks,
            "tracked_order_count": len(state.orders), "deferred_order_count": len(deferred)}


def performance_started_at(config: GridConfig, state: RunnerState) -> datetime | None:
    """Return this grid's income cutoff, with a safe fallback for pre-cutoff state files."""
    if state.baseline_started_at:
        try:
            return datetime.fromisoformat(state.baseline_started_at)
        except ValueError:
            return None
    # State files created before ``baseline_started_at`` was introduced cannot
    # distinguish historical runs exactly.  The latest successful start is the
    # only safe approximation and immediately excludes the older ledger history.
    return TradeJournal.for_state_file(config.state_file).latest_running_at()


def performance_snapshot(config: GridConfig, equity: float, positions: list[dict[str, Any]],
                         income: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Calculate strategy-scoped performance from its baseline and Binance ledger."""
    state = load_runner_state(StateStore(config.state_file))
    if state.baseline_account_equity is None:
        return None
    started_at = performance_started_at(config, state)
    if started_at is None:
        return None
    by_type: dict[str, float] = {}
    for entry in income:
        try:
            by_type[entry["incomeType"]] = by_type.get(entry["incomeType"], 0.0) + float(entry["income"])
        except (KeyError, TypeError, ValueError):
            continue
    total_pnl = equity - state.baseline_account_equity
    elapsed_seconds = max(1.0, (datetime.now(started_at.tzinfo) - started_at).total_seconds())
    # An opening commission or a small mark-to-market move during the first few
    # minutes becomes an absurd annual figure when linearly extrapolated.  Do not
    # present it as a meaningful strategy metric until a complete day has passed.
    annualized_roi = (total_pnl / config.initial_capital_usdt * 365 * 24 * 3600 / elapsed_seconds
                      if elapsed_seconds >= 24 * 3600 else None)
    return {"total_pnl": total_pnl, "invested_margin": config.initial_capital_usdt,
            "realized_pnl": by_type.get("REALIZED_PNL", 0.0),
            "unrealized_pnl": sum(float(item["unRealizedProfit"]) for item in positions),
            "funding_fee": by_type.get("FUNDING_FEE", 0.0),
            "commission": by_type.get("COMMISSION", 0.0),
            "annualized_roi": annualized_roi, "started_at": started_at.isoformat()}


def public_market_snapshot(symbol: str) -> dict[str, Any]:
    """Read public Testnet market data without accessing the trading account."""
    mark = public_binance_request("/fapi/v1/premiumIndex", {"symbol": symbol})
    ticker = public_binance_request("/fapi/v1/ticker/24hr", {"symbol": symbol})
    if not isinstance(mark, dict) or not isinstance(ticker, dict):
        raise BinanceApiError("Binance 行情返回了无效数据")
    try:
        return {"symbol": symbol, "mark_price": float(mark["markPrice"]),
                "index_price": float(mark["indexPrice"]),
                "change_percent": float(ticker["priceChangePercent"]),
                "high_price": float(ticker["highPrice"]), "low_price": float(ticker["lowPrice"]),
                "volume": float(ticker["quoteVolume"]), "updated_at": int(mark.get("time", 0))}
    except (KeyError, TypeError, ValueError) as error:
        raise BinanceApiError("Binance 行情返回了无效价格数据") from error


def public_binance_request(path: str, params: dict[str, Any]) -> Any:
    """Read an unsigned Testnet endpoint with a browser-like user agent."""
    url = f"{BINANCE_TESTNET_PUBLIC_URL}{path}?{urlencode(params)}"
    try:
        headers = {"User-Agent": "GridConsole/1.0"}
        # Testnet accepts unsigned market endpoints, but sending the configured
        # public API key matches the working account-read transport on stricter nodes.
        if os.getenv("BINANCE_API_KEY"):
            headers["X-MBX-APIKEY"] = os.environ["BINANCE_API_KEY"]
        request = Request(url, headers=headers)
        with urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode())
    except (OSError, ValueError) as error:
        raise BinanceApiError(f"Binance 行情读取失败：{error}") from error
    if isinstance(payload, dict) and payload.get("code", 0) not in (0, 200):
        raise BinanceApiError(f"Binance 行情返回异常：{payload}")
    return payload


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
                        raw_positions = positions_future.result()
                        positions = [public_position(item) for item in raw_positions]
                        open_orders = [public_order(item) for item in orders_future.result()]
                    state = load_runner_state(StateStore(config.state_file))
                    try:
                        pm2 = pm2_status()
                    except ValueError:
                        pm2 = {"known": False, "status": "unavailable", "pid": None, "restarts": 0}
                    started_at = performance_started_at(config, state)
                    income = exchange.income_history(int(started_at.timestamp() * 1000)) if started_at and state.baseline_account_equity is not None else []
                    result = {"mode": "binance_testnet", "dry_run": config.dry_run, "connected": True,
                              "mark_price": mark_price, "equity": equity, "roi": None,
                              "positions": positions, "open_orders": open_orders,
                              "performance": performance_snapshot(config, equity, raw_positions, income),
                              "strategy_health": strategy_health_snapshot(
                                  config, state, raw_positions, open_orders, pm2),
                              "message": "Testnet connected. No strategy runner is active yet; ROI starts when a strategy baseline is recorded."}
                self._json(result)
                return
            if path == "/api/market":
                config, _ = self._config_and_preview()
                symbol = parse_qs(urlparse(self.path).query).get("symbol", [config.symbol])[0]
                if symbol not in {"SOLUSDT", "BTCUSDT", "ETHUSDT"}:
                    raise ValueError("Unsupported market symbol")
                self._json(public_market_snapshot(symbol))
                return
            if path == "/api/runner-log":
                self._json(runner_console())
                return
            if path == "/api/pm2":
                self._json(pm2_status())
                return
            if path == "/api/grid-runs":
                config, _ = self._config_and_preview()
                state = load_runner_state(StateStore(config.state_file))
                runs = TradeJournal.for_state_file(config.state_file).grid_runs()
                for run in runs:
                    run["active_order_count"] = (
                        len(state.orders) if run["run_id"] == state.grid_run_id else 0
                    )
                self._json({"runs": runs})
                return
            if path.startswith("/api/grid-runs/") and path.endswith("/orders"):
                config, _ = self._config_and_preview()
                run_id = path.removeprefix("/api/grid-runs/").removesuffix("/orders").strip("/")
                if not run_id:
                    raise ValueError("Missing grid run ID")
                self._json({"run_id": run_id,
                            "orders": TradeJournal.for_state_file(config.state_file).grid_run_orders(run_id)})
                return
            return super().do_GET()
        except (ValueError, BinanceApiError) as error:
            self._json({"error": str(error)}, HTTPStatus.BAD_REQUEST)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in {"/api/config", "/api/order-rules", "/api/start", "/api/restart", "/api/exit", "/api/pm2"}:
            self._json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            data = json.loads(self.rfile.read(length))
            if not isinstance(data, dict) or not data:
                raise ValueError("Expected a JSON object")
            if path == "/api/pm2":
                self._json(pm2_action(str(data.get("action", ""))))
                return
            if path == "/api/restart":
                self._json(resume_grid())
                return
            if path == "/api/exit":
                self._json(exit_grid())
                return
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
                provisional = GridConfig.from_env(str(ENV_FILE), prefer_file=False)
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
                    GridConfig.from_env(str(ENV_FILE), prefer_file=False)
            finally:
                for key, old in old_environment.items():
                    if old is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = old
            write_env(values)
            if path == "/api/start":
                self._json(start_new_grid())
                return
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
