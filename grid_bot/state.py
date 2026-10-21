"""Crash-safe persistence for the risk state.

The peak ROI is risk-critical: losing it after a restart would weaken the configured
trailing exit. Writes use replace-on-success so a partially written file is never
treated as valid state.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from dataclasses import asdict, dataclass, field
from typing import Any

from .risk import RiskState


class StateStore:
    def __init__(self, filename: str) -> None:
        self.path = Path(filename)

    def load_risk_state(self) -> RiskState:
        if not self.path.exists():
            return RiskState()
        payload = json.loads(self.path.read_text())
        return RiskState(peak_roi=float(payload["peak_roi"]))

    def save_risk_state(self, state: RiskState) -> None:
        self._write({"peak_roi": state.peak_roi})

    def _write(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(payload, separators=(",", ":"))
        fd, temporary = tempfile.mkstemp(prefix=".state-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


@dataclass
class RunnerState:
    peak_roi: float = float("-inf")
    baseline_account_equity: float | None = None
    # Timestamp for the equity baseline.  It identifies one grid run across PM2
    # restarts, so dashboard income totals never include an earlier grid run.
    baseline_started_at: str | None = None
    # Immutable identifier for one user-created grid.  Unlike a process ID it
    # survives runner restarts and scopes audit/history records.
    grid_run_id: str | None = None
    phase: str = "NEW"
    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Replacement legs held back to avoid crossing another one of this account's
    # live orders at the same price.  They must survive a restart.
    deferred_orders: list[dict[str, Any]] = field(default_factory=list)
    # A generic Binance EXPIRED order is normally fail-closed.  This list is an
    # explicit, one-time operator approval created by ``grid_bot.recover`` after
    # it has checked the final exchange record.  It is consumed on reconciliation.
    approved_expired_order_ids: list[str] = field(default_factory=list)

    def risk_state(self) -> RiskState:
        return RiskState(self.peak_roi)


def load_runner_state(store: StateStore) -> RunnerState:
    if not store.path.exists():
        return RunnerState()
    try:
        data = json.loads(store.path.read_text())
        if not isinstance(data, dict):
            raise ValueError("state must be a JSON object")
        return RunnerState(peak_roi=float(data.get("peak_roi", float("-inf"))),
                           baseline_account_equity=data.get("baseline_account_equity"),
                           baseline_started_at=data.get("baseline_started_at"),
                           grid_run_id=data.get("grid_run_id"),
                           phase=data.get("phase", "NEW"), orders=data.get("orders", {}),
                           deferred_orders=data.get("deferred_orders", []),
                           approved_expired_order_ids=data.get("approved_expired_order_ids", []))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as error:
        # Do not silently reset a possibly live strategy: the operator must first
        # reconcile Binance orders and positions before replacing this state file.
        raise RuntimeError(
            f"Runner state {store.path} is unreadable; reconcile the account before repairing or removing it"
        ) from error


def save_runner_state(store: StateStore, state: RunnerState) -> None:
    store._write(asdict(state))
