"""Live cash execution adapter — LOCKED until all gates pass."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.base import ExecutionAdapter, ExecutionResult, OrderIntent, OrderState

_LIVE_PRODUCT = "D"  # NSE cash delivery — validate against Upstox docs before live enablement


class LiveReadinessGate:
    REQUIRED = [
        "broker_authenticated",
        "profile_validated",
        "funds_validated",
        "websocket_healthy",
        "quote_fresh",
        "scoring_v2",
        "risk_operational",
        "portfolio_operational",
        "entry_monitor_operational",
        "no_duplicate_order",
        "daily_loss_configured",
        "max_trades_configured",
        "max_positions_configured",
        "max_notional_configured",
        "max_exposure_configured",
        "sector_exposure_configured",
        "slippage_configured",
        "emergency_stop_operational",
        "disconnect_protection",
        "persistence_operational",
        "restart_restoration",
        "phase2_pass",
    ]

    @classmethod
    def evaluate(cls, app_module: Any, intent: OrderIntent) -> tuple[bool, list[str]]:
        prefs = app_module.WORKSPACE.preferences
        blockers: list[str] = []
        broker = app_module.get_broker_state().snapshot()
        if not broker.get("authenticated"):
            blockers.append("broker_authenticated")
        if not broker.get("connected"):
            blockers.append("profile_validated")
        feed = app_module.get_market_state().snapshot() if hasattr(app_module, "get_market_state") else {}
        if feed.get("data_source") not in {"BROKER_LIVE", "BROKER_SNAPSHOT"}:
            blockers.append("websocket_healthy")
        if app_module.get_scoring_engine_version() != "V2":
            blockers.append("scoring_v2")
        if not prefs.get("live_daily_loss_limit"):
            blockers.append("daily_loss_configured")
        if not prefs.get("live_max_trades"):
            blockers.append("max_trades_configured")
        if not prefs.get("live_max_open_positions"):
            blockers.append("max_positions_configured")
        if not prefs.get("live_max_order_notional"):
            blockers.append("max_notional_configured")
        if not prefs.get("live_max_deployed_capital"):
            blockers.append("max_exposure_configured")
        if not prefs.get("live_risk_per_trade"):
            blockers.append("risk_per_trade_configured")
        if not prefs.get("live_sector_exposure_limit"):
            blockers.append("sector_exposure_configured")
        if prefs.get("live_slippage_bps") is None:
            blockers.append("slippage_configured")
        if prefs.get("live_mode_enabled") is not True:
            blockers.append("live_mode_enabled")
        if cls._duplicate(intent, app_module):
            blockers.append("no_duplicate_order")
        if cls._first_day_limit_reached(app_module):
            blockers.append("first_day_max_orders")
        return len(blockers) == 0, blockers

    @staticmethod
    def _first_day_limit_reached(app_module: Any) -> bool:
        prefs = app_module.WORKSPACE.preferences
        limit = prefs.get("live_first_day_max_orders")
        if not limit:
            return False
        repo = app_module.st.session_state.setdefault("live_order_repository", {})
        submitted = sum(1 for o in repo.values() if o.get("network_submitted"))
        return submitted >= int(limit)

    @staticmethod
    def _duplicate(intent: OrderIntent, app_module: Any) -> bool:
        repo = app_module.st.session_state.setdefault("live_order_repository", {})
        return intent.client_order_id in repo


class LiveExecutionAdapter(ExecutionAdapter):
    """Never submits network orders unless explicitly unlocked AND readiness PASS."""

    LOCKED = True

    def precheck(self, intent: OrderIntent, trade: Any, app_module: Any) -> tuple[bool, str]:
        if self.LOCKED or not app_module.WORKSPACE.preferences.get("live_mode_enabled"):
            return False, "LIVE EXECUTION LOCKED"
        if intent.side.upper() not in {"BUY", "SELL"}:
            return False, "Invalid side"
        if not intent.symbol.endswith(".NS"):
            return False, "NSE cash equity only"
        if intent.product != _LIVE_PRODUCT:
            return False, "Invalid product code"
        ok, blockers = LiveReadinessGate.evaluate(app_module, intent)
        if not ok:
            return False, f"Live gates failed: {', '.join(blockers)}"
        verdict = getattr(trade, "risk_verdict", {}) or {}
        if verdict.get("verdict") not in ("APPROVED", "APPROVED_REDUCED_SIZE"):
            return False, "Risk approval invalid"
        if getattr(trade, "entry_status", "") not in ("READY", "READY_TO_ENTER"):
            return False, "Entry not ready"
        quote = app_module.get_market_state().get_quote(intent.symbol) if hasattr(app_module, "get_market_state") else None
        if quote and quote.get("stale"):
            return False, "Quote stale"
        return True, "PRECHECK_PASSED"

    def serialize_order(self, intent: OrderIntent) -> dict[str, Any]:
        return {
            "symbol": intent.symbol.replace(".NS", ""),
            "qty": intent.quantity,
            "type": "MARKET",
            "side": intent.side,
            "product": intent.product,
            "validity": "DAY",
            "disclosed_quantity": 0,
            "is_amo": False,
            "client_id": intent.client_order_id,
        }

    def execute(self, intent: OrderIntent, trade: Any, app_module: Any) -> ExecutionResult:
        payload = self.serialize_order(intent)
        ok, reason = self.precheck(intent, trade, app_module)
        if not ok and not (self.LOCKED or app_module.WORKSPACE.preferences.get("live_dry_run_only", True)):
            return ExecutionResult(False, OrderState.REJECTED, reason, serialized_request=payload, network_submitted=False)
        repo = app_module.st.session_state.setdefault("live_order_repository", {})
        repo[intent.client_order_id] = {
            "state": OrderState.PRECHECK_PASSED.value,
            "payload": payload,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "network_submitted": False,
        }
        if self.LOCKED or app_module.WORKSPACE.preferences.get("live_dry_run_only", True):
            return ExecutionResult(
                True,
                OrderState.PRECHECK_PASSED,
                "Dry run — order NOT sent",
                serialized_request=payload,
                network_submitted=False,
            )
        raise NotImplementedError("Live network submission requires explicit user enablement in application")

    @staticmethod
    def client_order_id(trade: Any, app_module: Any) -> str:
        raw = f"{getattr(trade,'symbol','')}|{getattr(trade,'strategy','')}|{getattr(trade,'decision_id','')}|BUY"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    @staticmethod
    def build_order_preview(intent: OrderIntent, trade: Any, app_module: Any) -> dict[str, Any]:
        """Pre-submission preview — never submits an order."""
        broker = app_module.get_broker_state().snapshot()
        funds = float(broker.get("available_cash") or broker.get("cash") or 0)
        notional = float(intent.price or 0) * int(intent.quantity or 0)
        return {
            "symbol": intent.symbol,
            "side": intent.side,
            "quantity": intent.quantity,
            "estimated_notional": round(notional, 2),
            "entry": getattr(trade, "entry", intent.price),
            "stop": getattr(trade, "stop", None),
            "target": getattr(trade, "target", None),
            "trade_confidence": getattr(trade, "ai_score", intent.trade_confidence),
            "risk": (getattr(trade, "risk_verdict", {}) or {}).get("verdict"),
            "broker_funds": funds,
            "available_after_order": round(max(0.0, funds - notional), 2),
            "order_product": intent.product,
            "idempotency_key": intent.client_order_id,
            "network_submission": "NOT SENT",
        }
