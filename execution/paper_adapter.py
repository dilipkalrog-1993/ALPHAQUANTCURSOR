"""Paper execution adapter — delegates to existing atomic paper trade."""

from __future__ import annotations

from typing import Any

from execution.base import ExecutionAdapter, ExecutionResult, OrderIntent, OrderState


class PaperExecutionAdapter(ExecutionAdapter):
    def precheck(self, intent: OrderIntent, trade: Any, app_module: Any) -> tuple[bool, str]:
        if intent.quantity <= 0 or intent.price <= 0:
            return False, "Invalid quantity or price"
        verdict = getattr(trade, "risk_verdict", {}) or {}
        if verdict.get("verdict") not in ("APPROVED", "APPROVED_REDUCED_SIZE"):
            return False, "Risk not approved"
        if getattr(trade, "entry_status", "") not in ("READY", "READY_TO_ENTER", "ENTRY_TRIGGERED"):
            ok, reason = app_module.entry_trigger_status(trade)
            if not ok:
                return False, reason
        return True, "OK"

    def execute(self, intent: OrderIntent, trade: Any, app_module: Any) -> ExecutionResult:
        ok, reason = self.precheck(intent, trade, app_module)
        if not ok:
            return ExecutionResult(False, OrderState.REJECTED, reason)
        pos, msg = app_module.create_atomic_paper_trade(trade)
        if pos is None:
            return ExecutionResult(False, OrderState.REJECTED, msg)
        return ExecutionResult(True, OrderState.FILLED, msg, order_id=intent.client_order_id)
