"""Execution adapter base types and order state machine."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class OrderState(str, Enum):
    INTENT_CREATED = "INTENT_CREATED"
    PRECHECK_PASSED = "PRECHECK_PASSED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    UNKNOWN_RECONCILIATION_REQUIRED = "UNKNOWN_RECONCILIATION_REQUIRED"


@dataclass
class OrderIntent:
    trade_id: str
    decision_id: str | None
    client_order_id: str
    symbol: str
    side: str
    quantity: int
    price: float
    product: str = "D"
    strategy: str = ""
    trade_confidence: float = 0.0
    score_version: str = "V2"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    success: bool
    state: OrderState
    message: str
    order_id: str | None = None
    serialized_request: dict[str, Any] | None = None
    network_submitted: bool = False


class ExecutionAdapter(ABC):
    @abstractmethod
    def execute(self, intent: OrderIntent, trade: Any, app_module: Any) -> ExecutionResult:
        ...

    @abstractmethod
    def precheck(self, intent: OrderIntent, trade: Any, app_module: Any) -> tuple[bool, str]:
        ...
