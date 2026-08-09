"""Normalized broker models — never fabricate missing fields."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _na(value: Any, default: str = "N/A") -> Any:
    return default if value is None else value


@dataclass
class BrokerFunds:
    broker: str
    available_cash: float | str = "N/A"
    used_margin: float | str = "N/A"
    available_margin: float | str = "N/A"
    collateral: float | str = "N/A"
    realized_pnl: float | str = "N/A"
    unrealized_pnl: float | str = "N/A"
    total_equity: float | str = "N/A"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "available_cash": self.available_cash,
            "used_margin": self.used_margin,
            "available_margin": self.available_margin,
            "collateral": self.collateral,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_equity": self.total_equity,
            "timestamp": self.timestamp,
        }


@dataclass
class BrokerHolding:
    broker: str
    symbol: str
    quantity: int | str = "N/A"
    average_price: float | str = "N/A"
    ltp: float | str = "N/A"
    current_value: float | str = "N/A"
    pnl: float | str = "N/A"
    pnl_percent: float | str = "N/A"

    def to_dict(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "average_price": self.average_price,
            "ltp": self.ltp,
            "current_value": self.current_value,
            "pnl": self.pnl,
            "pnl_percent": self.pnl_percent,
        }


@dataclass
class BrokerPosition:
    broker: str
    symbol: str
    side: str = "N/A"
    quantity: int | str = "N/A"
    average_price: float | str = "N/A"
    ltp: float | str = "N/A"
    pnl: float | str = "N/A"
    product: str = "N/A"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "average_price": self.average_price,
            "ltp": self.ltp,
            "pnl": self.pnl,
            "product": self.product,
            "timestamp": self.timestamp,
        }


@dataclass
class BrokerOrder:
    broker: str
    order_id: str
    symbol: str
    side: str = "N/A"
    quantity: int | str = "N/A"
    price: float | str = "N/A"
    order_type: str = "N/A"
    status: str = "N/A"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "order_type": self.order_type,
            "status": self.status,
            "timestamp": self.timestamp,
        }
