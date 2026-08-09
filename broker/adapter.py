"""Canonical BrokerAdapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from broker.models import BrokerFunds, BrokerHolding, BrokerOrder, BrokerPosition


@dataclass
class BrokerAdapterStatus:
    broker: str
    connected: bool = False
    status: str = "NOT_CONNECTED"
    message: str = ""
    last_validated: str | None = None
    token_expiry: str | None = None
    capabilities: dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "connected": self.connected,
            "status": self.status,
            "message": self.message,
            "last_validated": self.last_validated,
            "token_expiry": self.token_expiry,
            "capabilities": self.capabilities,
        }


class BrokerAdapter(ABC):
    """Unified broker interface for authentication, market data and execution."""

    name: str = "GENERIC"

    @abstractmethod
    def authenticate(self, credentials: dict[str, str]) -> BrokerAdapterStatus:
        ...

    @abstractmethod
    def disconnect(self) -> None:
        ...

    @abstractmethod
    def health(self) -> BrokerAdapterStatus:
        ...

    @abstractmethod
    def get_profile(self) -> dict[str, Any]:
        ...

    @abstractmethod
    def get_funds(self) -> BrokerFunds:
        ...

    @abstractmethod
    def get_holdings(self) -> list[BrokerHolding]:
        ...

    @abstractmethod
    def get_positions(self) -> list[BrokerPosition]:
        ...

    @abstractmethod
    def get_orders(self) -> list[BrokerOrder]:
        ...

    @abstractmethod
    def get_quote(self, symbol: str) -> dict[str, Any]:
        ...

    def subscribe_market_data(self, symbols: list[str]) -> bool:
        return False

    def unsubscribe_market_data(self, symbols: list[str]) -> bool:
        return False

    def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError(f"{self.name} live order placement is locked")

    def modify_order(self, order_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError(f"{self.name} order modification is locked")

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        raise NotImplementedError(f"{self.name} order cancellation is locked")

    def exit_position(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError(f"{self.name} live exit is locked")
