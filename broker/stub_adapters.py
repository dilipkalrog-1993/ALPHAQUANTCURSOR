"""Stub adapters for brokers not yet implemented."""

from __future__ import annotations

from typing import Any

from broker.adapter import BrokerAdapter, BrokerAdapterStatus
from broker.models import BrokerFunds, BrokerHolding, BrokerOrder, BrokerPosition


class NotImplementedBrokerAdapter(BrokerAdapter):
    name = "NOT_IMPLEMENTED"

    def __init__(self, broker_label: str = "BROKER"):
        self.broker_label = broker_label

    def authenticate(self, credentials: dict[str, str]) -> BrokerAdapterStatus:
        return BrokerAdapterStatus(
            broker=self.broker_label,
            connected=False,
            status="NOT_IMPLEMENTED",
            message=f"{self.broker_label} adapter is not implemented in this phase",
        )

    def disconnect(self) -> None:
        return None

    def health(self) -> BrokerAdapterStatus:
        return self.authenticate({})

    def get_profile(self) -> dict[str, Any]:
        return {"broker": self.broker_label, "status": "NOT_IMPLEMENTED"}

    def get_funds(self) -> BrokerFunds:
        return BrokerFunds(broker=self.broker_label)

    def get_holdings(self) -> list[BrokerHolding]:
        return []

    def get_positions(self) -> list[BrokerPosition]:
        return []

    def get_orders(self) -> list[BrokerOrder]:
        return []

    def get_quote(self, symbol: str) -> dict[str, Any]:
        return {"symbol": symbol, "status": "NOT_IMPLEMENTED"}


class ZerodhaStubAdapter(NotImplementedBrokerAdapter):
    name = "ZERODHA"

    def __init__(self):
        super().__init__("ZERODHA")
