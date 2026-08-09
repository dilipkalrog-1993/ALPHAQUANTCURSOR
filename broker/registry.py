"""Broker registry — supported brokers and adapter resolution."""

from __future__ import annotations

from typing import Type

from broker.adapter import BrokerAdapter

BROKER_REGISTRY: dict[str, dict[str, str]] = {
    "UPSTOX": {"display": "Upstox", "status": "IMPLEMENTED"},
    "ZERODHA": {"display": "Zerodha", "status": "NOT_IMPLEMENTED"},
    "NUVAMA": {"display": "Nuvama", "status": "NOT_IMPLEMENTED"},
    "ANGEL ONE": {"display": "Angel One", "status": "NOT_IMPLEMENTED"},
    "DHAN": {"display": "Dhan", "status": "NOT_IMPLEMENTED"},
    "FYERS": {"display": "Fyers", "status": "NOT_IMPLEMENTED"},
}


def get_adapter_class(broker_name: str) -> Type[BrokerAdapter] | None:
    key = (broker_name or "").strip().upper()
    if key in {"UPSTOX", "UPSTOX BROKING"}:
        from broker.upstox_adapter import UpstoxAdapter
        return UpstoxAdapter
    if key == "ZERODHA":
        from broker.stub_adapters import ZerodhaStubAdapter
        return ZerodhaStubAdapter
    if key in {"NUVAMA", "ANGEL ONE", "DHAN", "FYERS"}:
        from broker.stub_adapters import NotImplementedBrokerAdapter
        return NotImplementedBrokerAdapter
    return None
