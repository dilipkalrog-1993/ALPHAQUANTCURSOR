"""Market backend package with lazy compatibility exports.

Keeping ``__init__`` import-free prevents a backend submodule import from
pulling in snapshot/session adapters or optional dataframe dependencies.
"""
from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "InstrumentMaster": ("market.instrument_master", "InstrumentMaster"),
    "MarketState": ("market.market_state", "MarketState"),
    "get_market_state": ("market.market_state", "get_market_state"),
    "SubscriptionTierManager": ("market.subscription_tiers", "SubscriptionTierManager"),
    "get_market_snapshot": ("market.snapshots", "get_market_snapshot"),
    "get_broker_summary": ("market.snapshots", "get_broker_summary"),
    "get_portfolio_snapshot": ("market.snapshots", "get_portfolio_snapshot"),
    "get_opportunity_snapshot": ("market.snapshots", "get_opportunity_snapshot"),
    "get_report_snapshot": ("market.snapshots", "get_report_snapshot"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(module), attribute)
    globals()[name] = value
    return value
