"""Canonical MarketState — single authoritative quote store."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

import pandas as pd

MARKET_DATA_SOURCES = {
    "BROKER_LIVE": "LIVE",
    "BROKER_SNAPSHOT": "NEAR LIVE",
    "YFINANCE_INTRADAY_FALLBACK": "DELAYED",
    "HISTORICAL_CACHE": "STALE",
    "UNAVAILABLE": "OFFLINE",
}


class MarketState:
    """Thread-safe canonical market state for all consumers."""

    _instance: "MarketState | None" = None
    _lock = threading.Lock()

    def __init__(self):
        self.lock = threading.RLock()
        self.values: dict[str, Any] = {
            "data_source": "UNAVAILABLE",
            "health_status": "OFFLINE",
            "market_data_connected": False,
            "last_quote_time": None,
            "volatility_regime": "UNKNOWN",
            "market_regime": "INSUFFICIENT_DATA",
        }
        self.quotes: dict[str, dict[str, Any]] = {}
        self.breadth: dict[str, Any] = {}
        self.regime: dict[str, Any] = {}
        self.sectors: dict[str, Any] = {}
        self.volatility: dict[str, Any] = {}

    @classmethod
    def instance(cls) -> "MarketState":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def update(self, **changes: Any) -> None:
        with self.lock:
            self.values.update(changes)

    def publish_quotes(self, quotes: dict[str, dict[str, Any]], source: str) -> None:
        now = datetime.now(timezone.utc)
        with self.lock:
            for symbol, quote in quotes.items():
                received = quote.get("received_at") or now
                if isinstance(received, str):
                    received = pd.to_datetime(received, utc=True).to_pydatetime()
                previous = quote.get("previous_close")
                ltp = quote.get("ltp")
                valid_previous = previous is not None and float(previous) > 0
                change = float(ltp) - float(previous) if ltp is not None and valid_previous else None
                change_pct = (change / float(previous) * 100) if change is not None and previous else None
                age_ms = max(0.0, (now - received).total_seconds() * 1000)
                stale = age_ms > 30_000
                freshness_label = MARKET_DATA_SOURCES.get(source, "OFFLINE")
                if stale and source in {"BROKER_LIVE", "BROKER_SNAPSHOT"}:
                    freshness_label = "STALE"
                self.quotes[symbol] = {
                    "broker": quote.get("broker", self.values.get("broker_name")),
                    "instrument_key": quote.get("instrument_key", ""),
                    "symbol": symbol,
                    "exchange": quote.get("exchange", "NSE"),
                    "ltp": float(ltp) if ltp is not None else None,
                    "previous_close": float(previous) if valid_previous else None,
                    "previous_close_source": quote.get("previous_close_source", "BROKER_OHLC" if valid_previous else "UNAVAILABLE"),
                    "open": quote.get("open"),
                    "high": quote.get("high"),
                    "low": quote.get("low"),
                    "volume": quote.get("volume"),
                    "change": change,
                    "change_percent": change_pct,
                    "exchange_timestamp": quote.get("timestamp"),
                    "received_at": received.isoformat() if hasattr(received, "isoformat") else received,
                    "source": source,
                    "freshness_ms": round(age_ms, 1),
                    "freshness_label": freshness_label,
                    "stale": stale,
                }
            if quotes:
                self.values.update(
                    data_source=source,
                    last_quote_time=now.isoformat(),
                    market_data_connected=source in {"BROKER_LIVE", "BROKER_SNAPSHOT"},
                )

    def set_analytics(self, *, breadth=None, regime=None, sectors=None, volatility=None) -> None:
        with self.lock:
            if breadth is not None:
                self.breadth = breadth
            if regime is not None:
                self.regime = regime
                self.values["market_regime"] = regime.get("classification", regime.get("state", "INSUFFICIENT_DATA"))
            if sectors is not None:
                self.sectors = sectors
            if volatility is not None:
                self.volatility = volatility
                self.values["volatility_regime"] = volatility.get("classification", "UNKNOWN")

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                **self.values,
                "quotes": {k: dict(v) for k, v in self.quotes.items()},
                "breadth": dict(self.breadth),
                "regime": dict(self.regime),
                "sectors": dict(self.sectors),
                "volatility": dict(self.volatility),
            }

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        with self.lock:
            return dict(self.quotes.get(symbol) or self.quotes.get(symbol.replace(".NS", "")) or {}) or None


def get_market_state() -> MarketState:
    return MarketState.instance()
