"""Controlled Upstox market-data worker — one process-level poller."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

from market.instrument_master import UPSTOX_INDEX_KEYS
from market.market_state import get_market_state

log = logging.getLogger(__name__)


class UpstoxFeedWorker:
    """Single REST snapshot worker with reconnect/backoff and duplicate prevention."""

    _global_worker: "UpstoxFeedWorker | None" = None
    _global_lock = threading.Lock()

    def __init__(self):
        self.state = get_market_state()
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.profile: dict[str, Any] = {}
        self.last_message_at: str | None = None
        self.reconnect_count = 0
        self.backoff_seconds = 3.0

    @classmethod
    def instance(cls) -> "UpstoxFeedWorker":
        with cls._global_lock:
            if cls._global_worker is None:
                cls._global_worker = cls()
            return cls._global_worker

    def start(self, profile: dict[str, Any]) -> bool:
        with self.lock:
            self.profile = {k: v for k, v in profile.items() if k not in {"api_secret", "refresh_token", "totp"}}
            if self.thread and self.thread.is_alive():
                return False
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._run, name="alphaquant-upstox-feed", daemon=True)
            self.thread.start()
            return True

    def stop(self) -> None:
        self.stop_event.set()

    def health(self) -> dict[str, Any]:
        alive = bool(self.thread and self.thread.is_alive())
        return {
            "running": alive,
            "last_message_at": self.last_message_at,
            "reconnect_count": self.reconnect_count,
            "backoff_seconds": self.backoff_seconds,
        }

    def _run(self) -> None:
        while not self.stop_event.wait(self.backoff_seconds):
            token = str(self.profile.get("access_token") or "").strip()
            if not token:
                continue
            try:
                response = requests.get(
                    "https://api.upstox.com/v2/market-quote/quotes",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    params={"instrument_key": ",".join(UPSTOX_INDEX_KEYS.values())},
                    timeout=10,
                )
                response.raise_for_status()
                data = response.json().get("data", {})
                normalized = {}
                for symbol, key in UPSTOX_INDEX_KEYS.items():
                    raw = data.get(key) or {}
                    ohlc = raw.get("ohlc") or {}
                    ltp = raw.get("last_price") or raw.get("ltp")
                    previous = ohlc.get("close") or raw.get("previous_close")
                    if ltp is None:
                        continue
                    normalized[symbol] = {
                        "broker": "UPSTOX",
                        "instrument_key": key,
                        "ltp": float(ltp),
                        "open": ohlc.get("open"),
                        "high": ohlc.get("high"),
                        "low": ohlc.get("low"),
                        "previous_close": previous,
                        "previous_close_source": "BROKER_OHLC" if previous else "UNAVAILABLE",
                        "volume": raw.get("volume"),
                        "timestamp": raw.get("last_trade_time") or datetime.now(timezone.utc),
                        "received_at": datetime.now(timezone.utc),
                    }
                self.state.publish_quotes(normalized, "BROKER_SNAPSHOT")
                self.last_message_at = datetime.now(timezone.utc).isoformat()
                self.backoff_seconds = 3.0
            except Exception as exc:
                self.reconnect_count += 1
                self.backoff_seconds = min(30.0, self.backoff_seconds * 1.5)
                self.state.update(data_source="YFINANCE_INTRADAY_FALLBACK", health_status="DEGRADED")
                log.warning("Upstox feed worker degraded: %s", type(exc).__name__)
