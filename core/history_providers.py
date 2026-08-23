"""Canonical, typed historical-candle providers.

Provider implementations never decide whether an exchange instrument is
delisted.  That decision belongs to the authoritative instrument master.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import threading
import time
from typing import Any, Protocol
from urllib.parse import quote

import pandas as pd
import requests


class HistoryStatus(str, Enum):
    SUCCESS = "SUCCESS"
    NO_DATA = "NO_DATA"
    AUTH_FAILURE = "AUTH_FAILURE"
    RATE_LIMIT = "RATE_LIMIT"
    TIMEOUT = "TIMEOUT"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    DELISTED_CONFIRMED = "DELISTED_CONFIRMED"


@dataclass
class ProviderResult:
    symbol: str
    provider: str
    status: HistoryStatus
    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    start: str | None = None
    end: str | None = None
    rows: int = 0
    freshness: str | None = None
    error_category: str | None = None
    error_detail: str | None = None

    def __post_init__(self) -> None:
        self.rows = len(self.frame) if self.frame is not None else 0
        self.error_category = self.error_category or (None if self.status == HistoryStatus.SUCCESS else self.status.value)


class HistoricalProvider(Protocol):
    name: str
    def fetch(self, *, symbol: str, instrument_key: str | None, start: str, end: str,
              timeout: float) -> ProviderResult: ...


def classify_provider_error(exc: Any = None, *, status_code: int | None = None,
                            detail: str = "") -> HistoryStatus:
    """Classify transport/provider failures without guessing delisting."""
    message = f"{detail} {exc or ''}".lower()
    if status_code in {401, 403} or any(x in message for x in ("unauthorized", "invalid crumb", "access this feature", "forbidden")):
        return HistoryStatus.AUTH_FAILURE
    if status_code == 429 or "rate limit" in message or "too many requests" in message:
        return HistoryStatus.RATE_LIMIT
    if isinstance(exc, (TimeoutError, requests.Timeout)) or "timed out" in message or "timeout" in message:
        return HistoryStatus.TIMEOUT
    if status_code in {400, 404, 422} and any(x in message for x in ("invalid instrument", "invalid symbol", "instrument_key")):
        return HistoryStatus.INVALID_SYMBOL
    return HistoryStatus.PROVIDER_ERROR


class RateLimiter:
    """Thread-safe fixed-interval limiter shared by a provider instance."""
    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / max(requests_per_second, 0.01)
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            delay = self._next - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._next = max(self._next, time.monotonic()) + self.interval


class UpstoxHistoryProvider:
    name = "UPSTOX"
    base_url = "https://api.upstox.com/v2/historical-candle"

    def __init__(self, access_token: str = "", *, requests_per_second: float = 2.0,
                 session: Any = requests):
        self.access_token = access_token.strip()
        self.limiter = RateLimiter(requests_per_second)
        self.session = session

    def fetch(self, *, symbol: str, instrument_key: str | None, start: str, end: str,
              timeout: float) -> ProviderResult:
        if not self.access_token:
            return ProviderResult(symbol, self.name, HistoryStatus.AUTH_FAILURE, start=start, end=end,
                                  error_detail="Upstox access token is unavailable")
        if not instrument_key:
            return ProviderResult(symbol, self.name, HistoryStatus.INVALID_SYMBOL, start=start, end=end,
                                  error_detail="Authoritative instrument_key is missing")
        self.limiter.wait()
        url = f"{self.base_url}/{quote(instrument_key, safe='')}/day/{end}/{start}"
        try:
            response = self.session.get(url, headers={"Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"}, timeout=timeout)
            if response.status_code >= 400:
                detail = response.text[:500]
                status = classify_provider_error(status_code=response.status_code, detail=detail)
                return ProviderResult(symbol, self.name, status, start=start, end=end, error_detail=detail)
            candles = ((response.json().get("data") or {}).get("candles") or [])
            if not candles:
                return ProviderResult(symbol, self.name, HistoryStatus.NO_DATA, start=start, end=end,
                                      error_detail="Provider returned no candles")
            frame = pd.DataFrame(candles, columns=["Date", "Open", "High", "Low", "Close", "Volume", "OpenInterest"])
            frame = frame.set_index("Date").drop(columns=["OpenInterest"])
            return ProviderResult(symbol, self.name, HistoryStatus.SUCCESS, frame, start, end)
        except Exception as exc:
            status = classify_provider_error(exc)
            return ProviderResult(symbol, self.name, status, start=start, end=end,
                                  error_detail=f"{type(exc).__name__}: {exc}")


class YahooHistoryProvider:
    name = "YAHOO"

    def __init__(self, downloader: Any | None = None, *, requests_per_second: float = 1.0):
        self.downloader = downloader
        self.limiter = RateLimiter(requests_per_second)

    def fetch(self, *, symbol: str, instrument_key: str | None, start: str, end: str,
              timeout: float) -> ProviderResult:
        del instrument_key
        self.limiter.wait()
        try:
            if self.downloader is None:
                import yfinance as yf
                downloader = yf.download
            else:
                downloader = self.downloader
            raw = downloader(symbol, start=start, end=end, interval="1d", progress=False,
                             auto_adjust=True, threads=False, timeout=timeout)
            if raw is None or raw.empty:
                # Yahoo's misleading "possibly delisted" text is not evidence.
                return ProviderResult(symbol, self.name, HistoryStatus.NO_DATA, start=start, end=end,
                                      error_detail="Provider returned no candles; delisting is not inferred")
            return ProviderResult(symbol, self.name, HistoryStatus.SUCCESS, raw, start, end)
        except Exception as exc:
            status = classify_provider_error(exc)
            return ProviderResult(symbol, self.name, status, start=start, end=end,
                                  error_detail=f"{type(exc).__name__}: {exc}")
