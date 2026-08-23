"""Cache-first historical data orchestration and deterministic failover."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import pickle
import threading
import time
from typing import Any

import pandas as pd

from core.history import DEFAULT_CACHE_DIR, REQUIRED_ROWS, _atomic_pickle, _cache_path, normalize_history
from core.history_providers import HistoricalProvider, HistoryStatus, ProviderResult
from market.instrument_master import InstrumentMaster

MIN_BOOTSTRAP_DAYS = 550
MAX_FRESHNESS_DAYS = 7
_WRITE_LOCK = threading.Lock()


@dataclass
class HistoryRecord:
    symbol: str
    status: str
    frame: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)
    provider_attempts: list[dict[str, Any]] = field(default_factory=list)
    provider_selected: str | None = None
    failure_category: str | None = None
    failure_detail: str | None = None
    cache_hit: bool = False
    incremental_update: bool = False
    full_bootstrap: bool = False
    rows_written: int = 0

    def report(self) -> dict[str, Any]:
        value = asdict(self)
        value.pop("frame")
        return value


def read_cache(symbol: str, cache_dir: Path = DEFAULT_CACHE_DIR) -> pd.DataFrame:
    path = _cache_path(symbol, cache_dir)
    try:
        return normalize_history(pickle.loads(path.read_bytes())) if path.exists() else pd.DataFrame()
    except (OSError, EOFError, pickle.PickleError, ValueError, AttributeError):
        return pd.DataFrame()


def validate_ohlcv(frame: Any) -> tuple[pd.DataFrame, str | None]:
    from core.history import REQUIRED_OHLCV
    out = normalize_history(frame)
    if out.empty:
        return out, "NO_HISTORY"
    if not isinstance(out.index, pd.DatetimeIndex):
        try:
            out.index = pd.to_datetime(out.index, utc=True).tz_convert(None)
        except (TypeError, ValueError):
            return pd.DataFrame(), "BAD_OHLCV"
    else:
        # All providers and old cache generations merge on the same naive-UTC
        # daily index, even if one source returned an exchange offset.
        out.index = pd.to_datetime(out.index, utc=True).tz_convert(None)
    missing = [c for c in REQUIRED_OHLCV if c not in out]
    if missing or out[list(REQUIRED_OHLCV)].isna().any().any():
        return pd.DataFrame(), "BAD_OHLCV"
    if ((out.High < out.Low) | (out.High < out[["Open", "Close"]].max(axis=1)) |
            (out.Low > out[["Open", "Close"]].min(axis=1)) | (out.Volume < 0)).any():
        return pd.DataFrame(), "BAD_OHLCV"
    return out.sort_index()[~out.sort_index().index.duplicated(keep="last")], None


def cache_readiness(frame: pd.DataFrame, now: datetime | None = None) -> str:
    valid, error = validate_ohlcv(frame)
    if error:
        return error
    if len(valid) < REQUIRED_ROWS:
        return "INSUFFICIENT_ROWS"
    today = pd.Timestamp(now or datetime.now(timezone.utc)).tz_localize(None).normalize()
    last = pd.Timestamp(valid.index[-1]).tz_localize(None).normalize()
    return "HISTORY_READY" if (today - last).days <= MAX_FRESHNESS_DAYS else "HISTORY_STALE"


def _attempt(result: ProviderResult) -> dict[str, Any]:
    return {"provider": result.provider, "status": result.status.value,
            "rows": result.rows, "start": result.start, "end": result.end,
            "error_category": result.error_category, "error_detail": result.error_detail}


class HistoryService:
    def __init__(self, providers: list[HistoricalProvider], *, cache_dir: Path = DEFAULT_CACHE_DIR,
                 master: InstrumentMaster | None = None, retries: int = 2,
                 backoff: float = 0.5, timeout: float = 12.0):
        self.providers, self.cache_dir, self.master = providers, cache_dir, master or InstrumentMaster()
        self.retries, self.backoff, self.timeout = retries, backoff, timeout

    def repair(self, symbol: str, *, now: datetime | None = None) -> HistoryRecord:
        mapping = self.master.canonical_mapping(symbol)
        canonical = self.master.normalize_symbol(symbol)
        if self.master.authoritative_status(canonical) == "DELISTED":
            return HistoryRecord(canonical, "HISTORY_UNAVAILABLE",
                                 failure_category="DELISTED_CONFIRMED",
                                 failure_detail="Authoritative instrument master explicitly marks DELISTED")
        # Absence from a valid authoritative master means invalid for this run,
        # not confirmed delisted. Confirmation requires an explicit master row.
        if self.master.is_valid() and canonical not in set(self.master.symbols()):
            return HistoryRecord(canonical, "HISTORY_UNAVAILABLE", failure_category="INVALID_SYMBOL",
                                 failure_detail="Symbol is absent from the authoritative active master")
        cached = read_cache(canonical, self.cache_dir)
        current = pd.Timestamp(now or datetime.now(timezone.utc)).tz_localize(None).normalize()
        readiness = cache_readiness(cached, now)
        if readiness == "HISTORY_READY":
            return HistoryRecord(canonical, readiness, cached, provider_selected="CACHE", cache_hit=True)
        last = pd.Timestamp(cached.index[-1]).tz_localize(None).normalize() if not cached.empty else None
        start = (last + pd.Timedelta(days=1) if last is not None else current - pd.Timedelta(days=MIN_BOOTSTRAP_DAYS)).date().isoformat()
        end = (current + pd.Timedelta(days=1)).date().isoformat()
        attempts: list[dict[str, Any]] = []
        terminal: ProviderResult | None = None
        for provider in self.providers:
            for attempt_no in range(self.retries + 1):
                result = provider.fetch(symbol=mapping["historical_provider_symbol"],
                    instrument_key=mapping.get("instrument_key"), start=start, end=end, timeout=self.timeout)
                attempts.append({**_attempt(result), "attempt": attempt_no + 1})
                terminal = result
                if result.status == HistoryStatus.SUCCESS:
                    fresh, error = validate_ohlcv(result.frame)
                    if error:
                        attempts[-1].update(status="PROVIDER_ERROR", error_category="BAD_OHLCV",
                                            error_detail="Provider response failed OHLCV validation")
                        break
                    merged, error = validate_ohlcv(pd.concat([cached, fresh]))
                    if error:
                        break
                    with _WRITE_LOCK:
                        _atomic_pickle(_cache_path(canonical, self.cache_dir), merged)
                    return HistoryRecord(canonical, cache_readiness(merged, now), merged, attempts,
                        result.provider, cache_hit=False, incremental_update=not cached.empty,
                        full_bootstrap=cached.empty, rows_written=len(fresh))
                if result.status not in {HistoryStatus.RATE_LIMIT, HistoryStatus.TIMEOUT, HistoryStatus.PROVIDER_ERROR}:
                    break
                if attempt_no < self.retries:
                    time.sleep(min(self.backoff * 2 ** attempt_no, 4.0))
        category = (attempts[-1].get("error_category") if attempts else None) or (
            terminal.status.value if terminal else "PROVIDER_ERROR")
        detail = (attempts[-1].get("error_detail") if attempts else None) or (
            terminal.error_detail if terminal else "No historical providers configured")
        # Preserve valid LKG even when repair fails.
        return HistoryRecord(canonical, "HISTORY_UNAVAILABLE", cached, attempts,
                             failure_category=category, failure_detail=detail)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    temp.replace(path)
