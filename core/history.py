"""Canonical production OHLCV normalization and indicator preparation."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
import pickle
import threading
import time
from typing import Any

import numpy as np
import pandas as pd

from core.headless_pipeline import PipelineFailure

REQUIRED_OHLCV = ("Open", "High", "Low", "Close", "Volume")
REQUIRED_ROWS = 200
DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "history_cache"
INDICATOR_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "indicator_cache"
_CACHE_LOCK = threading.Lock()


@dataclass
class HistoryFetchResult:
    symbol: str
    frame: pd.DataFrame
    provider: str
    cache_hit: bool
    cache_seconds: float
    provider_seconds: float
    normalize_seconds: float
    fetched_rows: int = 0
    attempts: int = 0
    failure: str | None = None
    status: str = "SUCCESS"
    error_category: str | None = None
    error_detail: str | None = None
    provider_attempts: list[dict[str, Any]] | None = None
    provider_selected: str | None = None

    def timing_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value.pop("frame")
        return value


def _cache_path(symbol: str, cache_dir: Path) -> Path:
    safe = symbol.replace(".NS", "").replace("/", "_")
    return cache_dir / f"{safe}.pkl"


def _atomic_pickle(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{threading.get_ident()}.tmp")
    tmp.write_bytes(pickle.dumps(frame, protocol=pickle.HIGHEST_PROTOCOL))
    tmp.replace(path)


def load_incremental_history(
    symbol: str, *, cache_dir: Path = DEFAULT_CACHE_DIR, timeout: float = 12.0,
    retries: int = 2, backoff: float = 0.5, lookback_days: int = 550,
    now: datetime | None = None, downloader: Any | None = None,
) -> HistoryFetchResult:
    """Cache-first history loading with a bounded, incremental provider repair.

    ``downloader`` has the yfinance-compatible ``download`` signature and is
    injectable so this behaviour can be tested without making network calls.
    """
    # Resolve exchange renames before both provider access and cache lookup.
    # The returned symbol is canonical so stale aliases cannot leak downstream.
    from market.instrument_master import InstrumentMaster
    mapping = InstrumentMaster().canonical_mapping(symbol)
    symbol = mapping["historical_provider_symbol"]
    started = time.perf_counter()
    path = _cache_path(symbol, cache_dir)
    cached = pd.DataFrame()
    if path.exists():
        try:
            cached = normalize_history(pickle.loads(path.read_bytes()))
        except (OSError, pickle.PickleError, EOFError, AttributeError, ValueError):
            cached = pd.DataFrame()
    cache_seconds = time.perf_counter() - started
    current = pd.Timestamp(now or datetime.now(timezone.utc))
    current = (current.tz_convert(None) if current.tzinfo else current).normalize()
    if not cached.empty:
        last = pd.Timestamp(cached.index[-1])
        last = (last.tz_convert(None) if last.tzinfo else last).normalize()
    else:
        last = None
    # Daily data is complete enough when the cache reaches the previous UTC day.
    required_end = current - pd.Timedelta(days=1)
    if last is not None and last >= required_end:
        return HistoryFetchResult(symbol, cached, "cache", True, cache_seconds, 0.0, 0.0)

    if downloader is None:
        import yfinance as yf
        downloader = yf.download
    start = (last + pd.Timedelta(days=1)) if last is not None else current - pd.Timedelta(days=lookback_days)
    provider_started = time.perf_counter()
    raw = pd.DataFrame()
    failure = None
    error_category = None
    attempts = 0
    for attempt in range(max(1, retries + 1)):
        attempts = attempt + 1
        try:
            raw = downloader(symbol, start=start.strftime("%Y-%m-%d"),
                end=(current + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), interval="1d",
                progress=False, auto_adjust=True, threads=False, timeout=timeout)
            if raw is not None and not raw.empty:
                break
            failure = "EMPTY_RESPONSE"
            error_category = "NO_DATA"
        except (TimeoutError, ConnectionError, OSError) as exc:
            failure = f"{type(exc).__name__}: {exc}"
            from core.history_providers import classify_provider_error
            error_category = classify_provider_error(exc).value
        except Exception as exc:  # provider libraries use several request exception types
            failure = f"{type(exc).__name__}: {exc}"
            from core.history_providers import classify_provider_error
            error_category = classify_provider_error(exc).value
        if attempt < retries:
            time.sleep(min(backoff * (2 ** attempt), 2.0))
    provider_seconds = time.perf_counter() - provider_started
    normal_started = time.perf_counter()
    fresh = normalize_history(raw)
    merged = normalize_history(pd.concat([cached, fresh])) if not fresh.empty else cached
    normalize_seconds = time.perf_counter() - normal_started
    if not fresh.empty:
        with _CACHE_LOCK:
            _atomic_pickle(path, merged)
        failure = None
        error_category = None
    provider = "cache+yfinance" if not cached.empty else "yfinance"
    status = "SUCCESS" if not fresh.empty else "HISTORY_UNAVAILABLE"
    return HistoryFetchResult(symbol, merged, provider, False, cache_seconds, provider_seconds,
        normalize_seconds, len(fresh), attempts, failure, status, error_category, failure,
        [{"provider": "YAHOO", "status": error_category or "SUCCESS", "attempts": attempts}],
        "YAHOO" if not fresh.empty else None)


def normalize_history(raw: Any) -> pd.DataFrame:
    """Canonicalize a provider frame without inventing or forward-filling data."""
    if raw is None:
        return pd.DataFrame()
    frame = raw.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    aliases = {str(column).strip().lower(): column for column in frame.columns}
    rename = {aliases[name.lower()]: name for name in REQUIRED_OHLCV if name.lower() in aliases}
    frame = frame.rename(columns=rename)
    frame = frame.loc[:, ~frame.columns.duplicated()].sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    for column in REQUIRED_OHLCV:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _dates(frame: pd.DataFrame) -> tuple[str | None, str | None, str | None]:
    if frame.empty:
        return None, None, None
    first, last = frame.index[0], frame.index[-1]
    try:
        end = pd.Timestamp(last)
        if end.tzinfo is None:
            end = end.tz_localize("UTC")
        age = str(datetime.now(timezone.utc) - end.to_pydatetime()).split(".")[0]
    except Exception:
        age = None
    return str(first), str(last), age


def prepare_indicators(symbol: str, raw: Any, provider: str = "yfinance") -> tuple[pd.DataFrame | None, PipelineFailure | None]:
    """Validate history and compute the indicators required by production."""
    raw_rows = len(raw) if hasattr(raw, "__len__") else 0
    frame = normalize_history(raw)
    first, last, age = _dates(frame)
    base = dict(symbol=symbol, provider=provider, raw_rows=raw_rows, normalized_rows=len(frame),
                first_date=first, last_date=last, data_age=age)
    if frame.empty:
        return None, PipelineFailure(stage="history_ready", reason="NO_HISTORY", detail="Provider returned no history", **base)
    missing = [column for column in REQUIRED_OHLCV if column not in frame.columns]
    invalid = [column for column in REQUIRED_OHLCV if column in frame and
               (frame[column].isna().any() or not np.isfinite(frame[column]).all())]
    geometry_bad = all(column in frame for column in ("High", "Low", "Open", "Close")) and bool(
        ((frame.High < frame.Low) | (frame.High < frame[["Open", "Close"]].max(axis=1)) |
         (frame.Low > frame[["Open", "Close"]].min(axis=1))).any())
    if missing or invalid or geometry_bad:
        detail = "OHLC price geometry is invalid" if geometry_bad else "Missing/invalid OHLCV"
        return None, PipelineFailure(stage="history_ready", reason="BAD_OHLCV", detail=detail,
                                     missing_columns=missing, nan_columns=invalid, **base)
    if len(frame) < REQUIRED_ROWS:
        return None, PipelineFailure(stage="history_ready", reason="INSUFFICIENT_ROWS",
                                     detail=f"EMA200 requires >= {REQUIRED_ROWS} normalized rows", **base)
    # Candle-stable indicators are shared by broad/focus scans and warmup.
    # The key is deliberately symbol + timeframe + last completed candle.
    candle = str(frame.index[-1]).replace(":", "-").replace("/", "-").replace(" ", "_")
    indicator_path = INDICATOR_CACHE_DIR / f"{symbol.replace('.NS', '')}_1d_{candle}.pkl"
    if indicator_path.exists():
        try:
            cached_indicators = pickle.loads(indicator_path.read_bytes())
            if len(cached_indicators) == len(frame):
                return cached_indicators, None
        except (OSError, pickle.PickleError, EOFError, AttributeError, ValueError):
            pass
    try:
        out = frame.copy()
        out["EMA20"] = out.Close.ewm(span=20, adjust=False, min_periods=20).mean()
        out["EMA50"] = out.Close.ewm(span=50, adjust=False, min_periods=50).mean()
        out["EMA200"] = out.Close.ewm(span=200, adjust=False, min_periods=200).mean()
        delta = out.Close.diff()
        rs = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean() / (
            -delta.clip(upper=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
        out["RSI"] = 100 - 100 / (1 + rs)
        ranges = pd.concat([(out.High - out.Low), (out.High - out.Close.shift()).abs(),
                            (out.Low - out.Close.shift()).abs()], axis=1)
        out["ATR"] = ranges.max(axis=1).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
        out["AVG_VOLUME20"] = out.Volume.rolling(20).mean()
        required = ["EMA20", "EMA50", "EMA200", "RSI", "ATR", "AVG_VOLUME20"]
        bad = [column for column in required if pd.isna(out[column].iloc[-1]) or
               not np.isfinite(out[column].iloc[-1])]
        if bad:
            return None, PipelineFailure(stage="history_ready", reason="INDICATOR_NAN",
                                         detail="Latest required indicator is NaN/non-finite",
                                         nan_columns=bad, **base)
        with _CACHE_LOCK:
            _atomic_pickle(indicator_path, out)
        return out, None
    except Exception as exc:
        return None, PipelineFailure(stage="history_ready", reason="CALCULATION_FAILURE",
                                     detail=f"Indicator calculation raised {type(exc).__name__}",
                                     exception_type=type(exc).__name__, exception_message=str(exc), **base)
