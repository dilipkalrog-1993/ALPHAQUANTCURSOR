"""Canonical production OHLCV normalization and indicator preparation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from core.headless_pipeline import PipelineFailure

REQUIRED_OHLCV = ("Open", "High", "Low", "Close", "Volume")
REQUIRED_ROWS = 200


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
        return out, None
    except Exception as exc:
        return None, PipelineFailure(stage="history_ready", reason="CALCULATION_FAILURE",
                                     detail=f"Indicator calculation raised {type(exc).__name__}",
                                     exception_type=type(exc).__name__, exception_message=str(exc), **base)
