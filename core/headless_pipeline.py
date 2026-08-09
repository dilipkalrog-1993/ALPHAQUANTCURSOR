"""Production headless market-data, signal, persistence and UI reconciliation domain.

This module deliberately has no Streamlit dependency.  Command-line tools and UI
adapters may both consume these plain dataclasses and functions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
import json

import numpy as np
import pandas as pd

REQUIRED_OHLCV = ("Open", "High", "Low", "Close", "Volume")
REQUIRED_ROWS = 200


@dataclass
class PipelineFailure:
    symbol: str
    stage: str
    reason: str
    detail: str
    provider: str = "yfinance"
    raw_rows: int = 0
    normalized_rows: int = 0
    required_rows: int = REQUIRED_ROWS
    first_date: str | None = None
    last_date: str | None = None
    data_age: str | None = None
    missing_columns: list[str] = field(default_factory=list)
    nan_columns: list[str] = field(default_factory=list)
    exception_type: str | None = None
    exception_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Candidate:
    symbol: str
    strategy: str
    score: float
    entry: float
    stop: float
    target: float
    computed: bool = True


@dataclass
class StageAudit:
    input: int
    output: int
    rejections: dict[str, int] = field(default_factory=dict)

    @property
    def rejected(self) -> int:
        return sum(self.rejections.values())

    def to_dict(self) -> dict[str, Any]:
        return {"input": self.input, "output": self.output, "rejected": self.rejected,
                "rejections": self.rejections, "invariant_ok": self.input == self.output + self.rejected}


def normalize_history(raw: Any) -> pd.DataFrame:
    """Canonicalize a provider frame without inventing or forward-filling data."""
    if raw is None:
        return pd.DataFrame()
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    aliases = {str(c).strip().lower(): c for c in df.columns}
    rename = {old: name for name in REQUIRED_OHLCV for old in [aliases.get(name.lower())] if old is not None}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()].sort_index()
    df = df[~df.index.duplicated(keep="last")]
    for col in REQUIRED_OHLCV:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _dates(df: pd.DataFrame) -> tuple[str | None, str | None, str | None]:
    if df.empty:
        return None, None, None
    first, last = df.index[0], df.index[-1]
    try:
        end = pd.Timestamp(last)
        if end.tzinfo is None:
            end = end.tz_localize("UTC")
        age = str(datetime.now(timezone.utc) - end.to_pydatetime()).split(".")[0]
    except Exception:
        age = None
    return str(first), str(last), age


def prepare_indicators(symbol: str, raw: Any, provider: str = "yfinance") -> tuple[pd.DataFrame | None, PipelineFailure | None]:
    raw_rows = len(raw) if hasattr(raw, "__len__") else 0
    df = normalize_history(raw)
    first, last, age = _dates(df)
    base = dict(symbol=symbol, provider=provider, raw_rows=raw_rows, normalized_rows=len(df),
                first_date=first, last_date=last, data_age=age)
    if df.empty:
        return None, PipelineFailure(stage="download_normalization", reason="NO_HISTORY", detail="Provider returned no history", **base)
    missing = [c for c in REQUIRED_OHLCV if c not in df.columns]
    invalid = [c for c in REQUIRED_OHLCV if c in df and (df[c].isna().any() or not np.isfinite(df[c]).all())]
    geometry_bad = all(c in df for c in ("High", "Low", "Open", "Close")) and bool(
        ((df.High < df.Low) | (df.High < df[["Open", "Close"]].max(axis=1)) |
         (df.Low > df[["Open", "Close"]].min(axis=1))).any())
    if missing or invalid or geometry_bad:
        detail = "Missing/invalid OHLCV" if not geometry_bad else "OHLC price geometry is invalid"
        return None, PipelineFailure(stage="ohlcv_validation", reason="BAD_OHLCV", detail=detail,
                                     missing_columns=missing, nan_columns=invalid, **base)
    if len(df) < REQUIRED_ROWS:
        return None, PipelineFailure(stage="minimum_history_validation", reason="INSUFFICIENT_ROWS",
                                     detail=f"EMA200 requires >= {REQUIRED_ROWS} normalized rows", **base)
    try:
        out = df.copy()
        out["EMA20"] = out.Close.ewm(span=20, adjust=False, min_periods=20).mean()
        out["EMA50"] = out.Close.ewm(span=50, adjust=False, min_periods=50).mean()
        out["EMA200"] = out.Close.ewm(span=200, adjust=False, min_periods=200).mean()
        delta = out.Close.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
        rs = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean() / loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        out["RSI"] = 100 - 100 / (1 + rs)
        tr = pd.concat([(out.High-out.Low), (out.High-out.Close.shift()).abs(), (out.Low-out.Close.shift()).abs()], axis=1).max(axis=1)
        out["ATR"] = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        out["AVG_VOLUME20"] = out.Volume.rolling(20).mean()
        required = ["EMA20", "EMA50", "EMA200", "RSI", "ATR", "AVG_VOLUME20"]
        bad = [c for c in required if pd.isna(out[c].iloc[-1]) or not np.isfinite(out[c].iloc[-1])]
        if bad:
            return None, PipelineFailure(stage="indicator_preparation", reason="INDICATOR_NAN",
                                         detail="Latest required indicator is NaN/non-finite", nan_columns=bad, **base)
        return out, None
    except Exception as exc:
        return None, PipelineFailure(stage="indicator_preparation", reason="CALCULATION_FAILURE",
                                     detail=f"Indicator calculation raised {type(exc).__name__}",
                                     exception_type=type(exc).__name__, exception_message=str(exc), **base)


def compute_candidate(symbol: str, df: pd.DataFrame) -> Candidate | None:
    """Create a candidate only from a real momentum/trend strategy signal."""
    row = df.iloc[-1]
    signal = row.Close > row.EMA20 > row.EMA50 and row.Close > row.EMA200 and 45 <= row.RSI <= 75
    if not signal:
        return None
    atr = float(row.ATR)
    score = min(100.0, 50 + max(0.0, float(row.RSI)-45) + min(20, (float(row.Close/row.EMA50)-1)*200))
    return Candidate(symbol, "TREND_MOMENTUM", round(score, 2), float(row.Close),
                     round(float(row.Close)-1.5*atr, 2), round(float(row.Close)+3*atr, 2))


def persist_candidates(candidates: Iterable[Candidate], path: Path) -> int:
    rows = [asdict(c) for c in candidates]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8"); tmp.replace(path)
    return len(rows)


def reconcile_candidates(backend: Iterable[Any], persisted: Iterable[Any], filters: dict[str, Any] | None = None) -> dict[str, Any]:
    backend, persisted = list(backend), list(persisted)
    filters = filters or {}
    unfiltered = persisted
    displayed = [c for c in unfiltered if (not filters.get("search") or filters["search"].upper() in str(getattr(c, "symbol", c.get("symbol", "") if isinstance(c, dict) else "")).upper())
                 and float(getattr(c, "score", c.get("score", 0) if isinstance(c, dict) else 0)) >= float(filters.get("minimum_confidence", 0))]
    hidden = len(unfiltered) - len(displayed)
    return {"backend_candidates": len(backend), "persisted_candidates": len(persisted),
            "unfiltered_candidates": len(unfiltered), "displayed_candidates": len(displayed),
            "hidden_by_filters": hidden,
            "notice": f"{hidden} valid setups are hidden by your saved Opportunity Filters." if hidden else "",
            "reset_filters_available": True, "displayed": displayed}
