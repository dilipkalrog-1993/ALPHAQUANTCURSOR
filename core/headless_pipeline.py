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


def has_strategy_signal(df: pd.DataFrame) -> bool:
    """Cheap full-strategy gate, kept separate from candidate/V2 scoring."""
    row = df.iloc[-1]
    return bool(row.Close > row.EMA20 > row.EMA50 and row.Close > row.EMA200 and 45 <= row.RSI <= 75)


def score_candidate(symbol: str, df: pd.DataFrame) -> Candidate:
    """Materialize the risk levels and score after a strategy has signalled."""
    row = df.iloc[-1]
    atr = float(row.ATR)
    score = min(100.0, 50 + max(0.0, float(row.RSI)-45) + min(20, (float(row.Close/row.EMA50)-1)*200))
    return Candidate(symbol, "TREND_MOMENTUM", round(score, 2), float(row.Close),
                     round(float(row.Close)-1.5*atr, 2), round(float(row.Close)+3*atr, 2))


def compute_candidate(symbol: str, df: pd.DataFrame) -> Candidate | None:
    """Create a candidate only from a real momentum/trend strategy signal."""
    return score_candidate(symbol, df) if has_strategy_signal(df) else None


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


# Compatibility imports. Production callers use core.history; keeping these
# names avoids breaking older integrations while ensuring there is one path.
def normalize_history(raw: Any) -> pd.DataFrame:
    from core.history import normalize_history as canonical_normalize_history
    return canonical_normalize_history(raw)


def prepare_indicators(symbol: str, raw: Any, provider: str = "yfinance"):
    from core.history import prepare_indicators as canonical_prepare_indicators
    return canonical_prepare_indicators(symbol, raw, provider)
