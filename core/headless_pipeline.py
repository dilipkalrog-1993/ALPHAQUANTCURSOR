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
    trade_confidence: float
    entry: float
    stop: float
    target: float
    score_version: str = "V2"
    component_scores: dict[str, float] = field(default_factory=dict)
    risk_reward: float = 0.0
    data_source: str = "unknown"
    data_timestamp: str = ""
    data_freshness: str = "unknown"
    strategy_diagnostics: dict[str, Any] = field(default_factory=dict)
    positive_reasons: list[str] = field(default_factory=list)
    watch_items: list[str] = field(default_factory=list)
    confluence_bonus: float = 0.0
    scoring_profile: str = "AlphaQuant Default"
    scoring_profile_version: int = 1
    scoring_weights_snapshot: dict[str, float] = field(default_factory=dict)
    trade_score_v2: Any = field(default=None, repr=False, compare=False)
    computed: bool = True

    @property
    def score(self) -> float:
        """Compatibility display alias; confidence is owned exclusively by V2."""
        return self.trade_confidence


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


def score_candidate(
    symbol: str, df: pd.DataFrame, *, data_source: str = "unknown",
    scoring_profile: Any | None = None,
) -> Candidate:
    """Deprecated compatibility adapter that delegates to canonical Scoring V2."""
    from types import SimpleNamespace
    import scoring_engine_v2

    row = df.iloc[-1]
    atr = float(row.ATR)
    entry = float(row.Close)
    candidate = Candidate(
        symbol=symbol, strategy="BREAKOUT", trade_confidence=0.0, entry=entry,
        stop=round(entry - 1.5 * atr, 2), target=round(entry + 3 * atr, 2),
        risk_reward=2.0, data_source=data_source,
        data_timestamp=str(getattr(df.index[-1], "isoformat", lambda: df.index[-1])()),
        data_freshness="current",
        strategy_diagnostics={"gate": "trend_momentum", "signal": True},
    )
    stock = SimpleNamespace(data=df, indicators={}, score={}, sector="UNKNOWN")
    v2 = scoring_engine_v2.compute_trade_score_v2(
        stock, candidate, all_strategies=[candidate.strategy], scoring_profile=scoring_profile,
    )
    if v2.score_version != "V2":
        raise RuntimeError("Canonical scorer returned a non-V2 score")
    candidate.trade_confidence = v2.trade_confidence
    candidate.component_scores = {c.component: c.weighted_contribution for c in v2.all_components()}
    candidate.confluence_bonus = v2.confluence_bonus
    candidate.positive_reasons = list(v2.positive_reasons)
    candidate.watch_items = list(v2.watch_items)
    candidate.scoring_profile = v2.scoring_profile
    candidate.scoring_profile_version = v2.scoring_version
    candidate.scoring_weights_snapshot = dict(v2.scoring_weights_snapshot)
    candidate.strategy_diagnostics.update({"v2_gate_decision": v2.gate_decision})
    candidate.trade_score_v2 = v2
    return candidate


def compute_candidate(symbol: str, df: pd.DataFrame) -> Candidate | None:
    """Create a candidate only from a real momentum/trend strategy signal."""
    return score_candidate(symbol, df) if has_strategy_signal(df) else None


def persist_candidates(candidates: Iterable[Candidate], path: Path) -> int:
    rows = []
    for candidate in candidates:
        row = asdict(candidate)
        # The immutable scalar/component snapshot is the persistence contract;
        # the rich runtime score object is intentionally not JSON serialized.
        row.pop("trade_score_v2", None)
        rows.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8"); tmp.replace(path)
    return len(rows)


def reconcile_candidates(backend: Iterable[Any], persisted: Iterable[Any], filters: dict[str, Any] | None = None) -> dict[str, Any]:
    backend, persisted = list(backend), list(persisted)
    filters = filters or {}
    unfiltered = persisted
    displayed = [c for c in unfiltered if (not filters.get("search") or filters["search"].upper() in str(getattr(c, "symbol", c.get("symbol", "") if isinstance(c, dict) else "")).upper())
                 and float(getattr(c, "trade_confidence", c.get("trade_confidence", c.get("score", 0)) if isinstance(c, dict) else 0)) >= float(filters.get("minimum_confidence", 0))]
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
