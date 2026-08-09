"""Eligibility filter — cheap safety/data-quality checks only."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EligibilityAudit:
    input_count: int = 0
    eligible_count: int = 0
    rejections: dict[str, int] = field(default_factory=dict)

    def record(self, reason: str) -> None:
        self.rejections[reason] = self.rejections.get(reason, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        surv_pct = round(100.0 * self.eligible_count / self.input_count, 1) if self.input_count else 0.0
        return {
            "input": self.input_count,
            "survivors": self.eligible_count,
            "percentage": surv_pct,
            "rejections": dict(self.rejections),
        }


def filter_eligible(
    market_data: dict[str, Any],
    *,
    min_price: float = 20.0,
    max_price: float = 100_000.0,
    min_avg_volume: float = 100_000.0,
    min_bars: int = 50,
) -> tuple[list[tuple[str, Any]], EligibilityAudit]:
    """Return symbols passing eligibility (NOT opportunity) checks."""
    audit = EligibilityAudit(input_count=len(market_data))
    eligible: list[tuple[str, Any]] = []

    for sym, df in market_data.items():
        if df is None or not hasattr(df, "empty") or df.empty:
            audit.record("NO_HISTORY")
            continue
        if len(df) < min_bars:
            audit.record("INSUFFICIENT_CANDLES")
            continue
        try:
            close = float(df.iloc[-1]["Close"])
        except (TypeError, ValueError, KeyError, IndexError):
            audit.record("INVALID_CLOSE")
            continue
        if close < min_price:
            audit.record("MINIMUM_PRICE")
            continue
        if close > max_price:
            audit.record("MAXIMUM_PRICE")
            continue
        try:
            avg_vol = float(df["AVG_VOLUME20"].iloc[-1]) if "AVG_VOLUME20" in df.columns else float(df["Volume"].tail(20).mean())
        except (TypeError, ValueError, KeyError):
            avg_vol = 0.0
        if avg_vol < min_avg_volume:
            audit.record("MINIMUM_AVERAGE_VOLUME")
            continue
        try:
            turnover = close * avg_vol
            if turnover < 1_000_000:
                audit.record("LOW_TURNOVER")
                continue
        except (TypeError, ValueError):
            audit.record("TURNOVER_CHECK_FAILED")
            continue
        if "Close" in df.columns and df["Close"].isna().tail(5).any():
            audit.record("STALE_OR_MISSING_DATA")
            continue
        eligible.append((sym, df))
        audit.eligible_count += 1

    return eligible, audit
