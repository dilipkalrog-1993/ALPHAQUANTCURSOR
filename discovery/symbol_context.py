"""Precomputed per-symbol analysis context — shared across strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from discovery.opportunity_ranker import compute_opportunity_features


@dataclass
class SymbolAnalysisContext:
    symbol: str
    dataframe: Any
    features: dict[str, float] = field(default_factory=dict)
    structure_ready: bool = False
    quality_ready: bool = False

    @classmethod
    def from_dataframe(cls, symbol: str, df: Any) -> "SymbolAnalysisContext":
        return cls(symbol=symbol, dataframe=df, features=compute_opportunity_features(df))

    def attach_to_stock(self, stock: Any) -> None:
        stock._analysis_context = self
        stock._opportunity_features = self.features
