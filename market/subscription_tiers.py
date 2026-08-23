"""Tiered subscription universes."""

from __future__ import annotations

from typing import Iterable


class SubscriptionTierManager:
    MASTER = "MASTER"
    ELIGIBLE = "ELIGIBLE"
    ACTIVE = "ACTIVE"
    FOCUS = "FOCUS"
    CANDIDATES = "CANDIDATES"
    HOT = "HOT"
    POSITIONS = "POSITIONS"

    # Broad work is scheduled independently; safety-critical paths have no
    # dependency on completion of a broad or focus scan.
    BROAD_CADENCE_SECONDS = 600
    FOCUS_CADENCE_SECONDS = 60
    HOT_CADENCE = "EVENT"

    def __init__(self):
        self.master: set[str] = set()
        self.eligible: set[str] = set()
        self.active: set[str] = set()
        self.focus: set[str] = set()
        self.hot: set[str] = set()
        self.candidates: set[str] = set()
        self.positions: set[str] = set()

    def set_master(self, symbols: Iterable[str]) -> None:
        self.master = {self._norm(s) for s in symbols}

    def set_active(self, symbols: Iterable[str]) -> None:
        self.active = {self._norm(s) for s in symbols}

    def set_eligible(self, symbols: Iterable[str]) -> None:
        self.eligible = {self._norm(s) for s in symbols}

    def set_candidates(self, symbols: Iterable[str]) -> None:
        self.candidates = {self._norm(s) for s in symbols}

    def set_positions(self, symbols: Iterable[str]) -> None:
        self.positions = {self._norm(s) for s in symbols}

    def set_focus(self, symbols: Iterable[str]) -> None:
        self.focus = {self._norm(s) for s in symbols}

    def set_hot(self, symbols: Iterable[str]) -> None:
        self.hot = {self._norm(s) for s in symbols}

    def promote_to_hot(self, symbol: str) -> None:
        sym = self._norm(symbol)
        self.hot.add(sym)
        self.focus.add(sym)

    def snapshot(self) -> dict[str, list[str]]:
        return {
            "MASTER": sorted(self.master),
            "ELIGIBLE": sorted(self.eligible),
            "ACTIVE": sorted(self.active),
            "FOCUS": sorted(self.focus),
            "CANDIDATES": sorted(self.candidates),
            "HOT": sorted(self.hot),
            "POSITIONS": sorted(self.positions),
        }

    @staticmethod
    def _norm(symbol: str) -> str:
        sym = str(symbol or "").upper()
        return sym if sym.endswith(".NS") else f"{sym}.NS"
