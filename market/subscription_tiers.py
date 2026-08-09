"""Tiered subscription universes."""

from __future__ import annotations

from typing import Iterable


class SubscriptionTierManager:
    MASTER = "MASTER"
    ACTIVE = "ACTIVE"
    FOCUS = "FOCUS"
    HOT = "HOT"

    def __init__(self):
        self.master: set[str] = set()
        self.active: set[str] = set()
        self.focus: set[str] = set()
        self.hot: set[str] = set()

    def set_master(self, symbols: Iterable[str]) -> None:
        self.master = {self._norm(s) for s in symbols}

    def set_active(self, symbols: Iterable[str]) -> None:
        self.active = {self._norm(s) for s in symbols}

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
            "ACTIVE": sorted(self.active),
            "FOCUS": sorted(self.focus),
            "HOT": sorted(self.hot),
        }

    @staticmethod
    def _norm(symbol: str) -> str:
        sym = str(symbol or "").upper()
        return sym if sym.endswith(".NS") else f"{sym}.NS"
