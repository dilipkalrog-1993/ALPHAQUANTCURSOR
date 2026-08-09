"""Central instrument mapping cache."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_DEFAULT_CACHE = Path(__file__).resolve().parent.parent / "data" / "instrument_master.json"

# Seed index instruments used by Upstox
UPSTOX_INDEX_KEYS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "NIFTY MIDCAP": "NSE_INDEX|NIFTY MIDCAP 100",
    "INDIA VIX": "NSE_INDEX|India VIX",
}


class InstrumentMaster:
    """Resolve symbol ↔ exchange ↔ broker instrument key centrally."""

    def __init__(self, cache_path: Path | None = None):
        self.cache_path = cache_path or _DEFAULT_CACHE
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache = self._load()

    def _load(self) -> dict[str, Any]:
        if self.cache_path.exists():
            try:
                return json.loads(self.cache_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                pass
        return {"upstox": dict(UPSTOX_INDEX_KEYS), "symbols": {}}

    def _persist(self) -> None:
        temp = self.cache_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self._cache, indent=2), encoding="utf-8")
        temp.replace(self.cache_path)

    def normalize_symbol(self, symbol: str) -> str:
        sym = str(symbol or "").upper().strip()
        if sym.endswith(".NS") or sym.endswith(".BO"):
            return sym
        return f"{sym}.NS"

    def register(self, symbol: str, *, exchange: str = "NSE", upstox_key: str | None = None) -> dict[str, Any]:
        sym = self.normalize_symbol(symbol)
        entry = self._cache.setdefault("symbols", {}).setdefault(sym, {"exchange": exchange})
        if upstox_key:
            entry["upstox"] = upstox_key
            self._cache.setdefault("upstox", {})[sym.replace(".NS", "")] = upstox_key
        self._persist()
        return entry

    def resolve_upstox_key(self, symbol: str) -> str | None:
        bare = str(symbol or "").upper().replace(".NS", "").replace(".BO", "")
        if bare in self._cache.get("upstox", {}):
            return self._cache["upstox"][bare]
        entry = self._cache.get("symbols", {}).get(self.normalize_symbol(symbol), {})
        return entry.get("upstox")

    def bulk_seed_equity_symbols(self, symbols: list[str]) -> int:
        count = 0
        for symbol in symbols:
            sym = self.normalize_symbol(symbol)
            bare = sym.replace(".NS", "")
            key = f"NSE_EQ|{bare}"
            self.register(sym, upstox_key=key)
            count += 1
        return count
