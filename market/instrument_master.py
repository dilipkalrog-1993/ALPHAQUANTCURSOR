"""Central instrument mapping cache."""

from __future__ import annotations

import json
import gzip
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_CACHE = Path(__file__).resolve().parent.parent / "data" / "instrument_master.json"
UPSTOX_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

# Exchange renames verified against the NSE/Upstox instrument master.  Aliases
# preserve old saved workspaces; they are not fabricated broker instrument keys.
NSE_SYMBOL_ALIASES = {"LTIM": "LTM", "TATAMOTORS": "TMPV"}

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
        return {"upstox": dict(UPSTOX_INDEX_KEYS), "symbols": {}, "aliases": dict(NSE_SYMBOL_ALIASES)}

    def _persist(self) -> None:
        temp = self.cache_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self._cache, indent=2), encoding="utf-8")
        temp.replace(self.cache_path)

    def normalize_symbol(self, symbol: str) -> str:
        sym = str(symbol or "").upper().strip()
        suffix = ".BO" if sym.endswith(".BO") else ".NS"
        bare = sym.removesuffix(".NS").removesuffix(".BO")
        bare = self._cache.get("aliases", NSE_SYMBOL_ALIASES).get(bare, bare)
        return f"{bare}{suffix}"

    def register(self, symbol: str, *, exchange: str = "NSE", upstox_key: str | None = None) -> dict[str, Any]:
        sym = self.normalize_symbol(symbol)
        entry = self._cache.setdefault("symbols", {}).setdefault(sym, {"exchange": exchange})
        if upstox_key:
            entry["upstox"] = upstox_key
            self._cache.setdefault("upstox", {})[sym.replace(".NS", "")] = upstox_key
        self._persist()
        return entry

    def canonical_mapping(self, symbol: str) -> dict[str, Any]:
        canonical = self.normalize_symbol(symbol)
        entry = self._cache.get("symbols", {}).get(canonical, {})
        bare = canonical.removesuffix(".NS").removesuffix(".BO")
        return {"display_symbol": bare, "broker_symbol": entry.get("broker_symbol", bare),
                "historical_provider_symbol": entry.get("historical_provider_symbol", canonical),
                "instrument_key": entry.get("instrument_key") or entry.get("upstox"),
                "exchange": entry.get("exchange", "NSE")}

    def refresh_upstox(self, *, timeout: float = 20.0) -> int:
        """Refresh equities from Upstox's authoritative daily instrument file."""
        import requests
        response = requests.get(UPSTOX_MASTER_URL, timeout=timeout,
                                headers={"User-Agent": "AlphaQuant/1.0"})
        response.raise_for_status()
        payload = json.load(gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8"))
        symbols: dict[str, Any] = {}
        for row in payload:
            if row.get("segment") != "NSE_EQ" or row.get("instrument_type") not in {"EQ", "BE"}:
                continue
            broker_symbol = str(row.get("trading_symbol") or "").upper().strip()
            if not broker_symbol:
                continue
            historical = f"{broker_symbol}.NS"
            symbols[historical] = {"display_symbol": broker_symbol, "broker_symbol": broker_symbol,
                "historical_provider_symbol": historical, "instrument_key": row.get("instrument_key"),
                "upstox": row.get("instrument_key"), "exchange": "NSE"}
        if not symbols:
            raise ValueError("Authoritative instrument master contained no NSE equities")
        self._cache.update(symbols=symbols, aliases=dict(NSE_SYMBOL_ALIASES),
                           source=UPSTOX_MASTER_URL, refreshed_at=datetime.now(timezone.utc).isoformat())
        self._cache["upstox"] = {**UPSTOX_INDEX_KEYS,
            **{v["broker_symbol"]: v["instrument_key"] for v in symbols.values()}}
        self._persist()
        return len(symbols)

    def resolve_upstox_key(self, symbol: str) -> str | None:
        bare = str(symbol or "").upper().replace(".NS", "").replace(".BO", "")
        if bare in self._cache.get("upstox", {}):
            return self._cache["upstox"][bare]
        entry = self._cache.get("symbols", {}).get(self.normalize_symbol(symbol), {})
        return entry.get("instrument_key") or entry.get("upstox")

    def bulk_seed_equity_symbols(self, symbols: list[str]) -> int:
        count = 0
        for symbol in symbols:
            sym = self.normalize_symbol(symbol)
            bare = sym.replace(".NS", "")
            key = f"NSE_EQ|{bare}"
            self.register(sym, upstox_key=key)
            count += 1
        return count
