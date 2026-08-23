"""Central instrument mapping cache."""

from __future__ import annotations

import json
import gzip
import io
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

_DEFAULT_CACHE = Path(__file__).resolve().parent.parent / "data" / "instrument_master.json"
UPSTOX_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
MASTER_SCHEMA_VERSION = 2
VALID_INSTRUMENT_TYPES = {"EQUITY", "EQ", "BE"}

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

    def is_valid(self) -> bool:
        """A master is valid only when it contains real, keyed NSE equities."""
        symbols = self._cache.get("symbols")
        return bool(symbols) and all(
            str(k).endswith(".NS") and v.get("instrument_key")
            for k, v in symbols.items()
        )

    def bootstrap(self, *, max_age_hours: float = 24, timeout: float = 20,
                  retries: int = 2, force_refresh: bool = False,
                  include_etfs: bool = False) -> dict[str, Any]:
        """Load LKG first, then safely refresh a missing/stale master."""
        had_valid = self.is_valid()
        age = float("inf")
        try:
            stamp = datetime.fromisoformat(str(self.refreshed_at).replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - stamp).total_seconds() / 3600
        except (TypeError, ValueError):
            pass
        if had_valid and not force_refresh and age <= max_age_hours:
            return {"status": "FRESH", "count": len(self.symbols()), "refreshed": False,
                    "metadata": self._cache.get("metadata", {})}
        error = None
        for attempt in range(retries + 1):
            try:
                count = self.refresh_upstox(timeout=timeout, include_etfs=include_etfs)
                return {"status": "FRESH", "count": count, "refreshed": True,
                        "metadata": self._cache.get("metadata", {})}
            except Exception as exc:  # provider/network/schema failures are status, not data
                error = f"{type(exc).__name__}: {exc}"
                if attempt < retries:
                    time.sleep(min(2 ** attempt, 4))
        # refresh_upstox is transactional, so the in-memory/disk LKG is intact.
        if had_valid:
            return {"status": "STALE_FALLBACK", "count": len(self.symbols()),
                    "refreshed": False, "error": error,
                    "metadata": self._cache.get("metadata", {})}
        return {"status": "MASTER_UNAVAILABLE", "count": 0, "refreshed": False,
                "error": error, "metadata": {}}

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
        return {"display_symbol": entry.get("display_symbol", bare), "broker_symbol": entry.get("broker_symbol", bare),
                "historical_provider_symbol": entry.get("historical_provider_symbol", canonical),
                "instrument_key": entry.get("instrument_key") or entry.get("upstox"),
                "exchange": entry.get("exchange", "NSE")}

    def refresh_upstox(self, *, timeout: float = 20.0, include_etfs: bool = False) -> int:
        """Refresh equities from Upstox's authoritative daily instrument file."""
        # This is Upstox's public daily file and intentionally requires no token.
        request = urllib.request.Request(UPSTOX_MASTER_URL, headers={"User-Agent": "AlphaQuant/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content = response.read()
        payload = json.load(gzip.open(io.BytesIO(content), "rt", encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Unexpected Upstox instrument-master schema")
        symbols: dict[str, Any] = {}
        refreshed_at = datetime.now(timezone.utc).isoformat()
        stats = {"raw_instruments": len(payload), "nse_instruments": 0, "cash_equities": 0,
                 "excluded_etfs": 0, "excluded_invalid_suspended": 0,
                 "final_tradable_nse_equities": 0}
        for row in payload:
            if row.get("segment") != "NSE_EQ":
                continue
            stats["nse_instruments"] += 1
            instrument_type = str(row.get("instrument_type") or "").upper()
            if instrument_type not in VALID_INSTRUMENT_TYPES:
                continue
            stats["cash_equities"] += 1
            # Honour explicit classifications from the source.  Do not guess
            # from symbol suffixes: legitimate companies can contain strings
            # such as "ETF" in their names.
            classification = str(row.get("asset_type") or row.get("security_type") or "").upper()
            if not include_etfs and classification in {"ETF", "EXCHANGE_TRADED_FUND"}:
                stats["excluded_etfs"] += 1
                continue
            status = str(row.get("status") or row.get("trading_status") or "ACTIVE").upper()
            if status in {"SUSPENDED", "DELISTED", "INACTIVE", "DISABLED", "UNTRADEABLE"}:
                stats["excluded_invalid_suspended"] += 1
                continue
            broker_symbol = str(row.get("trading_symbol") or "").upper().strip()
            if not broker_symbol:
                continue
            historical = f"{broker_symbol}.NS"
            instrument_key = row.get("instrument_key")
            inferred_isin = (str(instrument_key).partition("|")[2]
                             if str(instrument_key or "").startswith("NSE_EQ|INE") else None)
            symbols[historical] = {"display_symbol": broker_symbol, "broker_symbol": broker_symbol,
                "historical_provider_symbol": historical, "instrument_key": instrument_key,
                "upstox": instrument_key, "isin": row.get("isin") or inferred_isin,
                "sector": row.get("sector"), "industry": row.get("industry"),
                "exchange": "NSE", "instrument_type": row.get("instrument_type"),
                "last_instrument_master_refresh": refreshed_at}
        if not symbols:
            raise ValueError("Authoritative instrument master contained no NSE equities")
        stats["final_tradable_nse_equities"] = len(symbols)
        metadata = {"provider": "Upstox", "source": UPSTOX_MASTER_URL,
                    "refresh_timestamp": refreshed_at, "instrument_count": len(payload),
                    "cash_equity_count": len(symbols), "schema_version": MASTER_SCHEMA_VERSION,
                    "source_status": "AUTHORITATIVE", "freshness": "FRESH", **stats}
        self._cache.update(symbols=symbols, aliases=dict(NSE_SYMBOL_ALIASES),
                           source=UPSTOX_MASTER_URL, refreshed_at=refreshed_at,
                           metadata=metadata,
                           filters={"include_etfs": include_etfs, "segment": "NSE_EQ",
                                    "instrument_types": sorted(VALID_INSTRUMENT_TYPES)})
        self._cache["upstox"] = {**UPSTOX_INDEX_KEYS,
            **{v["broker_symbol"]: v["instrument_key"] for v in symbols.values()}}
        self._persist()
        return len(symbols)

    @property
    def refreshed_at(self) -> str | None:
        return self._cache.get("refreshed_at")

    def equities(self) -> list[dict[str, Any]]:
        """Return the complete persisted NSE cash-equity master, never an index seed."""
        rows = []
        for symbol, stored in sorted(self._cache.get("symbols", {}).items()):
            row = {**stored, **self.canonical_mapping(symbol)}
            row["last_instrument_master_refresh"] = (stored.get("last_instrument_master_refresh") or
                                                       self._cache.get("refreshed_at"))
            rows.append(row)
        return rows

    def symbols(self) -> list[str]:
        return [row["historical_provider_symbol"] for row in self.equities()]

    def search(self, query: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """Search every master equity by symbol, ISIN, sector, or industry."""
        needle = str(query or "").strip().upper()
        rows: Iterable[dict[str, Any]] = self.equities()
        if needle:
            rows = (row for row in rows if any(needle in str(row.get(field) or "").upper()
                    for field in ("display_symbol", "broker_symbol", "historical_provider_symbol",
                                  "isin", "sector", "industry")))
        return list(rows)[:max(0, limit)]

    def resolve_upstox_key(self, symbol: str) -> str | None:
        bare = str(symbol or "").upper().replace(".NS", "").replace(".BO", "")
        if bare in self._cache.get("upstox", {}):
            return self._cache["upstox"][bare]
        entry = self._cache.get("symbols", {}).get(self.normalize_symbol(symbol), {})
        return entry.get("instrument_key") or entry.get("upstox")

    def authoritative_status(self, symbol: str) -> str | None:
        """Return only an explicitly persisted exchange/provider status."""
        entry = self._cache.get("symbols", {}).get(self.normalize_symbol(symbol), {})
        value = entry.get("status") or entry.get("trading_status")
        return str(value).upper() if value else None

    def bulk_seed_equity_symbols(self, symbols: list[str]) -> int:
        count = 0
        for symbol in symbols:
            sym = self.normalize_symbol(symbol)
            bare = sym.replace(".NS", "")
            key = f"NSE_EQ|{bare}"
            self.register(sym, upstox_key=key)
            count += 1
        return count
