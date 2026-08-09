"""Pre-market warmup — prepare caches before market open."""

from __future__ import annotations

import pickle
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
HISTORY_CACHE = ROOT / "data" / "history_cache"
STRUCTURE_CACHE = ROOT / "data" / "structure_cache"


def warmup_universe(symbols: list[str], app_module: Any | None = None) -> dict[str, Any]:
    """Load history, indicators, and structure caches without running strategies."""
    if app_module is None:
        sys.path.insert(0, str(ROOT))
        import appemergentquant_v3_1 as app_module

    import yfinance as yf
    from discovery.structure_cache import store_structure_cache
    from discovery.symbol_context import SymbolAnalysisContext

    HISTORY_CACHE.mkdir(parents=True, exist_ok=True)
    STRUCTURE_CACHE.mkdir(parents=True, exist_ok=True)
    prepared = 0
    repaired = 0
    failures: list[str] = []

    for sym in symbols:
        cache_file = HISTORY_CACHE / f"{sym.replace('.NS', '')}.pkl"
        try:
            if cache_file.exists():
                raw = pickle.loads(cache_file.read_bytes())
            else:
                raw = yf.download(sym, period="1y", interval="1d", progress=False, auto_adjust=True, threads=False)
                if raw is None or raw.empty:
                    failures.append(sym)
                    continue
                cache_file.write_bytes(pickle.dumps(raw))
                repaired += 1
            if hasattr(raw.columns, "nlevels") and raw.columns.nlevels > 1:
                raw.columns = raw.columns.get_level_values(0)
            df = app_module.calculate_indicators(raw)
            if df is None:
                failures.append(sym)
                continue
            prepared += 1
            stock = app_module.get_stock(sym)
            stock.set_dataframe(df)
            SymbolAnalysisContext.from_dataframe(sym, df).attach_to_stock(stock)
            app_module.calculate_trade_quality(stock)
            app_module.update_market_structure(stock)
            store_structure_cache(sym, df, {"market": dict(stock.market), "patterns": dict(stock.patterns)})
        except Exception:
            failures.append(sym)

    return {
        "prepared": prepared,
        "repaired_downloads": repaired,
        "failures": failures,
        "history_cache_dir": str(HISTORY_CACHE),
        "structure_cache_dir": str(STRUCTURE_CACHE),
    }
