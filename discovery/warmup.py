"""Headless pre-market warmup; it never initializes Streamlit or places orders."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from core.history import DEFAULT_CACHE_DIR as HISTORY_CACHE, load_incremental_history
from core.production_engine import run_production_pipeline
from discovery.structure_cache import store_structure_cache

ROOT = Path(__file__).resolve().parent.parent
STRUCTURE_CACHE = ROOT / "data" / "structure_cache"


def warmup_universe(symbols: list[str], app_module: Any | None = None) -> dict[str, Any]:
    """Repair history and prepare stable indicators plus the Active ranking.

    ``app_module`` remains accepted for compatibility but is intentionally not
    imported or used: pre-market preparation is an authoritative backend job.
    """
    results = []
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(symbols)))) as pool:
        futures = [pool.submit(load_incremental_history, symbol, timeout=6.0, retries=0) for symbol in symbols]
        for future in as_completed(futures):
            results.append(future.result())
    production = run_production_pipeline(
        [(r.symbol, r.frame, r.provider) for r in results], focus_limit=min(75, len(symbols)))
    # Persist candle-stable broad structures. Full UI strategies may enrich
    # this payload later without recomputing these unchanged rank features.
    for row in production["active"]:
        features = row["features"]
        store_structure_cache(row["symbol"], row["dataframe"], {
            "support_resistance": {"support": features["support"], "resistance": features["resistance"]},
            "relative_strength": features.get("dist_52w_high_pct"),
            "structural_trend": {"ema_aligned": features.get("ema_aligned"),
                "macd_bullish": features.get("macd_bullish")},
            "opportunity_score": row["opportunity_score"],
        })
    return {"master": len(symbols), "prepared": len(production["prepared"]),
        "eligible": len(production["eligible"]), "active": len(production["active"]),
        "focus": len(production["focus"]), "cache_hits": sum(r.cache_hit for r in results),
        "repaired_downloads": sum(bool(r.fetched_rows) for r in results),
        "failures": [r.symbol for r in results if r.failure] + [x["symbol"] for x in production["failures"]],
        "history_cache_dir": str(HISTORY_CACHE), "structure_cache_dir": str(STRUCTURE_CACHE),
        "timings": production["timings"], "orders_sent": 0}
