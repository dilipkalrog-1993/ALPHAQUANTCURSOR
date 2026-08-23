"""Headless pre-market warmup; it never initializes Streamlit or places orders."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from core.history import DEFAULT_CACHE_DIR as HISTORY_CACHE
from core.history_providers import UpstoxHistoryProvider, YahooHistoryProvider
from core.history_service import HistoryService
from broker.credential_store import CredentialStore
import json
from core.production_engine import run_production_pipeline
from discovery.structure_cache import store_structure_cache
from market.instrument_master import InstrumentMaster

ROOT = Path(__file__).resolve().parent.parent
STRUCTURE_CACHE = ROOT / "data" / "structure_cache"


def warmup_universe(symbols: list[str], app_module: Any | None = None) -> dict[str, Any]:
    """Repair history and prepare stable indicators plus the Active ranking.

    ``app_module`` remains accepted for compatibility but is intentionally not
    imported or used: pre-market preparation is an authoritative backend job.
    """
    token = ""
    try:
        connections = json.loads((ROOT / "data" / "broker_connections.json").read_text(encoding="utf-8"))
        profiles = connections.get("profiles") or {}
        selected = connections.get("default_market_data_broker") or next(iter(profiles), "")
        token = CredentialStore().load_profile_secrets(selected).get("access_token", "")
    except (OSError, ValueError, TypeError):
        pass
    service = HistoryService([UpstoxHistoryProvider(token), YahooHistoryProvider()], retries=1, timeout=8.0)
    results = []
    # Deliberately conservative: warmup must not starve live monitoring.
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(symbols)))) as pool:
        futures = [pool.submit(service.repair, symbol) for symbol in symbols]
        for future in as_completed(futures):
            results.append(future.result())
    production = run_production_pipeline(
        [(r.symbol, r.frame, r.provider_selected or "UNAVAILABLE") for r in results], focus_limit=min(75, len(symbols)))
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
        "repaired_downloads": sum(bool(r.rows_written) for r in results),
        "failures": [{"symbol": r.symbol, "category": r.failure_category,
                      "detail": r.failure_detail, "provider_attempts": r.provider_attempts}
                     for r in results if r.failure_category] + production["failures"],
        "history_cache_dir": str(HISTORY_CACHE), "structure_cache_dir": str(STRUCTURE_CACHE),
        "timings": production["timings"], "orders_sent": 0}


def warmup_current_nse(*, refresh_master: bool = True, include_etfs: bool = False) -> dict[str, Any]:
    """Pre-market entry point for the complete authoritative NSE universe."""
    master = InstrumentMaster()
    state = master.bootstrap(force_refresh=refresh_master, include_etfs=include_etfs)
    if state["status"] == "MASTER_UNAVAILABLE":
        return {**state, "instrument_master_count": 0, "orders_sent": 0}
    report = warmup_universe(master.symbols())
    report["instrument_master_count"] = len(master.symbols())
    report["instrument_master_refreshed_at"] = master.refreshed_at
    report["instrument_master_status"] = state["status"]
    report["instrument_master_metadata"] = state.get("metadata", {})
    return report
