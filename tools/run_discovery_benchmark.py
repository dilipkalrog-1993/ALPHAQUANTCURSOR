#!/usr/bin/env python3
"""Completely headless benchmark of the authoritative production pipeline.

This deliberately benchmarks cached data: provider performance belongs to the
real-NSE validator, while a repeatable benchmark must never initialize the UI.
"""
from __future__ import annotations

import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market.instrument_master import InstrumentMaster
DEFAULT_CACHE_DIR = ROOT / "data" / "history_cache"

def _cached_symbols() -> list[str]:
    return sorted(f"{p.stem}.NS" for p in DEFAULT_CACHE_DIR.glob("*.pkl"))


def _load(symbols: list[str]) -> tuple[list[tuple[str, Any, str]], float, int]:
    from core.history import normalize_history
    at = time.perf_counter()
    rows: list[tuple[str, Any, str]] = []
    for symbol in symbols:
        path = DEFAULT_CACHE_DIR / f"{symbol.removesuffix('.NS')}.pkl"
        try:
            frame = normalize_history(pickle.loads(path.read_bytes()))
            if not frame.empty:
                rows.append((symbol, frame, "persistent-cache"))
        except (OSError, EOFError, pickle.PickleError, ValueError, AttributeError):
            continue
    return rows, time.perf_counter() - at, len(rows)


def _run(label: str, symbols: list[str], *, requested: int | str,
         instrument_master_count: int, focus_limit: int = 100) -> dict[str, Any]:
    from core.production_engine import run_production_pipeline
    total_at = time.perf_counter()
    histories, cache_seconds, hits = _load(symbols)
    result = run_production_pipeline(histories, focus_limit=focus_limit)
    stages = result["diagnostics"].counts
    counts = {
        "instrument_master": instrument_master_count, "history_ready": stages["history_ready"],
        "evaluated": stages["master"], "eligible": stages["eligible"],
        "active": stages["active"], "focus": stages["focus"],
        "signals": stages["strategy_signals"], "v2_qualified": stages["v2_qualified"],
        "candidates": stages["candidates"],
        "hot": 0, "positions": 0,
    }
    timings = {"history_cache": round(cache_seconds, 6), **result["timings"]}
    timings["total"] = round(time.perf_counter() - total_at, 6)
    return {"label": label, "requested": requested, "selected_from_master": len(symbols),
        "instrument_master_count": instrument_master_count,
        "available_cached": hits, "evaluated": stages["master"], "missing_cache": len(symbols) - hits,
        "counts": counts, "signal_outcomes": result["signal_outcomes"], "cache_hits": hits,
        "cache_misses": len(symbols) - hits,
        "cache_hit_rate": round(hits / len(symbols), 4) if symbols else 0.0,
        "indicator_cache_hit_rate": None,
        "provider_fetches": 0, "provider_failures": [], "timings": timings}


def main() -> int:
    master_at = time.perf_counter()
    cached = _cached_symbols()
    master = InstrumentMaster()
    master_state = master.bootstrap()
    master_seconds = time.perf_counter() - master_at
    master_symbols = master.symbols()
    canonical_cached = [master.normalize_symbol(s) for s in cached]
    # An absent/stale master is reported honestly; cached files are never
    # promoted to an NSE universe. They are only intersected with the master.
    available = master_symbols
    master_count = len(master_symbols)
    if not master_count:
        print(json.dumps({"status": "MASTER_UNAVAILABLE", "instrument_master_count": 0,
                          "master": master_state, "timings": {"master_load_refresh": master_seconds},
                          "cached_files_preserved": len(cached)}, indent=2))
        return 2
    universes = []
    for size in (50, 200, 500, 1000):
        selected = available[:size]
        universes.append(_run(f"NSE {size} request", selected, requested=size,
                              instrument_master_count=master_count))
    universes.append(_run("FULL AVAILABLE NSE", available, requested="FULL AVAILABLE NSE",
                          instrument_master_count=master_count))
    report = {"engine": "core.production_engine", "headless": True, "universes": universes,
        "status": master_state["status"], "master": master_state,
        "master_load_refresh_seconds": round(master_seconds, 6),
        "cache_reconciliation": {"cache_files_discovered": len(cached),
            "matched_to_master": len(set(canonical_cached) & set(master_symbols)),
            "aliases_reconciled": sum(a != b for a, b in zip(cached, canonical_cached)),
            "invalid_cache_files": 0,
            "still_unmatched": len(set(canonical_cached) - set(master_symbols))},
        "cached_files_not_in_master": len(set(canonical_cached) - set(master_symbols)),
        "targets": {"focus_100_strategy_v2_seconds": 5, "nse_500_cheap_scan_seconds": 10,
                    "full_nse_cached_eligibility_ranking_seconds": 30}}
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Benchmark interrupted cleanly", file=sys.stderr)
        raise SystemExit(130)
