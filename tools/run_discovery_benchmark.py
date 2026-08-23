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

from core.history import DEFAULT_CACHE_DIR, normalize_history
from core.production_engine import run_production_pipeline

SEED50 = "ADANIENT ADANIPORTS APOLLOHOSP ASIANPAINT AXISBANK BAJAJ-AUTO BAJAJFINSV BAJFINANCE BEL BHARTIARTL BPCL BRITANNIA CIPLA COALINDIA DIVISLAB DRREDDY EICHERMOT GRASIM HCLTECH HDFCBANK HDFCLIFE HEROMOTOCO HINDALCO HINDUNILVR ICICIBANK INDUSINDBK INFY ITC JSWSTEEL KOTAKBANK LT LTIM M&M MARUTI NESTLEIND NTPC ONGC POWERGRID RELIANCE SBILIFE SBIN SHRIRAMFIN SUNPHARMA TATACONSUM TATAMOTORS TATASTEEL TCS TECHM TITAN TRENT".split()


def _cached_symbols() -> list[str]:
    return sorted(f"{p.stem}.NS" for p in DEFAULT_CACHE_DIR.glob("*.pkl"))


def _load(symbols: list[str]) -> tuple[list[tuple[str, Any, str]], float, int]:
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


def _run(label: str, symbols: list[str], focus_limit: int = 50) -> dict[str, Any]:
    total_at = time.perf_counter()
    histories, cache_seconds, hits = _load(symbols)
    result = run_production_pipeline(histories, focus_limit=focus_limit)
    stages = result["diagnostics"].counts
    counts = {
        "master": stages["master"], "eligible": stages["eligible"],
        "active": stages["active"], "focus": stages["focus"],
        "strategy_signals": stages["strategy_signals"], "candidates": stages["candidates"],
        "hot": 0, "positions": 0,
    }
    timings = {"history_cache": round(cache_seconds, 6), **result["timings"]}
    timings["total"] = round(time.perf_counter() - total_at, 6)
    return {"label": label, "requested_universe": len(symbols),
        "available_cached": hits, "evaluated": stages["master"], "missing_cache": len(symbols) - hits,
        "counts": counts, "signal_outcomes": result["signal_outcomes"], "cache_hits": hits,
        "cache_misses": len(symbols) - hits,
        "cache_hit_rate": round(hits / len(symbols), 4) if symbols else 0.0,
        "indicator_cache_hit_rate": None,
        "provider_fetches": 0, "provider_failures": [], "timings": timings}


def main() -> int:
    cached = _cached_symbols()
    seed = [f"{s}.NS" for s in SEED50]
    universes = [
        _run("nifty_50", seed, 50),
        _run("nse_200_requested", seed + [s for s in cached if s not in seed][:150], 50),
        _run("largest_available_cache", cached, 75),
    ]
    report = {"engine": "core.production_engine", "headless": True, "universes": universes,
        "targets": {"focus_50_seconds": 5, "nse_200_seconds": 15, "full_cheap_rank_seconds": 30}}
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Benchmark interrupted cleanly", file=sys.stderr)
        raise SystemExit(130)
