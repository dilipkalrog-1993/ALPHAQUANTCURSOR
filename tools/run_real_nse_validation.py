#!/usr/bin/env python3
"""Bounded real NSE validation using only the headless production domain."""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, median
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.history import load_incremental_history
from core.production_engine import run_production_pipeline

SOURCES = {"nifty_50":"https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "nifty_200":"https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv"}
SEED50 = "ADANIENT ADANIPORTS APOLLOHOSP ASIANPAINT AXISBANK BAJAJ-AUTO BAJAJFINSV BAJFINANCE BEL BHARTIARTL BPCL BRITANNIA CIPLA COALINDIA DIVISLAB DRREDDY EICHERMOT GRASIM HCLTECH HDFCBANK HDFCLIFE HEROMOTOCO HINDALCO HINDUNILVR ICICIBANK INDUSINDBK INFY ITC JSWSTEEL KOTAKBANK LT LTIM M&M MARUTI NESTLEIND NTPC ONGC POWERGRID RELIANCE SBILIFE SBIN SHRIRAMFIN SUNPHARMA TATACONSUM TATAMOTORS TATASTEEL TCS TECHM TITAN TRENT".split()


def fetch_universe(label: str) -> tuple[list[str], float, str | None]:
    import pandas as pd
    at = time.perf_counter()
    try:
        # pandas URL handling does not expose a timeout, so use requests explicitly.
        import requests
        response = requests.get(SOURCES[label], timeout=(3.0, 8.0))
        response.raise_for_status()
        from io import StringIO
        frame = pd.read_csv(StringIO(response.text))
        col = next(c for c in frame.columns if str(c).strip().lower() == "symbol")
        symbols = sorted({f"{str(x).strip()}.NS" for x in frame[col] if str(x).strip()})
        return symbols, time.perf_counter() - at, None
    except Exception as exc:
        if label == "nifty_50":
            return [f"{x}.NS" for x in SEED50], time.perf_counter() - at, f"{type(exc).__name__}: {exc}"
        raise


def distribution(scores: list[float]) -> dict[str, int]:
    out = {x:0 for x in ("0-39","40-49","50-59","60-69","70-79","80-89","90-100")}
    for score in scores:
        key = "0-39" if score < 40 else "40-49" if score < 50 else "50-59" if score < 60 else "60-69" if score < 70 else "70-79" if score < 80 else "80-89" if score < 90 else "90-100"
        out[key] += 1
    return out


def run_universe(symbols: list[str], label: str, universe_seconds: float) -> dict[str, Any]:
    started = time.perf_counter()
    print(f"[{label}] loading {len(symbols)} histories (16 bounded workers)", file=sys.stderr, flush=True)
    results = []
    pool = ThreadPoolExecutor(max_workers=min(16, max(1, len(symbols))), thread_name_prefix="nse-history")
    try:
        futures = {pool.submit(load_incremental_history, symbol, timeout=6.0, retries=0): symbol for symbol in symbols}
        for done, future in enumerate(as_completed(futures), 1):
            try:
                results.append(future.result())
            except Exception as exc:
                print(f"[{label}] {futures[future]} failed: {type(exc).__name__}", file=sys.stderr, flush=True)
            if done == 1 or done % 20 == 0 or done == len(futures):
                print(f"[{label}] history {done}/{len(futures)}", file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        for future in futures:
            future.cancel()
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        pool.shutdown(wait=True)
    production = run_production_pipeline([(r.symbol, r.frame, r.provider) for r in results], focus_limit=50)
    candidates = production["candidates"]
    scores = [c.score for c in candidates]
    provider_failures = [r.timing_dict() for r in results if r.failure]
    cache_hits = sum(r.cache_hit for r in results)
    timings = {"instrument_loading": round(universe_seconds, 3),
        "cache_lookup": round(sum(r.cache_seconds for r in results), 3),
        "provider_fetch": round(sum(r.provider_seconds for r in results), 3),
        "history_normalization": round(sum(r.normalize_seconds for r in results), 3),
        **production["timings"], "total": round(time.perf_counter()-started, 3)}
    stages = production["diagnostics"].counts
    return {"label":label, "counts":{"master":stages["master"], "eligible":stages["eligible"],
        "active":stages["active"], "focus":stages["focus"],
        "strategy_signals":stages["strategy_signals"], "candidates":stages["candidates"], "hot":0, "positions":0},
        "cache":{"hits":cache_hits,"misses":len(results)-cache_hits,
            "hit_rate":round(cache_hits/len(results),4) if results else 0.0},
        "provider_failures":provider_failures, "per_symbol_provider_timing":[r.timing_dict() for r in results],
        "failures":production["failures"], "score_distribution":distribution(scores),
        "score_summary":{"mean":round(mean(scores),2) if scores else None,
            "median":round(median(scores),2) if scores else None}, "timings":timings}


def main() -> int:
    report: dict[str, Any] = {"engine":"headless_v2", "universes":[]}
    for label in ("nifty_50", "nifty_200"):
        try:
            symbols, seconds, universe_failure = fetch_universe(label)
            result = run_universe(symbols[:200], label, seconds)
            result["universe_provider_failure"] = universe_failure
            report["universes"].append(result)
        except Exception as exc:
            report.setdefault("universe_failures", []).append({"label":label,"reason":f"{type(exc).__name__}: {exc}"})
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["universes"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Validation interrupted cleanly", file=sys.stderr)
        raise SystemExit(130)
