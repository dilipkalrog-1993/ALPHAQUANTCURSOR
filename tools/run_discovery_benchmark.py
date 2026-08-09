#!/usr/bin/env python3
"""Discovery pipeline benchmark — old eligibility-only vs new focus pipeline."""

from __future__ import annotations

import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CACHE_DIR = ROOT / "data" / "history_cache"


def _bootstrap(aq) -> None:
    ss = aq.st.session_state
    registry = list(ss.get("strategy_registry", []))
    ss.clear()
    ss["strategy_registry"] = registry
    ss["paper_capital"] = 500_000.0
    ss["paper_broker"] = {"connected": False, "cash": 500_000.0, "starting_capital": 500_000.0,
        "positions": {}, "orders": {}, "trade_history": [], "realized_pnl": 0.0, "risk": {}}
    ss["paper_positions"] = {}
    ss["trade_candidates"] = {}
    ss["final_trade_list"] = []
    ss["market_data"] = {}
    ss["stock_objects"] = {}
    ss["waiting_entry"] = {}
    ss["indicator_frame_cache"] = {}
    aq.WORKSPACE.preferences.update({
        "scoring_engine_version": "V2", "execution_mode": "PAPER",
        "operating_mode": "Fast Scan", "minimum_fast_ai_score": 70,
        "discovery_focus_limit_fast": 28,
    })


def _load_universe(aq, symbols: list[str]) -> tuple[dict[str, Any], float]:
    t0 = time.perf_counter()
    data = {}
    for sym in symbols:
        p = CACHE_DIR / f"{sym.replace('.NS', '')}.pkl"
        if not p.exists():
            continue
        raw = pickle.loads(p.read_bytes())
        if hasattr(raw.columns, "nlevels") and raw.columns.nlevels > 1:
            raw.columns = raw.columns.get_level_values(0)
        df = aq.calculate_indicators(raw)
        if df is not None:
            data[sym] = df
    return data, time.perf_counter() - t0


def _old_path(aq, market_data: dict[str, Any]) -> dict[str, Any]:
    min_price = aq.CONFIG.get("MIN_PRICE", 20)
    min_vol = aq.CONFIG.get("MIN_AVG_VOLUME", 100000)
    t0 = time.perf_counter()
    survivors = 0
    signals = 0
    for sym, df in market_data.items():
        if len(df) < 50:
            continue
        if float(df.iloc[-1]["Close"]) < min_price:
            continue
        if float(df["Volume"].tail(20).mean()) < min_vol:
            continue
        survivors += 1
        stock = aq.get_stock(sym)
        stock.set_dataframe(df)
        aq.calculate_trade_quality(stock)
        aq.update_market_structure(stock)
        aq.assign_sector(stock)
        before = set(aq.st.session_state.trade_candidates)
        aq.run_all_strategies(stock)
        after = set(aq.st.session_state.trade_candidates)
        if after - before:
            aq.run_batch1_signal_engines(stock)
            aq.run_batch2_signal_engines(stock)
            signals += 1
    with patch.object(aq, "is_market_open", return_value=True):
        final = aq.build_ai_consensus()
    return {
        "survivors": survivors,
        "strategy_evaluated": survivors,
        "signals": signals,
        "candidates": len(final or []),
        "total": round(time.perf_counter() - t0, 3),
    }


def _new_path(aq, market_data: dict[str, Any]) -> dict[str, Any]:
    from discovery.pipeline import DiscoveryPipeline

    t0 = time.perf_counter()
    aq.st.session_state.market_data = market_data
    try:
        aq.fetch_nifty_benchmark()
    except Exception:
        pass
    disc = DiscoveryPipeline(aq).run(market_data)
    t1 = time.perf_counter()
    with patch.object(aq, "is_market_open", return_value=True):
        final = aq.build_ai_consensus()
    timings = disc.timings.as_dict()
    timings["scoring_v2"] = round(time.perf_counter() - t1, 3)
    timings["total"] = round(time.perf_counter() - t0, 3)
    return {
        "eligible": disc.eligible_count,
        "focus": disc.focus_count,
        "strategy_evaluated": disc.strategy_evaluated,
        "signals": disc.strategy_signals,
        "candidates": len(final or []),
        "eligibility_audit": disc.eligibility_audit.to_dict(),
        "timings": timings,
        "total": timings["total"],
    }


def _nifty50(aq) -> list[str]:
    try:
        syms = aq.fetch_index_constituents(aq.NSE_INDEX_SOURCES["Nifty50"])
        return sorted({f"{s}.NS" if not str(s).endswith('.NS') else s for s in syms})[:50]
    except Exception:
        return [f"{s}.NS" for s in aq._NIFTY50_SEED]


def _nifty200(aq) -> list[str]:
    try:
        syms = aq.fetch_index_constituents(aq.NSE_INDEX_SOURCES["Nifty200"])
        return sorted({f"{s}.NS" if not str(s).endswith('.NS') else s for s in syms})[:200]
    except Exception:
        return []


def main() -> int:
    import appemergentquant_v3_1 as aq

    report: dict[str, Any] = {"universes": {}}
    for label, symbols in [("nifty_50", _nifty50(aq)), ("nifty_200", _nifty200(aq))]:
        if not symbols:
            continue
        _bootstrap(aq)
        data, cache_t = _load_universe(aq, symbols)
        _bootstrap(aq)
        old = _old_path(aq, data)
        _bootstrap(aq)
        new = _new_path(aq, data)
        report["universes"][label] = {
            "input": len(symbols),
            "cache_load_seconds": round(cache_t, 3),
            "old": old,
            "new": new,
        }

    # HOT entry path latency from entry monitor
    from market.entry_monitor import HotEntryMonitor
    from market.market_state import get_market_state
    latencies = []
    for _ in range(20):
        t0 = time.perf_counter()
        HotEntryMonitor(get_market_state()).evaluate(
            type("T", (), {"symbol": "RELIANCE.NS", "entry": 100, "entry_status": "READY"})()
        )
        latencies.append((time.perf_counter() - t0) * 1000)
    latencies.sort()
    report["hot_entry_path_ms"] = {
        "p50": round(latencies[len(latencies) // 2], 3),
        "p95": round(latencies[int(len(latencies) * 0.95) - 1], 3),
    }

    n50 = report["universes"].get("nifty_50", {}).get("new", {}).get("total", 999)
    n200 = report["universes"].get("nifty_200", {}).get("new", {}).get("total", 999)
    report["performance_target"] = n50 <= 5.0 and n200 <= 15.0

    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
