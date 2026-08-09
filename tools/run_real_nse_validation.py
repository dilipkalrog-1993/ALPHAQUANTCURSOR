#!/usr/bin/env python3
"""Real NSE validation using only the headless AlphaQuant domain layer."""
from __future__ import annotations

import json, pickle, sys, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import mean, median
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

from core.headless_pipeline import StageAudit, compute_candidate, prepare_indicators

CACHE_DIR = ROOT / "data" / "history_cache"
SOURCES = {
    "nifty_50": "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "nifty_200": "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv",
}
SEED50 = "ADANIENT ADANIPORTS APOLLOHOSP ASIANPAINT AXISBANK BAJAJ-AUTO BAJAJFINSV BAJFINANCE BEL BHARTIARTL BPCL BRITANNIA CIPLA COALINDIA DIVISLAB DRREDDY EICHERMOT GRASIM HCLTECH HDFCBANK HDFCLIFE HEROMOTOCO HINDALCO HINDUNILVR ICICIBANK INDUSINDBK INFY ITC JSWSTEEL KOTAKBANK LT LTIM M&M MARUTI NESTLEIND NTPC ONGC POWERGRID RELIANCE SBILIFE SBIN SHRIRAMFIN SUNPHARMA TATACONSUM TATAMOTORS TATASTEEL TCS TECHM TITAN TRENT".split()


def fetch_universe(label: str) -> list[str]:
    import pandas as pd
    try:
        frame = pd.read_csv(SOURCES[label])
        col = next(c for c in frame.columns if str(c).strip().lower() == "symbol")
        return sorted({f"{str(x).strip()}.NS" for x in frame[col] if str(x).strip()})
    except Exception:
        if label == "nifty_50": return [f"{x}.NS" for x in SEED50]
        raise


def fetch_history(symbol: str) -> tuple[str, Any, str]:
    import yfinance as yf
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{symbol.replace('.NS', '')}.pkl"
    if path.exists():
        try: return symbol, pickle.loads(path.read_bytes()), "cache/yfinance"
        except Exception: pass
    raw = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=True, threads=False, timeout=20)
    if raw is not None and not raw.empty: path.write_bytes(pickle.dumps(raw))
    return symbol, raw, "yfinance"


def distribution(scores: list[float]) -> dict[str, int]:
    out = {"0-39":0,"40-49":0,"50-59":0,"60-69":0,"70-79":0,"80-89":0,"90-100":0}
    for score in scores:
        key = "0-39" if score < 40 else "40-49" if score < 50 else "50-59" if score < 60 else "60-69" if score < 70 else "70-79" if score < 80 else "80-89" if score < 90 else "90-100"
        out[key] += 1
    return out


def run_universe(symbols: list[str], label: str) -> dict[str, Any]:
    started = time.perf_counter(); fetch_at = started
    with ThreadPoolExecutor(max_workers=min(8, len(symbols))) as pool:
        fetched = list(pool.map(fetch_history, symbols))
    fetch_seconds = time.perf_counter() - fetch_at
    prepared, failures = {}, []
    prep_at = time.perf_counter()
    for symbol, raw, provider in fetched:
        df, failure = prepare_indicators(symbol, raw, provider)
        if failure: failures.append(failure.to_dict())
        else: prepared[symbol] = df
    prep_seconds = time.perf_counter() - prep_at
    eligibility_rejections: dict[str,int] = {}
    eligible = {}
    for symbol, df in prepared.items():
        reason = None
        if float(df.Close.iloc[-1]) < 20: reason = "MINIMUM_PRICE"
        elif float(df.AVG_VOLUME20.iloc[-1]) < 100_000: reason = "MINIMUM_AVERAGE_VOLUME"
        if reason: eligibility_rejections[reason] = eligibility_rejections.get(reason,0)+1
        else: eligible[symbol] = df
    eligibility = StageAudit(len(prepared), len(eligible), eligibility_rejections)
    ranked = sorted(eligible.items(), key=lambda x: float(x[1].AVG_VOLUME20.iloc[-1]) * float(x[1].Close.iloc[-1]), reverse=True)
    focus = ranked[:28]
    focus_audit = StageAudit(len(eligible), len(focus), {"FOCUS_LIMIT": max(0,len(eligible)-len(focus))})
    candidates = [c for symbol, df in focus if (c := compute_candidate(symbol, df)) is not None]
    strategy_audit = StageAudit(len(focus), len(candidates), {"NO_STRATEGY_SIGNAL": len(focus)-len(candidates)})
    scores = [c.score for c in candidates]
    return {
        "label": label, "symbols_requested": len(symbols), "symbols_with_usable_data": len(prepared),
        "eligible": len(eligible), "focus": len(focus), "strategy_evaluated": len(focus),
        "strategy_signals": len(candidates), "candidates": len(candidates),
        "counter_definitions": {"strategy_evaluated":"focus symbols on which enabled strategies ran", "strategy_signals":"symbols that triggered at least one strategy", "candidates":"computed, materialized candidates; exactly one per signalled symbol"},
        "failures": failures,
        "stage_audits": {"history_to_indicators": StageAudit(len(symbols),len(prepared), _reason_counts(failures)).to_dict(), "eligibility":eligibility.to_dict(), "focus":focus_audit.to_dict(), "strategy":strategy_audit.to_dict()},
        "score_distribution": distribution(scores),
        "score_summary": {"mean":round(mean(scores),2) if scores else None,"median":round(median(scores),2) if scores else None,"highest":max(scores) if scores else None,"lowest":min(scores) if scores else None},
        "timings": {"history":round(fetch_seconds,3),"indicator_preparation":round(prep_seconds,3),"total":round(time.perf_counter()-started,3)},
    }


def _reason_counts(failures: list[dict[str,Any]]) -> dict[str,int]:
    result: dict[str,int] = {}
    for failure in failures: result[failure["reason"]] = result.get(failure["reason"],0)+1
    return result


def main() -> int:
    report = {"phase":"production_hardening", "engine":"headless_v2", "universes":[]}
    for label in ("nifty_50", "nifty_200"):
        try: report["universes"].append(run_universe(fetch_universe(label)[:200], label))
        except Exception as exc: report.setdefault("universe_failures",[]).append({"label":label,"reason":"PROVIDER_FAILURE","exception_type":type(exc).__name__,"exception_message":str(exc)})
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["universes"] and all(u["stage_audits"]["strategy"]["invariant_ok"] for u in report["universes"]) else 1


if __name__ == "__main__": raise SystemExit(main())
