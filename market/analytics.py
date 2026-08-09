"""Market breadth, sector, and volatility analytics from canonical MarketState."""

from __future__ import annotations

from typing import Any, Callable

from market.market_state import get_market_state

MIN_BREADTH_COVERAGE = 0.35


def compute_breadth(
    active_symbols: list[str],
    sector_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    state = get_market_state()
    quotes = state.snapshot().get("quotes", {})
    counts = {"advancing": 0, "declining": 0, "unchanged": 0, "unavailable": 0}
    tolerance = 1e-8
    for symbol in active_symbols:
        bare = symbol.replace(".NS", "").upper()
        quote = quotes.get(symbol) or quotes.get(f"{bare}.NS") or quotes.get(bare)
        if not quote:
            counts["unavailable"] += 1
            continue
        ltp = quote.get("ltp")
        previous = quote.get("previous_close")
        if quote.get("stale") or ltp is None or previous is None:
            counts["unavailable"] += 1
        elif abs(float(ltp) - float(previous)) <= tolerance * max(1.0, abs(float(previous))):
            counts["unchanged"] += 1
        elif float(ltp) > float(previous):
            counts["advancing"] += 1
        else:
            counts["declining"] += 1
    valid = counts["advancing"] + counts["declining"] + counts["unchanged"]
    total = len(active_symbols)
    coverage = valid / total if total else 0.0
    ratio = counts["advancing"] / max(1, counts["declining"])
    result = {
        **counts,
        "total": total,
        "valid": valid,
        "coverage_pct": round(coverage * 100, 1),
        "advance_decline_ratio": round(ratio, 2),
    }
    if coverage < MIN_BREADTH_COVERAGE:
        result["classification"] = "INSUFFICIENT DATA"
    elif ratio >= 1.5:
        result["classification"] = "STRONG BREADTH"
    elif ratio <= 0.67:
        result["classification"] = "WEAK BREADTH"
    else:
        result["classification"] = "BALANCED"
    state.set_analytics(breadth=result)
    return result


def compute_sector_state(
    active_symbols: list[str],
    sector_map: dict[str, str],
) -> dict[str, Any]:
    state = get_market_state()
    quotes = state.snapshot().get("quotes", {})
    buckets: dict[str, list[dict[str, Any]]] = {}
    for symbol in active_symbols:
        bare = symbol.replace(".NS", "").upper()
        sector = sector_map.get(bare, "UNKNOWN")
        quote = quotes.get(symbol) or quotes.get(f"{bare}.NS")
        if not quote or quote.get("ltp") is None:
            continue
        change_pct = quote.get("change_percent")
        if change_pct is None:
            continue
        buckets.setdefault(sector, []).append({"symbol": bare, "change_pct": float(change_pct)})

    sectors_out = []
    for sector, rows in buckets.items():
        if not rows:
            continue
        adv = sum(1 for r in rows if r["change_pct"] > 0.05)
        dec = sum(1 for r in rows if r["change_pct"] < -0.05)
        unch = len(rows) - adv - dec
        avg = sum(r["change_pct"] for r in rows) / len(rows)
        sorted_rows = sorted(rows, key=lambda r: r["change_pct"], reverse=True)
        sectors_out.append({
            "sector": sector,
            "average_change_pct": round(avg, 2),
            "advancing": adv,
            "declining": dec,
            "unchanged": unch,
            "valid_symbols": len(rows),
            "strongest": sorted_rows[0]["symbol"] if sorted_rows else "N/A",
            "weakest": sorted_rows[-1]["symbol"] if sorted_rows else "N/A",
            "relative_strength": "STRONG" if avg > 0.25 else "WEAK" if avg < -0.25 else "MIXED",
        })
    payload = {"sectors": sectors_out, "sector_count": len(sectors_out)}
    state.set_analytics(sectors=payload)
    return payload


def compute_volatility_state(vix_quote: dict[str, Any] | None = None, atr_pct: float | None = None) -> dict[str, Any]:
    state = get_market_state()
    vix_level = None
    if vix_quote and vix_quote.get("ltp") is not None:
        vix_level = float(vix_quote["ltp"])
    classification = "NORMAL"
    if vix_level is not None:
        if vix_level >= 25:
            classification = "EXTREME"
        elif vix_level >= 18:
            classification = "ELEVATED"
    if atr_pct is not None:
        if atr_pct >= 8:
            classification = "EXTREME"
        elif atr_pct >= 5 and classification == "NORMAL":
            classification = "ELEVATED"
        elif atr_pct <= 2 and vix_level is not None and vix_level < 14:
            classification = "EXPANSION_AFTER_COMPRESSION"
    payload = {
        "classification": classification,
        "india_vix": vix_level if vix_level is not None else "N/A",
        "atr_pct": atr_pct,
    }
    state.set_analytics(volatility=payload)
    return payload


def refresh_market_analytics(
    active_symbols: list[str],
    sector_map: dict[str, str],
) -> dict[str, Any]:
    breadth = compute_breadth(active_symbols, sector_map)
    sectors = compute_sector_state(active_symbols, sector_map)
    vix = get_market_state().get_quote("INDIA VIX")
    volatility = compute_volatility_state(vix)
    regime = _regime_from_analytics(breadth, volatility)
    get_market_state().set_analytics(regime=regime)
    return {"breadth": breadth, "sectors": sectors, "volatility": volatility, "regime": regime}


def _regime_from_analytics(breadth: dict[str, Any], volatility: dict[str, Any]) -> dict[str, Any]:
    if breadth.get("classification") == "INSUFFICIENT DATA":
        return {"classification": "INSUFFICIENT_DATA", "state": "INSUFFICIENT_DATA"}
    if volatility.get("classification") == "EXTREME":
        return {"classification": "VOLATILE", "state": "VOLATILE"}
    if breadth.get("classification") == "STRONG BREADTH":
        return {"classification": "STRONG_BREADTH", "state": "BULLISH"}
    if breadth.get("classification") == "WEAK BREADTH":
        return {"classification": "WEAK_BREADTH", "state": "BEARISH"}
    return {"classification": "RANGE_BOUND", "state": "RANGE_BOUND"}


def record_entry_evaluation_latency(start_perf: float) -> float:
    ms = ( __import__("time").perf_counter() - start_perf) * 1000
    tracker = get_market_state().values.setdefault("entry_eval_latency_ms", [])
    if isinstance(tracker, list):
        tracker.append(ms)
        if len(tracker) > 5000:
            del tracker[:-5000]
    return ms
