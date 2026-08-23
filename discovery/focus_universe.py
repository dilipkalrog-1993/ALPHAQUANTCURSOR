"""Focus universe selection — expensive strategies run only here."""

from __future__ import annotations

from typing import Any


MODE_LIMITS = {
    "Fast Scan": "fast",
    "FAST MARKET": "fast",
    "Full Universe": "full",
    "FULL RESEARCH": "full",
    "Custom Universe": "normal",
    "NORMAL": "normal",
    "Watchlist Only": "watchlist",
}


def focus_limit_for_mode(prefs: dict[str, Any]) -> int | None:
    """Return max focus size; None means entire eligible universe."""
    mode = str(prefs.get("operating_mode") or prefs.get("active_operating_mode") or "Fast Scan")
    bucket = MODE_LIMITS.get(mode, "fast")
    if bucket == "full":
        return int(prefs.get("discovery_focus_limit_full") or 9999)
    if bucket == "normal":
        return int(prefs.get("discovery_focus_limit_normal") or 75)
    if bucket == "watchlist":
        return int(prefs.get("discovery_focus_limit_watchlist") or 50)
    return int(prefs.get("discovery_focus_limit_fast") or 40)


def build_focus_universe(
    ranked: list[dict[str, Any]],
    *,
    limit: int | None,
    mandatory: set[str],
    min_opportunity_score: float = 8.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Select focus set: mandatory + top-ranked eligible symbols."""
    by_sym = {r["symbol"]: r for r in ranked}
    focus: list[dict[str, Any]] = []
    seen: set[str] = set()

    for sym in mandatory:
        if sym in by_sym and sym not in seen:
            focus.append(by_sym[sym])
            seen.add(sym)

    for row in ranked:
        sym = row["symbol"]
        if sym in seen:
            continue
        if row["opportunity_score"] < min_opportunity_score and sym not in mandatory:
            continue
        focus.append(row)
        seen.add(sym)
        if limit is not None and len(focus) >= limit:
            break

    meta = {
        "focus_limit": limit,
        "mandatory_count": len(mandatory),
        "focus_count": len(focus),
        "ranked_count": len(ranked),
    }
    return focus, meta
