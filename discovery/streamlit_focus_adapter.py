"""Application-state adapter for selecting mandatory discovery symbols."""
from __future__ import annotations

from typing import Any


def mandatory_symbols(session_state: Any, prefs: dict[str, Any]) -> set[str]:
    """Return UI-selected symbols that must receive strategy evaluation."""
    required: set[str] = set()
    for sym in session_state.get("watchlist", []) or []:
        bare = str(sym).upper().replace(".NS", "")
        required.add(f"{bare}.NS" if not bare.endswith(".NS") else bare)
    wl_name = prefs.get("selected_watchlist") or prefs.get("default_watchlist") or "Default"
    for sym in (prefs.get("watchlists") or {}).get(wl_name, []):
        bare = str(sym).upper().replace(".NS", "")
        required.add(f"{bare}.NS" if not bare.endswith(".NS") else bare)
    for sym in (session_state.get("waiting_entry") or {}):
        required.add(str(sym).upper() if str(sym).endswith(".NS") else f"{sym}.NS")
    for sym in (session_state.get("paper_positions") or {}):
        required.add(str(sym).upper() if str(sym).endswith(".NS") else f"{sym}.NS")
    for trade in (session_state.get("trade_candidates") or {}).values():
        symbol = getattr(trade, "symbol", None)
        if symbol:
            required.add(str(symbol).upper() if str(symbol).endswith(".NS") else f"{symbol}.NS")
    for sym in session_state.get("hot_universe", []) or []:
        required.add(str(sym).upper() if str(sym).endswith(".NS") else f"{sym}.NS")
    return required


__all__ = ["mandatory_symbols"]
