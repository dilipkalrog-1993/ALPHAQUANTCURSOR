"""Fast HOT-universe entry monitor using canonical MarketState quotes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable


class HotEntryMonitor:
    """Evaluate entry readiness from MarketState without full rescan."""

    def __init__(self, market_state, quote_lookup: Callable[[str], dict[str, Any] | None] | None = None):
        self.market_state = market_state
        self.quote_lookup = quote_lookup or market_state.get_quote

    def evaluate(self, trade) -> tuple[bool, str, dict[str, Any]]:
        symbol = getattr(trade, "symbol", "")
        quote = self.quote_lookup(symbol) or self.quote_lookup(symbol.replace(".NS", ""))
        detail = {"symbol": symbol, "evaluated_at": datetime.now(timezone.utc).isoformat()}
        if not quote:
            return False, "Waiting for canonical quote", {**detail, "entry_status": "WAITING_CONFIRMATION"}
        if quote.get("stale") or quote.get("freshness_label") == "STALE":
            return False, "Stale quote", {**detail, "entry_status": "BLOCKED_STALE_DATA"}
        price = quote.get("ltp")
        if price is None:
            return False, "Quote unavailable", {**detail, "entry_status": "WAITING_PRICE"}
        entry = float(getattr(trade, "entry", 0) or 0)
        if entry and float(price) < entry:
            return False, "Entry price not reached", {**detail, "entry_status": "WAITING_PRICE", "ltp": price}
        detail.update({"ltp": price, "freshness_ms": quote.get("freshness_ms"), "source": quote.get("source")})
        return True, "Entry trigger confirmed", {**detail, "entry_status": "READY_TO_ENTER"}
