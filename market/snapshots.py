"""Clean read APIs for UI and runtime consumers."""

from __future__ import annotations

from typing import Any

from market.market_state import get_market_state


def get_market_snapshot(active_symbols: list[str] | None = None) -> dict[str, Any]:
    state = get_market_state().snapshot()
    quotes = state.get("quotes", {})
    if active_symbols:
        wanted = {s.replace(".NS", "") for s in active_symbols}
        quotes = {k: v for k, v in quotes.items() if k.replace(".NS", "") in wanted or k in active_symbols}
    return {
        "data_source": state.get("data_source"),
        "freshness_label": _source_label(state.get("data_source")),
        "last_quote_time": state.get("last_quote_time"),
        "market_regime": state.get("market_regime"),
        "volatility_regime": state.get("volatility_regime"),
        "breadth": state.get("breadth", {}),
        "regime": state.get("regime", {}),
        "sectors": state.get("sectors", {}),
        "volatility": state.get("volatility", {}),
        "quotes": quotes,
        "quote_count": len(quotes),
    }


def get_broker_summary(connection_manager=None) -> dict[str, Any]:
    if connection_manager is None:
        from broker.connection_manager import BrokerConnectionManager
        connection_manager = BrokerConnectionManager()
    return {
        "connections": connection_manager.connection_summary(),
        "default_market_data_broker": connection_manager._connections.get("default_market_data_broker"),
        "default_execution_broker": connection_manager._connections.get("default_execution_broker"),
    }


def get_portfolio_snapshot(app_module: Any) -> dict[str, Any]:
    positions = getattr(getattr(app_module, "st", None), "session_state", {}).get("paper_positions", {})
    rows = []
    for symbol, pos in (positions or {}).items():
        rows.append({
            "symbol": symbol,
            "qty": getattr(pos, "qty", 0),
            "entry": getattr(pos, "entry", 0),
            "current_price": getattr(pos, "current_price", 0),
            "unrealized_pnl": getattr(pos, "unrealized_pnl", 0),
            "score_version": getattr(pos, "score_version", "N/A"),
            "trade_confidence": getattr(pos, "trade_confidence", "N/A"),
        })
    broker = getattr(getattr(app_module, "st", None), "session_state", {}).get("paper_broker", {})
    return {
        "open_positions": rows,
        "cash": broker.get("cash"),
        "realized_pnl": broker.get("realized_pnl"),
        "position_count": len(rows),
    }


def get_opportunity_snapshot(app_module: Any) -> dict[str, Any]:
    final = getattr(getattr(app_module, "st", None), "session_state", {}).get("final_trade_list", [])
    rows = []
    for trade in final or []:
        v2 = getattr(trade, "trade_score_v2", None)
        rows.append({
            "symbol": getattr(trade, "symbol", ""),
            "strategy": getattr(trade, "strategy", ""),
            "score_version": getattr(trade, "score_version", "V2"),
            "trade_confidence": getattr(trade, "ai_score", 0),
            "structure": getattr(v2.structure, "weighted_contribution", None) if v2 else None,
            "participation": getattr(v2.participation, "weighted_contribution", None) if v2 else None,
            "momentum": getattr(v2.momentum, "weighted_contribution", None) if v2 else None,
            "market_sector": getattr(v2.market_sector, "weighted_contribution", None) if v2 else None,
            "historical": getattr(v2.historical, "weighted_contribution", None) if v2 else None,
            "news": getattr(v2.news, "weighted_contribution", None) if v2 else None,
            "positive_reasons": getattr(trade, "positive_reasons_v2", []),
            "watch_items": getattr(trade, "watch_items_v2", []),
            "risk_status": (getattr(trade, "risk_verdict", {}) or {}).get("verdict"),
            "entry_status": getattr(trade, "entry_status", ""),
        })
    return {"candidates": rows, "count": len(rows)}


def get_report_snapshot(app_module: Any) -> dict[str, Any]:
    closed = getattr(getattr(app_module, "st", None), "session_state", {}).get("paper_history", [])
    return {
        "closed_trades": len(closed or []),
        "candidate_archive": len(getattr(getattr(app_module, "st", None), "session_state", {}).get("candidate_archive", [])),
        "decision_funnel": getattr(getattr(app_module, "st", None), "session_state", {}).get("decision_funnel", []),
    }


def _source_label(source: str | None) -> str:
    mapping = {
        "BROKER_LIVE": "LIVE",
        "BROKER_SNAPSHOT": "NEAR LIVE",
        "YFINANCE_INTRADAY_FALLBACK": "DELAYED",
        "HISTORICAL_CACHE": "STALE",
    }
    return mapping.get(source or "", "OFFLINE")
