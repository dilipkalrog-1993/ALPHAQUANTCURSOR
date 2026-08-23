"""Framework-free pre-market readiness inspection.

Values which cannot be proved from persisted backend state remain UNAVAILABLE;
they are never silently coerced to zero.  No function here enables live orders.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scoring_engine_v2 import TradeScoreV2


def _json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, ValueError, TypeError):
        return {}


def _token(profile: dict[str, Any]) -> str:
    if not profile.get("access_token"):
        return "UNAVAILABLE"
    raw = profile.get("token_expiry_date") or profile.get("token_expiry")
    if not raw:
        return "PRESENT_EXPIRY_UNKNOWN"
    try:
        expiry = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return "EXPIRED" if expiry < datetime.now(timezone.utc) else "VALID"
    except ValueError:
        return "PRESENT_EXPIRY_INVALID"


def build_pre_market_report(root: Path) -> dict[str, Any]:
    data = root / "data"
    connections = _json(data / "broker_connections.json")
    profiles = connections.get("profiles") or {}
    selected = connections.get("default_market_data_broker") or next(iter(profiles), None)
    public_profile = profiles.get(selected, {}) if selected else {}
    secrets = _json(data / "broker_secrets.json").get(selected, {}) if selected else {}
    secret_profile = {**public_profile, **secrets}
    summary = public_profile
    capabilities = (secret_profile or {}).get("capabilities") or {}
    market = _json(data / "market_state.json")
    cache_dir = data / "history_cache"
    cache_count = len(list(cache_dir.glob("*.pkl"))) if cache_dir.exists() else 0
    preferences = _json(data / "workspace.json").get("preferences", {})
    limits = ("live_max_order_notional", "live_max_deployed_capital", "live_max_trades",
              "live_daily_loss_limit", "live_risk_per_trade")
    limits_missing = [key for key in limits if preferences.get(key) is None]
    connected = bool(summary.get("connected"))
    cap = lambda key: "READY" if capabilities.get(key) is True else "UNAVAILABLE"
    checks = {
        "Broker Auth": "READY" if connected else "NOT_READY",
        "Token Status": _token(secret_profile or {}),
        "Profile": selected or "UNAVAILABLE",
        "Funds": cap("funds"), "Holdings": cap("holdings"),
        "Positions": cap("positions"), "Orders": cap("orders"),
        "Quote API": cap("quote_api"),
        "WebSocket": "READY" if market.get("data_source") == "BROKER_LIVE" else "UNAVAILABLE",
        "MarketState": "READY" if market.get("quotes") else "UNAVAILABLE",
        "History Cache": f"READY ({cache_count} symbols)" if cache_count else "UNAVAILABLE",
        "Scoring V2": "READY" if TradeScoreV2().score_version == "V2" else "NOT_READY",
        "Risk": "READY" if (root / "os_brains" / "risk_manager.py").exists() else "NOT_READY",
        "Persistence": "READY" if data.exists() and data.is_dir() else "NOT_READY",
        "Live Limits": "CONFIGURED" if not limits_missing else "NOT_READY",
        "News": "AVAILABLE" if (data / "news_state.json").exists() else "UNAVAILABLE",
    }
    critical = ("Broker Auth", "Token Status", "Profile", "Funds", "Quote API", "WebSocket",
                "MarketState", "History Cache", "Scoring V2", "Risk", "Persistence", "Live Limits")
    checks["Overall Readiness"] = "READY" if all(checks[k] in {"READY", "VALID", "CONFIGURED"} or checks[k].startswith("READY (") for k in critical) else "NOT_READY"
    return {"checks": checks, "details": {"broker_status": summary.get("status", "UNAVAILABLE"),
            "missing_live_limits": limits_missing, "live_orders": "LOCKED", "cache_symbols": cache_count}}
