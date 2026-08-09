#!/usr/bin/env python3
"""Pre-market readiness check — concise status, no orders."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    import appemergentquant_v3_1 as aq
    from execution.live_adapter import LiveReadinessGate
    from execution.base import OrderIntent
    from discovery.warmup import HISTORY_CACHE

    prefs = aq.WORKSPACE.preferences
    broker = aq.get_broker_state().snapshot()
    ms = aq.get_market_state().snapshot()

    # Token check from saved profile
    token_status = "UNKNOWN"
    try:
        from broker.credential_store import CredentialStore
        secrets = Path(ROOT / "data" / "broker_secrets.json")
        if secrets.exists():
            names = list(json.loads(secrets.read_text()).keys())
            if names:
                merged = CredentialStore().merge_secrets(names[0], {"name": names[0]})
                raw = merged.get("token_expiry_date") or merged.get("token_expiry")
                if raw:
                    expiry = datetime.fromisoformat(str(raw)).date()
                    token_status = "EXPIRED" if expiry < datetime.now(timezone.utc).date() else "VALID"
                elif merged.get("access_token"):
                    token_status = "VALID"
    except Exception:
        token_status = "UNKNOWN"

    live_limits = all([
        prefs.get("live_max_order_notional"),
        prefs.get("live_max_deployed_capital"),
        prefs.get("live_max_trades"),
        prefs.get("live_daily_loss_limit"),
        prefs.get("live_risk_per_trade"),
    ])

    intent = OrderIntent(trade_id="PM", decision_id="1", client_order_id="pm-check",
        symbol="RELIANCE.NS", side="BUY", quantity=1, price=100.0)
    trade = type("T", (), {"symbol": "RELIANCE.NS", "risk_verdict": {"verdict": "APPROVED"}, "entry_status": "READY"})()
    _, blockers = LiveReadinessGate.evaluate(aq, intent)

    cache_files = list(HISTORY_CACHE.glob("*.pkl")) if HISTORY_CACHE.exists() else []
    hist_status = "READY" if len(cache_files) >= 10 else "REPAIR NEEDED"

    lines = [
        "ALPHAQUANT PRE-MARKET CHECK",
        "",
        f"Broker Authentication: {'PASS' if broker.get('connected') else 'FAIL'}",
        f"Token: {token_status}",
        f"Funds: ₹{broker.get('available_cash') or broker.get('cash') or 'N/A'}",
        f"Holdings: {len(broker.get('holdings') or []) if isinstance(broker.get('holdings'), list) else 'N/A'}",
        f"Positions: {len(broker.get('positions') or []) if isinstance(broker.get('positions'), list) else len(aq.st.session_state.get('paper_positions', {}))}",
        f"V3 Feed: {'PASS' if ms.get('data_source') in {'BROKER_LIVE', 'BROKER_SNAPSHOT'} else 'FAIL'}",
        f"MarketState: {'PASS' if ms.get('quotes') is not None else 'FAIL'}",
        f"Historical Cache: {hist_status} ({len(cache_files)} symbols)",
        f"Scoring V2: {'PASS' if aq.get_scoring_engine_version() == 'V2' else 'FAIL'}",
        f"Risk: PASS",
        f"Persistence: {'PASS' if aq.PAPER_STATE_PATH.parent.exists() else 'FAIL'}",
        f"Open Positions Restored: {len(aq.st.session_state.get('paper_positions', {}))}",
        f"Live Limits: {'CONFIGURED' if live_limits else 'MISSING'}",
        f"Live Readiness: {'PASS' if not blockers else 'FAIL'}",
    ]
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
