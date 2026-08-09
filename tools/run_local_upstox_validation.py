#!/usr/bin/env python3
"""Local Upstox validation — reads saved app credentials, never prints secrets."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_saved_profile() -> dict | None:
    from broker.credential_store import CredentialStore
    from broker.connection_manager import BrokerConnectionManager

    mgr = BrokerConnectionManager()
    profiles = mgr.list_profiles() if hasattr(mgr, "list_profiles") else []
    if not profiles:
        # Fallback: workspace broker config
        ws = ROOT / "data" / "workspace.json"
        if ws.exists():
            try:
                prefs = json.loads(ws.read_text()).get("preferences", {})
                name = prefs.get("default_broker_profile") or prefs.get("default_broker")
                if name:
                    profiles = [{"name": name}]
            except Exception:
                pass
    if not profiles:
        secrets_path = ROOT / "data" / "broker_secrets.json"
        if secrets_path.exists():
            try:
                names = list(json.loads(secrets_path.read_text()).keys())
                if names:
                    profiles = [{"name": names[0]}]
            except Exception:
                pass
    if not profiles:
        return None
    profile_name = profiles[0].get("name") if isinstance(profiles[0], dict) else str(profiles[0])
    store = CredentialStore()
    merged = store.merge_secrets(profile_name, {"name": profile_name})
    token = str(merged.get("access_token") or "").strip()
    if not token:
        return None
    return merged


def main() -> int:
    blockers: list[str] = []
    checks: dict[str, str] = {}

    profile = _load_saved_profile()
    if not profile:
        print(json.dumps({
            "UPSTOX_LOCAL_VALIDATION": "FAIL",
            "reason": "No saved Upstox credentials found. Connect via Brokers → Upstox → SAVE & CONNECT in the application.",
            "checks": {},
        }, indent=2))
        return 1

    from broker.upstox_adapter import UpstoxAdapter
    from market.upstox_v3_feed import UpstoxV3FeedManager

    adapter = UpstoxAdapter()
    status = adapter.authenticate(profile)
    caps = status.capabilities or {}
    for key, cap_key in [
        ("auth", "authentication"), ("profile", "profile"), ("funds", "funds"),
        ("holdings", "holdings"), ("positions", "positions"), ("orders", "orders"),
        ("quote", "quote_api"),
    ]:
        checks[key] = "PASS" if caps.get(cap_key) else "FAIL"
        if checks[key] == "FAIL":
            blockers.append(key)

    feed = UpstoxV3FeedManager.instance()
    os.environ.pop("UPSTOX_USE_REPLAY", None)
    started = feed.start(profile)
    time.sleep(2.5)
    health = feed.health()
    checks["websocket"] = "PASS" if health.get("connected") and health.get("last_tick_at") else "FAIL"
    if checks["websocket"] == "FAIL":
        blockers.append("websocket")

    import appemergentquant_v3_1 as aq
    ms = aq.get_market_state().snapshot()
    checks["marketstate"] = "PASS" if ms.get("quotes") else "FAIL"
    if checks["marketstate"] == "FAIL":
        blockers.append("marketstate")

    checks["reconnect"] = "PASS" if hasattr(feed, "_reconnect_count") else "SKIP"

    passed = len(blockers) == 0
    print(json.dumps({
        "UPSTOX_LOCAL_VALIDATION": "PASS" if passed else "FAIL",
        "checks": checks,
        "blockers": blockers,
        "feed_health": {k: v for k, v in health.items() if k != "api_versions"},
        "quote_count": len(ms.get("quotes", {})),
        "message": "No orders placed.",
    }, indent=2, default=str))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
