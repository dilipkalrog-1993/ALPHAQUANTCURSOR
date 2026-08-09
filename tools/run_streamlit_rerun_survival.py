#!/usr/bin/env python3
"""Streamlit rerun survival — verify singleton workers persist across 50 init cycles."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    import appemergentquant_v3_1 as aq
    from market.upstox_v3_feed import UpstoxV3FeedManager

    aq.st.session_state.clear()
    ids_before = {
        "runtime": id(aq.get_core_runtime()),
        "broker_state": id(aq.get_broker_state()),
        "quote_worker": id(aq.get_broker_quote_worker()),
        "feed_manager": id(UpstoxV3FeedManager.instance()),
        "connection_manager": id(aq.get_broker_connection_manager()),
    }
    worker_id_before = UpstoxV3FeedManager.instance().worker_id

    for i in range(50):
        _ = aq.get_core_runtime()
        _ = aq.get_broker_state()
        _ = aq.get_broker_quote_worker()
        _ = UpstoxV3FeedManager.instance()
        _ = aq.get_broker_connection_manager()

    ids_after = {
        "runtime": id(aq.get_core_runtime()),
        "broker_state": id(aq.get_broker_state()),
        "quote_worker": id(aq.get_broker_quote_worker()),
        "feed_manager": id(UpstoxV3FeedManager.instance()),
        "connection_manager": id(aq.get_broker_connection_manager()),
    }
    worker_id_after = UpstoxV3FeedManager.instance().worker_id

    passed = ids_before == ids_after and worker_id_before == worker_id_after
    report = {
        "name": "streamlit_rerun_survival",
        "passed": passed,
        "iterations": 50,
        "ids_before": ids_before,
        "ids_after": ids_after,
        "worker_id": worker_id_after,
    }
    print(json.dumps(report, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
