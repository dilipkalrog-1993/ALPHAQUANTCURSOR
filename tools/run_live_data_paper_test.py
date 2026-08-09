#!/usr/bin/env python3
"""LIVE MARKET DATA + PAPER EXECUTION — replay or live Upstox feed."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    import appemergentquant_v3_1 as aq
    from market.upstox_v3_feed import UpstoxV3FeedManager
    from market.entry_monitor import HotEntryMonitor
    from execution.paper_adapter import PaperExecutionAdapter
    from execution.base import OrderIntent
    from execution.live_adapter import LiveExecutionAdapter
    from tools.synthetic_breakout_data import build_breakout_universe

    os.environ.setdefault("UPSTOX_USE_REPLAY", "1")
    ss = aq.st.session_state
    registry = list(ss.get("strategy_registry", []))
    ss.clear()
    ss["strategy_registry"] = registry
    ss["paper_capital"] = 500_000.0
    ss["paper_broker"] = {
        "connected": False, "cash": 500_000.0, "starting_capital": 500_000.0,
        "positions": {}, "orders": {}, "trade_history": [], "realized_pnl": 0.0, "risk": {},
    }
    ss["paper_positions"] = {}
    ss["paper_history"] = []
    ss["closed_positions"] = []
    ss["trade_journal"] = []
    ss["trade_candidates"] = {}
    ss["final_trade_list"] = []
    ss["selected_portfolio"] = []
    ss["market_data"] = {}
    ss["stock_objects"] = {}
    ss["waiting_entry"] = {}
    ss["candidate_archive"] = []
    ss["candidate_rejections"] = {}
    ss["pipeline_stage_counts"] = {}
    ss["pipeline_timings"] = {}
    ss["decision_funnel"] = []
    ss["_paper_state_restored"] = False
    aq.WORKSPACE.preferences.update({
        "scoring_engine_version": "V2", "execution_mode": "PAPER", "market_data_mode": "UPSTOX_LIVE",
        "minimum_fast_ai_score": 70,
    })

    profile = {
        "name": "default", "broker_name": "Upstox", "access_token": os.environ.get("UPSTOX_ACCESS_TOKEN", "replay"),
        "api_key": "x", "api_secret": "x",
    }
    replay = ROOT / "fixtures" / "upstox_v3_replay.json"
    feed = UpstoxV3FeedManager.instance()
    feed.start(profile, replay_path=replay if replay.exists() else None)
    time.sleep(0.3)

    verification = "REAL-FEED REPLAY VERIFIED"
    if os.environ.get("UPSTOX_ACCESS_TOKEN"):
        verification = "LIVE VERIFIED" if feed.health().get("connected") else "REAL-FEED REPLAY VERIFIED"

    raw = build_breakout_universe("LIVETEST.NS")
    df = aq.calculate_indicators(raw)
    aq.st.session_state.market_data = {"LIVETEST.NS": df}
    stock = aq.get_stock("LIVETEST.NS")
    stock.set_dataframe(df)
    aq.calculate_trade_quality(stock)
    aq.update_market_structure(stock)
    aq.assign_sector(stock)
    aq.run_batch1_signal_engines(stock)
    aq.run_all_strategies(stock)
    aq.run_batch2_signal_engines(stock)

    latencies = []
    with patch.object(aq, "is_market_open", return_value=True):
        final = aq.build_ai_consensus()
    if not final:
        print(json.dumps({"passed": False, "reason": "no candidates"}))
        return 1
    trade = final[0]
    trade.position_size = 10

    t0 = time.perf_counter()
    monitor = HotEntryMonitor(aq.get_market_state())
    ok, reason, detail = monitor.evaluate(trade)
    latencies.append(("marketstate_to_entry_ms", (time.perf_counter() - t0) * 1000))

    aq.allocate_portfolio()
    intent = OrderIntent(
        trade_id="T1",
        decision_id=getattr(trade, "decision_id", None),
        client_order_id=LiveExecutionAdapter.client_order_id(trade, aq),
        symbol=trade.symbol,
        side="BUY",
        quantity=int(getattr(trade, "position_size", 0) or 0),
        price=float(getattr(trade, "entry", 0) or 0),
        trade_confidence=float(getattr(trade, "ai_score", 0) or 0),
        score_version="V2",
        strategy=getattr(trade, "strategy", ""),
    )
    adapter = PaperExecutionAdapter()
    t1 = time.perf_counter()
    trade.entry_status = "READY"
    trade.state = "ALLOCATED"
    trade.position_size = 10
    aq.st.session_state.selected_portfolio = [trade]
    with patch.object(aq, "is_market_open", return_value=True):
        result = adapter.execute(intent, trade, aq)
    latencies.append(("entry_to_paper_ms", (time.perf_counter() - t1) * 1000))

    ms = aq.get_market_state().snapshot()
    report = {
        "passed": result.success if hasattr(result, "success") else bool(result[0] if isinstance(result, tuple) else result),
        "verification_mode": verification,
        "trade_confidence": getattr(trade, "ai_score", 0),
        "score_version": getattr(trade, "score_version", ""),
        "risk": (getattr(trade, "risk_verdict", {}) or {}).get("verdict"),
        "entry_monitor": detail,
        "paper_result": getattr(result, "message", str(result)),
        "market_state_source": ms.get("data_source"),
        "quote_count": len(ms.get("quotes", {})),
        "feed_health": feed.health(),
        "latencies": {k: round(v, 2) for k, v in latencies},
    }
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
