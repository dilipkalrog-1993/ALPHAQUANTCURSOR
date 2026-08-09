#!/usr/bin/env python3
"""End-to-end production pipeline acceptance using authentic brains."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.synthetic_breakout_data import build_breakout_universe, build_performance_universe


def _bootstrap_session(aq, state_dir: Path) -> None:
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
    ss["scan_universe"] = []
    ss["waiting_entry"] = {}
    ss["candidate_archive"] = []
    ss["candidate_rejections"] = {}
    ss["pipeline_stage_counts"] = {}
    ss["pipeline_timings"] = {}
    ss["decision_funnel"] = []
    ss["_paper_state_restored"] = False
    ss["autonomous_active"] = False
    aq.PAPER_STATE_PATH = state_dir / "paper_state.json"
    aq.WORKSPACE.preferences.update({
        "execution_mode": "PAPER",
        "paper_preset": "PAPER NORMAL",
        "scoring_engine_version": "V2",
        "minimum_fast_ai_score": 70,
        "minimum_confidence": 70,
        "require_deep_ai_before_entry": False,
        "signal_expiry_minutes": 120,
        "maximum_positions": 10,
    })


def _prepare_market_data(aq, symbols: list[str]) -> dict[str, Any]:
    prepared = {}
    for i, sym in enumerate(symbols):
        raw = build_breakout_universe(sym)
        df = aq.calculate_indicators(raw)
        if df is None:
            raise RuntimeError(f"Indicator calculation failed for {sym}")
        aq.assign_sector(aq.get_stock(sym))
        prepared[sym] = df
    return prepared


def _run_production_pipeline(aq, timings: dict[str, float]) -> dict[str, Any]:
    funnel = {
        "Universe": 0, "Data Ready": 0, "Fast Screen Passed": 0, "Strategy Signals": 0,
        "AI Approved": 0, "AI Rejected": 0, "Risk Approved": 0, "Risk Rejected": 0,
        "Portfolio Allocated": 0, "No Capital": 0, "Waiting Entry": 0, "Executed": 0,
        "Open Positions": 0, "Closed Positions": 0,
    }
    candidates_log: list[dict[str, Any]] = []

    t0 = time.perf_counter()
    universe = list(aq.st.session_state.market_data.keys())
    funnel["Universe"] = len(universe)

    # --- scan stage (production mirror) ---
    for symbol, df in aq.st.session_state.market_data.items():
        if df is None or df.empty:
            continue
        funnel["Data Ready"] += 1
        if len(df) < 50:
            continue
        price = float(df.iloc[-1]["Close"])
        if price < aq.CONFIG.get("MIN_PRICE", 20):
            continue
        avg_volume = float(df["Volume"].tail(20).mean())
        if avg_volume < aq.CONFIG.get("MIN_AVG_VOLUME", 100000):
            continue
        funnel["Fast Screen Passed"] += 1

        t_strat = time.perf_counter()
        df = aq.calculate_indicators(df)
        stock = aq.get_stock(symbol)
        stock.set_dataframe(df)
        aq.calculate_trade_quality(stock)
        aq.update_market_structure(stock)
        aq.assign_sector(stock)
        before = set(aq.st.session_state.trade_candidates)
        aq.run_batch1_signal_engines(stock)
        aq.run_all_strategies(stock)
        aq.run_batch2_signal_engines(stock)
        timings.setdefault("strategy_evaluation", 0.0)
        timings["strategy_evaluation"] += time.perf_counter() - t_strat

        new_keys = set(aq.st.session_state.trade_candidates) - before
        if new_keys:
            funnel["Strategy Signals"] += len(new_keys)
        for key in new_keys:
            trade = aq.st.session_state.trade_candidates[key]
            aq.validate_trade_candidate(stock, trade)
            aq.apply_sector_bonus(stock, trade)
            aq.calculate_position_size(trade)

    timings["fast_screen"] = time.perf_counter() - t0

    t_ai = time.perf_counter()
    final_list = aq.build_ai_consensus()
    timings["fast_ai"] = time.perf_counter() - t_ai

    for trade in final_list:
        verdict = getattr(trade, "risk_verdict", {}) or {}
        bd = getattr(trade, "ai_score_breakdown", {}) or {}
        entry = {
            "symbol": trade.symbol,
            "strategy": trade.strategy,
            "strategy_confidence": getattr(trade, "confidence", None),
            "ai_score": getattr(trade, "ai_score", None),
            "ai_breakdown": bd,
            "fast_ai_status": getattr(trade, "fast_ai_status", None),
            "deep_ai_status": getattr(trade, "deep_ai_status", None),
            "risk_verdict": verdict.get("verdict"),
            "state": getattr(trade, "state", None),
            "primary_blocker": getattr(trade, "primary_blocker", None),
        }
        candidates_log.append(entry)
        if getattr(trade, "fast_ai_status", "") == "APPROVED":
            funnel["AI Approved"] += 1
        else:
            funnel["AI Rejected"] += 1
        if verdict.get("verdict") == "APPROVED":
            funnel["Risk Approved"] += 1
        elif verdict.get("verdict") == "VETOED":
            funnel["Risk Rejected"] += 1

    t_risk = time.perf_counter()
    selected = aq.allocate_portfolio()
    timings["brain_6_portfolio"] = time.perf_counter() - t_risk
    for trade in aq.st.session_state.get("final_trade_list", []):
        if getattr(trade, "state", "") == "ALLOCATED":
            funnel["Portfolio Allocated"] += 1
        elif getattr(trade, "state", "") == "APPROVED_NO_CAPITAL":
            funnel["No Capital"] += 1

    t_entry = time.perf_counter()
    executed = 0
    for trade in selected:
        ok, reason = aq.entry_trigger_status(trade)
        if ok and trade.symbol not in aq.st.session_state.paper_positions:
            pos, msg = aq.create_atomic_paper_trade(trade)
            if pos is not None:
                executed += 1
                trade.state = "EXECUTED"
                aq.record_candidate_terminal_state(trade, stage="EXECUTED", blocker="", detail=msg)
            else:
                aq.record_candidate_terminal_state(trade, stage="NOT_SUBMITTED", blocker="NOT_SUBMITTED", detail=msg)
        else:
            funnel["Waiting Entry"] += 1
            blocker = getattr(trade, "entry_status", "WAITING_PRICE")
            aq.record_candidate_terminal_state(trade, stage=blocker, blocker=blocker, detail=reason)
    timings["entry_evaluation"] = time.perf_counter() - t_entry
    timings["paper_execution"] = timings["entry_evaluation"]
    funnel["Executed"] = executed
    funnel["Open Positions"] = len(aq.st.session_state.paper_positions)

    return {"funnel": funnel, "candidates": candidates_log, "selected": [t.symbol for t in selected]}


def _close_via_target(aq, symbol: str) -> dict[str, Any]:
    pos = aq.st.session_state.paper_positions.get(symbol)
    if pos is None:
        return {"closed": False, "reason": "no position"}
    target = float(pos.target1)
    df = aq.st.session_state.market_data[symbol].copy()
    df.iloc[-1, df.columns.get_loc("Close")] = target
    df.iloc[-1, df.columns.get_loc("High")] = max(float(df.iloc[-1]["High"]), target)
    aq.st.session_state.market_data[symbol] = df
    stock = aq.get_stock(symbol)
    stock.set_dataframe(df)

    reviewer_called = {"value": False}
    from os_brains import reviewer as brain7
    original = brain7.review_closed_trade

    def _wrap(position, app_module=None):
        reviewer_called["value"] = True
        return original(position, app_module)

    brain7.review_closed_trade = _wrap
    try:
        pos.update_price(target)
        pos.close_trade("TARGET", target)
        aq.archive_closed_position(pos)
        aq.st.session_state.paper_positions.pop(symbol, None)
    finally:
        brain7.review_closed_trade = original

    return {
        "closed": True,
        "reviewer_called": reviewer_called["value"],
        "realized_pnl": getattr(pos, "realized_pnl", 0),
        "exit_reason": getattr(pos, "exit_reason", ""),
    }


def _performance_baseline(aq) -> dict[str, Any]:
    results = {}
    for label, count in [("50", 50), ("200", 200), ("500", 500)]:
        universe = build_performance_universe(count)
        times = []
        for sym, raw in universe.items():
            t0 = time.perf_counter()
            df = aq.calculate_indicators(raw)
            if df is None:
                continue
            stock = aq.get_stock(sym)
            stock.set_dataframe(df)
            aq.calculate_trade_quality(stock)
            aq.run_all_strategies(stock)
            times.append(time.perf_counter() - t0)
        times.sort()
        if times:
            p50 = times[len(times) // 2]
            p95 = times[int(len(times) * 0.95) - 1]
            results[label] = {"count": len(times), "p50_ms": round(p50 * 1000, 2), "p95_ms": round(p95 * 1000, 2)}
    return results


def _restart_test(state_dir: Path, symbols: list[str]) -> dict[str, Any]:
    script = f"""
import json, sys
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
import appemergentquant_v3_1 as aq
from tools.synthetic_breakout_data import build_breakout_universe

aq.st.session_state.clear()
aq.st.session_state["_paper_state_restored"] = False
aq.st.session_state["market_data"] = {{}}
aq.st.session_state["stock_objects"] = {{}}
aq.st.session_state.setdefault("closed_positions", [])
aq.st.session_state.setdefault("trade_journal", [])
aq.PAPER_STATE_PATH = Path({str(state_dir / "paper_state.json")!r})
aq.restore_trading_state_once()

for sym in {symbols!r}:
    raw = build_breakout_universe(sym)
    df = aq.calculate_indicators(raw)
    if df is not None:
        aq.st.session_state.market_data[sym] = df
        stock = aq.get_stock(sym)
        stock.set_dataframe(df)

broker = aq.st.session_state.get("paper_broker", {{}})
open_positions = aq.st.session_state.get("paper_positions", {{}})
closed = aq.st.session_state.get("paper_history", [])
orders_before = len(broker.get("orders", {{}}))
closed_pnl = sum(float(getattr(p, "realized_pnl", 0) or 0) for p in closed)

restored = {{
    "open_positions": len(open_positions),
    "open_symbols": list(open_positions.keys()),
    "closed_trades": len(closed),
    "orders": orders_before,
    "cash": broker.get("cash"),
    "realized_pnl": broker.get("realized_pnl", 0),
    "closed_history_pnl": closed_pnl,
}}

post_close = {{}}
if open_positions:
    sym = next(iter(open_positions))
    pos = open_positions[sym]
    resume_price = float(pos.entry) * 1.01
    pos.update_price(resume_price)
    restored["monitor_resumed"] = True
    restored["unrealized_after_quote"] = pos.unrealized_pnl

    target = float(pos.target1 or pos.target3 or resume_price * 1.05)
    df = aq.st.session_state.market_data[sym].copy()
    df.iloc[-1, df.columns.get_loc("Close")] = target
    df.iloc[-1, df.columns.get_loc("High")] = max(float(df.iloc[-1]["High"]), target)
    aq.st.session_state.market_data[sym] = df
    stock = aq.get_stock(sym)
    stock.set_dataframe(df)
    pos.update_price(target)
    pos.close_trade("TARGET", target)
    aq.archive_closed_position(pos)
    aq.st.session_state.paper_positions.pop(sym, None)
    aq.persist_trading_state()

    broker_after = aq.st.session_state.get("paper_broker", {{}})
    post_close = {{
        "symbol": sym,
        "closed_after_restart": pos.status == "CLOSED",
        "exit_reason": getattr(pos, "exit_reason", ""),
        "realized_pnl": getattr(pos, "realized_pnl", 0),
        "open_positions_after": len(aq.st.session_state.get("paper_positions", {{}})),
        "closed_trades_after": len(aq.st.session_state.get("paper_history", [])),
        "orders_after": len(broker_after.get("orders", {{}})),
        "duplicate_orders": len(broker_after.get("orders", {{}})) > orders_before,
        "broker_realized_pnl": broker_after.get("realized_pnl", 0),
    }}

print(json.dumps({{"restored": restored, "post_close": post_close}}))
"""
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, cwd=ROOT)
    if proc.returncode != 0:
        return {"passed": False, "stderr": proc.stderr[-4000:], "stdout": proc.stdout}
    payload = json.loads(proc.stdout.strip())
    restored = payload["restored"]
    post_close = payload.get("post_close", {})
    passed = (
        restored["open_positions"] >= 1
        and restored["closed_trades"] >= 1
        and restored.get("realized_pnl", 0) > 0
        and restored.get("closed_history_pnl", 0) > 0
        and post_close.get("closed_after_restart", False)
        and not post_close.get("duplicate_orders", True)
    )
    return {"passed": passed, "restored": restored, "post_close": post_close}


def main() -> int:
    import appemergentquant_v3_1 as aq

    report: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "postgres_configured": bool(os.environ.get("DATABASE_URL")),
        "acceptance_criteria": {},
    }

    with tempfile.TemporaryDirectory(prefix="aq_accept_") as tmp:
        state_dir = Path(tmp)
        _bootstrap_session(aq, state_dir)
        symbols = ["TESTBREAK.NS", "TESTOPEN.NS"]
        market = _prepare_market_data(aq, symbols)
        aq.st.session_state.market_data = market
        aq.st.session_state.scan_universe = symbols

        timings: dict[str, float] = {}
        with patch.object(aq, "is_market_open", return_value=True):
            pipeline = _run_production_pipeline(aq, timings)

        report["pipeline"] = pipeline
        report["timings_ms"] = {k: round(v * 1000, 2) for k, v in timings.items()}

        # Close first filled symbol; keep second open if executed
        filled = list(aq.st.session_state.paper_positions.keys())
        exit_result = {}
        if filled:
            exit_result = _close_via_target(aq, filled[0])
            report["pipeline"]["funnel"]["Closed Positions"] = len(aq.st.session_state.get("paper_history", []))

        aq.persist_trading_state()

        # Resume monitoring on remaining open position
        monitor_result = {}
        if aq.st.session_state.paper_positions:
            sym = next(iter(aq.st.session_state.paper_positions))
            pos = aq.st.session_state.paper_positions[sym]
            new_price = float(pos.entry) * 1.01
            pos.update_price(new_price)
            monitor_result = {"symbol": sym, "updated_price": new_price, "unrealized": pos.unrealized_pnl}

        report["exit"] = exit_result
        report["monitor_resume"] = monitor_result
        report["restart"] = _restart_test(state_dir, symbols)
        report["performance_baseline"] = _performance_baseline(aq)

        # Acceptance criteria
        cands = pipeline["candidates"]
        primary = next((c for c in cands if c["symbol"] == "TESTBREAK.NS"), {})
        ac = report["acceptance_criteria"]
        ac["strategy_confidence_gt_zero"] = (primary.get("strategy_confidence") or 0) > 0
        ac["strategy_name"] = primary.get("strategy")
        ac["ai_score_explainable"] = bool(primary.get("ai_breakdown"))
        ac["brain_5_executed"] = primary.get("risk_verdict") in {"APPROVED", "VETOED"}
        ac["brain_6_executed"] = pipeline["funnel"]["Portfolio Allocated"] + pipeline["funnel"]["No Capital"] > 0
        ac["paper_order_created"] = pipeline["funnel"]["Executed"] > 0
        ac["position_opened"] = pipeline["funnel"]["Open Positions"] > 0 or report["restart"]["restored"]["open_positions"] > 0
        ac["reviewer_on_close"] = exit_result.get("reviewer_called", False)
        ac["restart_open_position"] = report["restart"].get("passed", False)
        ac["restart_post_close"] = report["restart"].get("post_close", {}).get("closed_after_restart", False)
        ac["realized_pnl_restored"] = (report["restart"].get("restored", {}).get("realized_pnl") or 0) > 0
        ac["deep_ai_fast_path"] = primary.get("deep_ai_status") == "FAST_PATH"
        ac["all_pass"] = all([
            ac["strategy_confidence_gt_zero"],
            ac["ai_score_explainable"],
            ac["brain_5_executed"],
            ac["brain_6_executed"],
            ac["paper_order_created"],
            ac["position_opened"],
            ac["reviewer_on_close"],
            ac["restart_open_position"],
            ac["restart_post_close"],
            ac["realized_pnl_restored"],
        ])

    print(json.dumps(report, indent=2, default=str))
    return 0 if report["acceptance_criteria"].get("all_pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
