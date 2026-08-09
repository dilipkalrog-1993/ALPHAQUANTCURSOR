#!/usr/bin/env python3
"""Deterministic Scoring Engine V2 acceptance tests."""

from __future__ import annotations

import json
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from scoring_engine_v2 import (
    MAX_CONFLUENCE_BONUS,
    compute_trade_score_v2,
    apply_volatility_risk_hint,
)
from tools.synthetic_breakout_data import build_breakout_universe


@dataclass
class MockStock:
    symbol: str = "TEST.NS"
    data: pd.DataFrame | None = None
    patterns: dict = field(default_factory=dict)
    indicators: dict = field(default_factory=dict)
    market: dict = field(default_factory=dict)
    score: dict = field(default_factory=dict)
    sector: str = "IT"


@dataclass
class MockCandidate:
    symbol: str = "TEST.NS"
    strategy: str = "BREAKOUT"
    entry: float = 150.0
    stop: float = 140.0
    target1: float = 180.0
    risk_reward: float = 3.0
    confidence: float = 100.0


def _run_aq_indicators(raw: pd.DataFrame) -> tuple[MockStock, pd.DataFrame]:
    import appemergentquant_v3_1 as aq

    df = aq.calculate_indicators(raw)
    stock = MockStock(symbol="TEST.NS", data=df)
    stock.market = {"TREND": "UPTREND", "REGIME": "TRENDING_BULL", "MARKET_STRENGTH": 75}
    stock.score = {
        "sector": 75,
        "mtf_alignment": 100,
        "relative_strength": 72,
        "smart_money": 12,
        "batch1_bonus": 0,
        "batch2_bonus": 0,
    }
    stock.indicators = {"BREAKOUT_LEVEL": float(df.iloc[-1]["Close"]) * 0.998}
    stock.patterns = {
        "BREAKOUT_READY": True,
        "BREAKOUT": True,
        "FALSE_BREAKOUT": False,
        "FRESH_DEMAND": [{"High": 145, "Low": 138}],
    }
    return stock, df


def _assert_range(score: float, lo: float = 0, hi: float = 100, label: str = "") -> None:
    if not (lo <= score <= hi):
        raise AssertionError(f"{label} score {score} not in [{lo}, {hi}]")


def test_strong_breakout_above_vwap() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    last = df.iloc[-1]
    assert float(last["Close"]) >= float(last["VWAP"])
    cand = MockCandidate(entry=float(last["Close"]) * 0.99)
    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT"], news_payload={"news_status": "NO_NEWS"})
    _assert_range(v2.trade_confidence, label="strong_breakout")
    return {"name": "strong_breakout_above_vwap", "confidence": v2.trade_confidence, "passed": v2.trade_confidence >= 70}


def test_breakout_below_vwap_waits() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    df = df.copy()
    df.iloc[-1, df.columns.get_loc("Close")] = float(df.iloc[-1]["VWAP"]) * 0.97
    df.iloc[-1, df.columns.get_loc("High")] = float(df.iloc[-1]["Close"]) + 1
    stock.data = df
    cand = MockCandidate(entry=float(df.iloc[-1]["Close"]) * 1.01)

    def entry_gate(trade):
        price = float(df.iloc[-1]["Close"])
        vwap = float(df.iloc[-1]["VWAP"])
        if price < vwap:
            trade.entry_status = "WAITING_VWAP"
            return False, "price below VWAP"
        return True, "ok"

    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT"], entry_status_fn=entry_gate)
    passed = v2.trade_confidence >= 50 and "VWAP" in (v2.entry_blocker or v2.participation.explanations[-1] if v2.participation.explanations else "")
    return {"name": "breakout_below_vwap", "confidence": v2.trade_confidence, "entry": v2.entry_status, "passed": passed}


def test_breakout_into_major_resistance() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    df = df.copy()
    high52 = float(df["High"].max())
    df.iloc[-1, df.columns.get_loc("Close")] = high52 * 0.999
    df.iloc[-1, df.columns.get_loc("HIGH52")] = high52
    stock.data = df
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT"])
    ext_sub = next(s for s in v2.structure.subscores if s.name == "entry_location_extension")
    passed = ext_sub.normalized <= 1.5
    return {"name": "breakout_into_major_resistance", "confidence": v2.trade_confidence, "extension_pts": ext_sub.normalized, "passed": passed}


def test_demand_zone_support() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    stock.patterns["FRESH_DEMAND"] = [{"High": 140, "Low": 130}]
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT"])
    ds = next(s for s in v2.structure.subscores if s.name == "demand_supply_positioning")
    passed = ds.normalized >= 4
    return {"name": "demand_zone_support", "confidence": v2.trade_confidence, "ds_pts": ds.normalized, "passed": passed}


def test_high_rsi_accelerating_momentum() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    df = df.copy()
    # Raise RSI monotonically across lookback so divergence check stays false.
    for i in range(-14, 0):
        df.iloc[i, df.columns.get_loc("RSI")] = 58 + (i + 14) * 0.9
        df.iloc[i, df.columns.get_loc("MACD")] = 0.5 + (i + 14) * 0.15
        df.iloc[i, df.columns.get_loc("MACD_SIGNAL")] = 0.4
    df.iloc[-1, df.columns.get_loc("RVOL")] = 2.0
    stock.data = df
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT"])
    rsi_sub = next(s for s in v2.momentum.subscores if s.name == "rsi_context")
    passed = rsi_sub.normalized >= 0.5 and v2.trade_confidence >= 60
    return {"name": "high_rsi_accelerating", "confidence": v2.trade_confidence, "rsi_pts": rsi_sub.normalized, "passed": passed}


def test_high_rsi_bearish_divergence() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    df = df.copy()
    base_close = float(df.iloc[-15]["Close"])
    for i in range(-14, 0):
        df.iloc[i, df.columns.get_loc("Close")] = base_close * (1 + (i + 14) * 0.008)
        df.iloc[i, df.columns.get_loc("RSI")] = 76 - (i + 14) * 0.75
    stock.data = df
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT"])
    rsi_sub = next(s for s in v2.momentum.subscores if s.name == "rsi_context")
    passed = rsi_sub.normalized <= 0.5
    return {"name": "high_rsi_bearish_divergence", "confidence": v2.trade_confidence, "rsi_pts": rsi_sub.normalized, "passed": passed}


def test_elevated_volatility_reduced_size() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    close = float(df.iloc[-1]["Close"])
    df.iloc[-1, df.columns.get_loc("ATR")] = close * 0.065  # 6.5% elevated
    stock.data = df
    verdict = apply_volatility_risk_hint(stock, {"verdict": "APPROVED", "reason": "ok"})
    passed = verdict.get("verdict") == "APPROVED_REDUCED_SIZE"
    return {"name": "elevated_volatility_reduced", "verdict": verdict.get("verdict"), "passed": passed}


def test_extreme_volatility_veto_path() -> dict:
    raw = build_breakout_universe()
    stock, df = _run_aq_indicators(raw)
    close = float(df.iloc[-1]["Close"])
    df.iloc[-1, df.columns.get_loc("ATR")] = close * 0.09  # 9% — Brain5 would veto
    stock.data = df
    import os_brains.risk_manager as rm

    cand = SimpleNamespace(symbol="T", risk_reward=3, position_size=100, capital_required=15000)
    portfolio = {"capital": 500000, "open_count": 0, "max_positions": 10, "sector_exposure": {}}
    import appemergentquant_v3_1 as aq

    stock_obj = aq.get_stock("X") if False else stock
    # Use risk manager directly with mock stock-like object
    class S:
        data = df
        sector = "IT"
        indicators = {}

    verdict = rm.evaluate(cand, S(), None, portfolio, aq)
    passed = "VOLATILITY" in verdict.get("vetoed_by", [])
    return {"name": "extreme_volatility_veto", "verdict": verdict.get("verdict"), "passed": passed}


def test_results_miss_forward_guidance() -> dict:
    news = {
        "news_status": "ACTIVE",
        "news_sentiment": -1,
        "news_relevance": 80,
        "news_summary": "Q1 results miss expectations but management raises full-year guidance and cites strong order book",
        "news_risk": 40,
    }
    raw = build_breakout_universe()
    stock, _ = _run_aq_indicators(raw)
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, news_payload=news)
    passed = v2.news.weighted_contribution >= 5 and any("MIXED" in e or "forward" in e.lower() for e in v2.news.explanations)
    return {"name": "results_miss_forward_positive", "news_pts": v2.news.weighted_contribution, "passed": passed}


def test_results_beat_negative_guidance() -> dict:
    news = {
        "news_status": "ACTIVE",
        "news_sentiment": 1,
        "news_relevance": 75,
        "news_summary": "Results beat estimates but management cuts guidance citing weak demand",
        "news_risk": 45,
    }
    raw = build_breakout_universe()
    stock, _ = _run_aq_indicators(raw)
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, news_payload=news)
    passed = v2.news.weighted_contribution <= 6
    return {"name": "results_beat_guidance_cut", "news_pts": v2.news.weighted_contribution, "passed": passed}


def test_critical_news_veto_separate() -> dict:
    news = {
        "news_status": "ACTIVE",
        "news_sentiment": -1,
        "news_relevance": 90,
        "news_risk": 80,
        "news_veto_reason": "News risk veto: fraud investigation announced",
        "news_summary": "fraud accounting concern sebi",
    }
    raw = build_breakout_universe()
    stock, _ = _run_aq_indicators(raw)
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, news_payload=news)
    passed = v2.news.weighted_contribution == 0 and news["news_veto_reason"] is not None
    return {"name": "critical_news_veto", "news_pts": v2.news.weighted_contribution, "passed": passed}


def test_confluence_cap() -> dict:
    raw = build_breakout_universe()
    stock, _ = _run_aq_indicators(raw)
    cand = MockCandidate()
    v2 = compute_trade_score_v2(
        stock, cand,
        all_strategies=["BREAKOUT", "VCP", "ORDER_BLOCK", "FVG"],
    )
    passed = v2.confluence_bonus <= MAX_CONFLUENCE_BONUS
    return {"name": "confluence_cap", "bonus": v2.confluence_bonus, "passed": passed}


def test_missing_historical_no_fabrication() -> dict:
    raw = build_breakout_universe()
    stock, _ = _run_aq_indicators(raw)
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, analog_report={"matched_analogs_count": 0})
    passed = "MISSING:historical_analogs" in v2.historical.missing_inputs and v2.historical.weighted_contribution == 0
    return {"name": "missing_historical", "hist_pts": v2.historical.weighted_contribution, "passed": passed}


def test_score_always_bounded() -> dict:
    raw = build_breakout_universe()
    stock, _ = _run_aq_indicators(raw)
    cand = MockCandidate()
    v2 = compute_trade_score_v2(stock, cand, all_strategies=["BREAKOUT", "VCP", "FVG"])
    _assert_range(v2.trade_confidence)
    passed = v2.trade_confidence <= 100
    return {"name": "score_bounded", "confidence": v2.trade_confidence, "passed": passed}


def _bootstrap_aq_session(aq, state_dir: Path) -> None:
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
    aq.PAPER_STATE_PATH = state_dir / "paper_state.json"
    aq.WORKSPACE.preferences.update({
        "scoring_engine_version": "V2",
        "execution_mode": "PAPER",
        "minimum_fast_ai_score": 70,
        "minimum_confidence": 70,
    })


def test_v2_pipeline_integration() -> dict:
    import appemergentquant_v3_1 as aq

    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        _bootstrap_aq_session(aq, state_dir)
        raw = build_breakout_universe("TESTBREAK.NS")
        df = aq.calculate_indicators(raw)
        aq.st.session_state.market_data = {"TESTBREAK.NS": df}
        stock = aq.get_stock("TESTBREAK.NS")
        stock.set_dataframe(df)
        aq.calculate_trade_quality(stock)
        aq.update_market_structure(stock)
        aq.assign_sector(stock)
        aq.run_batch1_signal_engines(stock)
        aq.run_all_strategies(stock)
        aq.run_batch2_signal_engines(stock)
        with patch.object(aq, "is_market_open", return_value=True):
            final = aq.build_ai_consensus()
        if not final:
            return {"name": "v2_pipeline_integration", "passed": False, "reason": "no candidates"}
        t = final[0]
        passed = (
            getattr(t, "score_version", "") == "V2"
            and 0 <= float(getattr(t, "ai_score", -1)) <= 100
            and hasattr(t, "trade_score_v2")
        )
        return {"name": "v2_pipeline_integration", "confidence": getattr(t, "ai_score", 0), "passed": passed}


def test_stale_quote_blocks_execution() -> dict:
    import appemergentquant_v3_1 as aq
    from datetime import datetime, timedelta, timezone

    with tempfile.TemporaryDirectory() as tmp:
        _bootstrap_aq_session(aq, Path(tmp))
        trade = SimpleNamespace(
            symbol="STALE.NS",
            strategy="BREAKOUT",
            entry=150.0,
            stop=140.0,
            target1=180.0,
            position_size=10,
            signal_time=datetime.now(timezone.utc).isoformat(),
            score_version="V2",
            ai_score=85,
        )
        broker = aq.get_broker_state()
        stale_time = datetime.now(timezone.utc) - timedelta(seconds=120)
        broker.publish_quotes(
            {"STALE.NS": {"ltp": 151.0, "previous_close": 148.0, "received_at": stale_time}},
            "BROKER_LIVE",
        )
        pos, msg = aq.create_atomic_paper_trade(trade)
        passed = pos is None and "stale" in msg.lower()
        return {"name": "stale_quote_blocks_execution", "passed": passed, "message": msg}


def test_restart_persistence_v2() -> dict:
    import appemergentquant_v3_1 as aq
    import subprocess

    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        aq.st.session_state.clear()
        aq.PAPER_STATE_PATH = state_dir / "paper_state.json"
        aq.st.session_state["paper_capital"] = 500_000.0
        aq.st.session_state["paper_broker"] = {"connected": False, "cash": 400_000.0, "starting_capital": 500_000.0,
            "positions": {}, "orders": {"O1": {"order_id": "O1", "score_version": "V2", "trade_confidence": 84}}, "trade_history": [], "realized_pnl": 0, "risk": {}}
        from appemergentquant_v3_1 import PaperPosition
        pos = PaperPosition(symbol="TESTOPEN.NS", strategy="BREAKOUT", qty=100, entry=150.0, stop=140.0,
            target1=180.0, confidence=100, ai_score=84)
        pos.initialise()
        pos.score_version = "V2"
        aq.st.session_state.paper_positions = {"TESTOPEN.NS": pos}
        aq.persist_trading_state()
        script = f"""
import json, sys
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
import appemergentquant_v3_1 as aq
aq.st.session_state.clear()
aq.PAPER_STATE_PATH = Path({str(state_dir / "paper_state.json")!r})
aq.restore_trading_state_once()
pos = aq.st.session_state.paper_positions.get("TESTOPEN.NS")
print(json.dumps({{"restored": pos is not None, "ai_score": getattr(pos, "ai_score", None)}}))
"""
        proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, cwd=ROOT)
        restored = json.loads(proc.stdout.strip()) if proc.returncode == 0 else {}
        passed = restored.get("restored") is True
        return {"name": "restart_persistence_v2", "passed": passed, "detail": restored}


TESTS = [
    test_strong_breakout_above_vwap,
    test_breakout_below_vwap_waits,
    test_breakout_into_major_resistance,
    test_demand_zone_support,
    test_high_rsi_accelerating_momentum,
    test_high_rsi_bearish_divergence,
    test_elevated_volatility_reduced_size,
    test_extreme_volatility_veto_path,
    test_results_miss_forward_guidance,
    test_results_beat_negative_guidance,
    test_critical_news_veto_separate,
    test_confluence_cap,
    test_missing_historical_no_fabrication,
    test_score_always_bounded,
    test_v2_pipeline_integration,
    test_stale_quote_blocks_execution,
    test_restart_persistence_v2,
]


def main() -> int:
    results = []
    failed = []
    for fn in TESTS:
        try:
            r = fn()
            results.append(r)
            if not r.get("passed", False):
                failed.append(r)
        except Exception as exc:
            r = {"name": fn.__name__, "passed": False, "error": str(exc)}
            results.append(r)
            failed.append(r)

    report = {"total": len(results), "passed": len(results) - len(failed), "failed": len(failed), "tests": results}
    print(json.dumps(report, indent=2, default=str))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
