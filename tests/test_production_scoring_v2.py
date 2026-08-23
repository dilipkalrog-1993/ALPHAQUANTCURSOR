"""Deterministic invariants for the one production confidence engine."""

from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.scoring_profiles import DEFAULT_WEIGHTS, ScoringProfile
from scoring_engine_v2 import MAX_CONFLUENCE_BONUS, compute_trade_score_v2


def _inputs():
    close = np.linspace(100, 130, 220)
    data = pd.DataFrame({
        "Open": close - .2, "High": close + 1, "Low": close - 1,
        "Close": close, "Volume": np.full(220, 500_000),
        "EMA20": close - 1, "EMA50": close - 2, "EMA200": close - 4,
        "RSI": np.full(220, 62), "ATR": np.full(220, 2),
    })
    stock = SimpleNamespace(data=data, indicators={}, score={}, sector="UNKNOWN")
    candidate = SimpleNamespace(strategy="BREAKOUT", entry=130.0, entry_status="")
    return stock, candidate


def test_approved_default_weights_and_bounded_total():
    stock, candidate = _inputs()
    score = compute_trade_score_v2(stock, candidate)
    assert DEFAULT_WEIGHTS == {"structure":30.0,"participation":20.0,"momentum":20.0,
                               "market_sector":10.0,"historical":10.0,"news":10.0}
    assert score.scoring_weights_snapshot == DEFAULT_WEIGHTS
    assert 0 <= score.trade_confidence <= 100


def test_default_profile_exactly_reproduces_unconfigured_v2():
    stock, candidate = _inputs()
    baseline = compute_trade_score_v2(stock, candidate)
    configured = compute_trade_score_v2(stock, candidate, scoring_profile=ScoringProfile())
    assert configured.trade_confidence == baseline.trade_confidence
    assert configured.to_gate_breakdown()["v2_components"] == baseline.to_gate_breakdown()["v2_components"]


def test_confluence_is_added_once_and_capped():
    stock, candidate = _inputs()
    without = compute_trade_score_v2(stock, candidate, all_strategies=["BREAKOUT"])
    with_many = compute_trade_score_v2(
        stock, candidate,
        all_strategies=["BREAKOUT", "VCP", "ORDER_BLOCK", "PRICE SQUEEZE"],
    )
    assert with_many.confluence_bonus <= MAX_CONFLUENCE_BONUS
    assert with_many.trade_confidence - without.trade_confidence <= MAX_CONFLUENCE_BONUS


def test_missing_history_and_forward_guidance_and_critical_veto():
    stock, candidate = _inputs()
    missing = compute_trade_score_v2(stock, candidate, analog_report={"matched_analogs_count": 0})
    assert missing.historical.weighted_contribution == 0
    assert "MISSING:historical_analogs" in missing.historical.missing_inputs

    constructive = compute_trade_score_v2(stock, candidate, news_payload={
        "results_actual": "MISS", "forward_guidance": "POSITIVE", "management_commentary": "POSITIVE",
        "news_status": "AVAILABLE",
    })
    assert constructive.news.weighted_contribution >= 5

    payload = {"news_status": "ACTIVE", "news_sentiment": -1, "news_relevance": 90,
               "news_risk": 80, "news_veto_reason": "Verified fraud investigation",
               "news_summary": "fraud accounting concern sebi"}
    critical = compute_trade_score_v2(stock, candidate, news_payload=payload)
    assert critical.news.weighted_contribution == 0


def test_risk_and_entry_modules_do_not_rewrite_confidence():
    from pathlib import Path
    risk = Path("os_brains/risk_manager.py").read_text(encoding="utf-8")
    entry = Path("market/entry_monitor.py").read_text(encoding="utf-8")
    forbidden = ("candidate.trade_confidence =", "trade.trade_confidence =",
                 "candidate.ai_score =", "trade.ai_score =")
    assert not any(token in risk for token in forbidden)
    assert not any(token in entry for token in forbidden)
