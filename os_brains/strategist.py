"""
AlphaQuant OS - Brain 4: Strategist.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md section 2): produce candidate
trades. The only Brain allowed to say "this looks like an opportunity."

Absorbs build_ai_consensus's existing per-symbol scoring (grouping
candidates the strategy registry + Batch 1/2 signal engines already
produced, picking the best one per symbol) and extends it with Brain 1
(Market Observer), Brain 2 (Market Historian) and Brain 3 (Historical
Analog Engine) evidence - the pieces the architecture doc calls out as new
for this phase. Never checks portfolio-level risk, sizes a position, or
has the final word - Brain 5 (risk_manager) and Brain 6 (portfolio_manager)
sit downstream of every candidate this module produces.
"""

import logging

from os_brains.market_observer import observe
from os_brains.market_historian import get_regime_context
from os_brains.historical_analog_engine import find_analogs
from os_brains.setup_vector import (
    build_setup_vector_row,
    compute_pattern_flag_series,
    compute_relative_strength_series,
)

# How much of Brain 3's expected_value can move a candidate's ai_score,
# gated on sample_confidence so a HIGH-confidence analog match can move
# the score meaningfully while a LOW-confidence one (few historical
# matches) barely moves it at all.
ANALOG_SCORE_WEIGHT = {"HIGH": 40, "MEDIUM": 20, "LOW": 5}
ANALOG_SCORE_CAP = 10

_EMPTY_ANALOG_REPORT_TEMPLATE = {
    "as_of": None, "setup_vector": None, "matched_analogs_count": 0,
    "win_rate": None, "expected_return": None, "expected_drawdown": None,
    "recovery_time_days": None, "typical_holding_period_days": None,
    "probability_of_success": None, "sample_confidence": "LOW",
}


def _build_analog_report(stock, symbol, app_module):
    """
    Builds the current-day setup vector for `stock` (the same feature
    function used by the backfill pipeline, so historical and live
    vectors stay comparable per the architecture doc) and looks up
    Brain 3's AnalogReport for it. Returns a "no evidence yet" AnalogReport
    shape rather than raising on any failure - Brain 4 must still be able
    to produce a candidate when Market Memory isn't reachable or the
    symbol's setup can't be vectorized (e.g. too little price history).
    """
    empty_report = dict(_EMPTY_ANALOG_REPORT_TEMPLATE, symbol=symbol)
    try:
        nifty_df = getattr(app_module, "nifty_benchmark_df", None)
        if nifty_df is None or stock.data is None or len(stock.data) < 30:
            return empty_report
        flags = compute_pattern_flag_series(stock.data)
        rs_series = compute_relative_strength_series(stock.data, nifty_df)
        vector = build_setup_vector_row(stock.data, len(stock.data) - 1, flags, rs_series)
        if vector is None:
            return empty_report
        return find_analogs(symbol, vector, as_of_date=stock.data.index[-1].date())
    except Exception as e:
        logging.warning(f"STRATEGIST analog lookup failed for {symbol}: {e}")
        return empty_report


def enrich_candidate(stock, candidate, app_module):
    """
    Attaches Brain 1/2/3 evidence to an already-generated TradeCandidate
    (produced earlier by the strategy registry + Batch 1/2 signal engines
    - this function finalizes the evidence behind ONE candidate, it does
    not create new candidates from scratch). Sets, as additional
    attributes on `candidate`:
        regime_context, analog_report, evidence_summary, expected_value,
        market_observation
    Also folds a bounded analog-based adjustment into candidate.ai_score,
    so the historical evidence Phase 1 only displayed now actually
    influences which trades are favored.
    """
    symbol = candidate.symbol

    try:
        observation = observe(stock, app_module)
    except Exception as e:
        logging.warning(f"STRATEGIST observation failed for {symbol}: {e}")
        observation = None

    try:
        if "REGIME" not in stock.market:
            app_module.detect_market_regime(stock)
        regime_context = get_regime_context(stock)
    except Exception as e:
        logging.warning(f"STRATEGIST regime context failed for {symbol}: {e}")
        regime_context = {
            "current_regime": "SIDEWAYS", "current_regime_strength": 50,
            "nearest_historical_regimes": [],
        }

    analog_report = _build_analog_report(stock, symbol, app_module)

    win_rate = analog_report.get("win_rate")
    expected_return = analog_report.get("expected_return")
    expected_drawdown = analog_report.get("expected_drawdown")
    sample_confidence = analog_report.get("sample_confidence", "LOW")

    expected_value = 0.0
    if win_rate is not None and expected_return is not None and expected_drawdown is not None:
        expected_value = (win_rate * expected_return) - ((1 - win_rate) * abs(expected_drawdown))

    evidence_summary = []
    if observation is not None and observation.get("sector"):
        rank = observation["sector"].get("relative_rank", 50) or 50
        evidence_summary.append({
            "factor": "sector_relative_strength",
            "direction": "+" if rank >= 60 else "-",
            "weight": 1,
            "note": f"sector={observation['sector'].get('name')} rank={rank}",
        })
    evidence_summary.append({
        "factor": "regime",
        "direction": "-" if regime_context["current_regime"] == "TRENDING_BEAR" else "+",
        "weight": 1,
        "note": f"{regime_context['current_regime']} (strength {regime_context['current_regime_strength']})",
    })
    if analog_report.get("matched_analogs_count", 0) > 0:
        evidence_summary.append({
            "factor": "historical_analogs",
            "direction": "+" if expected_value > 0 else "-",
            "weight": {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(sample_confidence, 1),
            "note": (
                f"{analog_report['matched_analogs_count']} matches, {sample_confidence} confidence, "
                f"win_rate={round(win_rate * 100, 1) if win_rate is not None else None}%, "
                f"expected_return={round(expected_return * 100, 2) if expected_return is not None else None}%"
            ),
        })

    analog_score_adjustment = 0
    if analog_report.get("matched_analogs_count", 0) > 0:
        weight = ANALOG_SCORE_WEIGHT.get(sample_confidence, 5)
        analog_score_adjustment = max(
            -ANALOG_SCORE_CAP, min(ANALOG_SCORE_CAP, round(expected_value * weight, 1))
        )
        candidate.ai_score = round(candidate.ai_score + analog_score_adjustment, 2)
        candidate.add_reason(
            f"[HistoricalAnalog] {analog_report['matched_analogs_count']} analogs "
            f"({sample_confidence} confidence) -> ai_score adjustment {analog_score_adjustment:+}"
        )

    candidate.regime_context = regime_context
    candidate.analog_report = analog_report
    candidate.evidence_summary = evidence_summary
    candidate.expected_value = round(expected_value, 4)
    candidate.market_observation = observation

    logging.info(
        f"STRATEGIST symbol={symbol} ai_score={candidate.ai_score} "
        f"expected_value={candidate.expected_value} analog_adjustment={analog_score_adjustment} "
        f"regime={regime_context['current_regime']} analogs={analog_report.get('matched_analogs_count', 0)}"
    )

    return candidate
