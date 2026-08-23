"""Pure, headless production orchestration.

Application-framework adapters belong outside this backend module.  In
particular, importing this module must never initialize or reference Streamlit.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import time
import math

from core.diagnostics import PipelineDiagnostics
from core.headless_pipeline import Candidate, StageAudit, has_strategy_signal, score_candidate, persist_candidates
from core.history import prepare_indicators
from discovery.eligibility import filter_eligible
from discovery.focus_universe import build_focus_universe
from discovery.opportunity_ranker import rank_eligible
from discovery.funnel_store import DEFAULT_FUNNEL_PATH, persist_funnel


def run_production_pipeline(
    histories: Iterable[tuple[str, Any, str]], *, focus_limit: int = 28,
    persistence_path: Path | None = None, scoring_profile: Any | None = None,
    funnel_path: Path | None = DEFAULT_FUNNEL_PATH,
) -> dict[str, Any]:
    """Run history → eligibility → focus → strategy/V2 candidate → persistence."""
    histories = list(histories)
    total_at = time.perf_counter()
    prepared: dict[str, Any] = {}
    providers: dict[str, str] = {}
    failures = []
    diagnostics = PipelineDiagnostics()
    diagnostics.record("master", len(histories))
    indicator_at = time.perf_counter()
    for symbol, raw, provider in histories:
        frame, failure = prepare_indicators(symbol, raw, provider)
        if failure:
            failures.append(failure.to_dict())
        else:
            prepared[symbol] = frame
            providers[symbol] = provider
    failure_counts: dict[str, int] = {}
    for failure in failures:
        failure_counts[failure["reason"]] = failure_counts.get(failure["reason"], 0) + 1
    diagnostics.record("history_ready", len(prepared), failure_counts)
    indicator_seconds = time.perf_counter() - indicator_at

    eligibility_at = time.perf_counter()
    eligible, eligibility_audit = filter_eligible(prepared, min_bars=200)
    eligibility_seconds = time.perf_counter() - eligibility_at
    diagnostics.record("eligible", len(eligible), eligibility_audit.rejections)
    ranking_at = time.perf_counter()
    ranked = rank_eligible(eligible)
    ranking_seconds = time.perf_counter() - ranking_at
    active_limit = max(focus_limit, min(len(ranked), focus_limit * 3))
    active = ranked[:active_limit]
    diagnostics.record("active", len(active))
    focus_at = time.perf_counter()
    focus, focus_meta = build_focus_universe(active, limit=focus_limit, mandatory=set(), min_opportunity_score=0)
    focus_seconds = time.perf_counter() - focus_at
    diagnostics.record("focus", len(focus))
    signalled: list[dict[str, Any]] = []
    strategy_at = time.perf_counter()
    for row in focus:
        if has_strategy_signal(row["dataframe"]):
            signalled.append(row)
    strategy_seconds = time.perf_counter() - strategy_at
    v2_at = time.perf_counter()
    # score_candidate is a compatibility adapter, but it delegates directly to
    # scoring_engine_v2.compute_trade_score_v2; no local confidence arithmetic
    # is permitted in production.
    scored: list[Candidate] = [score_candidate(
        row["symbol"], row["dataframe"], data_source=providers.get(row["symbol"], "unknown"),
        scoring_profile=scoring_profile,
    ) for row in signalled]
    candidates: list[Candidate] = []
    signal_outcomes: list[dict[str, Any]] = []
    rejection_counts: dict[str, int] = {}
    for candidate in scored:
        v2 = candidate.trade_score_v2
        missing = sorted({item for component in v2.all_components() for item in component.missing_inputs})
        reason = None
        if not all(math.isfinite(value) and value > 0 for value in (candidate.entry, candidate.stop, candidate.target)):
            reason = "INVALID_ENTRY" if not math.isfinite(candidate.entry) or candidate.entry <= 0 else (
                "INVALID_STOP" if not math.isfinite(candidate.stop) or candidate.stop <= 0 else "INVALID_TARGET")
        elif not candidate.stop < candidate.entry < candidate.target:
            reason = "INVALID_STOP" if candidate.stop >= candidate.entry else "INVALID_TARGET"
        elif v2.gate_decision != "APPROVED":
            reason = "MISSING_COMPONENT_DATA" if v2.trade_confidence is None else "V2_BELOW_THRESHOLD"
        if reason is None:
            candidates.append(candidate)
        else:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        signal_outcomes.append({
            "symbol": candidate.symbol, "strategy": candidate.strategy,
            "strategy_signal_status": "SIGNALLED",
            "score_version": v2.score_version,
            "profile_name": v2.scoring_profile,
            "profile_version": v2.scoring_version,
            "component_weights_snapshot": dict(v2.scoring_weights_snapshot),
            "trade_confidence": v2.trade_confidence,
            "v2_component_breakdown": {c.component: c.weighted_contribution for c in v2.all_components()},
            "v2_threshold": v2.gate_threshold,
            "v2_qualified": v2.gate_decision == "APPROVED",
            "missing_component_data": missing,
            "risk_status": "NOT_REACHED" if reason else "NOT_EVALUATED_BY_DISCOVERY",
            "candidate_creation_result": "CREATED" if reason is None else "REJECTED",
            "exact_rejection_reason": reason,
        })
    v2_seconds = time.perf_counter() - v2_at
    diagnostics.record("strategy_signals", len(signalled), {"NO_STRATEGY_SIGNAL": len(focus) - len(signalled)})
    diagnostics.record("v2_qualified", len(candidates), rejection_counts)
    diagnostics.record("candidates", len(candidates), rejection_counts)
    persistence_at = time.perf_counter()
    persisted = persist_candidates(candidates, persistence_path) if persistence_path is not None else len(candidates)
    persistence_seconds = time.perf_counter() - persistence_at
    diagnostics.record("persisted", persisted)
    # One compact record per requested master symbol powers normal UI search
    # without rerunning strategies or V2 outside Focus.
    ranks = {row["symbol"]: index + 1 for index, row in enumerate(ranked)}
    focus_symbols = {row["symbol"] for row in focus}
    signal_by_symbol = {row["symbol"] for row in signalled}
    outcomes = {row["symbol"]: row for row in signal_outcomes}
    failure_by_symbol = {row["symbol"]: row for row in failures}
    rejection_by_symbol = {item.symbol: None for item in candidates}
    states = {}
    for symbol, _raw, _provider in histories:
        outcome = outcomes.get(symbol, {})
        failure = failure_by_symbol.get(symbol)
        eligible_symbol = symbol in eligible
        states[symbol] = {
            "symbol": symbol, "master": True, "history_ready": symbol in prepared,
            "eligible": eligible_symbol,
            "rejected_reason": (failure or {}).get("reason") if failure else
                (None if eligible_symbol else "ELIGIBILITY_FILTER"),
            "active_rank": ranks.get(symbol) if symbol in {r["symbol"] for r in active} else None,
            "focus": symbol in focus_symbols,
            "strategy_signal": symbol in signal_by_symbol,
            "v2_confidence": outcome.get("trade_confidence"),
            "v2_qualified": outcome.get("v2_qualified", False),
            "candidate_state": outcome.get("candidate_creation_result", "NOT_REACHED"),
            "risk_state": outcome.get("risk_status", "NOT_REACHED"),
            "entry_state": "WAITING" if symbol in rejection_by_symbol else "NOT_REACHED",
        }
    if funnel_path is not None:
        persist_funnel(states, funnel_path)
    timings = {"indicator_preparation": indicator_seconds, "eligibility": eligibility_seconds,
        "broad_ranking": ranking_seconds, "focus_selection": focus_seconds,
        "strategies": strategy_seconds, "v2_scoring": v2_seconds,
        "persistence": persistence_seconds, "total": time.perf_counter() - total_at}
    return {"candidates": candidates, "failures": failures, "prepared": prepared,
            "eligible": eligible, "active": active, "focus": focus, "focus_meta": focus_meta,
            "signal_outcomes": signal_outcomes,
            "funnel_states": states,
            "timings": {key: round(value, 6) for key, value in timings.items()},
            "eligibility_audit": eligibility_audit, "diagnostics": diagnostics}


__all__ = ["StageAudit", "run_production_pipeline"]
