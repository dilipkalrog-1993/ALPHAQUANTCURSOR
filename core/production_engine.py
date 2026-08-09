"""Single production orchestration entry used by Streamlit and headless tools."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from core.diagnostics import PipelineDiagnostics
from core.headless_pipeline import Candidate, StageAudit, compute_candidate, persist_candidates
from core.history import prepare_indicators
from discovery.eligibility import filter_eligible
from discovery.focus_universe import build_focus_universe
from discovery.opportunity_ranker import rank_eligible


def run_production_pipeline(
    histories: Iterable[tuple[str, Any, str]], *, focus_limit: int = 28,
    persistence_path: Path | None = None,
) -> dict[str, Any]:
    """Run history → eligibility → focus → strategy/V2 candidate → persistence."""
    histories = list(histories)
    prepared: dict[str, Any] = {}
    failures = []
    diagnostics = PipelineDiagnostics()
    for symbol, raw, provider in histories:
        frame, failure = prepare_indicators(symbol, raw, provider)
        if failure:
            failures.append(failure.to_dict())
        else:
            prepared[symbol] = frame
    failure_counts: dict[str, int] = {}
    for failure in failures:
        failure_counts[failure["reason"]] = failure_counts.get(failure["reason"], 0) + 1
    diagnostics.record("history_ready", len(prepared), failure_counts)

    eligible, eligibility_audit = filter_eligible(prepared, min_bars=200)
    diagnostics.record("eligible", len(eligible), eligibility_audit.rejections)
    ranked = rank_eligible(eligible)
    focus, focus_meta = build_focus_universe(ranked, limit=focus_limit, mandatory=set(), min_opportunity_score=0)
    diagnostics.record("focus", len(focus))
    candidates: list[Candidate] = []
    for row in focus:
        candidate = compute_candidate(row["symbol"], row["dataframe"])
        if candidate is not None:
            candidates.append(candidate)
    diagnostics.record("strategy_signals", len(candidates), {"NO_STRATEGY_SIGNAL": len(focus) - len(candidates)})
    diagnostics.record("v2_qualified", len(candidates))
    persisted = persist_candidates(candidates, persistence_path) if persistence_path is not None else len(candidates)
    diagnostics.record("persisted", persisted)
    return {"candidates": candidates, "failures": failures, "prepared": prepared,
            "eligible": eligible, "focus": focus, "focus_meta": focus_meta,
            "eligibility_audit": eligibility_audit, "diagnostics": diagnostics}


def run_streamlit_discovery(app_module: Any, **kwargs: Any) -> Any:
    """Normalize UI history canonically, then delegate business rules to discovery."""
    from discovery.pipeline import DiscoveryPipeline
    source = app_module.st.session_state.market_data
    prepared: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for symbol, raw in source.items():
        frame, failure = prepare_indicators(symbol, raw, "streamlit market cache")
        if failure:
            failures.append(failure.to_dict())
        else:
            prepared[symbol] = frame
    # Make downstream strategies and entry monitoring consume exactly the
    # normalized frames counted by history_ready rather than a parallel copy.
    app_module.st.session_state.market_data = prepared
    result = DiscoveryPipeline(app_module).run(prepared, **kwargs)
    diagnostics = PipelineDiagnostics()
    reasons: dict[str, int] = {}
    for failure in failures:
        reasons[failure["reason"]] = reasons.get(failure["reason"], 0) + 1
    diagnostics.record("history_ready", len(prepared), reasons)
    diagnostics.record("eligible", result.eligible_count, result.eligibility_audit.rejections)
    diagnostics.record("focus", result.focus_count)
    diagnostics.record("strategy_signals", result.strategy_signals)
    diagnostics.record("v2_qualified", result.candidates)
    app_module.st.session_state["production_diagnostics"] = diagnostics.to_dict()
    app_module.st.session_state["history_failures"] = failures
    result.history_ready_count = len(prepared)
    result.production_diagnostics = diagnostics.to_dict()
    return result


__all__ = ["StageAudit", "run_production_pipeline", "run_streamlit_discovery"]
