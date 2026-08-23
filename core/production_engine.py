"""Pure, headless production orchestration.

Application-framework adapters belong outside this backend module.  In
particular, importing this module must never initialize or reference Streamlit.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import time

from core.diagnostics import PipelineDiagnostics
from core.headless_pipeline import Candidate, StageAudit, has_strategy_signal, score_candidate, persist_candidates
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
    total_at = time.perf_counter()
    prepared: dict[str, Any] = {}
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
    candidates: list[Candidate] = [score_candidate(row["symbol"], row["dataframe"]) for row in signalled]
    v2_seconds = time.perf_counter() - v2_at
    diagnostics.record("strategy_signals", len(signalled), {"NO_STRATEGY_SIGNAL": len(focus) - len(signalled)})
    diagnostics.record("candidates", len(candidates))
    diagnostics.record("v2_qualified", len(candidates))
    persistence_at = time.perf_counter()
    persisted = persist_candidates(candidates, persistence_path) if persistence_path is not None else len(candidates)
    persistence_seconds = time.perf_counter() - persistence_at
    diagnostics.record("persisted", persisted)
    timings = {"indicator_preparation": indicator_seconds, "eligibility": eligibility_seconds,
        "broad_ranking": ranking_seconds, "focus_selection": focus_seconds,
        "strategies": strategy_seconds, "v2_scoring": v2_seconds,
        "persistence": persistence_seconds, "total": time.perf_counter() - total_at}
    return {"candidates": candidates, "failures": failures, "prepared": prepared,
            "eligible": eligible, "active": active, "focus": focus, "focus_meta": focus_meta,
            "timings": {key: round(value, 6) for key, value in timings.items()},
            "eligibility_audit": eligibility_audit, "diagnostics": diagnostics}


__all__ = ["StageAudit", "run_production_pipeline"]
