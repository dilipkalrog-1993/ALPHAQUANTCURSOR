"""Application adapter between Streamlit state and the production backend.

This module is intentionally outside :mod:`core.production_engine` so command
line consumers cannot acquire UI dependencies through the backend import graph.
"""
from __future__ import annotations

from typing import Any

from core.diagnostics import PipelineDiagnostics
from core.history import prepare_indicators
from discovery.pipeline import DiscoveryPipeline


def run_streamlit_discovery(app_module: Any, **kwargs: Any) -> Any:
    """Normalize UI history canonically, then run the application pipeline."""
    source = app_module.st.session_state.market_data
    prepared: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for symbol, raw in source.items():
        frame, failure = prepare_indicators(symbol, raw, "streamlit market cache")
        if failure:
            failures.append(failure.to_dict())
        else:
            prepared[symbol] = frame
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


__all__ = ["run_streamlit_discovery"]
