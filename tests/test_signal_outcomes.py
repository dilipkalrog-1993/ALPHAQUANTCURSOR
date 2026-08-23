import pickle

from core.history import DEFAULT_CACHE_DIR, normalize_history
from core.production_engine import run_production_pipeline


def test_every_signal_has_explicit_v2_outcome():
    histories = []
    for path in list(DEFAULT_CACHE_DIR.glob("*.pkl"))[:8]:
        histories.append((f"{path.stem}.NS", normalize_history(pickle.loads(path.read_bytes())), "cache"))
    if not histories:
        return
    result = run_production_pipeline(histories, focus_limit=8)
    assert len(result["signal_outcomes"]) == result["diagnostics"].counts["strategy_signals"]
    for outcome in result["signal_outcomes"]:
        assert outcome["score_version"] == "V2"
        assert outcome["component_weights_snapshot"]
        assert outcome["candidate_creation_result"] in {"CREATED", "REJECTED"}
        if outcome["candidate_creation_result"] == "REJECTED":
            assert outcome["exact_rejection_reason"]
