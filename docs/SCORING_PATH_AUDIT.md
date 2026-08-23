# Production scoring-path audit

Audit date: 2026-08-23. Search terms: `score_candidate(`,
`compute_trade_score`, `compute_trade_score_v2`, `score_version`,
`scoring_engine_version`, `V1`, `V2`, `strategy_count`, `RR bonus`, `batch1`,
and `batch2`.

## Classification

| Path | Classification | Confidence authority |
|---|---|---|
| `scoring_engine_v2.compute_trade_score_v2` | **PRODUCTION** | The sole V2 implementation and sole normal-production Trade Confidence authority. |
| `core.headless_pipeline.score_candidate` | **PRODUCTION adapter (deprecated name)** | Geometry/candidate adapter only; calls `compute_trade_score_v2` and copies its result without local score arithmetic. Used by the headless production orchestrator and therefore benchmark/validation. |
| `appemergentquant_v3_1.build_ai_consensus` V2 branch | **PRODUCTION compatibility entry point** | Calls the same `compute_trade_score_v2`; Streamlit consumes the resulting candidate rather than rescoring it. |
| `appemergentquant_v3_1.build_ai_consensus` unreachable additive branch and `compute_fast_ai_score` | **LEGACY** | Retained only to read/diagnose historical V1 behavior. `get_scoring_engine_version()` returns V2 unconditionally, so normal production cannot enter it. |
| Batch 1/Batch 2 signal engines and `discovery.opportunity_ranker` | **PRODUCTION signal/ranking evidence** | Discovery evidence only. Their bonuses/ranking values are not added to V2 Trade Confidence. |
| `tools/run_scoring_v2_tests.py` and `tests/*` scorers | **TEST** | Deterministic acceptance calls; not application entry points. |
| Documentation formulas describing Fast AI/V1 | **LEGACY documentation** | Historical audit material, not executable scoring. |

No dead executable confidence scorer was found. The removed headless arithmetic
(`50 + RSI + EMA-distance`) was the architecture defect: it labeled a locally
computed score as a production candidate without invoking V2. The compatibility
function bearing its old name now delegates to the canonical engine, records the
full immutable profile/component snapshot, and rejects a non-V2 result.

## Ownership boundaries

Brain 5 returns a risk verdict; Brain 6 allocates/sizes; the Entry Engine returns
timing readiness. None assigns `trade_confidence` or `ai_score`. Risk may veto a
candidate, including for verified critical news, but does not rescore it.

LIVE CASH GUARDED remains locked. This integration does not submit an order and
adds no live-order test.
