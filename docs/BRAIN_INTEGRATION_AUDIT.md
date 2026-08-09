# Brain Integration Audit

**Date:** 2026-08-09  
**Branch:** `cursor/stabilization-aed7` (merged with `origin/main` @ `7a6e357`)  
**Monolith:** `appemergentquant_v3_1.py`  
**Package:** `os_brains/`

## Executive Summary

The authentic AlphaQuant OS brain files were uploaded to GitHub at the **repository root with scrambled filenames** (not under `os_brains/`). This audit maps each authentic module to its correct package location, documents runtime invocation, and identifies overlapping logic still owned by the monolith.

**All 12 `os_brains/` modules are now AUTHENTIC ORIGINAL** (generated PR #1 compatibility code has been replaced).

---

## Upload Filename Scramble (Critical Finding)

GitHub commit `7a6e357` added brain files at repo root with **wrong filenames**:

| Uploaded root file | Actual content | Correct `os_brains/` path |
|-------------------|----------------|---------------------------|
| `__init__.py` | Brain 1 — Market Observer | `market_observer.py` |
| `market_observer.py` | Postgres db/schema | `db.py` |
| `historical_analog_engine.py` | Brain 2 — Market Historian | `market_historian.py` |
| `backfill.py` | Brain 3 — Historical Analog Engine | `historical_analog_engine.py` |
| `reviewer.py` | setup_vector shared module | `setup_vector.py` |
| `risk_manager.py` | Brain 4 — Strategist | `strategist.py` |
| `market_historian.py` | Brain 5 — Risk Manager | `risk_manager.py` |
| `setup_vector.py` | Brain 6 — Portfolio Manager | `portfolio_manager.py` |
| `experience_memory.py` | Brain 7 — Reviewer | `reviewer.py` |
| `db.py` | Experience Memory store | `experience_memory.py` |
| `strategist.py` | Package `__init__` docstring | `__init__.py` |
| `portfolio_manager.py` | Pipeline Manager | `pipeline_manager.py` |

Root-level scrambled copies have been **removed from git**; canonical copies live only under `os_brains/`.

---

## Per-Brain Integration Map

### Brain 1 — Market Observer

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/market_observer.py` |
| **Key functions** | `observe(stock, app_module)`, `observe_market(stocks)` |
| **Invoked from monolith** | `build_ai_consensus()` → `enrich_candidate()` (Brain 4); `show_alphaquant_os_panel()` (read-only UI) |
| **Overlapping monolith logic** | Batch 1 engines (`run_batch1_signal_engines`), sector assignment (`assign_sector`), relative strength |
| **Runtime controller** | **Authentic Brain 1** when `require_deep_ai_before_entry=True`; otherwise **not called** in normal Paper flow |
| **Conflicts** | Monolith Batch 1 bonuses still feed `ai_score` independently of Brain 1 observation |
| **Missing integration** | Brain 1 not invoked during `execute_scan_pipeline()` initialize stage (only labels say "Market Observer") |
| **Source of truth** | Brain 1 for structured observation; monolith Batch 1 for fast-screen scoring bonuses |

### Brain 2 — Market Historian

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/market_historian.py` |
| **Key functions** | `get_regime_context(stock)`, `seed_regime_catalog()` |
| **Invoked from monolith** | `enrich_candidate()`; `show_alphaquant_os_panel()` |
| **Overlapping monolith logic** | `detect_market_regime(stock)`, `apply_market_regime_bonus()`, `market_regime_snapshot()` |
| **Runtime controller** | **Authentic Brain 2** inside enrich path; monolith `detect_market_regime` for strategy scoring |
| **Conflicts** | Two regime systems: monolith `stock.market["REGIME"]` vs Brain 2 Postgres `historical_regimes` catalog |
| **Missing integration** | `DATABASE_URL` required; without Postgres, `seed_regime_catalog()` fails (non-fatal in UI panel) |
| **Source of truth** | Brain 2 for historical regime catalog; monolith for live indicator-based regime flags |

### Brain 3 — Historical Analog Engine

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/historical_analog_engine.py` |
| **Key functions** | `find_analogs(symbol, setup_vector_raw, as_of_date, ...)` |
| **Invoked from monolith** | `enrich_candidate()` → `_build_analog_report()`; `show_alphaquant_os_panel()` |
| **Overlapping monolith logic** | None direct; analog adjustment folded into `ai_score` by Brain 4 |
| **Runtime controller** | **Authentic Brain 3** when enrich runs; returns empty LOW-confidence report if Postgres/backfill empty |
| **Conflicts** | None when Postgres populated; empty DB → zero analog matches → no ai_score adjustment |
| **Missing integration** | `backfill.py` pipeline not wired to run automatically; no `DATABASE_URL` in cloud env |
| **Source of truth** | Brain 3 exclusively |

### Brain 4 — Strategist

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/strategist.py` |
| **Key functions** | `enrich_candidate(stock, candidate, app_module)` |
| **Invoked from monolith** | `build_ai_consensus()` when `require_deep_ai_before_entry=True` (default **False**) |
| **Overlapping monolith logic** | Entire `build_ai_consensus()` — grouping, `ai_score` formula, fast AI gate, news effects |
| **Runtime controller** | **Monolith `build_ai_consensus()`** controls scoring in default Paper mode; Brain 4 is optional deep enrichment |
| **Conflicts** | Monolith computes `ai_score` before Brain 4; Brain 4 can adjust `ai_score` via analog weight only when called |
| **Missing integration** | Default config skips Brain 4 entirely |
| **Source of truth** | Monolith for fast path; Brain 4 for deep evidence when enabled |

### Brain 5 — Risk Manager

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/risk_manager.py` |
| **Key functions** | `evaluate(...)`, `build_portfolio_state(app_module)` |
| **Invoked from monolith** | `build_ai_consensus()` (always for fast-AI-approved candidates); `allocate_portfolio()` |
| **Overlapping monolith logic** | `validate_trade_candidate()` (MIN_TRADE_QUALITY 70, MIN_RR 2.5, trend filter); news veto in consensus |
| **Runtime controller** | **Authentic Brain 5** for portfolio-level vetoes (EXPOSURE, CORRELATION, LIQUIDITY, VOLATILITY, MACRO, RISK_REWARD, EVENT) |
| **Conflicts** | Monolith pre-filters before Brain 5; news veto applied in monolith after Brain 5 evaluate in some paths |
| **Missing integration** | `regime_context` often `None` when Brain 4 skipped → MACRO checks degraded |
| **Source of truth** | Brain 5 for portfolio vetoes; monolith for pre-strategy candidate validation |

### Brain 6 — Portfolio Manager

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/portfolio_manager.py` |
| **Key functions** | `allocate(approved_candidates, portfolio_state, app_module)` |
| **Invoked from monolith** | `allocate_portfolio()` → `execute_scan_pipeline()` portfolio stage |
| **Overlapping monolith logic** | `calculate_position_size()` runs before allocation; discovery mode caps in paper stage |
| **Runtime controller** | **Authentic Brain 6** for capital allocation ranking |
| **Conflicts** | Monolith sizes positions before Brain 6; Brain 6 may mark `APPROVED_NO_CAPITAL` |
| **Missing integration** | None critical |
| **Source of truth** | Brain 6 for allocation; monolith for initial position size estimate |

### Brain 7 — Reviewer

| Field | Value |
|-------|-------|
| **Authentic source** | `os_brains/reviewer.py` |
| **Key functions** | `review_closed_trade(position, app_module=None)` |
| **Invoked from monolith** | `PaperPosition.close_trade()` |
| **Overlapping monolith logic** | None |
| **Runtime controller** | **Authentic Brain 7** on every close (graceful fail if Postgres unavailable) |
| **Conflicts** | Monolith calls `review_closed_trade(self)` without `app_module` (optional param — OK) |
| **Missing integration** | Experience Memory writes require Postgres |
| **Source of truth** | Brain 7 exclusively |

### Supporting Modules

| Module | Role | Status |
|--------|------|--------|
| `os_brains/db.py` | Postgres connection + DDL | AUTHENTIC — requires `DATABASE_URL` |
| `os_brains/experience_memory.py` | Decision/outcome persistence | AUTHENTIC — graceful degrade |
| `os_brains/setup_vector.py` | Feature vector construction | AUTHENTIC |
| `os_brains/pipeline_manager.py` | Headless pipeline orchestration | AUTHENTIC |
| `os_brains/backfill.py` | Duplicate of Brain 3 engine | Same as `historical_analog_engine.py` |

---

## Score Contract (Zero-Confidence Investigation)

| Field | Source | Range | Zero allowed? |
|-------|--------|-------|---------------|
| **Strategy Confidence** | `TradeCandidate.confidence` from strategy engines | 0–100 | Yes — no strategy signal |
| **ai_score** | Monolith `build_ai_consensus()` formula | unbounded sum | Can be 0 if confidence=0 |
| **ai_confidence** | Batch 2 `signal_stock.score["ai_confidence"]` | default 50 | Not same as ai_score |
| **batch1_bonus** | Monolith Batch 1 engines | varies | 0 if engines don't run |
| **batch2_bonus** | Monolith Batch 2 engines | varies | 0 if engines don't run |
| **analog adjustment** | Brain 4 `enrich_candidate` | ±10 cap | 0 if Brain 4 not called or no analogs |
| **fast_ai_status** | Monolith vs `minimum_fast_ai_score` (70) | APPROVED/REJECTED | REJECTED → AI_REJECTED, no Risk eval |

### Why candidates showed 0 strategy confidence in UI (fixed)

**Root cause:** `_normal_opportunity_frame()` displayed **AI Score** in the **Confidence** column, masking raw strategy confidence.

**Secondary root cause:** `validate_trade_candidate()` read stale `stock.score["quality"]` computed *before* strategy engines added pattern points — causing valid BREAKOUT setups to fail TQI gate.

**Fix (Commit pipeline proof):**
- `normalize_strategy_confidence()` canonical contract on `save_trade_candidate()`
- `compute_ai_score_breakdown()` with explicit `MISSING:*` inputs
- Recalculate aggregate quality inside `validate_trade_candidate()` before TQI check
- UI shows separate Strategy Confidence / AI Score / Primary Blocker columns

---

1. **No strategy signal** — scan produces zero `trade_candidates` (most common on empty/small universe)
2. **Fast screen rejection** — price/volume filters in `scan_stage()`
3. **validate_trade_candidate** — TQI < 70, RR < 2.5, not UPTREND → state WATCHLIST not READY
4. **Fast AI gate** — `ai_score < minimum_fast_ai_score (70)` → `AI_REJECTED`, Brain 5 never runs
5. **Brain 5 veto** — EXPOSURE, LIQUIDITY, RISK_REWARD, MACRO, EVENT, etc.
6. **Brain 6** — `APPROVED_NO_CAPITAL` when slots/cash exhausted
7. **Entry monitor** — WAITING_PRICE, WAITING_VWAP, WAITING_VOLUME, STALE_QUOTE, EXPIRED, MARKET_CLOSED
8. **Postgres absent** — Brain 3/7/Experience Memory degrade silently; does not block trades but removes analog boost

---

## Pipeline Stage Ownership

```
Universe          → monolith build_default_scan_universe_for_pipeline()
Market Observer   → monolith initialize_stage (label only; Brain 1 not called)
Market Historian  → monolith label step (Brain 2 not called unless enrich)
Fast screen       → monolith scan_stage()
Strategy signals  → monolith run_all_strategies + Batch 1/2
AI Consensus      → monolith build_ai_consensus() [+ Brain 4 if deep AI enabled]
Risk Manager      → os_brains.risk_manager.evaluate()  ✓ AUTHENTIC
Portfolio Manager → os_brains.portfolio_manager.allocate()  ✓ AUTHENTIC
Entry Monitor     → monolith entry_trigger_status() + create_atomic_paper_trade()
Paper Execution   → monolith PaperBroker / create_atomic_paper_trade()
Position Monitor  → monolith monitor_open_positions()
Reviewer          → os_brains.reviewer.review_closed_trade()  ✓ AUTHENTIC
Persistence       → monolith JSON paper_state.json + Brain experience_memory (Postgres)
```

---

## Recommended Source of Truth (No refactor yet)

| Concern | Recommended owner |
|---------|-------------------|
| Strategy pattern detection | Monolith strategy registry (until extracted) |
| Fast AI score + gate | Monolith (document formula; Brain 4 optional) |
| Portfolio risk veto | **Authentic Brain 5** |
| Capital allocation | **Authentic Brain 6** |
| Post-trade learning | **Authentic Brain 7** + experience_memory |
| Market observation | **Authentic Brain 1** (enable in default path) |
| Regime + analogs | **Authentic Brains 2–3** (requires Postgres + backfill) |

---

## Next Integration Steps (Commit 2+)

1. Provision `DATABASE_URL` or add SQLite fallback adapter in `os_brains/db.py`
2. Wire Brain 1 `observe()` into pipeline initialize stage
3. Enable Brain 4 enrich by default for Paper DISCOVERY mode only (keep Live conservative)
4. Unify regime: monolith `detect_market_regime` should feed Brain 2 context
5. Run `backfill.py` once to populate analog dataset
6. Add headless pipeline test with synthetic OHLCV meeting MIN_TRADE_QUALITY + MIN_RR
