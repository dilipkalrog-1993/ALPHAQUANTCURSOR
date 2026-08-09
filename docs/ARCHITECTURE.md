# AlphaQuant Architecture Map (Phase 0 Discovery)

**Date:** 2026-08-09  
**Repository:** https://github.com/dilipkalro-hash/AlphaQuant

## Repository Inventory

| Item | Location | Status |
|------|----------|--------|
| Canonical UI entry | `app.py` | **Supported** |
| Legacy monolith | `appemergentquant_v3_1.py` | Retained, not primary |
| OS Brains package | `os_brains/` | Restored (was referenced but missing from GitHub upload) |
| News service | `news_intelligence.py` | Restored |
| Streamlit config | `.streamlit/config.toml` | Present |
| Dependencies | `requirements.txt` | Present |
| Headless diagnostics | `tools/run_alphaquant_diagnostics.py` | Present |
| Tests | — | Not yet in repository |

## Application Structure

The production codebase is primarily a **12,859-line monolith** (`appemergentquant_v3_1.py`) containing:

- Streamlit UI (Market / Configuration / Trading / Reports)
- Universe engine (inlined `universe_engine.py`)
- Indicator, strategy, and signal engines
- Paper broker and execution adapters
- Upstox/YFinance market-data providers
- Persistence via JSON (`data/paper_state.json`, `data/workspace.json`)
- Core runtime worker (`AlphaQuantCoreRuntime`)

External modules extracted/restored for headless operation:

```
os_brains/
  db.py                  SQLite WAL schema
  pipeline_manager.py    Headless pipeline runner
  risk_manager.py        Brain 5
  portfolio_manager.py   Brain 6
  strategist.py          Brain 4 enrichment
  reviewer.py            Brain 7
  experience_memory.py   Decision audit trail
  market_historian.py    Regime catalog
  market_observer.py     Sector/RS observation
  historical_analog_engine.py
  setup_vector.py
news_intelligence.py     RSS news + briefing
```

## Data Flow (Target)

```
Market Data (Upstox WS / snapshot / yfinance)
        ↓
MarketState / AuthoritativeBrokerState
        ↓
Fast Screen → Strategies → AI Consensus → Risk → Portfolio
        ↓
Entry Monitor → Paper/Live Execution → Positions → Reports
        ↓
Persistence (JSON + SQLite)
```

## Entry Points

| File | Role | Action |
|------|------|--------|
| `app.py` | **Single supported entry** | Use for all deployments |
| `appemergentquant_v3_1.py` | Legacy upload / full monolith | Do not delete; documented in LEGACY_ENTRY_POINTS.md |

## Known Gaps (updated after authentic brain merge)

1. Authentic brains uploaded at repo root with **scrambled filenames** — remapped into `os_brains/` on stabilization branch (see `docs/BRAIN_INTEGRATION_AUDIT.md`).
2. Authentic brains require **PostgreSQL** (`DATABASE_URL`); cloud dev env has no Postgres — Brains 2/3/7 degrade gracefully.
3. Live order routing remains disabled by design.
4. Full end-to-end pipeline with live/cached market data not yet automated headlessly.

## Phase Roadmap

1. **Commit 1 (this PR):** Import/runtime stabilization, missing modules, diagnostics CLI
2. Commit 2: Canonical market data + WebSocket reliability
3. Commit 3: Headless diagnostics + score audit
4. Commit 4: Paper lifecycle + persistence hardening
5. Commit 5: Performance architecture (universe tiers)
6. Commit 6: UI professionalization
7. Commit 7: News + speech
8. Commit 8: Live-readiness safeguards
