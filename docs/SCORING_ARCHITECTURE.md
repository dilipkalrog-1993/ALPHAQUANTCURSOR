# AlphaQuant Scoring Architecture

**Branch:** `cursor/stabilization-aed7`  
**Audit date:** 2026-08-09  
**Scope:** Current production scoring, decision, and weighting — read-only forensic audit.  
**Primary sources:** `appemergentquant_v3_1.py`, `os_brains/*`, `news_intelligence.py`

---

## Executive Summary

AlphaQuant decides trades through a **layered pipeline**. No single score determines everything. The dominant gate in default Paper mode is:

1. **Strategy engines** produce `TradeCandidate` objects with per-strategy `confidence` (0–100).
2. **Per-symbol consensus** picks the strongest candidate by `(confidence, risk_reward)`.
3. **Fast AI score** sums strategy confidence + count bonus + RR bonus + Batch 1 + Batch 2.
4. **Fast AI gate** (`minimum_fast_ai_score` = **70**) must pass before Brain 5 runs.
5. **Brain 5 Risk Manager** can **VETO** (hard gates).
6. **Brain 6 Portfolio Manager** allocates capital (ranking differs FAST vs DEEP path).
7. **Entry Monitor** is separate — price/VWAP/volume/expiry gates execution.

**FAST PATH (default):** Brains 1–4 mostly bypassed. Brains 5, 6, 7 active.  
**DEEP PATH:** `require_deep_ai_before_entry=True` → Brain 4 `enrich_candidate()` runs Brains 1–3.

---

## Master Flow Diagram

```
MARKET CONTEXT
  │  detect_market_regime(): REGIME, MARKET_STRENGTH (0–100)
  │  assign_sector(): sector bucket score
  │  nifty benchmark for relative strength
  ↓  [Not wired to apply_market_regime_bonus in pipeline — dead code path]
TECHNICAL SIGNALS
  │  calculate_indicators(): EMA, RSI, MACD, ADX, ATR, BB, VWAP, RVOL, 52w
  │  calculate_trade_quality(): TQI 0–100+ (trend/momentum/volume/volatility/risk)
  │  update_market_structure(): BOS, CHOCH, swings
  ↓
FAST SCREEN
  │  len(df)≥50, price∈[MIN_PRICE,100000], avg_vol≥100,000
  ↓  output: symbol passes / rejected (FILTERED_*)
STRATEGY SCORE
  │  run_batch1 → run_all_strategies → run_batch2
  │  Each strategy sets confidence 0–100, entry/stop/target, RR
  ↓  output: TradeCandidate per (symbol, strategy)
CONSENSUS
  │  Group by symbol; best = max(confidence, risk_reward)
  │  strategy_count = # strategies for symbol
  │  apply_sector_bonus on trade.confidence (+10/+5/−5) — BEFORE consensus pick in scan loop
  ↓  output: winning candidate per symbol
FAST AI
  │  ai_score = conf + count×5 + RR×5 + batch1 + batch2 [+ analog ±10 if DEEP]
  │  threshold: 70 → APPROVED / REJECTED
  ↓  output: fast_ai_status
DEEP INTELLIGENCE (optional)
  │  Brain 1 observe → Brain 2 regime_context → Brain 3 analog_report
  │  Brain 4 enrich: expected_value, analog_score_adjustment ±10
  ↓  output: regime_context, expected_value (Brain 6 rank key)
NEWS
  │  Batch 2 earnings penalty (−15/−3) already in batch2_bonus → ai_score
  │  news_intelligence.candidate_effect: ±5 confidence (NOT added to ai_score)
  │  CRITICAL news → risk VETO overlay
  ↓
RISK (Brain 5)
  │  EXPOSURE, CORRELATION, LIQUIDITY, VOLATILITY, MACRO, RISK_REWARD, EVENT
  ↓  output: APPROVED / VETOED
PORTFOLIO (Brain 6)
  │  Rank: FAST → (EV=0, ai_score, confidence); DEEP → (expected_value, ai_score, confidence)
  │  Position size from calculate_position_size; sector cap 40%
  ↓  output: ALLOCATED / APPROVED_NO_CAPITAL
ENTRY
  │  price≥entry, price≥VWAP, volume≥avg20, ai_score≥70 (60 discovery)
  │  signal_expiry_minutes (default 30)
  ↓  output: READY / WAITING_* / EXPIRED
EXECUTION
  │  create_atomic_paper_trade: idempotent paper fill
  ↓
REVIEW / LEARNING (Brain 7)
  │  Post-close: calibration_delta → Brain 3 probability_of_success
  │  Does NOT feed Fast AI or Brain 4 ai_score today
```

---

## Stage 1: Universe & Fast Screen

| Step | Function | Input | Threshold | Output |
|------|----------|-------|-----------|--------|
| Universe build | `build_default_scan_universe_for_pipeline()` | NIFTY lists, filters | workspace filters | symbol list |
| Quote pre-filter | `build_scan_universe()` | price, volume, turnover | price (20–20000), vol 100k, turnover 10M | filtered universe |
| Fast screen | `execute_scan_pipeline` scan stage | OHLCV | ≥50 bars, price≥20, avg vol≥100k | pass/fail |
| Indicators | `calculate_indicators()` | daily 1y | ≥200 rows required | enriched DataFrame |

---

## Stage 2: Trade Quality Index (TQI)

**Function:** `calculate_trade_quality()` → `score_trend/momentum/volume/volatility/risk`

| Category | Max pts | Rules |
|----------|---------|-------|
| Trend | 20 | +5 each: EMA20>50, 50>100, 100>200, Close>EMA20 |
| Momentum | 19 | RSI 55–70: +8; RSI>70: +4; MACD>signal: +7 |
| Volume | 15 | RVOL≥2: +15; ≥1.5: +10; ≥1.2: +5 |
| Volatility | 10 | ATR% 1–5: +10; <1: +5 |
| Risk | 10 | fixed +10 (ATR stop computed) |
| **Typical max** | **~74** | pattern/market/sector/news categories exist but default path sums above |

**Gate:** `validate_trade_candidate()` requires `quality ≥ 70` (recalculated at validation time).

**State mapping:** ≥90 HIGH CONVICTION; ≥80 BUY; ≥70 READY; ≥60 WATCHLIST; else REJECT.

---

## Stage 3: Production Strategies

All registered strategies are **LONG-only**. Registry order (priority):

| Priority | Name | Creates TradeCandidate |
|----------|------|------------------------|
| 5 | MARKET REGIME | No (sets `stock.market` only) |
| 10 | PRICE SQUEEZE | Yes |
| 20 | DEMAND & SUPPLY | Yes |
| 30 | VCP | Yes |
| 40 | BREAKOUT | Yes |
| 50 | ORDER BLOCK | Yes |
| 60 | FVG | Yes |

**Not registered:** `LIQUIDITY_SWEEP` (code exists, never called).

### Shared mechanics

- **RR:** `target1 = entry + 3 × (entry − stop)`; `risk_reward = reward/risk`
- **Storage key:** `{symbol}_{strategy}` via `save_trade_candidate()`
- **Confidence contract:** `normalize_strategy_confidence()` → 0–100 float

### Strategy confidence formulas (from code)

#### BREAKOUT
| Component | Points |
|-----------|--------|
| Base | 70 |
| UPTREND | +10 |
| RVOL ≥ 1.5 | +10 |
| MACD bullish | +10 |
| **Max** | **100** |

State: READY if RR≥2.5 else WATCHLIST.

#### VCP
| Component | Points |
|-----------|--------|
| Base | 75 |
| UPTREND | +10 |
| RVOL > 1.2 | +10 |
| MACD bullish | +5 |
| **Max** | **100** |

State: always READY (no RR/conf gate at save).

#### DEMAND & SUPPLY
| Component | Points |
|-----------|--------|
| UPTREND | +20 |
| TQI quality ≥ 70 | +20 |
| Zone strength ≥ 70 | +20 |
| Fresh demand zone | +20 |
| RVOL ≥ 1.2 | +10 |
| MACD bullish | +10 |
| **Max** | **100** |

State: READY if conf≥70 AND RR≥2.5.

#### ORDER BLOCK
| Component | Points |
|-----------|--------|
| Base | 70 |
| UPTREND | +10 |
| RVOL ≥ 1.2 | +10 |
| TQI quality ≥ 80 | +10 |
| **Max** | **100** |

#### FVG
| Component | Points |
|-----------|--------|
| Base | 70 |
| UPTREND | +10 |
| RVOL ≥ 1.20 | +10 |
| MACD bullish | +10 |
| **Max** | **100** |

#### PRICE SQUEEZE (two-phase)
Creation: up to +20 uptrend, +20 EMA stack, +20 squeeze≥50, +10 dryup, +10 NR7, +10 inside bar, +10 ATR contraction.  
Confirmation: +10 RVOL≥1.80, +5 MACD, +5 RSI 55–70.

---

## Stage 4: Multi-Strategy Consensus

**Function:** `build_ai_consensus()`

| Question | Answer |
|----------|--------|
| Can two strategies score same symbol? | Yes — separate keys `{symbol}_{strategy}` |
| How combined? | **Not averaged.** Best wins: `max(trades, key=(confidence, risk_reward))` |
| Strategy count effect? | `strategy_count × 5` added to **ai_score** only |
| Contradictory strategies? | Coexist in `trade_candidates`; only best advances |
| Example 80 vs 40 | 80 wins unless 40 has higher RR and equal confidence |

**Sector bonus** (`apply_sector_bonus`, applied per-candidate in scan before consensus):
- Sector score ≥80: +10 conf; ≥60: +5; ≤40: −5 (cap 100)

**Dead path:** `apply_market_regime_bonus()` (+10 bull, −15 bear, −5 sideways, +5 strength≥80) — **never called** in pipeline.

---

## Stage 5: Fast AI Score

### Formula

```
count_bonus     = strategy_count × 5
rr_bonus        = risk_reward × 5
raw_ai_score    = strategy_confidence + count_bonus + rr_bonus + batch1_bonus + batch2_bonus
final_ai_score  = raw_ai_score + analog_score_adjustment + news_effect_on_confidence*
decision        = APPROVED if final_ai_score ≥ minimum_fast_ai_score (70)
```

`*` **`news_effect_on_confidence` is NOT added to ai_score in current code** — stored on candidate only after breakdown computed.

### Batch 1 contents (`run_batch1_signal_engines`)

| Signal | Condition | Bonus |
|--------|-----------|-------|
| MTF alignment | 100 (all 3 TFs agree) | +8 |
| MTF alignment | ≥60 | +4 |
| Relative strength | ≥70 | +6 |
| Relative strength | ≤30 | −4 |
| Sector score | ≥80 | +5 |
| Sector score | ≤40 | −3 |
| Volume profile | ≥70 (ABOVE_POC) | +4 |
| **Clamp** | | **[-10, +25]** |

MTF alignment: Daily + 1H + 15M trend labels; 100 if all agree, 60 if agree partial, 30 if conflict.

### Batch 2 contents (`run_batch2_signal_engines`)

```
bonus = smart_money×1.0 + (institutional−50)×0.4 + false_breakout_penalty×1.0 + news_earnings_penalty×1.0
bonus = clamp(bonus, -30, +30)
```

**Smart money** (−15 to +30 clamp): BOS +15, bear BOS −10, CHOCH +8, OB +3 each (max 9), bull sweep +10, bear sweep −6, FVG +2 each (max 6), etc.

**Institutional** (0–100): base 50; OBV+ADL rising +15; both falling −15; partial ±6; vol z≥2 +8; absorption +10.

**False breakout:** prior failed breakout −15; exhaustion candle −8.

**News/earnings:** earnings ≤5d −15; ≥3 headlines −3; floor −20.

### Theoretical bounds (Fast path, no analog)

| Component | Min | Max | Typical |
|-----------|-----|-----|---------|
| strategy_confidence | 0 | 100 | 70–100 |
| count_bonus | 5 | 30 (6 strategies) | 10 |
| rr_bonus | 12.5 (RR=2.5) | unbounded* | 15 (RR=3) |
| batch1 | −10 | 25 | 4–8 |
| batch2 | −30 | 30 | 0–24 |
| **raw_ai_score** | **−40** | **~200+** | **120–160** |

*RR bonus scales with target geometry; strategies fix 3R so RR often ≈3.

### What threshold 70 means

A candidate with **strategy_confidence ≥ 70 alone passes** even with zero batch bonuses.  
With conf=60, needs +10 from batches/count/RR.  
With conf=50 and RR=3 (+15), count=2 (+10), needs batch1+batch2 ≥ −5.

### Dominance analysis

At acceptance-test values (conf=100, count=2, RR=3): strategy confidence = **63.6%** of raw_ai (100/157.2). Batch 2 = **15.4%**. No single batch component exceeds strategy confidence in typical strong setups.

---

## Stage 6: Deep Intelligence Path

Enabled when `require_deep_ai_before_entry=True` AND fast gate passed.

### Brain 1 — Market Observer
- **Role:** Observation only — no scores, no vetoes.
- **Outputs:** price, volume, breadth, sector, RS, news metadata.
- **Decision impact:** None (FAST or DEEP).

### Brain 2 — Market Historian
- **Inputs:** `stock.market["REGIME"]`, `MARKET_STRENGTH` from `detect_market_regime()`.
- **Similarity:** trend match +60, sideways partial +20; vol bucket match +40.
- **Output:** top 3 historical regime matches.
- **Decision impact:** DEEP only → `regime_context` → Brain 5 **MACRO** checks.

### Brain 3 — Historical Analog Engine
- **Similarity:** cosine on z-scored 8-feature vector; min 0.60; top 50 neighbors.
- **Win rate:** mean(forward_return > 0) at best horizon (5/10/20d).
- **Expected value:** `win_rate×expected_return − (1−win_rate)×|drawdown|`
- **Analog adjustment (Brain 4):** `clamp(EV × weight, −10, +10)` where weight = HIGH 40, MED 20, LOW 5.
- **60% vs 80% win rate:** Changes EV and thus analog adjustment up to ±10 ai_score points — **only if DEEP path enabled and Postgres populated**.

### Brain 4 — Strategist
- Orchestrates 1–3 enrichment; adjusts `ai_score` by analog term.
- **Does not duplicate Fast AI** — adds bounded analog adjustment only.
- **Gap:** Fast gate runs before enrichment; gate may not re-evaluate after analog adj unless breakdown block updates status.

---

## Stage 7: News Intelligence

**Module:** `news_intelligence.py` (default **disabled**)

| Field | Rule |
|-------|------|
| relevance | min(100, 20 + 15×symbol_matches) |
| sentiment | keyword POSITIVE/NEGATIVE/NEUTRAL |
| urgency | CRITICAL keywords → crash/halt/fraud |
| risk | CRITICAL→80; NEGATIVE→40; else 10 |
| confidence effect | +5 / −5 / 0 |
| **VETO** | urgency=CRITICAL AND risk≥70 |

**Separate from Batch 2** earnings penalty (always runs via yfinance prefetch).

---

## Stage 8: Brain 5 — Risk Manager

| Check | Type | Threshold |
|-------|------|-----------|
| EXPOSURE | **VETO** | open_count ≥ MAX_OPEN_POSITIONS (10) |
| CORRELATION | **VETO** | sector exposure + trade > **40%** |
| LIQUIDITY | **VETO** | no data; avg vol < 100k; turnover < 10M; position > **10%** avg volume |
| VOLATILITY | **VETO** | ATR/Close > **8%** OR RVOL > **4.0** |
| MACRO | **VETO** (DEEP) | TRENDING_BEAR + strength ≥ **70** |
| MACRO | **PENALTY** (DEEP) | mild bear: min_rr × **1.2** (3.0) |
| RISK_REWARD | **VETO** | RR < **2.5** (or 3.0 mild bear) |
| EVENT | **VETO** | earnings ≤ **5** days |

Plus overlay: **FAST_AI_GATE** (not Brain 5), **news_veto_reason**.

---

## Stage 9: Brain 6 — Portfolio Manager

**Ranking key:**
- FAST: `(expected_value=0, ai_score, confidence)`
- DEEP: `(expected_value, ai_score, confidence)`

**Position size** (`calculate_position_size`):
```
risk_amount = capital × (RISK_PER_TRADE / 100)    # default 1%
quantity = int(risk_amount / (entry - stop))
quantity = min(quantity, int(capital × 0.10 / entry))   # MAX_CAPITAL_PER_TRADE 10%
```

**Zero allocation reasons:** no slots; qty rounds to 0; insufficient cash; sector 40% cap exhausted.

---

## Stage 10: Entry Engine

**Function:** `entry_trigger_status()` — separate from trade quality.

| Condition | Type | Rule |
|-----------|------|------|
| Signal expiry | **REJECTION** | > signal_expiry_minutes (30) |
| Entry price | **WAIT** | price ≥ entry |
| VWAP | **WAIT** | price ≥ VWAP |
| Volume | **WAIT** | volume ≥ 20-bar avg |
| AI score | **WAIT** | ai_score ≥ 70 (60 in PAPER DISCOVERY) |

---

## Stage 11: Brain 7 — Reviewer & Learning Loop

**Trigger:** `PaperPosition.close_trade()` → `review_closed_trade()`

**Stores:** decisions, trade_outcomes, trade_reviews, calibration_state (Postgres via experience_memory)

**Calibration delta:** ±0.05 × {HIGH:1.0, MED:0.6, LOW:0.3}

**Feedback today:**
- ✅ Brain 3 `probability_of_success = win_rate + calibration_delta`
- ❌ Does NOT update Fast AI, strategy confidence, or Brain 4 analog ai_score adjustment

---

## FAST vs DEEP Path Summary

| Stage | FAST (default) | DEEP |
|-------|----------------|------|
| Brain 1 | Skipped | Called; no score impact |
| Brain 2 | Skipped | regime_context → Brain 5 MACRO |
| Brain 3 | Skipped | analog_report → Brain 4 ±10 ai_score |
| Brain 4 enrich | Skipped | expected_value → Brain 6 rank |
| Brain 5 | All except MACRO | Full including MACRO |
| Brain 6 | Rank by ai_score | Rank by expected_value |
| Brain 7 | Post-close only | Post-close only |

---

## Known Architectural Notes (factual, not recommendations)

1. **`apply_market_regime_bonus` never called** — regime bonus dead code.
2. **`news_effect_on_confidence` not in ai_score** — stored but not summed.
3. **Triple RR counting:** strategy gate, ai_score rr_bonus, Brain 5 RISK_REWARD.
4. **RVOL/trend/volume** appear in TQI, strategies, Batch 1/2, and Brain 5 VOLATILITY.
5. **Sector** in batch1_bonus AND apply_sector_bonus (confidence, separate from ai_score).
6. **Fast gate before deep enrichment** — analog adjustment may not re-gate consistently.

See `docs/SCORING_PARAMETER_CATALOG.md` for full parameter inventory and `reports/scoring_sensitivity.csv` for one-variable sensitivity examples.
