# AlphaQuant Scoring Parameter Catalog

**Branch:** `cursor/stabilization-aed7`  
**Audit type:** Read-only — values taken from current code only.

Legend: **HG** = Hard Gate · **V** = Veto · **S** = Score · **P** = Penalty · **SZ** = Position-size input · **C** = Context only

Missing → zero? Documented per parameter. Higher better? **↑** yes · **↓** penalty · **—** contextual.

---

## Global Configuration

| Engine | Parameter | Current Weight | Threshold | Range | Hard Gate | Veto | Used By | Duplicate Usage | Comments |
|--------|-----------|----------------|-----------|-------|-----------|------|---------|-----------------|----------|
| CONFIG | MIN_PRICE | — | 20 | price | HG | — | Fast screen, validate_trade_candidate | — | Below → filtered |
| CONFIG | MIN_AVG_VOLUME | — | 100,000 | shares | HG | — | Fast screen, Brain 5 LIQUIDITY | Volume in TQI, entry | Missing vol → fail |
| CONFIG | MIN_AVG_TURNOVER | — | 10,000,000 | INR | HG | V | Brain 5 LIQUIDITY | — | turnover = avg_vol×close |
| CONFIG | MAX_OPEN_POSITIONS | — | 10 | count | HG | V | Brain 5 EXPOSURE | Brain 6 slots | — |
| CONFIG | RISK_PER_TRADE | 1.0% | — | % capital | — | — | calculate_position_size | — | SZ input |
| CONFIG | MIN_RR / MIN_RISK_REWARD | — | 2.5 | ratio | HG | V | validate, Brain 5, strategies | ai_score RR×5 | Triple use |
| Workspace | minimum_fast_ai_score | — | 70 | score | HG | V | build_ai_consensus FAST gate | entry_trigger | Same threshold entry |
| Workspace | minimum_confidence | — | 70 | 0–100 | — | — | entry_trigger | UI filters | Discovery: max(50,60) |
| Workspace | signal_expiry_minutes | — | 30 | minutes | HG | — | entry_trigger_status | — | EXPIRED state |
| Workspace | require_deep_ai_before_entry | — | False | bool | — | — | build_ai_consensus | — | Enables DEEP path |
| Workspace | paper_trading_capital | — | 500,000 | INR | — | — | position size, Brain 6 | — | — |

---

## Trade Quality Index (TQI)

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| TQI | trend EMA20>50 | +5 | — | 0–20 | HG | — | validate (≥70) | Strategies uptrend | Missing EMA → 0 pts |
| TQI | trend EMA50>100 | +5 | — | 0–20 | HG | — | validate | — | — |
| TQI | trend EMA100>200 | +5 | — | 0–20 | HG | — | validate | — | — |
| TQI | trend Close>EMA20 | +5 | — | 0–20 | HG | — | validate | — | — |
| TQI | momentum RSI 55–70 | +8 | 55–70 | 0–19 | HG | — | validate | BREAKOUT PS confirm | RSI missing → 0 |
| TQI | momentum RSI >70 | +4 | >70 | 0–19 | HG | — | validate | — | Overbought partial |
| TQI | momentum MACD>signal | +7 | — | 0–19 | HG | — | validate | All strategies MACD | — |
| TQI | volume RVOL≥2 | +15 | 2.0 | 0–15 | HG | — | validate | Strategy RVOL, B5 vol | Quadruple RVOL |
| TQI | volume RVOL≥1.5 | +10 | 1.5 | 0–15 | HG | — | validate | — | — |
| TQI | volume RVOL≥1.2 | +5 | 1.2 | 0–15 | HG | — | validate | — | — |
| TQI | volatility ATR% 1–5 | +10 | 1–5% | 0–10 | HG | — | validate | Brain 5 ATR% | — |
| TQI | volatility ATR% <1 | +5 | <1% | 0–10 | HG | — | validate | — | — |
| TQI | risk ATR stop | +10 | fixed | 10 | HG | — | validate | position sizing | Always +10 if data |
| TQI | quality sum | — | **70** | 0–74+ | **HG** | — | validate_trade_candidate | — | Recalc at validation |
| TQI | UPTREND gate | — | UPTREND | enum | **HG** | — | validate | Strategies | Not SIDEWAYS/BEAR |

---

## Strategy Confidence (per TradeCandidate)

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Contract | normalize_strategy_confidence | — | — | 0–100 | — | — | save_trade_candidate | — | 0–1 scaled ×100 |
| BREAKOUT | base | 70 | — | 70–100 | S | — | Fast AI | — | Explicit zero if no signal |
| BREAKOUT | UPTREND | +10 | TREND==UPTREND | — | S | — | Fast AI | TQI, batch1 MTF | — |
| BREAKOUT | RVOL | +10 | ≥1.5 | — | S | — | Fast AI | TQI, detect_breakout | — |
| BREAKOUT | MACD | +10 | MACD>signal | — | S | — | Fast AI | TQI momentum | — |
| VCP | base | 75 | — | 75–100 | S | — | Fast AI | — | Always READY at save |
| DEMAND_SUPPLY | uptrend | +20 | UPTREND | — | S | — | Fast AI | — | Needs zone |
| DEMAND_SUPPLY | quality | +20 | TQI≥70 | — | S | — | Fast AI | validate gate | Double TQI |
| DEMAND_SUPPLY | zone strength | +20 | ≥70 | — | S | — | Fast AI | — | — |
| DEMAND_SUPPLY | fresh demand | +20 | pattern | — | S | — | Fast AI | — | — |
| ORDER_BLOCK | quality | +10 | TQI≥80 | — | S | — | Fast AI | validate 70 | Stricter |
| PRICE_SQUEEZE | multi-component | varies | squeeze≥50 | 0–100 | S | — | Fast AI | — | Two-phase confirm |
| Sector bonus | apply_sector_bonus | +10/+5/−5 | 80/60/40 | 0–100 | S | — | consensus pick | batch1 sector | **Separate from ai_score** |
| Regime bonus | apply_market_regime_bonus | +10/−15/−5 | — | — | — | — | **NOT CALLED** | detect_market_regime | Dead path |

---

## Batch 1 Signal Engines

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Batch1 | MTF alignment | +8 | 100 | 30/60/100 | S | — | raw_ai_score | Daily trend elsewhere | Missing → 0 bonus |
| Batch1 | MTF alignment | +4 | ≥60 | — | S | — | raw_ai_score | — | — |
| Batch1 | relative_strength | +6 | ≥70 | 0–100 RS | S | — | raw_ai_score | Brain 1 RS | RS=50+2×delta% |
| Batch1 | relative_strength | −4 | ≤30 | — | P | — | raw_ai_score | — | — |
| Batch1 | sector score | +5 | ≥80 | 0–100 | S | — | raw_ai_score | apply_sector_bonus | Double sector |
| Batch1 | sector score | −3 | ≤40 | — | P | — | raw_ai_score | — | — |
| Batch1 | volume_profile | +4 | ≥70 ABOVE_POC | 30/50/70 | S | — | raw_ai_score | — | 120d lookback |
| Batch1 | clamp | — | — | **[-10, 25]** | — | — | batch1_bonus | — | Missing components → 0 |

---

## Batch 2 Signal Engines

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Batch2 | smart_money | ×1.0 | — | [-15,30] | S/P | — | raw_ai_score | Strategy OB/FVG/LQ | Post-strategy patterns |
| Batch2 | institutional | ×0.4 | base 50 | [0,100] | S/P | — | raw_ai_score | vol z in B5 | (score−50)×0.4 |
| Batch2 | false_breakout | ×1.0 | — | 0/−15/−8 | P | — | raw_ai_score | BREAKOUT pattern | — |
| Batch2 | news_earnings | ×1.0 | 5d/3 headlines | [−20,0] | P | — | raw_ai_score | Brain 5 EVENT, NewsIntel | Triple earnings |
| Batch2 | clamp | — | — | **[-30, 30]** | — | — | batch2_bonus | — | — |
| Batch2 | ai_confidence | — | — | 50+bonus | C | — | display | — | Not same as ai_score |

### Smart Money sub-scores

| Parameter | Points |
|-----------|--------|
| BOS bullish | +15 |
| BOS bearish | −10 |
| CHOCH | +8 |
| Bullish OB each | +3 (max 9) |
| Bearish OB dominates | −5 |
| Bullish sweep | +10 |
| Bearish sweep | −6 |
| Bullish FVG each | +2 (max 6) |
| Bearish FVG dominates | −3 |

### Institutional sub-scores

| Parameter | Points |
|-----------|--------|
| Base | 50 |
| OBV+ADL rising | +15 |
| OBV+ADL falling | −15 |
| Partial alignment | ±6 |
| Volume z≥2 | +8 |
| Absorption candle | +10 |

---

## Fast AI Consensus

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Fast AI | strategy_confidence | ×1 | — | 0–100 | S | — | raw_ai | strategy engines | Dominant term |
| Fast AI | strategy_count_bonus | ×5 per strategy | — | 5–30+ | S | — | raw_ai | — | Count not quality |
| Fast AI | risk_reward_contribution | ×5 per RR unit | — | 12.5+ | S | — | raw_ai | MIN_RR gate, B5 | Triple RR |
| Fast AI | batch1_contribution | ×1 | — | [-10,25] | S | — | raw_ai | — | — |
| Fast AI | batch2_contribution | ×1 | — | [-30,30] | S | — | raw_ai | — | — |
| Fast AI | raw_ai_score | sum | — | ~−40 to 200 | S | — | gate | — | — |
| Fast AI | FAST gate | — | **70** | — | **HG** | **V** | Brain 5 entry | entry monitor | AI_REJECTED |
| Fast AI | news_effect_on_confidence | ±5 | — | −5/0/+5 | — | — | **NOT in ai_score** | NewsIntel | Stored only |
| Deep | analog_score_adjustment | EV×weight | cap ±10 | [-10,10] | S | — | ai_score DEEP | Brain 3 EV | Only if DEEP+DB |

---

## Brain 1 — Market Observer

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Brain1 | observe() outputs | — | — | dict | C | — | evidence_summary | Batch1 RS, sector | No scoring |
| Brain1 | sector relative_rank | — | 60 | rank | C | — | evidence display | batch1 sector | Direction ± only |

---

## Brain 2 — Market Historian

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Brain2 | regime similarity trend | +60 | match | 0–100 | C | — | regime_context | detect_market_regime | DEEP only |
| Brain2 | regime similarity vol | +40 | bucket | 0–100 | C | — | Brain 5 MACRO | — | — |
| Brain2 | current_regime_strength | — | 70 | 0–100 | — | V | Brain 5 bear veto | MARKET_STRENGTH monolith | DEEP only |

### detect_market_regime (monolith, feeds Brain 2)

| Parameter | Effect on strength |
|-----------|-------------------|
| Base | 50 |
| Bull/bear EMA stack | +20, sets REGIME |
| ADX ≥ 25 | +10 |
| RVOL ≥ 1.5 | +10 |
| ATR > 20d avg | +10 |
| Gap up/down | +5 |
| EMA20 rising 5 bars | +5 |
| Cap | 100 |

---

## Brain 3 — Historical Analog Engine

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Brain3 | MIN_SIMILARITY | — | 0.60 | 0–1 | — | — | neighbor filter | — | Cosine similarity |
| Brain3 | TOP_N_NEIGHBORS | — | 50 | — | — | — | analog set | — | — |
| Brain3 | win_rate | — | — | 0–1 | S | — | Brain 4 EV | — | mean(ret>0) |
| Brain3 | expected_return | — | — | % | S | — | Brain 4 EV | — | best horizon |
| Brain3 | sample_confidence HIGH | — | n≥40 | — | C | — | analog weight 40 | — | — |
| Brain3 | sample_confidence MED | — | n≥10 | — | C | — | weight 20 | — | — |
| Brain3 | probability_of_success | win_rate+cal | — | 0–1 | S | — | **NOT in ai_score** | Brain 7 cal | Feedback loop |
| Brain3 | setup_vector features | — | — | 15 features | C | — | similarity | TQI/strategies | See setup_vector.py |

---

## Brain 4 — Strategist

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| Brain4 | candidate pick | max(conf,RR) | — | — | S | — | per-symbol winner | — | Not ai_score pick |
| Brain4 | ANALOG_SCORE_WEIGHT | 5/20/40 | — | — | S | — | analog adj | — | × expected_value |
| Brain4 | ANALOG_SCORE_CAP | — | ±10 | — | S | — | ai_score | — | DEEP only |
| Brain4 | expected_value | formula | — | float | S | — | Brain 6 rank | — | DEEP rank primary |
| Brain4 | evidence_summary weights | 1–3 | — | — | C | — | Brain 7 review | — | Display/learning |

---

## News Intelligence

| Engine | Parameter | Weight | Threshold | Range | HG | V | Used By | Duplicate | Comments |
|--------|-----------|--------|-----------|-------|----|---|---------|-------------|----------|
| News | enabled | — | False default | bool | — | — | all news | — | Disabled → NO_NEWS |
| News | relevance | 20+15×symbols | — | 0–100 | C | — | candidate_effect | — | — |
| News | sentiment effect | ±5 | POS/NEG | −5/0/+5 | — | — | **not ai_score** | batch2 headlines | — |
| News | risk score | 10/40/80 | CRITICAL | 0–100 | — | V | veto overlay | — | risk≥70 + CRITICAL |
| News | CRITICAL keywords | — | — | — | — | **V** | Brain 5 overlay | — | Hard veto |

---

## Brain 5 — Risk Manager

| Engine | Parameter | Threshold | Type | Veto Code | Used By |
|--------|-----------|-----------|------|-----------|---------|
| Brain5 | max open positions | 10 | HG/V | EXPOSURE | Pre-entry |
| Brain5 | sector exposure cap | 40% | HG/V | CORRELATION | Pre-entry |
| Brain5 | min avg volume | 100,000 | HG/V | LIQUIDITY | Pre-entry |
| Brain5 | min turnover | 10,000,000 | HG/V | LIQUIDITY | Pre-entry |
| Brain5 | position vs avg vol | 10% | HG/V | LIQUIDITY | SZ check |
| Brain5 | ATR% of price | 8% | HG/V | VOLATILITY | Pre-entry |
| Brain5 | RVOL ceiling | 4.0× | HG/V | VOLATILITY | Pre-entry |
| Brain5 | min risk/reward | 2.5 (3.0 mild bear) | HG/V | RISK_REWARD | Pre-entry |
| Brain5 | bear regime strength | 70 | HG/V | MACRO | DEEP only |
| Brain5 | earnings window | 5 days | HG/V | EVENT | Pre-entry |

---

## Brain 6 — Portfolio Manager

| Engine | Parameter | Formula / Rule | Zero allocation cause |
|--------|-----------|----------------|----------------------|
| Brain6 | rank key FAST | (0, ai_score, confidence) | — |
| Brain6 | rank key DEEP | (expected_value, ai_score, confidence) | — |
| Brain6 | position size | risk 1% / stop distance, cap 10% capital | qty=0 |
| Brain6 | sector cap | 40% — may shrink qty | headroom=0 |
| Brain6 | slots | max_positions − open | no slots |
| Brain6 | cash | capital_required ≤ remaining | insufficient cash |

---

## Entry Monitor

| Engine | Parameter | Threshold | Type | Blocks |
|--------|-----------|-----------|------|--------|
| Entry | signal expiry | 30 min | REJECTION | EXPIRED |
| Entry | price vs entry | price ≥ entry | WAIT | WAITING_PRICE |
| Entry | VWAP | price ≥ VWAP | WAIT | WAITING_VWAP |
| Entry | volume | vol ≥ avg20 | WAIT | WAITING_VOLUME |
| Entry | ai_score | ≥70 (60 discovery) | WAIT | WAITING_AI_SCORE |

---

## Brain 7 — Reviewer

| Engine | Parameter | Value | Feeds back? |
|--------|-----------|-------|-------------|
| Brain7 | was_correct | STOP→False, TARGET→True, else pnl>0 | calibration only |
| Brain7 | calibration_delta | ±0.05 × confidence scale | Brain 3 prob_success |
| Brain7 | evidence mattered/misled | hindsight | stored only |
| Brain7 | lessons_learned | text | stored only |

**Does NOT feed:** Fast AI, strategy confidence, Brain 4 ai_score, Brain 5 thresholds.

---

## Technical Indicators Inventory

| Indicator | Calculation | Timeframe | Used By | Purpose | Weight/Threshold | Hard Gate? | Missing data |
|-----------|-------------|-----------|---------|---------|------------------|------------|--------------|
| EMA 20/50/100/200 | ta.ema | Daily 1y | TQI, strategies, regime | Trend | +5 each TQI | TQI/UPTREND | No indicator → 0 score |
| RSI 14 | ta.rsi | Daily | TQI, PS confirm | Momentum | 55–70 +8 | TQI | 0 momentum pts |
| MACD 12/26/9 | ta.macd | Daily | TQI, all strategies | Momentum | +7 TQI, +5–10 strat | — | 0 |
| ADX 14 | ta.adx | Daily | detect_market_regime | Trend strength | ≥25 +10 strength | — | skip check |
| ATR 14 | ta.atr | Daily | TQI, stops, Brain5 | Risk/vol | 8% veto; 1–5% +10 TQI | B5 VOLATILITY | veto if no data |
| Bollinger 20,2 | ta.bbands | Daily | PRICE_SQUEEZE | Squeeze width | percentile tiers | — | no squeeze |
| VWAP | ta.vwap | Daily session | entry_trigger | Entry confirm | price≥VWAP | Entry WAIT | uses price fallback |
| RVOL | Vol/AVG_VOL20 | Daily | TQI, strategies, B5 | Volume | 1.2–2.0 tiers; 4.0 veto | B5 | 0 volume pts |
| AVG_VOLUME20 | rolling 20 | Daily | Fast screen, entry | Liquidity | 100k min | HG | filtered |
| HIGH52/LOW52 | 252 roll | Daily | setup_vector | Distance features | analog only | — | analog empty |
| Volume profile POC | 120d bins | Daily | Batch1 | Structure | ABOVE_POC=70 | — | None → 50 default |
| MTF 1H/15M | yfinance intraday | 60d/5d | Batch1 | Alignment | 100/60/30 | — | UNKNOWN excluded |
| Relative strength | 63d vs NIFTY | Daily | Batch1 | RS score | 50+2×delta | — | None → 50 default |
| OBV/ADL proxy | computed | 20d | Batch2 institutional | Flow | ±15 | — | score=50 |
| BOS/CHOCH | swing logic | Daily | Smart money | Structure | +15/+8 | — | false |
| Order blocks | 40 bar | Daily | ORDER_BLOCK strat | Entry zone | conf +70 base | — | no candidate |
| FVG | 0.30% gap | Daily | FVG strat | Entry zone | conf +70 | — | no candidate |
| VCP contraction | 120 bar swings | Daily | VCP strat | Pattern | base 75 | — | no candidate |
| Breakout level | 20 high +0.2% | Daily | BREAKOUT | Entry | base 70 | — | no candidate |
| Demand/supply zones | 20 bar base | Daily | DS strat | Entry | zone strength 75 | — | no candidate |
| Liquidity sweep | 15 bar | Daily | **unregistered** | Pattern | base 75 | — | dead path |
| Supertrend | — | — | **NOT FOUND** | — | — | — | — |
| Delivery volume | — | — | **NOT FOUND** | — | — | — | — |
| ROC/Momentum ind | — | — | **NOT FOUND** as named | — | — | — | — |
| Candlestick patterns | partial | Daily | exhaustion, NR7, inside | Pattern | PS bonuses | — | — |

---

## Weight Concentration Summary

| Component | Max Contribution | Typical % of raw_ai | Hard Gate | Can Veto | Double Counting |
|-----------|------------------|---------------------|-----------|----------|-----------------|
| strategy_confidence | 100 | 55–70% | via TQI/strat gates | via low conf | Sector via bonus too |
| strategy_count×5 | 30 | 5–10% | — | — | — |
| risk_reward×5 | 15+ | 8–12% | MIN_RR 2.5 | Brain5 RR | Strategy + AI + B5 |
| batch1_bonus | 25 | 3–8% | — | — | Trend/sector/RS |
| batch2_bonus | 30 | 10–20% | — | — | Volume, news, patterns |
| analog adjustment | 10 | 0% FAST | — | — | Historical via EV |
| news confidence | 5 | **0% (not summed)** | — | CRITICAL veto | batch2 earnings |
| TQI quality | 74 | pre-AI gate | **70** | WATCHLIST | Overlaps strategy inputs |
| Brain 5 checks | — | — | multiple | **YES** | Liquidity/vol/RR |
| Entry gates | — | — | expiry | WAIT | Separate from quality |

---

*End of catalog. See `docs/SCORING_ARCHITECTURE.md` for flow and `reports/scoring_sensitivity.csv` for one-variable scenarios.*
