"""
AlphaQuant Scoring Engine V2 — the authoritative 0–100 trade confidence.

Separate from Risk (Brain 5), Portfolio (Brain 6), and Entry timing. Legacy
scores may be displayed for old records, but are never a production path.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable

# Top-level component weights (must sum to 100)
WEIGHT_STRUCTURE = 30.0
WEIGHT_PARTICIPATION = 20.0
WEIGHT_MOMENTUM = 20.0
WEIGHT_MARKET_SECTOR = 10.0
WEIGHT_HISTORICAL = 10.0
WEIGHT_NEWS = 10.0

MAX_CONFLUENCE_BONUS = 5.0

# Strategy → independent signal families (for confluence, not raw count)
STRATEGY_SIGNAL_FAMILIES: dict[str, list[str]] = {
    "BREAKOUT": ["STRUCTURE", "MOMENTUM", "VOLUME"],
    "VCP": ["STRUCTURE", "VOLATILITY_COMPRESSION", "VOLUME"],
    "DEMAND_SUPPLY": ["STRUCTURE", "MEAN_REVERSION"],
    "ORDER_BLOCK": ["STRUCTURE", "SMART_MONEY"],
    "FVG": ["STRUCTURE", "SMART_MONEY"],
    "PRICE SQUEEZE": ["VOLATILITY_COMPRESSION", "STRUCTURE"],
    "LIQUIDITY_SWEEP": ["STRUCTURE", "SMART_MONEY"],
}

# Historical evidence sample-size scaling
def _historical_sample_factor(n: int | None) -> float:
    if n is None or n <= 0:
        return 0.0
    if n < 10:
        return 0.15
    if n < 30:
        return 0.45
    if n < 100:
        return 0.75
    return 1.0


@dataclass
class SubScore:
    name: str
    max_points: float
    raw_value: float | None
    normalized: float  # 0..max_points
    explanation: str = ""
    missing: bool = False


@dataclass
class ComponentScore:
    component: str
    weight: float
    subscores: list[SubScore] = field(default_factory=list)
    raw_total: float = 0.0
    weighted_contribution: float = 0.0
    missing_inputs: list[str] = field(default_factory=list)
    explanations: list[str] = field(default_factory=list)
    data_freshness: str = ""

    def finalize(self) -> None:
        self.raw_total = sum(s.normalized for s in self.subscores)
        self.weighted_contribution = round(min(self.weight, self.raw_total), 2)
        self.explanations = [s.explanation for s in self.subscores if s.explanation and not s.missing]


@dataclass
class TradeScoreV2:
    score_version: str = "V2"
    trade_confidence: float = 0.0
    structure: ComponentScore = field(default_factory=lambda: ComponentScore("structure", WEIGHT_STRUCTURE))
    participation: ComponentScore = field(default_factory=lambda: ComponentScore("participation", WEIGHT_PARTICIPATION))
    momentum: ComponentScore = field(default_factory=lambda: ComponentScore("momentum", WEIGHT_MOMENTUM))
    market_sector: ComponentScore = field(default_factory=lambda: ComponentScore("market_sector", WEIGHT_MARKET_SECTOR))
    historical: ComponentScore = field(default_factory=lambda: ComponentScore("historical", WEIGHT_HISTORICAL))
    news: ComponentScore = field(default_factory=lambda: ComponentScore("news", WEIGHT_NEWS))
    confluence_bonus: float = 0.0
    confluence_families: list[str] = field(default_factory=list)
    positive_reasons: list[str] = field(default_factory=list)
    watch_items: list[str] = field(default_factory=list)
    entry_blocker: str = ""
    risk_status: str = ""
    entry_status: str = ""
    computed_at: str = ""
    gate_threshold: float = 70.0
    gate_decision: str = "PENDING"
    scoring_profile: str = "AlphaQuant Default"
    scoring_version: int = 1
    scoring_weights_snapshot: dict[str, float] = field(default_factory=dict)

    def all_components(self) -> list[ComponentScore]:
        return [
            self.structure,
            self.participation,
            self.momentum,
            self.market_sector,
            self.historical,
            self.news,
        ]

    def finalize(self, threshold: float = 70.0) -> None:
        total = sum(c.weighted_contribution for c in self.all_components())
        total = min(100.0, max(0.0, round(total + self.confluence_bonus, 2)))
        self.trade_confidence = total
        self.gate_threshold = threshold
        self.gate_decision = "APPROVED" if total >= threshold else "REJECTED"
        self.computed_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["components"] = {
            c.component: {
                "weight": c.weight,
                "score": f"{c.weighted_contribution:.0f}/{c.weight:.0f}",
                "weighted_contribution": c.weighted_contribution,
                "missing_inputs": c.missing_inputs,
                "explanations": c.explanations,
                "subscores": [asdict(s) for s in c.subscores],
            }
            for c in self.all_components()
        }
        return d

    def to_gate_breakdown(self) -> dict[str, Any]:
        """Compatibility shape for existing diagnostics expecting ai_score_breakdown."""
        return {
            "scoring_engine_version": "V2",
            "strategy_confidence": self.trade_confidence,
            "strategy_count_bonus": 0,
            "risk_reward_contribution": 0,
            "batch1_contribution": None,
            "batch2_contribution": None,
            "historical_analog_contribution": self.historical.weighted_contribution,
            "strategist_contribution": None,
            "news_contribution": self.news.weighted_contribution,
            "raw_ai_score": self.trade_confidence,
            "final_ai_score": self.trade_confidence,
            "threshold": self.gate_threshold,
            "decision": self.gate_decision,
            "missing_inputs": [
                m for c in self.all_components() for m in c.missing_inputs
            ],
            "deep_ai_path": "V2_HIERARCHICAL",
            "v2_components": {
                c.component: c.weighted_contribution for c in self.all_components()
            },
        }


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _last_row(stock) -> dict[str, Any] | None:
    df = getattr(stock, "data", None)
    if df is None or df.empty:
        return None
    return df.iloc[-1].to_dict()


def _safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _ema_slope(df, col: str = "EMA20", lookback: int = 5) -> float:
    if df is None or len(df) < lookback + 1 or col not in df.columns:
        return 0.0
    recent = df[col].iloc[-lookback - 1 :]
    if recent.iloc[0] == 0:
        return 0.0
    return (recent.iloc[-1] - recent.iloc[0]) / abs(recent.iloc[0]) * 100


def _macd_accel(df) -> float:
    if df is None or len(df) < 4 or "MACD" not in df.columns:
        return 0.0
    macd = df["MACD"].iloc[-4:]
    deltas = macd.diff().dropna()
    if deltas.empty:
        return 0.0
    return float(deltas.iloc[-1] - deltas.iloc[-2]) if len(deltas) >= 2 else float(deltas.iloc[-1])


def _rsi_divergence_bearish(df, lookback: int = 14) -> bool:
    if df is None or len(df) < lookback + 5 or "RSI" not in df.columns:
        return False
    price = df["Close"].iloc[-lookback:]
    rsi = df["RSI"].iloc[-lookback:]
    price_higher = price.iloc[-1] > price.iloc[0]
    rsi_lower = rsi.iloc[-1] < rsi.iloc[0]
    return price_higher and rsi_lower and _safe_float(rsi.iloc[-1]) > 65


def _compute_confluence(all_strategies: list[str]) -> tuple[float, list[str]]:
    families: set[str] = set()
    for strat in all_strategies:
        for fam in STRATEGY_SIGNAL_FAMILIES.get(strat, ["STRUCTURE"]):
            families.add(fam)
    if len(families) <= 1:
        return 0.0, sorted(families)
    bonus = min(MAX_CONFLUENCE_BONUS, (len(families) - 1) * 2.5)
    return round(bonus, 2), sorted(families)


# ---------------------------------------------------------------------------
# Strategy-specific structure profiles
# ---------------------------------------------------------------------------

def _score_breakout_structure(stock, candidate) -> ComponentScore:
    comp = ComponentScore("structure", WEIGHT_STRUCTURE)
    last = _last_row(stock)
    df = stock.data
    missing = []

    if last is None:
        comp.missing_inputs = ["MISSING:price_data"]
        return comp

    patterns = getattr(stock, "patterns", {}) or {}
    indicators = getattr(stock, "indicators", {}) or {}

    # Breakout/resistance quality (8)
    brk_pts = 0.0
    if patterns.get("BREAKOUT_READY"):
        brk_pts += 5.0
    if patterns.get("BREAKOUT") and not patterns.get("FALSE_BREAKOUT"):
        brk_pts += 2.0
    if not patterns.get("FALSE_BREAKOUT"):
        brk_pts += 1.0
    else:
        brk_pts = max(0, brk_pts - 4)
    if patterns.get("BREAKOUT_EXHAUSTION"):
        brk_pts = max(0, brk_pts - 3)
    brk_pts = _clamp(brk_pts, 0, 8)
    comp.subscores.append(SubScore(
        "breakout_resistance_quality", 8, brk_pts, brk_pts,
        "Clean resistance breakout" if brk_pts >= 6 else "Weak or questionable breakout",
    ))

    # Support/resistance structure (6)
    sr_pts = 0.0
    close = _safe_float(last.get("Close"))
    ema20 = _safe_float(last.get("EMA20"))
    if ema20 and close > ema20:
        sr_pts += 2.0
    resistance = indicators.get("BREAKOUT_LEVEL")
    if resistance and close >= _safe_float(resistance) * 0.998:
        sr_pts += 2.0
    if len(getattr(stock, "patterns", {}).get("FRESH_DEMAND", []) or []) > 0:
        sr_pts += 2.0
    elif ema20 and close > ema20 * 1.01:
        sr_pts += 1.0
    sr_pts = _clamp(sr_pts, 0, 6)
    comp.subscores.append(SubScore(
        "support_resistance_structure", 6, sr_pts, sr_pts,
        "Support structure intact below" if sr_pts >= 4 else "Unclear support/resistance",
    ))

    # Demand/supply positioning (6)
    ds_pts = 0.0
    fresh_demand = patterns.get("FRESH_DEMAND") or []
    fresh_supply = patterns.get("FRESH_SUPPLY") or []
    if fresh_demand:
        ds_pts += 4.0
    if not fresh_supply or close > (fresh_supply[0].get("Low", close) if fresh_supply else close):
        ds_pts += 2.0
    if fresh_supply and close < (fresh_supply[0].get("High", 0) if fresh_supply else 0):
        ds_pts = max(0, ds_pts - 3)
    ds_pts = _clamp(ds_pts, 0, 6)
    comp.subscores.append(SubScore(
        "demand_supply_positioning", 6, ds_pts, ds_pts,
        "Demand zone support below" if ds_pts >= 4 else "Supply overhead or weak demand",
    ))

    # Breakout acceptance/retest (5)
    retest_pts = 3.0 if patterns.get("BREAKOUT_READY") else 1.0
    if patterns.get("FALSE_BREAKOUT"):
        retest_pts = 0.0
    retest_pts = _clamp(retest_pts, 0, 5)
    comp.subscores.append(SubScore(
        "breakout_acceptance_retest", 5, retest_pts, retest_pts,
        "Breakout holding above level" if retest_pts >= 3 else "Failed retest or unconfirmed",
    ))

    # Entry location/extension (3)
    ext_pts = 2.0
    atr = _safe_float(last.get("ATR"), 1)
    entry = _safe_float(getattr(candidate, "entry", close))
    if resistance and atr > 0:
        ext = (close - _safe_float(resistance)) / atr
        if ext > 2.5:
            ext_pts = 0.5
            comp.explanations.append("Excessive extension from breakout")
        elif ext > 1.5:
            ext_pts = 1.0
    high52 = _safe_float(last.get("HIGH52"))
    if high52 and close > high52 * 0.995:
        ext_pts = max(0, ext_pts - 1.0)
        comp.explanations.append("Breakout into major 52w resistance zone")
    ext_pts = _clamp(ext_pts, 0, 3)
    comp.subscores.append(SubScore(
        "entry_location_extension", 3, ext_pts, ext_pts,
        "Room before next resistance" if ext_pts >= 2 else "Extended or into major resistance",
    ))

    # Higher-timeframe structure (2)
    mtf = _safe_float(stock.score.get("mtf_alignment"), 30)
    htf_pts = 2.0 if mtf >= 100 else 1.5 if mtf >= 60 else 0.5 if mtf >= 30 else 0.0
    comp.subscores.append(SubScore(
        "higher_timeframe_structure", 2, mtf, htf_pts,
        "Higher timeframe aligned" if htf_pts >= 1.5 else "Mixed higher timeframe structure",
    ))

    comp.finalize()
    return comp


def _score_generic_structure(stock, candidate, profile: str) -> ComponentScore:
    """Simplified structure scoring for non-BREAKOUT strategies."""
    if profile == "BREAKOUT":
        return _score_breakout_structure(stock, candidate)

    comp = ComponentScore("structure", WEIGHT_STRUCTURE)
    last = _last_row(stock)
    if last is None:
        comp.missing_inputs = ["MISSING:price_data"]
        return comp

    patterns = getattr(stock, "patterns", {}) or {}
    close = _safe_float(last.get("Close"))
    ema20 = _safe_float(last.get("EMA20"))

    # Pattern presence (12)
    pat_pts = 6.0
    if profile == "VCP" and patterns.get("VCP"):
        pat_pts = 10.0
    elif profile == "DEMAND_SUPPLY" and patterns.get("FRESH_DEMAND"):
        pat_pts = 10.0
    elif profile in ("ORDER_BLOCK", "FVG") and (
        patterns.get("BULLISH_ORDER_BLOCKS") or patterns.get("BULLISH_FVG")
    ):
        pat_pts = 9.0
    elif profile == "PRICE SQUEEZE" and _safe_float(stock.patterns.get("PRICE_SQUEEZE", 0)) >= 50:
        pat_pts = 9.0
    comp.subscores.append(SubScore("pattern_quality", 12, pat_pts, _clamp(pat_pts, 0, 12), f"{profile} pattern quality"))

    # Support (8)
    sup_pts = 4.0 if ema20 and close > ema20 else 1.0
    comp.subscores.append(SubScore("support_structure", 8, sup_pts, _clamp(sup_pts, 0, 8), "Support below"))

    # Extension/risk of chase (5)
    ext_pts = 3.0
    comp.subscores.append(SubScore("extension", 5, ext_pts, _clamp(ext_pts, 0, 5), "Entry extension"))

    # HTF (5)
    mtf = _safe_float(stock.score.get("mtf_alignment"), 30)
    htf_pts = 4.0 if mtf >= 60 else 2.0
    comp.subscores.append(SubScore("htf_structure", 5, mtf, _clamp(htf_pts, 0, 5), "HTF structure"))

    comp.finalize()
    return comp


def _score_participation(stock, *, used_rvol_in_structure: bool = False) -> ComponentScore:
    comp = ComponentScore("participation", WEIGHT_PARTICIPATION)
    last = _last_row(stock)
    if last is None:
        comp.missing_inputs = ["MISSING:price_data"]
        return comp

    rvol = _safe_float(last.get("RVOL"))
    close = _safe_float(last.get("Close"))
    vwap = _safe_float(last.get("VWAP"), close)
    vol = _safe_float(last.get("Volume"))

    # Relative/breakout volume (8) — confirmation, not liquidity gate
    if used_rvol_in_structure:
        vol_pts = 4.0 if rvol >= 1.5 else 2.0 if rvol >= 1.2 else 0.5
        comp.explanations.append("RVOL partially credited in structure; participation uses confirmation tier")
    else:
        vol_pts = 8.0 if rvol >= 2.0 else 6.0 if rvol >= 1.5 else 3.0 if rvol >= 1.2 else 0.0
    comp.subscores.append(SubScore(
        "relative_breakout_volume", 8, rvol, _clamp(vol_pts, 0, 8),
        f"RVOL {rvol:.1f}x" + (" — strong participation" if rvol >= 1.5 else ""),
    ))

    # VWAP relationship (5)
    if vwap <= 0:
        vwap_pts = 0.0
        missing_vwap = True
    elif close >= vwap * 1.002:
        vwap_pts = 5.0
        missing_vwap = False
    elif close >= vwap * 0.998:
        vwap_pts = 3.0
        missing_vwap = False
    else:
        vwap_pts = 1.0
        missing_vwap = False
        comp.explanations.append("Below VWAP — valid setup but entry may wait")
    comp.subscores.append(SubScore(
        "vwap_relationship", 5, close - vwap, _clamp(vwap_pts, 0, 5),
        "Above VWAP" if vwap_pts >= 4 else "Near/below VWAP",
        missing=missing_vwap and vwap <= 0,
    ))

    # Volume expansion (3)
    df = stock.data
    avg20 = float(df["Volume"].tail(20).mean()) if df is not None and len(df) >= 20 else vol
    exp_ratio = vol / avg20 if avg20 else 1.0
    exp_pts = 3.0 if exp_ratio >= 1.8 else 2.0 if exp_ratio >= 1.3 else 0.5
    comp.subscores.append(SubScore(
        "volume_expansion", 3, exp_ratio, _clamp(exp_pts, 0, 3),
        "Volume expanding on signal bar" if exp_pts >= 2 else "Muted volume expansion",
    ))

    # Smart money (2) — distinct from raw RVOL
    sm = _safe_float(stock.score.get("smart_money"), 0)
    sm_pts = 2.0 if sm >= 15 else 1.0 if sm >= 8 else 0.0
    comp.subscores.append(SubScore(
        "institutional_smart_money", 2, sm, _clamp(sm_pts, 0, 2),
        "Smart-money concepts supportive" if sm_pts >= 1 else "",
    ))

    # OBV/ADL (2)
    obv_trend = stock.indicators.get("OBV_TREND") if hasattr(stock, "indicators") else None
    adl = _safe_float(stock.indicators.get("ADL_TREND", 0) if hasattr(stock, "indicators") else 0)
    acc_pts = 0.0
    if obv_trend == "RISING" and adl > 0:
        acc_pts = 2.0
    elif obv_trend == "RISING" or adl > 0:
        acc_pts = 1.0
    comp.subscores.append(SubScore(
        "obv_adl_accumulation", 2, adl, _clamp(acc_pts, 0, 2),
        "Accumulation evidence" if acc_pts >= 1 else "",
    ))

    comp.finalize()
    return comp


def _score_momentum(stock) -> ComponentScore:
    comp = ComponentScore("momentum", WEIGHT_MOMENTUM)
    last = _last_row(stock)
    df = stock.data
    if last is None:
        comp.missing_inputs = ["MISSING:price_data"]
        return comp

    rsi = _safe_float(last.get("RSI"), 50)
    adx = _safe_float(last.get("ADX"), 0)
    macd = _safe_float(last.get("MACD"))
    macd_sig = _safe_float(last.get("MACD_SIGNAL"))
    rs = _safe_float(stock.score.get("relative_strength"), 50)

    # RS / price momentum (5)
    rs_pts = 5.0 if rs >= 70 else 3.5 if rs >= 55 else 1.0 if rs >= 45 else 0.0
    comp.subscores.append(SubScore("relative_strength", 5, rs, _clamp(rs_pts, 0, 5), f"RS score {rs:.0f}"))

    # ADX trend strength (4)
    adx_pts = 4.0 if adx >= 30 else 2.5 if adx >= 25 else 1.0 if adx >= 20 else 0.0
    comp.subscores.append(SubScore("adx_trend_strength", 4, adx, _clamp(adx_pts, 0, 4), f"ADX {adx:.1f}"))

    # EMA structure + slope (4)
    ema_pts = 0.0
    if _safe_float(last.get("EMA20")) > _safe_float(last.get("EMA50")) > _safe_float(last.get("EMA100")):
        ema_pts += 2.5
    slope = _ema_slope(df, "EMA20", 5)
    if slope > 0.3:
        ema_pts += 1.5
    elif slope > 0:
        ema_pts += 0.5
    comp.subscores.append(SubScore("ema_structure_slope", 4, slope, _clamp(ema_pts, 0, 4), "EMA stack bullish"))

    # MACD momentum/acceleration (3)
    macd_pts = 0.0
    if macd > macd_sig:
        macd_pts += 1.5
    accel = _macd_accel(df)
    if accel > 0:
        macd_pts += 1.5
    elif macd > macd_sig:
        macd_pts += 0.5
    comp.subscores.append(SubScore("macd_acceleration", 3, accel, _clamp(macd_pts, 0, 3), "MACD accelerating" if accel > 0 else "MACD bullish"))

    # MTF momentum (2)
    mtf = _safe_float(stock.score.get("mtf_alignment"), 30)
    mtf_pts = 2.0 if mtf >= 100 else 1.0 if mtf >= 60 else 0.0
    comp.subscores.append(SubScore("mtf_momentum", 2, mtf, _clamp(mtf_pts, 0, 2), "Multi-timeframe momentum aligned"))

    # RSI (1) — not auto-bearish when overbought
    bear_div = _rsi_divergence_bearish(df)
    if bear_div:
        rsi_pts = 0.0
        comp.explanations.append("RSI bearish divergence with elevated RSI")
    elif 55 <= rsi <= 72 and (macd > macd_sig or accel > 0):
        rsi_pts = 1.0
    elif rsi > 72 and (macd > macd_sig and _safe_float(last.get("RVOL")) >= 1.3):
        rsi_pts = 0.75  # elevated but momentum supports
        comp.explanations.append("RSI elevated but momentum/volume remain positive")
    elif 45 <= rsi <= 55:
        rsi_pts = 0.5
    else:
        rsi_pts = 0.25
    comp.subscores.append(SubScore("rsi_context", 1, rsi, _clamp(rsi_pts, 0, 1), f"RSI {rsi:.0f} contextual"))

    # Other oscillator confirmation (1)
    other_pts = 0.5 if macd > macd_sig and adx >= 20 else 0.0
    comp.subscores.append(SubScore("oscillator_confirmation", 1, adx, _clamp(other_pts, 0, 1), ""))

    comp.finalize()
    return comp


def _volatility_regime(stock) -> tuple[str, float]:
    last = _last_row(stock)
    if last is None:
        return "UNKNOWN", 0.0
    atr_pct = _safe_float(last.get("ATR")) / max(_safe_float(last.get("Close")), 1) * 100
    patterns = getattr(stock, "patterns", {}) or {}
    squeeze = _safe_float(patterns.get("PRICE_SQUEEZE", 0))

    if atr_pct >= 8:
        return "EXTREME", atr_pct
    if atr_pct >= 5:
        return "ELEVATED", atr_pct
    if squeeze >= 50 and atr_pct <= 3:
        return "COMPRESSION_RELEASE", atr_pct
    return "NORMAL", atr_pct


def _score_market_sector(stock) -> ComponentScore:
    comp = ComponentScore("market_sector", WEIGHT_MARKET_SECTOR)
    market = getattr(stock, "market", {}) or {}

    rs_sector = _safe_float(stock.score.get("sector"), 50)
    sec_pts = 3.0 if rs_sector >= 80 else 2.0 if rs_sector >= 60 else 0.5 if rs_sector <= 40 else 1.5
    comp.subscores.append(SubScore("sector_relative_strength", 3, rs_sector, _clamp(sec_pts, 0, 3), "Sector strength"))

    regime = market.get("REGIME", "SIDEWAYS")
    strength = _safe_float(market.get("MARKET_STRENGTH"), 50)
    if regime == "TRENDING_BULL":
        reg_pts = 2.0
    elif regime == "SIDEWAYS":
        reg_pts = 1.0
    else:
        reg_pts = 0.0
    comp.subscores.append(SubScore("market_regime", 2, strength, _clamp(reg_pts, 0, 2), f"Regime {regime}"))

    # Breadth proxy from market strength
    breadth_pts = 2.0 if strength >= 70 else 1.0 if strength >= 55 else 0.0
    comp.subscores.append(SubScore("market_breadth", 2, strength, _clamp(breadth_pts, 0, 2), "Market breadth proxy"))

    vol_reg, atr_pct = _volatility_regime(stock)
    if vol_reg == "NORMAL":
        vol_pts = 2.0
    elif vol_reg == "COMPRESSION_RELEASE":
        vol_pts = 2.0
        comp.explanations.append("Volatility expansion after compression — supportive for breakout")
    elif vol_reg == "ELEVATED":
        vol_pts = 1.0
        comp.explanations.append("Market volatility elevated")
    else:
        vol_pts = 0.0
        comp.explanations.append("Extreme volatility regime")
    comp.subscores.append(SubScore("volatility_regime", 2, atr_pct, _clamp(vol_pts, 0, 2), vol_reg))

    rs = _safe_float(stock.score.get("relative_strength"), 50)
    idx_pts = 1.0 if rs >= 55 else 0.0
    comp.subscores.append(SubScore("index_alignment", 1, rs, _clamp(idx_pts, 0, 1), "Vs index"))

    comp.data_freshness = "session"
    comp.finalize()
    return comp


def _score_historical(candidate, analog_report: dict | None) -> ComponentScore:
    comp = ComponentScore("historical", WEIGHT_HISTORICAL)
    report = analog_report or getattr(candidate, "analog_report", None) or {}

    n = report.get("matched_analogs_count") or 0
    factor = _historical_sample_factor(n if n else None)

    if n <= 0 or factor == 0:
        comp.missing_inputs.append("MISSING:historical_analogs")
        for name, mx in [
            ("historical_analog_success", 4),
            ("expected_return", 2),
            ("historical_mae_drawdown", 1),
            ("strategy_symbol_history", 1),
            ("brain7_calibration", 2),
        ]:
            comp.subscores.append(SubScore(name, mx, None, 0.0, "", missing=True))
        comp.finalize()
        return comp

    win_rate = report.get("win_rate")
    exp_ret = report.get("expected_return")
    exp_dd = report.get("expected_drawdown")
    cal = report.get("probability_of_success")
    if cal is None and win_rate is not None:
        cal = win_rate

    analog_pts = 0.0
    if win_rate is not None:
        analog_pts = 4.0 * _clamp(win_rate, 0, 1) * factor
    comp.subscores.append(SubScore(
        "historical_analog_success", 4, win_rate, _clamp(analog_pts, 0, 4),
        f"Historical win rate {win_rate*100:.0f}% (n={n})" if win_rate else "",
    ))

    ret_pts = 0.0
    if exp_ret is not None:
        ret_pts = 2.0 * _clamp(exp_ret * 10 + 0.5, 0, 1) * factor
    comp.subscores.append(SubScore(
        "expected_return", 2, exp_ret, _clamp(ret_pts, 0, 2),
        f"Expected return {exp_ret:.2%}" if exp_ret is not None else "",
    ))

    dd_pts = 0.0
    if exp_dd is not None:
        dd_pts = 1.0 * (1.0 - _clamp(abs(exp_dd) * 5, 0, 1)) * factor
    comp.subscores.append(SubScore(
        "historical_mae_drawdown", 1, exp_dd, _clamp(dd_pts, 0, 1), "",
    ))

    hist_pts = 0.5 * factor
    comp.subscores.append(SubScore("strategy_symbol_history", 1, n, _clamp(hist_pts, 0, 1), ""))

    cal_pts = 0.0
    if cal is not None:
        cal_pts = 2.0 * _clamp(cal, 0, 1) * factor
    comp.subscores.append(SubScore(
        "brain7_calibration", 2, cal, _clamp(cal_pts, 0, 2),
        "Calibration-adjusted probability" if cal else "",
    ))

    comp.finalize()
    return comp


def _interpret_news_v2(news_payload: dict[str, Any]) -> dict[str, Any]:
    """Upgrade news interpretation; never fabricate unavailable fields."""
    status = news_payload.get("news_status", "DISABLED")
    base = {
        "reported_result_direction": "UNKNOWN",
        "expectation_surprise": "UNKNOWN",
        "management_tone": "UNKNOWN",
        "guidance_direction": "UNKNOWN",
        "demand_outlook": "UNKNOWN",
        "forward_outlook": "UNKNOWN",
        "event_risk": news_payload.get("news_risk", 0),
        "source_quality": "RSS" if status == "ACTIVE" else "UNKNOWN",
        "news_score": 5.0,
        "news_explanation": "",
        "critical_veto": news_payload.get("news_veto_reason"),
    }
    if status in ("DISABLED", "NO_NEWS"):
        base["news_score"] = 5.0  # neutral midpoint of 10-pt component
        base["news_explanation"] = "No verified news catalyst data"
        return base

    sentiment = news_payload.get("news_sentiment", 0)
    relevance = news_payload.get("news_relevance", 0)
    summary = (news_payload.get("news_summary") or "").lower()

    # Results-style heuristic when keywords present — mark UNKNOWN unless explicit
    has_results = any(w in summary for w in ("result", "earnings", "quarter", "ebitda", "revenue"))
    if has_results:
        base["reported_result_direction"] = "NEGATIVE" if sentiment < 0 else "POSITIVE" if sentiment > 0 else "UNKNOWN"
        base["management_tone"] = "UNKNOWN"
        base["guidance_direction"] = "UNKNOWN"
        base["expectation_surprise"] = "UNKNOWN"

    # Forward-guidance keywords (still UNKNOWN for consensus unless verified)
    if any(w in summary for w in ("guidance", "outlook", "demand", "order book", "pipeline")):
        if any(w in summary for w in ("raise", "strong demand", "robust", "upgrade")):
            base["forward_outlook"] = "MODERATELY_POSITIVE"
            base["guidance_direction"] = "POSITIVE"
        elif any(w in summary for w in ("cut", "weak demand", "caution", "headwind")):
            base["forward_outlook"] = "NEGATIVE"
            base["guidance_direction"] = "NEGATIVE"
        else:
            base["forward_outlook"] = "MIXED"

    # Composite news score within 10-pt component (5 base + adjustments)
    score = 5.0
    if sentiment > 0:
        score += 2.0
    elif sentiment < 0:
        score -= 2.0
    if relevance >= 70:
        score += 1.0
    if base["forward_outlook"] == "MODERATELY_POSITIVE":
        score += 2.0
    elif base["forward_outlook"] == "NEGATIVE":
        score -= 2.0
    elif base["forward_outlook"] == "MIXED":
        score += 0.5
    if base["reported_result_direction"] == "NEGATIVE" and base["forward_outlook"] == "MODERATELY_POSITIVE":
        base["news_explanation"] = "Results miss but forward commentary constructive — MIXED/MODERATELY POSITIVE"
        score = max(score, 6.0)
    if base["reported_result_direction"] == "POSITIVE" and base["forward_outlook"] == "NEGATIVE":
        base["news_explanation"] = "Results beat but guidance weak — treated cautiously"
        score = min(score, 4.0)

    if news_payload.get("news_veto_reason"):
        score = 0.0
        base["news_explanation"] = "Critical news risk — score zeroed; veto separate"

    base["news_score"] = _clamp(score, 0, 10)
    if not base["news_explanation"]:
        base["news_explanation"] = f"News {status}; sentiment={sentiment}; relevance={relevance}"
    return base


def _score_news(news_payload: dict[str, Any]) -> ComponentScore:
    comp = ComponentScore("news", WEIGHT_NEWS)
    interp = _interpret_news_v2(news_payload)

    weights = [
        ("reported_result", 3.0, interp["reported_result_direction"]),
        ("expectation_surprise", 2.0, interp["expectation_surprise"]),
        ("forward_outlook", 3.0, interp["forward_outlook"]),
        ("event_risk", 1.0, interp["event_risk"]),
        ("source_quality", 1.0, interp["source_quality"]),
    ]
    for name, mx, val in weights:
        if val == "UNKNOWN":
            comp.missing_inputs.append(f"MISSING:news_{name}")
            pts = mx * 0.5  # neutral partial
        elif val in ("POSITIVE", "MODERATELY_POSITIVE"):
            pts = mx
        elif val in ("NEGATIVE",):
            pts = 0.0
        elif val == "MIXED":
            pts = mx * 0.6
        elif isinstance(val, (int, float)):
            pts = _clamp(float(val) / 100 * mx, 0, mx)
        else:
            pts = mx * 0.5
        comp.subscores.append(SubScore(name, mx, val, pts, str(val)))

    # Override with composite news_score normalized to component weight
    composite = interp["news_score"]
    comp.raw_total = _clamp(composite, 0, WEIGHT_NEWS)
    comp.weighted_contribution = round(comp.raw_total, 2)
    comp.explanations = [interp["news_explanation"]] if interp["news_explanation"] else []
    comp.data_freshness = news_payload.get("news_timestamp") or "unknown"
    return comp


def compute_trade_score_v2(
    stock,
    candidate,
    *,
    all_strategies: list[str] | None = None,
    analog_report: dict | None = None,
    news_payload: dict | None = None,
    gate_threshold: float = 70.0,
    entry_status_fn: Callable | None = None,
    scoring_profile: Any | None = None,
) -> TradeScoreV2:
    """
    Compute hierarchical Trade Confidence 0–100 for a candidate.

    Does NOT include risk, allocation, or entry timing in the score.
    """
    strategy = getattr(candidate, "strategy", "BREAKOUT") or "BREAKOUT"
    profile = strategy if strategy != "PRICE SQUEEZE" else "PRICE SQUEEZE"
    if strategy == "DEMAND_SUPPLY":
        profile = "DEMAND_SUPPLY"

    result = TradeScoreV2()
    result.structure = _score_generic_structure(stock, candidate, profile)
    result.participation = _score_participation(stock, used_rvol_in_structure=(profile == "BREAKOUT"))
    result.momentum = _score_momentum(stock)
    result.market_sector = _score_market_sector(stock)
    result.historical = _score_historical(candidate, analog_report)
    result.news = _score_news(news_payload or {})

    # Reweight the existing V2 component quality without changing any strategy,
    # risk, R/R, news-veto, or entry gate.  The immutable snapshot travels with
    # the score so later profile edits cannot reinterpret historical trades.
    if scoring_profile is not None:
        snapshot = scoring_profile.snapshot(strategy) if hasattr(scoring_profile, "snapshot") else dict(scoring_profile)
        weights = snapshot["weights"]
        from core.scoring_profiles import validate_weights
        validate_weights(weights)
        for component in result.all_components():
            quality = component.weighted_contribution / component.weight if component.weight else 0.0
            component.weight = float(weights[component.component])
            component.weighted_contribution = round(min(component.weight, quality * component.weight), 2)
        result.scoring_profile = snapshot.get("name", "Custom")
        result.scoring_version = int(snapshot.get("version", 1))
        result.scoring_weights_snapshot = dict(weights)
    else:
        result.scoring_weights_snapshot = {c.component: c.weight for c in result.all_components()}

    strategies = all_strategies or [strategy]
    bonus, families = _compute_confluence(strategies)
    result.confluence_bonus = bonus
    result.confluence_families = families
    if bonus > 0:
        result.structure.explanations.append(
            f"Independent signal-family confluence ({', '.join(families)}) +{bonus}"
        )

    # Human-readable reasons
    for comp in result.all_components():
        for ex in comp.explanations:
            if ex.startswith("~") or "elevated" in ex.lower() or "caution" in ex.lower():
                result.watch_items.append(ex.lstrip("~ "))
            elif comp.weighted_contribution >= comp.weight * 0.5 and ex:
                result.positive_reasons.append(f"+ {ex}")

    if result.structure.weighted_contribution >= 22:
        result.positive_reasons.insert(0, "+ Strong market structure / setup quality")
    if result.participation.weighted_contribution >= 14:
        result.positive_reasons.append("+ Participation confirming")
    if result.momentum.weighted_contribution >= 14:
        result.positive_reasons.append("+ Momentum/trend supportive")

    vol_reg, _ = _volatility_regime(stock)
    if vol_reg == "ELEVATED":
        result.watch_items.append("Market volatility elevated")
    elif vol_reg == "EXTREME":
        result.watch_items.append("Extreme volatility — risk review critical")

    if entry_status_fn is not None:
        ok, reason = entry_status_fn(candidate)
        result.entry_status = getattr(candidate, "entry_status", "READY" if ok else "WAITING")
        if not ok:
            result.entry_blocker = reason
    else:
        result.entry_status = getattr(candidate, "entry_status", "")

    result.finalize(gate_threshold)
    return result


def apply_volatility_risk_hint(stock, risk_verdict: dict[str, Any]) -> dict[str, Any]:
    """Post Brain-5 hint for elevated (non-veto) volatility — does not weaken vetoes."""
    if risk_verdict.get("verdict") != "APPROVED":
        return risk_verdict
    last = _last_row(stock)
    if last is None:
        return risk_verdict
    atr_pct = _safe_float(last.get("ATR")) / max(_safe_float(last.get("Close")), 1) * 100
    if 5.0 <= atr_pct < 8.0:
        return {
            **risk_verdict,
            "verdict": "APPROVED_REDUCED_SIZE",
            "position_size_multiplier": 0.65,
            "reason": f"{risk_verdict.get('reason', '')}; elevated volatility ({atr_pct:.1f}% ATR) — reduced size",
        }
    return risk_verdict
