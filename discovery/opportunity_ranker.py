"""Vectorized opportunity ranker — NOT Trade Confidence."""

from __future__ import annotations

from typing import Any


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        v = float(val)
        return default if v != v else v
    except (TypeError, ValueError):
        return default


def compute_opportunity_features(df: Any) -> dict[str, float]:
    """Cheap features from precomputed indicator columns."""
    last = df.iloc[-1]
    close = _safe_float(last.get("Close"))
    high52 = _safe_float(last.get("HIGH52"), close)
    low52 = _safe_float(last.get("LOW52"), close)
    resistance = _safe_float(df["High"].iloc[-21:-1].max()) if len(df) > 21 else close
    support = _safe_float(df["Low"].iloc[-21:-1].min()) if len(df) > 21 else close
    vwap = _safe_float(last.get("VWAP"), close)
    rvol = _safe_float(last.get("RVOL"))
    adx = _safe_float(last.get("ADX"))
    atr = _safe_float(last.get("ATR"), 1.0)
    ema20 = _safe_float(last.get("EMA20"), close)
    ema50 = _safe_float(last.get("EMA50"), close)
    macd = _safe_float(last.get("MACD"))
    macd_sig = _safe_float(last.get("MACD_SIGNAL"))
    bb_u = _safe_float(last.get("BB_UPPER"), close)
    bb_l = _safe_float(last.get("BB_LOWER"), close)
    bb_m = _safe_float(last.get("BB_MIDDLE"), close) or close
    bb_width = ((bb_u - bb_l) / bb_m * 100) if bb_m else 0.0

    if "BB_WIDTH" in df.columns:
        recent_width = _safe_float(last.get("BB_WIDTH"))
        hist_width = df["BB_WIDTH"].tail(60).dropna()
        width_pct = float(hist_width.rank(pct=True).iloc[-1]) if len(hist_width) > 5 else 0.5
    else:
        recent_width = bb_width
        width_pct = 0.5

    dist_res_pct = ((resistance - close) / close * 100) if close else 99.0
    dist_sup_pct = ((close - support) / close * 100) if close else 99.0
    dist_52w_high_pct = ((high52 - close) / high52 * 100) if high52 else 99.0
    dist_vwap_pct = abs((close - vwap) / close * 100) if close and vwap else 99.0
    ema_aligned = 1.0 if close > ema20 > ema50 else 0.0
    macd_bull = 1.0 if macd > macd_sig else 0.0
    atr_pct = (atr / close * 100) if close else 0.0

    return {
        "close": close,
        "dist_resistance_pct": dist_res_pct,
        "dist_support_pct": dist_sup_pct,
        "dist_52w_high_pct": dist_52w_high_pct,
        "dist_vwap_pct": dist_vwap_pct,
        "rvol": rvol,
        "adx": adx,
        "ema_aligned": ema_aligned,
        "macd_bullish": macd_bull,
        "bb_width": recent_width,
        "bb_compression_pctile": width_pct,
        "atr_pct": atr_pct,
        "resistance": resistance,
        "support": support,
        "high52": high52,
    }


def compute_opportunity_score(features: dict[str, float]) -> tuple[float, dict[str, float]]:
    """Preliminary opportunity score (0–100). Does NOT affect Scoring V2."""
    score = 0.0
    parts: dict[str, float] = {}

    # Price proximity — near breakout / 52w high / support
    prox = 0.0
    if features["dist_resistance_pct"] <= 2.0:
        prox += 18
    elif features["dist_resistance_pct"] <= 5.0:
        prox += 10
    if features["dist_52w_high_pct"] <= 3.0:
        prox += 12
    elif features["dist_52w_high_pct"] <= 8.0:
        prox += 6
    if features["dist_support_pct"] <= 3.0:
        prox += 8
    parts["price_proximity"] = prox
    score += prox

    # Participation
    part = 0.0
    if features["rvol"] >= 1.5:
        part += 15
    elif features["rvol"] >= 1.2:
        part += 10
    elif features["rvol"] >= 1.0:
        part += 4
    parts["participation"] = part
    score += part

    # Momentum
    mom = 0.0
    if features["adx"] >= 25:
        mom += 10
    elif features["adx"] >= 20:
        mom += 6
    if features["ema_aligned"]:
        mom += 10
    if features["macd_bullish"]:
        mom += 6
    parts["momentum"] = mom
    score += mom

    # Volatility structure — compression favors squeeze/VCP/breakout prep
    vol = 0.0
    if features["bb_compression_pctile"] <= 0.25:
        vol += 12
    elif features["bb_compression_pctile"] <= 0.40:
        vol += 6
    if 1.0 <= features["atr_pct"] <= 4.0:
        vol += 4
    parts["volatility_structure"] = vol
    score += vol

    # VWAP proximity
    vwap_pts = 6 if features["dist_vwap_pct"] <= 1.5 else (3 if features["dist_vwap_pct"] <= 3.0 else 0)
    parts["vwap_proximity"] = vwap_pts
    score += vwap_pts

    return min(100.0, score), parts


def rank_eligible(eligible: list[tuple[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for sym, df in eligible:
        feats = compute_opportunity_features(df)
        opp_score, breakdown = compute_opportunity_score(feats)
        ranked.append({
            "symbol": sym,
            "opportunity_score": round(opp_score, 2),
            "features": feats,
            "breakdown": breakdown,
            "dataframe": df,
        })
    ranked.sort(key=lambda r: r["opportunity_score"], reverse=True)
    return ranked
