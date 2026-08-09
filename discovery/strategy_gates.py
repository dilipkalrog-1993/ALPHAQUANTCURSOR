"""Early-exit gates before expensive strategy functions."""

from __future__ import annotations

from typing import Any, Callable


def _feat(stock: Any, key: str, default: float = 0.0) -> float:
    ctx = getattr(stock, "_analysis_context", None)
    if ctx and key in ctx.features:
        return float(ctx.features[key])
    return default


def gate_breakout(stock: Any) -> bool:
    dist = _feat(stock, "dist_resistance_pct", 99.0)
    rvol = _feat(stock, "rvol", 0.0)
    return dist <= 8.0 or rvol >= 1.1


def gate_vcp(stock: Any) -> bool:
    compression = _feat(stock, "bb_compression_pctile", 1.0)
    dist_high = _feat(stock, "dist_52w_high_pct", 99.0)
    return compression <= 0.45 or dist_high <= 12.0


def gate_price_squeeze(stock: Any) -> bool:
    return _feat(stock, "bb_compression_pctile", 1.0) <= 0.50


def gate_demand_supply(stock: Any) -> bool:
    dist_sup = _feat(stock, "dist_support_pct", 99.0)
    dist_res = _feat(stock, "dist_resistance_pct", 99.0)
    return dist_sup <= 8.0 or dist_res <= 8.0


def gate_order_block(stock: Any) -> bool:
    dist_sup = _feat(stock, "dist_support_pct", 99.0)
    ema = _feat(stock, "ema_aligned", 0.0)
    return dist_sup <= 10.0 or ema >= 1.0


def gate_fvg(stock: Any) -> bool:
    dist_vwap = _feat(stock, "dist_vwap_pct", 99.0)
    mom = _feat(stock, "macd_bullish", 0.0)
    return dist_vwap <= 5.0 or mom >= 1.0


STRATEGY_GATES: dict[str, Callable[[Any], bool]] = {
    "BREAKOUT": gate_breakout,
    "VCP": gate_vcp,
    "PRICE SQUEEZE": gate_price_squeeze,
    "DEMAND & SUPPLY": gate_demand_supply,
    "ORDER BLOCK": gate_order_block,
    "FVG": gate_fvg,
}


def should_run_strategy(name: str, stock: Any) -> bool:
    if not getattr(stock, "_analysis_context", None):
        return True
    gate = STRATEGY_GATES.get(name)
    if gate is None:
        return True
    return gate(stock)
