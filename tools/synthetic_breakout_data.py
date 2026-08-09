"""Deterministic OHLCV that satisfies production BREAKOUT strategy rules."""

from __future__ import annotations

import numpy as np
import pandas as pd

BREAKOUT_LOOKBACK = 20
BREAKOUT_BUFFER = 0.002


def _swing_uptrend(days: int = 260, seed: int = 42, start: float = 120.0) -> pd.DataFrame:
    """Construct HH/HL swing structure compatible with classify_market_structure()."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(end=pd.Timestamp.utcnow().normalize(), periods=days)
    price = start
    rows = []
    for i in range(days):
        cycle = i % 12
        if cycle in (0, 1, 2, 3, 4, 5, 6):
            price += 0.45 + rng.normal(0, 0.05)
        else:
            price -= 0.25 + rng.normal(0, 0.04)
        price = max(55.0, price)
        hi = price + rng.uniform(0.6, 1.1)
        lo = price - rng.uniform(0.6, 1.0)
        vol = int(rng.integers(220_000, 320_000))
        rows.append({"Open": price - 0.2, "High": hi, "Low": lo, "Close": price, "Volume": vol})
    df = pd.DataFrame(rows, index=dates)
    df.index.name = "Date"
    return df


def _finalize_breakout_bar(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    resistance = float(out["High"].iloc[-BREAKOUT_LOOKBACK - 1 : -1].max())
    breakout_close = round(resistance * (1 + BREAKOUT_BUFFER + 0.0015), 2)
    idx = out.index[-1]
    # Moderate breakout to avoid RSI saturation while clearing resistance
    out.at[idx, "Close"] = breakout_close
    out.at[idx, "High"] = breakout_close + 0.8
    out.at[idx, "Low"] = min(float(out.at[idx, "Low"]), breakout_close - 1.2)
    out.at[idx, "Open"] = breakout_close - 0.35
    avg_vol = float(out["Volume"].iloc[-21:-1].mean())
    out.at[idx, "Volume"] = int(max(avg_vol * 2.0, 420_000))
    return out


def build_breakout_universe(symbol: str = "TESTBREAK.NS", days: int = 260) -> pd.DataFrame:
    return _finalize_breakout_bar(_swing_uptrend(days, seed=42))


def build_hold_universe(symbol: str = "TESTHOLD.NS", days: int = 260) -> pd.DataFrame:
    return _swing_uptrend(days, seed=99, start=88.0)


def build_performance_universe(count: int, days: int = 260, seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    return {
        f"PERF{i:04d}.NS": _swing_uptrend(days, seed=int(rng.integers(0, 1_000_000)), start=float(rng.uniform(70, 180)))
        for i in range(count)
    }
