"""
AlphaQuant OS — shared setup-vector construction (Brain 3 design detail,
ALPHAQUANT_OS_ARCHITECTURE.md section 4).

A "setup vector" is a fixed-length, named set of features describing a
single (symbol, trading_day)'s market setup. The same feature function is
used by:

  - the backfill pipeline (historical days, computed once per symbol from
    the already-indicator-enriched dataframe), and
  - the Historical Analog Engine (the "live" / current-day vector for a
    symbol being scanned right now),

so historical and live vectors are guaranteed comparable, per the
architecture doc.

FEATURE_NAMES defines both the fixed ordering used for the numpy similarity
array in historical_analog_engine.py and the JSONB keys stored in
market_memory.daily_snapshots.setup_vector.
"""

import numpy as np
import pandas as pd


# Continuous features are z-score normalized (population-wide) before
# similarity search. Boolean/flag features are kept as 0/1 and are not
# normalized (see historical_analog_engine.NORMALIZED_FEATURES).
FEATURE_NAMES = [
    "trend_ordinal",
    "adx",
    "rvol",
    "atr_ratio",
    "dist_from_52w_high",
    "dist_from_52w_low",
    "relative_strength",
    "volume_zscore",
    "fresh_demand_zone",
    "fresh_supply_zone",
    "order_block",
    "fair_value_gap",
    "bos",
    "choch",
    "liquidity_sweep",
]

# Subset of FEATURE_NAMES that gets z-score normalized against the
# population. The pattern-flag booleans are intentionally excluded — they
# are already on a comparable 0/1 scale across every symbol.
NORMALIZED_FEATURES = [
    "trend_ordinal",
    "adx",
    "rvol",
    "atr_ratio",
    "dist_from_52w_high",
    "dist_from_52w_low",
    "relative_strength",
    "volume_zscore",
]

# Holding horizons (trading days) at which forward_outcomes rows are
# computed. Brain 3 reports whichever horizon has the strongest edge for a
# given setup rather than a single hardcoded horizon for everything.
HOLDING_HORIZONS = [5, 10, 20]


def _trend_ordinal(row):
    try:
        if row["EMA20"] > row["EMA50"] > row["EMA200"]:
            return 2.0
        if row["EMA20"] < row["EMA50"] < row["EMA200"]:
            return 0.0
    except (KeyError, TypeError):
        pass
    return 1.0


def compute_pattern_flag_series(df):
    """
    Computes per-day boolean pattern flags across the WHOLE history in a
    small number of full-history passes, instead of re-running the
    stock-mutating strategy functions once per day (which would be O(n^2)
    and would also fight over the shared StockObject.patterns dict).

    - fresh_demand_zone / fresh_supply_zone / order_block / fair_value_gap:
      each existing zone detector (detect_demand_supply, detect_order_blocks,
      detect_fair_value_gaps) already scans the FULL dataframe in one pass
      and returns zones tagged with the bar Index where they originated.
      A day is flagged True for `first_seen_index <= day_index <= first_seen_index + zone_active_window`.
    - bos / choch / liquidity_sweep: the existing detectors
      (detect_market_structure_shift, detect_liquidity_sweep) only ever
      evaluate the LAST bar against a trailing lookback window. This
      reproduces that exact definition for every historical bar using
      vectorized pandas rolling operations over the same lookback windows
      and thresholds already defined in app.py (BOS_LOOKBACK=20,
      LIQUIDITY_LOOKBACK=15), rather than looping the whole detector once
      per day.

    Returns a DataFrame aligned to df.index with boolean columns:
    fresh_demand_zone, fresh_supply_zone, order_block, fair_value_gap,
    bos, choch, liquidity_sweep.
    """
    n = len(df)
    flags = pd.DataFrame(
        False,
        index=df.index,
        columns=[
            "fresh_demand_zone",
            "fresh_supply_zone",
            "order_block",
            "fair_value_gap",
            "bos",
            "choch",
            "liquidity_sweep",
        ],
    )

    # ---- Demand / Supply zones (reuses app.py's own single-pass logic) ----
    try:
        from app import (
            detect_impulse_candle,
            detect_base,
            BASE_LOOKBACK,
        )
        zone_active_window = 20
        for i in range(25, n - 3):
            if detect_base(df, i):
                before = df.iloc[i - 1]
                after = df.iloc[i + 1]
                if detect_impulse_candle(df, i + 1):
                    end = min(i + zone_active_window, n - 1)
                    if after["Close"] > before["Close"]:
                        flags.iloc[i:end + 1, flags.columns.get_loc("fresh_demand_zone")] = True
                    else:
                        flags.iloc[i:end + 1, flags.columns.get_loc("fresh_supply_zone")] = True
    except Exception:
        pass

    # ---- Order Blocks (reuses app.py's ORDER_BLOCK_LOOKBACK definition) ----
    try:
        from app import ORDER_BLOCK_LOOKBACK
        if n >= ORDER_BLOCK_LOOKBACK:
            zone_active_window = 20
            for i in range(5, n - 3):
                candle = df.iloc[i]
                next1 = df.iloc[i + 1]
                next2 = df.iloc[i + 2]
                rng = candle["High"] - candle["Low"]
                if rng == 0:
                    continue
                is_bullish_ob = (
                    candle["Close"] < candle["Open"]
                    and next1["Close"] > next1["Open"]
                    and next2["Close"] > next2["Open"]
                    and next2["Close"] > candle["High"]
                )
                is_bearish_ob = (
                    candle["Close"] > candle["Open"]
                    and next1["Close"] < next1["Open"]
                    and next2["Close"] < next2["Open"]
                    and next2["Close"] < candle["Low"]
                )
                if is_bullish_ob or is_bearish_ob:
                    end = min(i + zone_active_window, n - 1)
                    flags.iloc[i:end + 1, flags.columns.get_loc("order_block")] = True
    except Exception:
        pass

    # ---- Fair Value Gaps (reuses app.py's FVG_MIN_GAP_PERCENT threshold) ----
    try:
        from app import FVG_MIN_GAP_PERCENT
        zone_active_window = 20
        for i in range(2, n):
            c1 = df.iloc[i - 2]
            c3 = df.iloc[i]
            gap_up = False
            gap_down = False
            if c3["Low"] > c1["High"] and c1["High"] > 0:
                gap_up = ((c3["Low"] - c1["High"]) / c1["High"]) * 100 >= FVG_MIN_GAP_PERCENT
            if c3["High"] < c1["Low"] and c1["Low"] > 0:
                gap_down = ((c1["Low"] - c3["High"]) / c1["Low"]) * 100 >= FVG_MIN_GAP_PERCENT
            if gap_up or gap_down:
                end = min(i + zone_active_window, n - 1)
                flags.iloc[i:end + 1, flags.columns.get_loc("fair_value_gap")] = True
    except Exception:
        pass

    # ---- BOS / CHOCH (vectorized adaptation of detect_market_structure_shift) ----
    try:
        from app import BOS_LOOKBACK
        prior_high = df["High"].shift(1).rolling(BOS_LOOKBACK).max()
        prior_low = df["Low"].shift(1).rolling(BOS_LOOKBACK).min()
        bullish_bos = df["Close"] > prior_high
        bearish_bos = df["Close"] < prior_low
        flags["bos"] = (bullish_bos | bearish_bos).fillna(False)
        # CHOCH: a break in the opposite direction to the immediately
        # preceding BOS within the same lookback window (character change).
        bos_dir = pd.Series(0, index=df.index)
        bos_dir[bullish_bos.fillna(False)] = 1
        bos_dir[bearish_bos.fillna(False)] = -1
        prev_dir = bos_dir.replace(0, np.nan).ffill().shift(1)
        flags["choch"] = (
            (bos_dir != 0) & (prev_dir.notna()) & (bos_dir != prev_dir)
        ).fillna(False)
    except Exception:
        pass

    # ---- Liquidity Sweep (vectorized adaptation of detect_liquidity_sweep) ----
    try:
        from app import LIQUIDITY_LOOKBACK
        prev_low = df["Low"].shift(1).rolling(LIQUIDITY_LOOKBACK).min()
        prev_high = df["High"].shift(1).rolling(LIQUIDITY_LOOKBACK).max()
        bullish_sweep = (df["Low"] < prev_low) & (df["Close"] > prev_low)
        bearish_sweep = (df["High"] > prev_high) & (df["Close"] < prev_high)
        flags["liquidity_sweep"] = (bullish_sweep | bearish_sweep).fillna(False)
    except Exception:
        pass

    return flags


def compute_relative_strength_series(df, nifty_df, lookback=63):
    """
    Per-day relative strength: stock's trailing `lookback`-day return minus
    NIFTY's trailing `lookback`-day return over the same window, aligned by
    date. This mirrors calculate_relative_strength's definition but computed
    for every historical day at once (that function only computes it for
    the current/last day of a live scan).
    """
    stock_ret = df["Close"].pct_change(lookback) * 100
    nifty_close = nifty_df["Close"].reindex(df.index, method="ffill")
    nifty_ret = nifty_close.pct_change(lookback) * 100
    return (stock_ret - nifty_ret).fillna(0.0)


def build_setup_vector_row(df, idx, pattern_flags, relative_strength_series):
    """
    Builds the raw (pre-normalization) feature dict for a single row `idx`
    of an indicator-enriched dataframe (must already have EMA20/50/200,
    ADX, RVOL, ATR, HIGH52, LOW52, AVG_VOLUME20 columns from
    app.calculate_indicators). Returns None if required columns are missing
    or NaN (e.g. inside the indicator warm-up window).
    """
    row = df.iloc[idx]
    required = ["EMA20", "EMA50", "EMA200", "ADX", "RVOL", "ATR", "HIGH52", "LOW52", "Close", "Volume"]
    for col in required:
        if col not in df.columns or pd.isna(row.get(col)):
            return None

    atr_avg20 = df["ATR"].iloc[max(0, idx - 19):idx + 1].mean()
    atr_ratio = float(row["ATR"] / atr_avg20) if atr_avg20 and atr_avg20 > 0 else 1.0

    vol_window = df["Volume"].iloc[max(0, idx - 19):idx + 1]
    vol_std = vol_window.std()
    volume_zscore = float((row["Volume"] - vol_window.mean()) / vol_std) if vol_std and vol_std > 0 else 0.0

    high52 = row["HIGH52"]
    low52 = row["LOW52"]

    vector = {
        "trend_ordinal": float(_trend_ordinal(row)),
        "adx": float(row["ADX"]),
        "rvol": float(row["RVOL"]) if pd.notna(row["RVOL"]) else 1.0,
        "atr_ratio": atr_ratio,
        "dist_from_52w_high": float((high52 - row["Close"]) / high52) if high52 else 0.0,
        "dist_from_52w_low": float((row["Close"] - low52) / low52) if low52 else 0.0,
        "relative_strength": float(relative_strength_series.iloc[idx]),
        "volume_zscore": volume_zscore,
        "fresh_demand_zone": float(bool(pattern_flags["fresh_demand_zone"].iloc[idx])),
        "fresh_supply_zone": float(bool(pattern_flags["fresh_supply_zone"].iloc[idx])),
        "order_block": float(bool(pattern_flags["order_block"].iloc[idx])),
        "fair_value_gap": float(bool(pattern_flags["fair_value_gap"].iloc[idx])),
        "bos": float(bool(pattern_flags["bos"].iloc[idx])),
        "choch": float(bool(pattern_flags["choch"].iloc[idx])),
        "liquidity_sweep": float(bool(pattern_flags["liquidity_sweep"].iloc[idx])),
    }
    return vector


def vector_to_array(vector):
    """Fixed-order numpy array for a setup_vector dict, per FEATURE_NAMES."""
    return np.array([vector.get(name, 0.0) for name in FEATURE_NAMES], dtype=float)
