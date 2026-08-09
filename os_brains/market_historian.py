"""
AlphaQuant OS — Brain 2: Market Historian.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md section 2): remember
historical market behavior at the *regime* level, and compare the current
per-symbol regime classification (already produced by app.detect_market_regime)
against a seeded catalog of named historical regimes.

Never evaluates a specific stock's setup (that is Brain 3) and never
produces a trade candidate (that is Brain 4).
"""

import json
import logging

from os_brains.db import get_connection, dict_cursor


# Seeded catalog of named historical market regimes. Dates are broad,
# well-known windows for the Indian market; `characteristics` is the
# structured description Market Historian compares the current regime
# against (see _similarity below).
REGIME_CATALOG = [
    {
        "name": "2008 Global Financial Crisis",
        "start_date": "2008-01-01",
        "end_date": "2009-03-31",
        "characteristics": {
            "trend_bias": "TRENDING_BEAR",
            "volatility_regime": "EXTREME",
            "breadth": "NEGATIVE",
        },
        "notes": "Global credit crisis; NIFTY drew down ~60% peak-to-trough.",
    },
    {
        "name": "2009 Post-Crisis Recovery Rally",
        "start_date": "2009-04-01",
        "end_date": "2010-12-31",
        "characteristics": {
            "trend_bias": "TRENDING_BULL",
            "volatility_regime": "HIGH",
            "breadth": "POSITIVE",
        },
        "notes": "Sharp V-shaped recovery off the 2008-09 lows.",
    },
    {
        "name": "2013 Taper Tantrum",
        "start_date": "2013-05-01",
        "end_date": "2013-09-30",
        "characteristics": {
            "trend_bias": "TRENDING_BEAR",
            "volatility_regime": "HIGH",
            "breadth": "NEGATIVE",
        },
        "notes": "Fed taper announcement drove EM currency/equity selloff, incl. INR/NIFTY.",
    },
    {
        "name": "2016 Demonetisation Shock",
        "start_date": "2016-11-01",
        "end_date": "2017-01-31",
        "characteristics": {
            "trend_bias": "SIDEWAYS",
            "volatility_regime": "HIGH",
            "breadth": "MIXED",
        },
        "notes": "Sudden liquidity shock; sharp but short-lived drawdown, quick stabilization.",
    },
    {
        "name": "2020 COVID Crash",
        "start_date": "2020-02-01",
        "end_date": "2020-03-31",
        "characteristics": {
            "trend_bias": "TRENDING_BEAR",
            "volatility_regime": "EXTREME",
            "breadth": "NEGATIVE",
        },
        "notes": "Fastest ~38% NIFTY drawdown on record, global pandemic shock.",
    },
    {
        "name": "2020-2021 Post-COVID Recovery",
        "start_date": "2020-04-01",
        "end_date": "2021-12-31",
        "characteristics": {
            "trend_bias": "TRENDING_BULL",
            "volatility_regime": "HIGH",
            "breadth": "POSITIVE",
        },
        "notes": "Liquidity-driven V-shaped rally; small/mid-caps led.",
    },
    {
        "name": "2022 Rate Hike Cycle",
        "start_date": "2022-02-01",
        "end_date": "2022-12-31",
        "characteristics": {
            "trend_bias": "SIDEWAYS",
            "volatility_regime": "HIGH",
            "breadth": "MIXED",
        },
        "notes": "Global central-bank tightening; choppy, range-bound, sector rotation.",
    },
    {
        "name": "2023-2024 Bull Market",
        "start_date": "2023-04-01",
        "end_date": "2024-09-30",
        "characteristics": {
            "trend_bias": "TRENDING_BULL",
            "volatility_regime": "LOW",
            "breadth": "POSITIVE",
        },
        "notes": "Broad-based, low-volatility uptrend; new all-time highs.",
    },
    {
        "name": "2024 General Election Period",
        "start_date": "2024-04-01",
        "end_date": "2024-06-15",
        "characteristics": {
            "trend_bias": "SIDEWAYS",
            "volatility_regime": "HIGH",
            "breadth": "MIXED",
        },
        "notes": "Pre/post election result volatility spike, sharp single-day moves.",
    },
    {
        "name": "2024-2025 Liquidity Tightening / Correction",
        "start_date": "2024-10-01",
        "end_date": "2025-03-31",
        "characteristics": {
            "trend_bias": "TRENDING_BEAR",
            "volatility_regime": "HIGH",
            "breadth": "NEGATIVE",
        },
        "notes": "FII outflows, valuation reset, broad mid/small-cap correction.",
    },
]


def seed_regime_catalog():
    """
    Inserts the seeded catalog above into market_memory.historical_regimes.
    Idempotent: relies on the UNIQUE(name) constraint, ON CONFLICT DO
    UPDATE keeps the catalog in sync if this list is edited later.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for regime in REGIME_CATALOG:
                cur.execute(
                    """
                    INSERT INTO market_memory.historical_regimes
                        (name, start_date, end_date, characteristics, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date,
                        characteristics = EXCLUDED.characteristics,
                        notes = EXCLUDED.notes
                    """,
                    (
                        regime["name"],
                        regime["start_date"],
                        regime["end_date"],
                        json.dumps(regime["characteristics"]),
                        regime["notes"],
                    ),
                )
        conn.commit()
        logging.info(f"MARKET_HISTORIAN_SEED: {len(REGIME_CATALOG)} regimes upserted")
    finally:
        conn.close()


def _volatility_bucket(strength):
    if strength >= 85:
        return "EXTREME"
    if strength >= 70:
        return "HIGH"
    if strength >= 55:
        return "LOW"
    return "LOW"


def _similarity(current_regime, current_strength, catalog_row):
    """
    Simple, explainable rule-based similarity (0-100) between the current
    per-symbol regime classification and a catalog entry's characteristics.
    Phase 1 scope deliberately keeps this rule-based rather than a learned
    model — see ALPHAQUANT_OS_ARCHITECTURE.md section 4, which allows a
    "comparably simple" substitute metric as long as the AnalogReport/
    RegimeContext output contract is unchanged.
    """
    score = 0
    chars = catalog_row["characteristics"]
    if chars.get("trend_bias") == current_regime:
        score += 60
    elif current_regime == "SIDEWAYS" or chars.get("trend_bias") == "SIDEWAYS":
        score += 20

    current_vol_bucket = _volatility_bucket(current_strength)
    if chars.get("volatility_regime") == current_vol_bucket:
        score += 40
    elif chars.get("volatility_regime") in ("HIGH", "EXTREME") and current_vol_bucket in ("HIGH", "EXTREME"):
        score += 20

    return min(score, 100)


def get_regime_context(stock, top_n=3):
    """
    Brain 2's output contract (RegimeContext): the current per-symbol
    regime (read from stock.market, populated by app.detect_market_regime,
    which must have already been run on `stock`) plus the nearest
    historical regimes by rule-based similarity.

    Does not mutate `stock` and does not evaluate a specific setup — that
    is Brain 3 (historical_analog_engine).
    """
    current_regime = stock.market.get("REGIME", "SIDEWAYS")
    current_strength = stock.market.get("MARKET_STRENGTH", 50)

    conn = get_connection()
    try:
        with dict_cursor(conn) as cur:
            cur.execute(
                "SELECT name, start_date, end_date, characteristics, notes "
                "FROM market_memory.historical_regimes"
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    scored = []
    for row in rows:
        sim = _similarity(current_regime, current_strength, row)
        scored.append({
            "regime_name": row["name"],
            "date_range": f"{row['start_date']} to {row['end_date']}",
            "similarity_score": sim,
            "characteristics": row["characteristics"],
            "how_it_resolved": row["notes"],
        })
    scored.sort(key=lambda r: r["similarity_score"], reverse=True)

    return {
        "as_of": None,
        "current_regime": current_regime,
        "current_regime_strength": current_strength,
        "nearest_historical_regimes": scored[:top_n],
    }
