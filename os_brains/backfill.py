"""
AlphaQuant OS — Brain 3: Historical Analog Engine.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md sections 2 & 4): given a
specific stock's current setup, find statistically similar historical
setups within the backfilled market_memory.daily_snapshots dataset and
report their aggregate forward outcomes.

Never decides whether to trade — it only reports historical evidence for
Brain 4 (Strategist, a later task) to weigh.
"""

import json
import logging

import numpy as np

from os_brains.db import get_connection, dict_cursor
from os_brains.setup_vector import FEATURE_NAMES, NORMALIZED_FEATURES, HOLDING_HORIZONS, vector_to_array

TOP_N_NEIGHBORS = 50
MIN_SIMILARITY = 0.60
MIN_ANALOGS_FOR_HIGH_CONFIDENCE = 40
MIN_ANALOGS_FOR_MEDIUM_CONFIDENCE = 10
SAME_SYMBOL_EXCLUSION_DAYS = max(HOLDING_HORIZONS)


def _fetch_population(conn, exclude_symbol=None, exclude_around_date=None):
    """
    Loads every backfilled (symbol, trading_day) setup_vector + forward
    outcomes, joined, into memory. At Phase 1 scale (~tens of thousands of
    rows) this is cheap; the architecture doc calls out that normalization
    stats should be recomputed periodically rather than per query, which
    this satisfies by doing one population fetch per find_analogs() call
    rather than a running online computation.
    """
    query = """
        SELECT
            s.id AS snapshot_id,
            s.symbol,
            s.trading_day,
            s.setup_vector,
            f.holding_period_days,
            f.forward_return,
            f.max_drawdown,
            f.max_favorable_move,
            f.recovered_by_day
        FROM market_memory.daily_snapshots s
        JOIN market_memory.forward_outcomes f ON f.snapshot_id = s.id
    """
    with dict_cursor(conn) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return rows


def _population_stats(rows):
    """Mean/std per normalized feature across the whole fetched population."""
    if not rows:
        return None, None
    seen = {}
    for row in rows:
        seen[row["snapshot_id"]] = row["setup_vector"]
    matrix = np.array([
        [vec.get(name, 0.0) for name in NORMALIZED_FEATURES]
        for vec in seen.values()
    ])
    mean = matrix.mean(axis=0)
    std = matrix.std(axis=0)
    std[std == 0] = 1.0
    return mean, std


def _normalize(vector, mean, std):
    normalized = dict(vector)
    for i, name in enumerate(NORMALIZED_FEATURES):
        normalized[name] = (vector.get(name, 0.0) - mean[i]) / std[i]
    return normalized


def find_analogs(symbol, setup_vector_raw, as_of_date=None, top_n=TOP_N_NEIGHBORS,
                  min_similarity=MIN_SIMILARITY):
    """
    Brain 3's output contract (AnalogReport). `setup_vector_raw` is the
    RAW (pre-normalization) feature dict for the symbol's current setup,
    built with os_brains.setup_vector.build_setup_vector_row (or the
    equivalent live construction). Excludes matches from the same symbol
    within its own likely holding window to avoid a setup "matching itself"
    (architecture doc section 4).
    """
    conn = get_connection()
    try:
        rows = _fetch_population(conn)
    finally:
        conn.close()

    if not rows:
        return {
            "symbol": symbol,
            "as_of": str(as_of_date) if as_of_date else None,
            "setup_vector": setup_vector_raw,
            "matched_analogs_count": 0,
            "win_rate": None,
            "expected_return": None,
            "expected_drawdown": None,
            "recovery_time_days": None,
            "typical_holding_period_days": None,
            "probability_of_success": None,
            "sample_confidence": "LOW",
        }

    mean, std = _population_stats(rows)
    query_vec = vector_to_array(_normalize(setup_vector_raw, mean, std))

    # Group forward_outcomes by snapshot_id/holding_period for aggregation,
    # and keep one normalized vector per snapshot for similarity search.
    by_snapshot = {}
    for row in rows:
        sid = row["snapshot_id"]
        if sid not in by_snapshot:
            if row["symbol"] == symbol and as_of_date is not None:
                days_apart = abs((row["trading_day"] - as_of_date).days)
                if days_apart <= SAME_SYMBOL_EXCLUSION_DAYS:
                    continue
            normalized_vec = _normalize(row["setup_vector"], mean, std)
            by_snapshot[sid] = {
                "symbol": row["symbol"],
                "trading_day": row["trading_day"],
                "vector": vector_to_array(normalized_vec),
                "outcomes": {},
            }
        if sid in by_snapshot:
            by_snapshot[sid]["outcomes"][row["holding_period_days"]] = {
                "forward_return": row["forward_return"],
                "max_drawdown": row["max_drawdown"],
                "max_favorable_move": row["max_favorable_move"],
                "recovered_by_day": row["recovered_by_day"],
            }

    if not by_snapshot:
        matched = []
    else:
        ids = list(by_snapshot.keys())
        matrix = np.stack([by_snapshot[i]["vector"] for i in ids])
        query_norm = np.linalg.norm(query_vec)
        matrix_norms = np.linalg.norm(matrix, axis=1)
        denom = (matrix_norms * query_norm)
        denom[denom == 0] = 1e-9
        similarities = (matrix @ query_vec) / denom

        order = np.argsort(-similarities)[:top_n]
        matched = [
            (ids[i], float(similarities[i]))
            for i in order
            if similarities[i] >= min_similarity
        ]

    matched_count = len(matched)

    if matched_count == 0:
        return {
            "symbol": symbol,
            "as_of": str(as_of_date) if as_of_date else None,
            "setup_vector": setup_vector_raw,
            "matched_analogs_count": 0,
            "win_rate": None,
            "expected_return": None,
            "expected_drawdown": None,
            "recovery_time_days": None,
            "typical_holding_period_days": None,
            "probability_of_success": None,
            "sample_confidence": "LOW",
        }

    # Pick the holding horizon with the strongest historical edge
    # (highest |mean forward_return| among horizons with enough coverage).
    best_horizon = None
    best_stats = None
    for horizon in HOLDING_HORIZONS:
        returns = []
        drawdowns = []
        favorable = []
        recoveries = []
        for sid, _sim in matched:
            outcome = by_snapshot[sid]["outcomes"].get(horizon)
            if outcome and outcome["forward_return"] is not None:
                returns.append(float(outcome["forward_return"]))
                if outcome["max_drawdown"] is not None:
                    drawdowns.append(float(outcome["max_drawdown"]))
                if outcome["max_favorable_move"] is not None:
                    favorable.append(float(outcome["max_favorable_move"]))
                if outcome["recovered_by_day"] is not None:
                    recoveries.append(float(outcome["recovered_by_day"]))
        if not returns:
            continue
        mean_return = float(np.mean(returns))
        win_rate = float(np.mean([1.0 if r > 0 else 0.0 for r in returns]))
        stats = {
            "horizon": horizon,
            "win_rate": win_rate,
            "expected_return": mean_return,
            "expected_drawdown": float(np.mean(drawdowns)) if drawdowns else None,
            "recovery_time_days": float(np.mean(recoveries)) if recoveries else None,
            "n": len(returns),
        }
        if best_stats is None or abs(mean_return) > abs(best_stats["expected_return"]):
            best_horizon = horizon
            best_stats = stats

    if best_stats is None:
        sample_confidence = "LOW"
        win_rate = None
        expected_return = None
        expected_drawdown = None
        recovery_time_days = None
        probability_of_success = None
    else:
        if best_stats["n"] >= MIN_ANALOGS_FOR_HIGH_CONFIDENCE:
            sample_confidence = "HIGH"
        elif best_stats["n"] >= MIN_ANALOGS_FOR_MEDIUM_CONFIDENCE:
            sample_confidence = "MEDIUM"
        else:
            sample_confidence = "LOW"

        win_rate = best_stats["win_rate"]
        expected_return = best_stats["expected_return"]
        expected_drawdown = best_stats["expected_drawdown"]
        recovery_time_days = best_stats["recovery_time_days"]

        # probability_of_success = win_rate adjusted by Brain 7's calibration
        # delta for this setup's archetype, once the Reviewer task
        # populates experience_memory.calibration_state. Until then no
        # calibration rows exist, so this is win_rate unchanged — exactly
        # the "no trading decisions change yet" scope of this task.
        calibration_delta = _get_calibration_delta(symbol)
        probability_of_success = max(0.0, min(1.0, win_rate + calibration_delta))

    return {
        "symbol": symbol,
        "as_of": str(as_of_date) if as_of_date else None,
        "setup_vector": setup_vector_raw,
        "matched_analogs_count": matched_count,
        "win_rate": win_rate,
        "expected_return": expected_return,
        "expected_drawdown": expected_drawdown,
        "recovery_time_days": recovery_time_days,
        "typical_holding_period_days": best_horizon,
        "probability_of_success": probability_of_success,
        "sample_confidence": sample_confidence,
    }


def _get_calibration_delta(symbol):
    """
    Reads experience_memory.calibration_state for this setup's archetype.
    Returns 0.0 (no adjustment) when no calibration rows exist yet, which
    is always true until the Reviewer task (Task #4) starts populating it.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT avg_calibration_delta FROM experience_memory.calibration_state "
                "WHERE setup_archetype = %s",
                (symbol,),
            )
            row = cur.fetchone()
            return float(row[0]) if row else 0.0
    except Exception as e:
        logging.warning(f"HISTORICAL_ANALOG_ENGINE calibration lookup failed: {e}")
        return 0.0
    finally:
        conn.close()
