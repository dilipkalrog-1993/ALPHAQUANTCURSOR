"""
AlphaQuant OS — Experience Memory store (Brain 7 support module).

Owns every read/write against the experience_memory schema (see
ALPHAQUANT_OS_ARCHITECTURE.md section 3 and os_brains/db.py's DDL). Mirrors
the pattern already used for Market Memory (market_observer.py /
market_historian.py wrap reads/writes so app.py and the Brains never issue
raw SQL directly): this module is the only place that touches
experience_memory.* tables directly.

Every public function degrades gracefully — it logs a warning and returns
None/False on any DB failure rather than raising, so a Postgres hiccup
never breaks the trading pipeline itself (same contract as Brain 4/5/6).

Incremental-update hook (b) from ALPHAQUANT_OS_ARCHITECTURE.md section 7
("new completed trade -> append Experience Memory") is implemented as the
three functions below being called at the natural points in app.py's
decision/open/close lifecycle: record_decision() when a candidate gets its
RiskVerdict, update_allocation()/mark_open() when Brain 6/execution decide
its capital fate, and record_outcome() (called by os_brains.reviewer) when
a position actually closes.
"""

import json
import logging


def _safe_num(value):
    """
    Coerces numpy scalar types (np.float64, np.int64, etc. - which
    PaperPosition's P&L fields are, since they flow through
    pandas/numpy math) to native Python numbers before they reach
    psycopg2. psycopg2 has no adapter for numpy scalars, so without this
    it silently falls back to the value's repr() (e.g.
    "np.float64(-830.94)") embedded unquoted in the SQL text, which
    Postgres then tries to parse as an identifier/schema reference and
    the insert fails - caught by this module's blanket except and only
    ever surfaced as a logged warning, so a trade's outcome/review row
    could go permanently missing without anything crashing.
    """
    if value is None:
        return None
    if hasattr(value, "item"):
        return value.item()
    return value
from datetime import datetime

from os_brains.db import get_connection, dict_cursor


def _safe_json(obj):
    """JSON-serializes arbitrary (possibly non-JSON-native) values, e.g.
    datetimes/numpy scalars attached to TradeCandidate/PaperPosition
    objects, by falling back to str() for anything json can't handle."""
    return json.dumps(obj, default=str)


def _candidate_snapshot(candidate):
    """
    Whitelisted, JSON-safe snapshot of the parts of a TradeCandidate worth
    remembering for review later. Deliberately does not dump `vars(candidate)`
    wholesale — some attributes (e.g. market_observation) are large/derived
    and are already captured separately via regime_context/analog_report.
    """
    return {
        "symbol": candidate.symbol,
        "strategy": candidate.strategy,
        "state": getattr(candidate, "state", None),
        "direction": getattr(candidate, "direction", None),
        "entry": getattr(candidate, "entry", None),
        "stop": getattr(candidate, "stop", None),
        "target1": getattr(candidate, "target1", None),
        "target2": getattr(candidate, "target2", None),
        "target3": getattr(candidate, "target3", None),
        "risk_reward": getattr(candidate, "risk_reward", None),
        "confidence": getattr(candidate, "confidence", None),
        "ai_score": getattr(candidate, "ai_score", None),
        "expected_value": getattr(candidate, "expected_value", None),
        "evidence_summary": getattr(candidate, "evidence_summary", None),
        "position_size": getattr(candidate, "position_size", None),
        "capital_required": getattr(candidate, "capital_required", None),
        "reasons": getattr(candidate, "reasons", None),
    }


def record_decision(candidate, regime_context, analog_report, risk_verdict):
    """
    Appends one row to experience_memory.decisions for a candidate that has
    just received its RiskVerdict (Brain 5) — the architecture doc's "no
    trade is a decision too" principle, so this is called for every
    candidate build_ai_consensus evaluates, vetoed or not. Returns the new
    decision_id (int) so app.py can carry it through open/close, or None if
    the write failed (candidate.decision_id stays None and later hooks
    simply skip review for that candidate — never blocks the trading path).
    """
    try:
        outcome_state = "NO_TRADE" if risk_verdict.get("verdict") == "VETOED" else "PENDING"
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO experience_memory.decisions
                        (symbol, decided_at, trade_candidate, regime_context,
                         analog_report, risk_verdict, outcome_state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        candidate.symbol,
                        datetime.now(),
                        _safe_json(_candidate_snapshot(candidate)),
                        _safe_json(regime_context),
                        _safe_json(analog_report),
                        _safe_json(risk_verdict),
                        outcome_state,
                    ),
                )
                decision_id = cur.fetchone()[0]
            conn.commit()
            return decision_id
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY record_decision failed for {getattr(candidate, 'symbol', '?')}: {e}")
        return None


def update_allocation(decision_id, allocation_decision, outcome_state):
    """
    Called once Brain 6 (portfolio_manager) decides a candidate's capital
    fate (ALLOCATED / APPROVED_NO_CAPITAL). Attaches the AllocationDecision
    and moves outcome_state to NO_TRADE (never got capital) or leaves it at
    PENDING (allocated, but not yet actually opened as a paper trade —
    mark_open() below is what flips it to OPEN once open_paper_trade
    actually succeeds).
    """
    if decision_id is None:
        return False
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE experience_memory.decisions
                    SET allocation_decision = %s, outcome_state = %s
                    WHERE id = %s AND outcome_state <> 'CLOSED'
                    """,
                    (_safe_json(allocation_decision), outcome_state, decision_id),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY update_allocation failed for decision_id={decision_id}: {e}")
        return False


def mark_open(decision_id):
    """Flips a decision's outcome_state to OPEN once its paper trade has
    actually been opened (open_paper_trade succeeded)."""
    if decision_id is None:
        return False
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE experience_memory.decisions
                    SET outcome_state = 'OPEN'
                    WHERE id = %s AND outcome_state <> 'CLOSED'
                    """,
                    (decision_id,),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY mark_open failed for decision_id={decision_id}: {e}")
        return False


def get_decision(decision_id):
    """Fetches the stored trade_candidate/regime_context/analog_report/
    risk_verdict for a decision — what Brain 7 needs to judge, in
    hindsight, whether the evidence it was given actually panned out."""
    if decision_id is None:
        return None
    try:
        conn = get_connection()
        try:
            with dict_cursor(conn) as cur:
                cur.execute(
                    """
                    SELECT id, symbol, decided_at, trade_candidate, regime_context,
                           analog_report, risk_verdict, allocation_decision, outcome_state
                    FROM experience_memory.decisions
                    WHERE id = %s
                    """,
                    (decision_id,),
                )
                return cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY get_decision failed for decision_id={decision_id}: {e}")
        return None


def record_outcome(decision_id, opened_at, closed_at, entry, exit_price, exit_reason, pnl,
                    max_drawdown=None, max_profit=None):
    """Appends the realized result of a closed trade to
    experience_memory.trade_outcomes. Called once, at close, by
    os_brains.reviewer — never mutated afterwards (a closed trade's outcome
    is a historical fact)."""
    if decision_id is None:
        return None
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO experience_memory.trade_outcomes
                        (decision_id, opened_at, closed_at, entry, exit_price,
                         exit_reason, pnl, max_drawdown, max_profit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (decision_id, opened_at, closed_at, _safe_num(entry), _safe_num(exit_price),
                     exit_reason, _safe_num(pnl), _safe_num(max_drawdown), _safe_num(max_profit)),
                )
                outcome_id = cur.fetchone()[0]
            conn.commit()
            return outcome_id
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY record_outcome failed for decision_id={decision_id}: {e}")
        return None


def record_review(decision_id, was_correct, evidence_that_mattered, evidence_that_misled,
                   confidence_calibration_delta, lessons_learned):
    """Appends Brain 7's TradeReview verdict for a closed decision and
    flips outcome_state to CLOSED — the point at which this decision is
    "done" and has fully fed back into Experience Memory."""
    if decision_id is None:
        return None
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO experience_memory.trade_reviews
                        (decision_id, was_correct, evidence_that_mattered,
                         evidence_that_misled, confidence_calibration_delta, lessons_learned)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (decision_id, was_correct, _safe_json(evidence_that_mattered),
                     _safe_json(evidence_that_misled), _safe_num(confidence_calibration_delta), lessons_learned),
                )
                review_id = cur.fetchone()[0]
                cur.execute(
                    "UPDATE experience_memory.decisions SET outcome_state = 'CLOSED' WHERE id = %s",
                    (decision_id,),
                )
            conn.commit()
            return review_id
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY record_review failed for decision_id={decision_id}: {e}")
        return None


def upsert_calibration(setup_archetype, confidence_calibration_delta):
    """
    Rolls a new confidence_calibration_delta into the running average for
    `setup_archetype` (see os_brains.reviewer for how the archetype key is
    derived). This running average IS the continuous-learning mechanism:
    os_brains.historical_analog_engine._get_calibration_delta() reads
    exactly this table/column to nudge future probability_of_success for
    the same archetype, per ALPHAQUANT_OS_ARCHITECTURE.md section 7.
    """
    if setup_archetype is None:
        return False
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO experience_memory.calibration_state
                        (setup_archetype, sample_count, avg_calibration_delta, last_updated_at)
                    VALUES (%s, 1, %s, now())
                    ON CONFLICT (setup_archetype) DO UPDATE SET
                        avg_calibration_delta = (
                            (experience_memory.calibration_state.avg_calibration_delta
                                * experience_memory.calibration_state.sample_count)
                            + %s
                        ) / (experience_memory.calibration_state.sample_count + 1),
                        sample_count = experience_memory.calibration_state.sample_count + 1,
                        last_updated_at = now()
                    """,
                    (setup_archetype, _safe_num(confidence_calibration_delta), _safe_num(confidence_calibration_delta)),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY upsert_calibration failed for archetype={setup_archetype}: {e}")
        return False


def get_calibration(setup_archetype):
    """Reads the current calibration row for `setup_archetype`, or None if
    no trade for it has been reviewed yet."""
    try:
        conn = get_connection()
        try:
            with dict_cursor(conn) as cur:
                cur.execute(
                    "SELECT setup_archetype, sample_count, avg_calibration_delta, last_updated_at "
                    "FROM experience_memory.calibration_state WHERE setup_archetype = %s",
                    (setup_archetype,),
                )
                return cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        logging.warning(f"EXPERIENCE_MEMORY get_calibration failed for archetype={setup_archetype}: {e}")
        return None
