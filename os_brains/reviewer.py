"""
AlphaQuant OS - Brain 7: Reviewer.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md sections 2 & 7): review every
completed paper trade against the decision that opened it, write the
verdict to Experience Memory, and roll a confidence-calibration adjustment
into experience_memory.calibration_state - the mechanism by which future
Strategist/Historical-Analog-Engine confidence for a similar setup actually
moves in response to what happened last time, closing the continuous-
learning loop the architecture doc calls for.

Never re-decides a trade and never touches app.py's live position objects
beyond reading them - it only reads a just-closed PaperPosition plus the
Decision Brain 4/5 already recorded for it, and writes to Experience
Memory. Called once per position, exactly when it transitions to CLOSED
(see app.py's PaperPosition.close_trade, the single choke point every
close path - old update_paper_trade/mark_closed and the newer
check_stop_loss/check_target3 - already funnels through).
"""

import logging

from os_brains import experience_memory


def setup_archetype_for(symbol, strategy=None):
    """
    The bucketing key calibration_state rolls up against. Kept as just the
    symbol (not a coarser strategy/regime bucket) so it exactly matches the
    key os_brains.historical_analog_engine._get_calibration_delta() already
    reads by - that lookup was written in Task 3, before this module
    existed, so this task keeps the key format Brain 3 depends on rather
    than introducing a second, incompatible archetype scheme.
    """
    return symbol


def _evidence_verdict(evidence_summary, was_correct):
    """
    Splits the evidence_summary Brain 4 attached to the original candidate
    into what "mattered" (pointed the same direction as the actual outcome)
    vs what "misled" (pointed the other way) - hindsight in the plainest
    reproducible sense: any factor scored as '+' that turned out to be
    right is evidence that mattered, and mattered contra-evidence is what
    misled the decision.
    """
    mattered, misled = [], []
    outcome_sign = "+" if was_correct else "-"
    for item in evidence_summary or []:
        bucket = mattered if item.get("direction") == outcome_sign else misled
        bucket.append(item)
    return mattered, misled


def _lessons_learned(symbol, strategy, was_correct, exit_reason, pnl, analog_report, mattered, misled):
    verdict = "played out as expected" if was_correct else "did not play out as expected"
    lines = [
        f"{symbol} ({strategy}) {verdict}: closed via {exit_reason} with P&L {round(pnl, 2)}."
    ]
    sample_confidence = (analog_report or {}).get("sample_confidence")
    matched = (analog_report or {}).get("matched_analogs_count") or 0
    if matched:
        win_rate = (analog_report or {}).get("win_rate")
        lines.append(
            f"Historical analogs at entry: {matched} matches ({sample_confidence} confidence), "
            f"win_rate={round(win_rate * 100, 1) if win_rate is not None else 'n/a'}%."
        )
    else:
        lines.append("No historical analog evidence was available at entry.")
    if mattered:
        lines.append("Evidence that mattered: " + "; ".join(m.get("factor", "?") for m in mattered) + ".")
    if misled:
        lines.append("Evidence that misled: " + "; ".join(m.get("factor", "?") for m in misled) + ".")
    return " ".join(lines)


def _confidence_calibration_delta(was_correct, analog_report):
    """
    Bounded nudge applied to future probability_of_success for this
    archetype. Scaled down when the original analog evidence was thin
    (LOW/no sample_confidence) so one lucky/unlucky trade on scant evidence
    can't swing future confidence as hard as a well-evidenced one.
    """
    base = 0.05
    confidence_scale = {"HIGH": 1.0, "MEDIUM": 0.6, "LOW": 0.3}.get(
        (analog_report or {}).get("sample_confidence"), 0.3
    )
    delta = base * confidence_scale
    return round(delta if was_correct else -delta, 4)


def review_closed_trade(position, app_module=None):
    """
    Brain 7's entry point. `position` is a just-closed PaperPosition
    (status == "CLOSED"). Returns the TradeReview dict written to
    Experience Memory, or None if this position has no linked decision_id
    (e.g. it predates this task, or record_decision failed at entry) -
    there is nothing to review a trade against without the Decision that
    opened it, so this degrades to a no-op rather than guessing.
    """
    decision_id = getattr(position, "decision_id", None)
    if decision_id is None:
        logging.info(f"REVIEWER symbol={position.symbol} has no decision_id - skipping review")
        return None

    decision = experience_memory.get_decision(decision_id)
    if decision is None:
        logging.warning(f"REVIEWER symbol={position.symbol} decision_id={decision_id} not found - skipping review")
        return None

    candidate_snapshot = decision.get("trade_candidate") or {}
    analog_report = decision.get("analog_report") or {}
    evidence_summary = candidate_snapshot.get("evidence_summary") or []

    pnl = position.total_pnl() if hasattr(position, "total_pnl") else position.realized_pnl
    exit_reason = getattr(position, "exit_reason", "") or ""

    if "STOP" in exit_reason.upper():
        was_correct = False
    elif "TARGET" in exit_reason.upper():
        was_correct = True
    else:
        was_correct = pnl > 0

    mattered, misled = _evidence_verdict(evidence_summary, was_correct)
    lessons_learned = _lessons_learned(
        position.symbol, position.strategy, was_correct, exit_reason, pnl,
        analog_report, mattered, misled,
    )
    confidence_calibration_delta = _confidence_calibration_delta(was_correct, analog_report)

    opened_at = getattr(position, "entry_time", None)
    closed_at = getattr(position, "exit_time", None)

    experience_memory.record_outcome(
        decision_id=decision_id,
        opened_at=opened_at,
        closed_at=closed_at,
        entry=position.entry,
        exit_price=getattr(position, "current_price", position.entry),
        exit_reason=exit_reason,
        pnl=pnl,
        max_drawdown=getattr(position, "max_drawdown", None),
        max_profit=getattr(position, "max_profit", None),
    )

    experience_memory.record_review(
        decision_id=decision_id,
        was_correct=was_correct,
        evidence_that_mattered=mattered,
        evidence_that_misled=misled,
        confidence_calibration_delta=confidence_calibration_delta,
        lessons_learned=lessons_learned,
    )

    archetype = setup_archetype_for(position.symbol, position.strategy)
    experience_memory.upsert_calibration(archetype, confidence_calibration_delta)

    logging.info(
        f"REVIEWER symbol={position.symbol} decision_id={decision_id} was_correct={was_correct} "
        f"pnl={round(pnl, 2)} calibration_delta={confidence_calibration_delta}"
    )

    return {
        "decision_id": decision_id,
        "was_correct": was_correct,
        "evidence_that_mattered": mattered,
        "evidence_that_misled": misled,
        "confidence_calibration_delta": confidence_calibration_delta,
        "lessons_learned": lessons_learned,
    }
