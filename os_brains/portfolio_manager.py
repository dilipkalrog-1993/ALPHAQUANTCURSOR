"""
AlphaQuant OS - Brain 6: Portfolio Manager.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md section 2): for every
RiskVerdict: APPROVED candidate, decide position size and portfolio-level
allocation - sector caps, overall exposure, and cash reserve. This module
has final authority over sizing: app.calculate_position_size still supplies
the risk-based baseline quantity/capital for a trade, but Portfolio Manager
can shrink or reject that sizing to respect portfolio/sector limits, and
decides the ORDER approved candidates get funded in - ranked by
expected_value (Brain 4's historical-analog-informed estimate), not just
ai_score, per the architecture doc.
"""

import logging

from os_brains.risk_manager import MAX_SECTOR_EXPOSURE_PCT


def allocate(approved_candidates, portfolio_state, app_module):
    """
    Brain 6's output contract (AllocationDecision), one per approved
    candidate considered. Candidates that pass Risk but get no capital
    (slots/sector/cash already exhausted) are still returned with
    position_size=0 and a rationale - "no capital available" stays a
    visible outcome instead of a silent drop, matching the veto-visibility
    rule Risk Manager already follows.
    """
    decisions = []

    if not approved_candidates:
        return decisions

    capital = portfolio_state["capital"]
    max_positions = portfolio_state["max_positions"]
    remaining_slots = max(0, max_positions - portfolio_state["open_count"])

    sector_allocation = dict(portfolio_state["sector_exposure"])
    remaining_capital = capital

    ranked = sorted(
        approved_candidates,
        key=lambda c: (
            getattr(c, "expected_value", 0) or 0,
            getattr(c, "ai_score", 0) or 0,
            getattr(c, "confidence", 0) or 0,
        ),
        reverse=True,
    )

    funded = 0

    for candidate in ranked:

        symbol = candidate.symbol
        sector = getattr(app_module.get_stock(symbol), "sector", None) or "UNKNOWN"

        if funded >= remaining_slots:
            decisions.append(_no_capital_decision(
                candidate, sector_allocation, remaining_capital,
                "no open position slots remaining"
            ))
            continue

        app_module.calculate_position_size(candidate)

        if not candidate.position_size or candidate.position_size <= 0:
            decisions.append(_no_capital_decision(
                candidate, sector_allocation, remaining_capital,
                "position size rounds to zero at current risk sizing"
            ))
            continue

        if candidate.capital_required > remaining_capital:
            decisions.append(_no_capital_decision(
                candidate, sector_allocation, remaining_capital,
                "insufficient remaining capital"
            ))
            continue

        est_pct = (candidate.capital_required / capital * 100) if capital else 0.0
        current_sector_pct = sector_allocation.get(sector, 0.0)

        if sector != "UNKNOWN" and (current_sector_pct + est_pct) > MAX_SECTOR_EXPOSURE_PCT:
            # Try shrinking the position to fit under the sector cap
            # instead of rejecting outright - a smaller allocation to a
            # good setup is still better than none.
            headroom_pct = max(0.0, MAX_SECTOR_EXPOSURE_PCT - current_sector_pct)
            headroom_capital = capital * (headroom_pct / 100)
            shrink_qty = int(headroom_capital / candidate.entry) if candidate.entry else 0
            if shrink_qty <= 0:
                decisions.append(_no_capital_decision(
                    candidate, sector_allocation, remaining_capital,
                    f"sector {sector} already at/near the {MAX_SECTOR_EXPOSURE_PCT}% cap"
                ))
                continue
            candidate.position_size = min(candidate.position_size, shrink_qty)
            candidate.capital_required = round(candidate.position_size * candidate.entry, 2)
            candidate.add_reason(f"[PortfolioManager] Position trimmed to respect sector cap ({sector})")

        candidate.portfolio_weight = round((candidate.capital_required / capital) * 100, 2) if capital else 0
        remaining_capital -= candidate.capital_required
        sector_allocation[sector] = sector_allocation.get(sector, 0.0) + candidate.portfolio_weight
        candidate.state = "ALLOCATED"
        candidate.sector_allocation_after = dict(sector_allocation)
        candidate.cash_reserved = round(remaining_capital, 2)
        rationale = (
            f"Ranked #{funded + 1} by expected value "
            f"({round(getattr(candidate, 'expected_value', 0) or 0, 4)}); "
            f"allocated {candidate.position_size} shares "
            f"({candidate.portfolio_weight}% of capital)"
        )
        candidate.allocation_rationale = rationale
        funded += 1

        decisions.append({
            "symbol": symbol,
            "position_size": candidate.position_size,
            "capital_required": candidate.capital_required,
            "sector_allocation_after": dict(sector_allocation),
            "cash_reserved": candidate.cash_reserved,
            "rationale": rationale,
        })

    logging.info(
        f"PORTFOLIO_MANAGER approved_in={len(approved_candidates)} funded={funded} "
        f"remaining_capital={round(remaining_capital, 2)}"
    )

    return decisions


def _no_capital_decision(candidate, sector_allocation, remaining_capital, reason):
    candidate.position_size = 0
    candidate.capital_required = 0
    candidate.state = "APPROVED_NO_CAPITAL"
    candidate.allocation_rationale = reason
    return {
        "symbol": candidate.symbol,
        "position_size": 0,
        "capital_required": 0,
        "sector_allocation_after": dict(sector_allocation),
        "cash_reserved": round(remaining_capital, 2),
        "rationale": reason,
    }
