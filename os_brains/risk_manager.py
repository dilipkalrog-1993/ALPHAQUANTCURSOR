"""
AlphaQuant OS - Brain 5: Risk Manager.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md section 2): veto. The only
Brain with unconditional authority to reject a TradeCandidate before it can
be acted on. Every veto is explicit and logged - a vetoed candidate is
never silently dropped; the AI Decision Engine in app.py keeps it visible
in the final trade list with its RiskVerdict attached.

Checks performed (every check runs, not just the first hit, so `vetoed_by`
can report every reason a candidate failed):
  EXPOSURE     - portfolio already at MAX_OPEN_POSITIONS real open trades
  CORRELATION  - adding this trade would push its sector's exposure (as a
                 % of capital, based on currently OPEN positions) past the
                 cap - the practical proxy for "correlation across
                 currently open positions" this codebase can compute
                 without a full covariance model
  LIQUIDITY    - below CONFIG's minimum avg volume/turnover, or the
                 trade's own (pre-Brain-6) position size is too large
                 relative to average volume
  VOLATILITY   - ATR% of price or RVOL is in an extreme/abnormal range
  RISK_REWARD  - risk/reward ratio below the floor (tightened, not just
                 left unchanged, in a mild bear regime - see MACRO)
  MACRO        - current regime (Brain 2) is TRENDING_BEAR with high
                 strength -> veto outright; a milder bear regime tightens
                 the RISK_REWARD floor instead of vetoing (the architecture
                 doc's "auto-tighten or veto" is implemented as a graduated
                 response, not an all-or-nothing switch)
  EVENT        - an earnings release is imminent (within
                 app.NEWS_EARNINGS_WARNING_DAYS)
"""

import logging

MAX_SECTOR_EXPOSURE_PCT = 40.0
MAX_POSITION_VOLUME_PCT = 10.0
DEFAULT_MIN_RISK_REWARD = 2.5
MACRO_BEAR_VETO_STRENGTH = 70
MACRO_BEAR_TIGHTEN_MULTIPLIER = 1.2
VOLATILITY_ATR_PCT_CEILING = 8.0
VOLATILITY_RVOL_CEILING = 4.0


def build_portfolio_state(app_module):
    """
    Snapshots the CURRENT real portfolio (open paper positions, capital,
    per-sector exposure) that Brain 5 vetoes against and Brain 6 allocates
    against. Deliberately derived only from state app.py already
    maintains (session_state.paper_positions / paper_capital) - this is a
    read-only summary both downstream Brains share, not new decision logic.
    """
    capital = app_module.st.session_state.paper_capital
    open_positions = app_module.st.session_state.paper_positions

    sector_exposure = {}
    for position in open_positions.values():
        stock = app_module.get_stock(position.symbol)
        sector = getattr(stock, "sector", None) or "UNKNOWN"
        capital_used = (position.entry or 0) * (position.quantity or 0)
        pct = (capital_used / capital * 100) if capital else 0.0
        sector_exposure[sector] = sector_exposure.get(sector, 0.0) + pct

    return {
        "capital": capital,
        "open_count": len(open_positions),
        "max_positions": app_module.CONFIG["MAX_OPEN_POSITIONS"],
        "sector_exposure": sector_exposure,
    }


def evaluate(candidate, stock, regime_context, portfolio_state, app_module):
    """
    Brain 5's output contract (RiskVerdict). `candidate` is a TradeCandidate
    already enriched by Brain 4 (os_brains.strategist.enrich_candidate) -
    this function only reads it, never mutates it, so the veto decision
    stays auditable independent of what Brain 4 produced.
    """
    vetoed_by = []
    notes = []

    # ---- EXPOSURE ----
    if portfolio_state["open_count"] >= portfolio_state["max_positions"]:
        vetoed_by.append("EXPOSURE")
        notes.append(
            f"portfolio already at max open positions "
            f"({portfolio_state['open_count']}/{portfolio_state['max_positions']})"
        )

    # ---- CORRELATION (sector concentration proxy) ----
    sector = getattr(stock, "sector", None) or "UNKNOWN"
    existing_sector_pct = portfolio_state["sector_exposure"].get(sector, 0.0)
    est_capital = getattr(candidate, "capital_required", 0) or 0
    est_pct = (est_capital / portfolio_state["capital"] * 100) if portfolio_state["capital"] else 0.0
    if sector != "UNKNOWN" and (existing_sector_pct + est_pct) > MAX_SECTOR_EXPOSURE_PCT:
        vetoed_by.append("CORRELATION")
        notes.append(
            f"sector {sector} exposure would reach "
            f"{round(existing_sector_pct + est_pct, 1)}% > {MAX_SECTOR_EXPOSURE_PCT}% cap"
        )

    # ---- LIQUIDITY ----
    df = stock.data
    last = df.iloc[-1] if df is not None and len(df) else None
    config = app_module.CONFIG
    if last is None:
        vetoed_by.append("LIQUIDITY")
        notes.append("no price data available")
    else:
        avg_volume = last.get("AVG_VOLUME20", 0) or 0
        turnover = avg_volume * (last.get("Close", 0) or 0)
        if avg_volume < config["MIN_AVG_VOLUME"]:
            vetoed_by.append("LIQUIDITY")
            notes.append(f"avg volume {int(avg_volume)} < {config['MIN_AVG_VOLUME']} minimum")
        elif turnover < config["MIN_AVG_TURNOVER"]:
            vetoed_by.append("LIQUIDITY")
            notes.append(f"avg turnover {int(turnover)} < {config['MIN_AVG_TURNOVER']} minimum")
        else:
            position_size = getattr(candidate, "position_size", 0) or 0
            if avg_volume > 0 and (position_size / avg_volume * 100) > MAX_POSITION_VOLUME_PCT:
                vetoed_by.append("LIQUIDITY")
                notes.append(
                    f"position size {position_size} is "
                    f"{round(position_size / avg_volume * 100, 1)}% of average volume "
                    f"(> {MAX_POSITION_VOLUME_PCT}% cap)"
                )

    # ---- VOLATILITY ----
    if last is not None:
        atr = last.get("ATR", None)
        close = last.get("Close", None)
        rvol = last.get("RVOL", None)
        if atr and close and close > 0 and (atr / close * 100) > VOLATILITY_ATR_PCT_CEILING:
            vetoed_by.append("VOLATILITY")
            notes.append(
                f"ATR is {round(atr / close * 100, 1)}% of price (> {VOLATILITY_ATR_PCT_CEILING}% ceiling)"
            )
        elif rvol and rvol > VOLATILITY_RVOL_CEILING:
            vetoed_by.append("VOLATILITY")
            notes.append(f"RVOL {round(rvol, 1)}x is above the {VOLATILITY_RVOL_CEILING}x ceiling")

    # ---- MACRO (regime) ----
    min_rr = getattr(app_module, "MIN_RR", DEFAULT_MIN_RISK_REWARD)
    current_regime = regime_context.get("current_regime") if regime_context else None
    current_strength = regime_context.get("current_regime_strength", 0) if regime_context else 0
    if current_regime == "TRENDING_BEAR":
        if current_strength >= MACRO_BEAR_VETO_STRENGTH:
            vetoed_by.append("MACRO")
            notes.append(
                f"strong bearish regime (strength {current_strength}) - capital preservation over allocation"
            )
        else:
            min_rr = round(min_rr * MACRO_BEAR_TIGHTEN_MULTIPLIER, 2)
            notes.append(f"mild bearish regime - risk/reward floor tightened to {min_rr}")

    # ---- RISK_REWARD ----
    risk_reward = getattr(candidate, "risk_reward", 0) or 0
    if risk_reward < min_rr:
        vetoed_by.append("RISK_REWARD")
        notes.append(f"risk/reward {risk_reward} below the {min_rr} floor")

    # ---- EVENT ----
    warning_days = getattr(app_module, "NEWS_EARNINGS_WARNING_DAYS", 5)
    days_to_earnings = stock.indicators.get("DAYS_TO_EARNINGS")
    if days_to_earnings is not None and days_to_earnings <= warning_days:
        vetoed_by.append("EVENT")
        notes.append(f"earnings in {days_to_earnings}d (<= {warning_days}d window)")

    verdict = "VETOED" if vetoed_by else "APPROVED"
    reason = "; ".join(notes) if notes else "within all limits"

    logging.info(
        f"RISK_MANAGER symbol={candidate.symbol} verdict={verdict} "
        f"vetoed_by={vetoed_by} reason={reason}"
    )

    return {
        "candidate_symbol": candidate.symbol,
        "verdict": verdict,
        "vetoed_by": vetoed_by,
        "reason": reason,
    }
