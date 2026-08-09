"""
AlphaQuant OS — Brain 1: Market Observer.

Responsibility (ALPHAQUANT_OS_ARCHITECTURE.md section 2): observe current
conditions. Never decides, never scores toward a trade, never vetoes.

This module does not duplicate Batch 1/2 signal logic — it calls the
existing functions in app.py (idempotently: each is only run if its output
isn't already present on the StockObject) and assembles their raw outputs
into a single MarketObservation, per the "absorbs existing code" mapping in
the architecture doc.
"""

import logging


def observe(stock, app_module):
    """
    Builds a MarketObservation for a single symbol. `stock` must already
    have `.data` (OHLCV + indicators) populated. `app_module` is the
    imported alphaquant.app module (passed explicitly rather than imported
    at module load time, so this stays usable both from the Streamlit app
    process and from standalone scripts/tests).
    """
    app = app_module

    if "SECTOR_NAME" not in stock.indicators:
        try:
            app.assign_sector(stock)
        except Exception as e:
            logging.warning(f"MARKET_OBSERVER sector failed for {stock.symbol}: {e}")

    if "RS_NIFTY_RETURN_PCT" not in stock.indicators:
        try:
            app.calculate_relative_strength(stock)
        except Exception as e:
            logging.warning(f"MARKET_OBSERVER relative_strength failed for {stock.symbol}: {e}")

    if "VOLUME_POC" not in stock.indicators and hasattr(app, "calculate_volume_profile"):
        try:
            app.calculate_volume_profile(stock)
        except Exception as e:
            logging.warning(f"MARKET_OBSERVER volume_profile failed for {stock.symbol}: {e}")

    if "OBV_TREND" not in stock.indicators and hasattr(app, "analyze_institutional_activity"):
        try:
            app.analyze_institutional_activity(stock)
        except Exception as e:
            logging.warning(f"MARKET_OBSERVER institutional_activity failed for {stock.symbol}: {e}")

    if "REGIME" not in stock.market:
        try:
            app.detect_market_regime(stock)
        except Exception as e:
            logging.warning(f"MARKET_OBSERVER regime failed for {stock.symbol}: {e}")

    df = stock.data
    last = df.iloc[-1] if df is not None and len(df) else None

    observation = {
        "symbol": stock.symbol,
        "timestamp": None,
        "price": {
            "last": float(last["Close"]) if last is not None else None,
            "ohlc": {
                "open": float(last["Open"]) if last is not None else None,
                "high": float(last["High"]) if last is not None else None,
                "low": float(last["Low"]) if last is not None else None,
                "close": float(last["Close"]) if last is not None else None,
            } if last is not None else None,
            "atr": float(last["ATR"]) if last is not None and "ATR" in df.columns and last.notna().get("ATR", False) else None,
            "rvol": float(last["RVOL"]) if last is not None and "RVOL" in df.columns and last.notna().get("RVOL", False) else None,
        },
        "volume": {
            "value": float(last["Volume"]) if last is not None else None,
            "zscore": stock.indicators.get("VOLUME_ZSCORE"),
            "obv_trend": stock.indicators.get("OBV_TREND"),
            "adl_trend": stock.indicators.get("ADL_TREND"),
        },
        "breadth": None,
        "sector": {
            "name": stock.indicators.get("SECTOR_NAME"),
            "relative_rank": stock.score.get("sector"),
        },
        "relative_strength": stock.indicators.get("RS_NIFTY_RETURN_PCT"),
        "news": {
            "days_to_earnings": stock.news.get("days_to_earnings"),
            "recent_headlines": stock.news.get("recent_headlines", []),
        },
        "macro": None,
        "futures_oi": None,
    }
    return observation


def observe_market(stocks):
    """
    Market-wide MarketObservation (symbol=None). `stocks` is the list of
    StockObject instances from the current scan, used to compute breadth
    (advancers/decliners/unchanged) without any new data downloads.
    """
    advancers = 0
    decliners = 0
    unchanged = 0
    for stock in stocks:
        df = stock.data
        if df is None or len(df) < 2:
            continue
        change = df["Close"].iloc[-1] - df["Close"].iloc[-2]
        if change > 0:
            advancers += 1
        elif change < 0:
            decliners += 1
        else:
            unchanged += 1

    return {
        "symbol": None,
        "timestamp": None,
        "price": None,
        "volume": None,
        "breadth": {
            "advancers": advancers,
            "decliners": decliners,
            "unchanged": unchanged,
        },
        "sector": None,
        "relative_strength": None,
        "news": None,
        "macro": None,
        "futures_oi": None,
    }
