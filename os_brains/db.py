"""
AlphaQuant OS — Postgres connection + schema management.

Owns the connection helper and the idempotent DDL for the two Memory
stores described in ALPHAQUANT_OS_ARCHITECTURE.md section 3:

  - market_memory     : facts about the market, independent of AlphaQuant's
                         own decisions. Populated by the Phase 1 backfill
                         pipeline (this task).
  - experience_memory  : facts about what AlphaQuant itself decided and how
                         it turned out. Schema created now; populated later
                         by the Reviewer/continuous-learning task.

No other module should issue raw DDL — always go through apply_schema()
here so the schema stays a single source of truth.
"""

import os
import logging

import psycopg2
import psycopg2.extras


def get_connection():
    """
    Returns a new psycopg2 connection using the project's pre-configured
    Postgres database (DATABASE_URL). Callers are responsible for closing
    the connection (use as a context manager: `with get_connection() as conn:`).
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set — the Replit Postgres database must be "
            "provisioned before AlphaQuant OS Brains can be used."
        )
    return psycopg2.connect(dsn)


MARKET_MEMORY_DDL = """
CREATE SCHEMA IF NOT EXISTS market_memory;

CREATE TABLE IF NOT EXISTS market_memory.daily_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    trading_day     DATE NOT NULL,
    close           NUMERIC,
    setup_vector    JSONB NOT NULL,
    raw_indicators  JSONB,
    pattern_flags   JSONB,
    sector          TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingestion_phase SMALLINT NOT NULL,
    UNIQUE (symbol, trading_day)
);
CREATE INDEX IF NOT EXISTS idx_daily_snapshots_trading_day
    ON market_memory.daily_snapshots (trading_day);
CREATE INDEX IF NOT EXISTS idx_daily_snapshots_setup_vector
    ON market_memory.daily_snapshots USING GIN (setup_vector);

CREATE TABLE IF NOT EXISTS market_memory.forward_outcomes (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_id         BIGINT NOT NULL REFERENCES market_memory.daily_snapshots(id)
                             ON DELETE CASCADE,
    holding_period_days INT NOT NULL,
    forward_return      NUMERIC,
    max_drawdown        NUMERIC,
    max_favorable_move  NUMERIC,
    recovered_by_day    INT,
    UNIQUE (snapshot_id, holding_period_days)
);

CREATE TABLE IF NOT EXISTS market_memory.historical_regimes (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    characteristics JSONB NOT NULL,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS market_memory.structural_levels (
    id           BIGSERIAL PRIMARY KEY,
    symbol       TEXT NOT NULL,
    level_type   TEXT NOT NULL,
    price_low    NUMERIC NOT NULL,
    price_high   NUMERIC NOT NULL,
    first_seen   DATE NOT NULL,
    last_valid   DATE,
    strength     INT,
    metadata     JSONB
);
CREATE INDEX IF NOT EXISTS idx_structural_levels_symbol_type
    ON market_memory.structural_levels (symbol, level_type);

CREATE TABLE IF NOT EXISTS market_memory.ingestion_coverage (
    symbol          TEXT PRIMARY KEY,
    first_day       DATE NOT NULL,
    last_day        DATE NOT NULL,
    phase           SMALLINT NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

EXPERIENCE_MEMORY_DDL = """
CREATE SCHEMA IF NOT EXISTS experience_memory;

CREATE TABLE IF NOT EXISTS experience_memory.decisions (
    id                  BIGSERIAL PRIMARY KEY,
    symbol              TEXT NOT NULL,
    decided_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    trade_candidate     JSONB NOT NULL,
    regime_context      JSONB NOT NULL,
    analog_report       JSONB NOT NULL,
    risk_verdict        JSONB NOT NULL,
    allocation_decision JSONB,
    outcome_state       TEXT NOT NULL DEFAULT 'PENDING'
);
CREATE INDEX IF NOT EXISTS idx_decisions_symbol_decided_at
    ON experience_memory.decisions (symbol, decided_at);
CREATE INDEX IF NOT EXISTS idx_decisions_outcome_state
    ON experience_memory.decisions (outcome_state);

CREATE TABLE IF NOT EXISTS experience_memory.trade_outcomes (
    id              BIGSERIAL PRIMARY KEY,
    decision_id     BIGINT NOT NULL REFERENCES experience_memory.decisions(id)
                         ON DELETE CASCADE,
    opened_at       TIMESTAMPTZ NOT NULL,
    closed_at       TIMESTAMPTZ NOT NULL,
    entry           NUMERIC NOT NULL,
    exit_price      NUMERIC NOT NULL,
    exit_reason     TEXT NOT NULL,
    pnl             NUMERIC NOT NULL,
    max_drawdown    NUMERIC,
    max_profit      NUMERIC
);

CREATE TABLE IF NOT EXISTS experience_memory.trade_reviews (
    id                            BIGSERIAL PRIMARY KEY,
    decision_id                   BIGINT NOT NULL REFERENCES experience_memory.decisions(id)
                                       ON DELETE CASCADE,
    reviewed_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    was_correct                   BOOLEAN NOT NULL,
    evidence_that_mattered        JSONB,
    evidence_that_misled          JSONB,
    confidence_calibration_delta  NUMERIC NOT NULL,
    lessons_learned               TEXT
);

CREATE TABLE IF NOT EXISTS experience_memory.calibration_state (
    setup_archetype        TEXT PRIMARY KEY,
    sample_count            INT NOT NULL DEFAULT 0,
    avg_calibration_delta   NUMERIC NOT NULL DEFAULT 0,
    last_updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS experience_memory.portfolio_snapshots (
    id             BIGSERIAL PRIMARY KEY,
    snapshot_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    open_positions INT NOT NULL,
    closed_trades  INT NOT NULL,
    net_pnl        NUMERIC NOT NULL,
    win_rate       NUMERIC,
    metadata       JSONB
);
"""


def apply_schema():
    """
    Creates both Memory schemas if they do not already exist. Safe to call
    repeatedly (idempotent) — every statement uses IF NOT EXISTS.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(MARKET_MEMORY_DDL)
            cur.execute(EXPERIENCE_MEMORY_DDL)
        conn.commit()
        logging.info("ALPHAQUANT_OS_SCHEMA: market_memory + experience_memory ready")
    finally:
        conn.close()


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply_schema()
    print("Schema applied.")
