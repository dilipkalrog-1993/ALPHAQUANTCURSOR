"""
AlphaQuant OS — Brains package.

This package implements the persistent data layer and the first three
"Brains" described in ALPHAQUANT_OS_ARCHITECTURE.md:

  - db / schema        : Postgres connection + Market Memory / Experience
                          Memory schema (idempotent DDL).
  - market_observer     : Brain 1 — structured observation snapshot.
  - market_historian    : Brain 2 — historical regime catalog + comparison.
  - setup_vector        : shared feature-vector construction used by both
                          the backfill pipeline and the live analog lookup.
  - historical_analog_engine : Brain 3 — similarity search over backfilled
                          setups.
  - backfill            : resumable Phase 1 backfill pipeline.

Nothing in this package changes how trades are scored, ranked, or vetoed.
It is deliberately read/observe-only with respect to the existing scan
pipeline in app.py — see ALPHAQUANT_OS_ARCHITECTURE.md section 6 for the
mapping of existing code to Brains.
"""

from os_brains import experience_memory

__all__ = ["experience_memory"]
