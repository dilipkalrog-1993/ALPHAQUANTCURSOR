# PostgreSQL Configuration (AlphaQuant OS Brains)

Authentic Brains **2 (Market Historian)**, **3 (Historical Analog Engine)**, and **7 (Reviewer / Experience Memory)** use PostgreSQL via `DATABASE_URL`.

## Production setup

```bash
export DATABASE_URL="postgresql://user:password@host:5432/alphaquant"
```

On first run the application calls `os_brains.db.apply_schema()` idempotently.

## Backfill (Brain 3 dataset)

```bash
export DATABASE_URL="..."
python -m os_brains.backfill   # if exposed, or run backfill.py via project tooling
```

## Cloud / local development without Postgres

- **FAST PATH** (default Paper mode) remains fully testable without Postgres.
- Deep AI enrichment reports `DEEP_AI_UNAVAILABLE` when `DATABASE_URL` is unset.
- Brains 5 and 6 (Risk, Portfolio) do **not** require Postgres.
- Do not substitute SQLite for the authentic intelligence database in production.

## Verification

```bash
python tools/run_alphaquant_diagnostics.py --trace-brains
python tools/run_full_pipeline_acceptance.py
```
