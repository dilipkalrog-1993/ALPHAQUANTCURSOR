# Legacy and Duplicate Entry Points

## Supported

- **`app.py`** — Canonical Streamlit entry point. Always use:
  ```bash
  streamlit run app.py
  ```

## Historical / Obsolete

| File | Notes |
|------|-------|
| `appemergentquant_v3_1.py` | Original single-file upload (v3.0.0 monolith). Contains entire UI, engines, and broker adapters inline. Imported by `app.py`. **Do not run directly** unless debugging import order. |
| Inlined `universe_engine.py` | Embedded inside monolith lines ~980–1608. Not a separate file. |

## Referenced but never uploaded to GitHub (restored in stabilization)

- `os_brains/` package
- `news_intelligence.py`

These were imported by the monolith but absent from the repository root at discovery time.

## Not present in repository

- `app.py` (older versions referenced in monolith comments)
- Separate strategy/brain source trees
- Automated test suite
- PDF/Excel report exporters

Do **not** delete `appemergentquant_v3_1.py` without migrating remaining inline engines to packages.
