#!/usr/bin/env python3
"""Headless pre-market readiness report.  This command never submits orders."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.readiness import build_pre_market_report


def main() -> int:
    report = build_pre_market_report(ROOT)
    print("ALPHAQUANT PRE-MARKET CHECK")
    for label, value in report["checks"].items():
        print(f"{label}: {value}")
    print("Readiness Details: " + json.dumps(report["details"], sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
