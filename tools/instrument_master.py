#!/usr/bin/env python3
"""Refresh or inspect the authoritative NSE cash-equity master."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from market.instrument_master import InstrumentMaster

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    master = InstrumentMaster()
    state = master.bootstrap(force_refresh=args.refresh)
    probes = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "HINDALCO", "LTIM", "TATAMOTORS"]
    print(json.dumps({**state, "path": str(master.cache_path),
        "probes": {p: master.normalize_symbol(p) in set(master.symbols()) for p in probes},
        "representative_symbols": master.symbols()[:20]}, indent=2))
    return 0 if state["status"] != "MASTER_UNAVAILABLE" else 2

if __name__ == "__main__":
    raise SystemExit(main())
