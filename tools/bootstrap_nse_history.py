#!/usr/bin/env python3
"""Safe, resumable, bounded NSE daily-history bootstrap."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.credential_store import CredentialStore
from core.history import DEFAULT_CACHE_DIR
from core.history_providers import UpstoxHistoryProvider, YahooHistoryProvider
from core.history_service import HistoryService, atomic_json, cache_readiness, read_cache
from market.instrument_master import InstrumentMaster

CHECKPOINT = ROOT / "data" / "history_bootstrap_checkpoint.json"


def _token() -> str:
    direct = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
    if direct:
        return direct
    connections = ROOT / "data" / "broker_connections.json"
    try:
        payload = json.loads(connections.read_text(encoding="utf-8"))
        profiles = payload.get("profiles") or {}
        selected = payload.get("default_market_data_broker") or next(iter(profiles), "")
        return CredentialStore().load_profile_secrets(selected).get("access_token", "")
    except (OSError, ValueError, TypeError):
        return ""


def providers(name: str):
    choices = {"upstox": lambda: [UpstoxHistoryProvider(_token())],
               "yahoo": lambda: [YahooHistoryProvider()],
               "auto": lambda: [UpstoxHistoryProvider(_token()), YahooHistoryProvider()]}
    return choices[name]()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    size = p.add_mutually_exclusive_group()
    size.add_argument("--limit", type=int, default=50, help="bounded symbol count (default: 50)")
    size.add_argument("--full", action="store_true", help="process the complete master")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--incremental", action="store_true", default=True,
                   help="fetch only candles after the last cached candle (default)")
    p.add_argument("--provider", choices=("auto", "upstox", "yahoo"), default="auto")
    p.add_argument("--resume", action="store_true", help="skip symbols completed by the prior run")
    p.add_argument("--checkpoint", type=Path, default=CHECKPOINT)
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.workers < 1 or args.workers > 32:
        raise SystemExit("--workers must be between 1 and 32")
    master = InstrumentMaster()
    state = master.bootstrap(force_refresh=False)
    symbols = master.symbols()
    if not symbols:
        print("Instrument master unavailable", file=sys.stderr)
        return 2
    selected = symbols if args.full else symbols[:max(0, args.limit)]
    completed: dict[str, dict] = {}
    if args.resume and args.checkpoint.exists():
        try:
            completed = json.loads(args.checkpoint.read_text(encoding="utf-8")).get("completed", {})
        except (OSError, ValueError, TypeError):
            completed = {}
    cached = sum(not read_cache(s).empty for s in symbols)
    # Failed attempts are deliberately retried on resume; only durable ready
    # results are complete.
    resumed_ready = {s for s, row in completed.items() if row.get("status") == "HISTORY_READY"}
    work = [s for s in selected if s not in resumed_ready]
    print(f"Master: {len(symbols)}")
    print(f"Already cached: {cached}")
    print(f"Need repair/bootstrap: {len(symbols) - cached}")
    service = HistoryService(providers(args.provider), master=master)
    started, results, interrupted, processed = time.monotonic(), [], False, 0
    try:
        with ThreadPoolExecutor(max_workers=min(args.workers, len(work) or 1)) as pool:
            future_map = {pool.submit(service.repair, symbol): symbol for symbol in work}
            for number, future in enumerate(as_completed(future_map), 1):
                record = future.result().report()
                processed += 1
                completed[future_map[future]] = record
                results.append(record)
                atomic_json(args.checkpoint, {"master_refreshed_at": master.refreshed_at,
                    "provider": args.provider, "completed": completed})
                if number % 25 == 0 or number == len(work):
                    print(f"{number} / {len(work)} complete", flush=True)
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupted; completed checkpoint and caches are safe.", file=sys.stderr)
    elapsed = time.monotonic() - started
    failures = Counter(r.get("failure_category") for r in results if r.get("failure_category"))
    summary = {"master_count": len(symbols), "selected": len(selected),
        "cache_hits": sum(bool(r.get("cache_hit")) for r in results),
        "incremental_updates": sum(bool(r.get("incremental_update")) for r in results),
        "full_bootstraps": sum(bool(r.get("full_bootstrap")) for r in results),
        "successes": sum(r.get("status") == "HISTORY_READY" for r in results),
        "provider_failures_by_category": dict(failures),
        "invalid_symbols": failures.get("INVALID_SYMBOL", 0),
        "confirmed_delisted": failures.get("DELISTED_CONFIRMED", 0),
        "rows_written": sum(int(r.get("rows_written") or 0) for r in results),
        "elapsed_seconds": round(elapsed, 3),
        "average_symbols_per_second": round(processed / elapsed, 3) if elapsed else 0,
        "interrupted": interrupted, "checkpoint": str(args.checkpoint),
        "instrument_master_status": state["status"]}
    print(json.dumps(summary, indent=2, default=str))
    return 130 if interrupted else 0


if __name__ == "__main__":
    raise SystemExit(main())
