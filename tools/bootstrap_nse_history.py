#!/usr/bin/env python3
"""Safe, resumable, bounded NSE daily-history bootstrap."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.credential_store import CredentialStore
from core.history import DEFAULT_CACHE_DIR, _cache_path
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
    cache_states = Counter()
    readiness_by_symbol = {}
    for symbol in selected:
        frame = read_cache(symbol)
        state_name = cache_readiness(frame)
        present = _cache_path(symbol, DEFAULT_CACHE_DIR).exists()
        if state_name == "NO_HISTORY" and present:
            state_name = "BAD_OHLCV"
        readiness_by_symbol[symbol] = state_name
        cache_states[state_name] += 1
        if present:
            cache_states["CACHE_FILES_PRESENT"] += 1
        else:
            cache_states["CACHE_MISSING"] += 1
    # Failed attempts are deliberately retried on resume; only durable ready
    # results are complete.
    resumed_ready = {s for s, row in completed.items() if row.get("status") == "HISTORY_READY"
                     and readiness_by_symbol.get(s) == "HISTORY_READY"}
    work = [s for s in selected if s not in resumed_ready]
    provider_list = providers(args.provider)
    upstox = next((p for p in provider_list if p.name == "UPSTOX"), None)
    preflight_counts = Counter()
    if upstox is not None and not upstox.access_token:
        print("UPSTOX HISTORICAL: AUTH NOT AVAILABLE")
        if any(p.name == "YAHOO" for p in provider_list):
            print("Fallback provider: YAHOO")
        provider_list.remove(upstox)
    elif upstox is not None and selected:
        # Fail fast and visibly on token problems rather than silently hiding
        # thousands of identical Upstox failures behind Yahoo fallback.
        probe_end = datetime.now(timezone.utc).date()
        probe_start = probe_end - timedelta(days=14)
        mapping = master.canonical_mapping(selected[0])
        probe = upstox.fetch(symbol=mapping["historical_provider_symbol"],
            instrument_key=mapping.get("instrument_key"), start=probe_start.isoformat(),
            end=probe_end.isoformat(), timeout=12.0)
        print(f"UPSTOX HISTORICAL PREFLIGHT: {selected[0]} | {probe.status.value}")
        preflight_counts[("UPSTOX", probe.status.value)] += 1
        if probe.status.value == "AUTH_FAILURE":
            if any(p.name == "YAHOO" for p in provider_list):
                print("Fallback provider: YAHOO")
            provider_list.remove(upstox)
    print(f"master_count: {len(symbols)}")
    print(f"selected_count: {len(selected)}")
    print(f"cache_files_present: {cache_states['CACHE_FILES_PRESENT']}")
    print(f"cache_ready_and_fresh: {cache_states['HISTORY_READY']}")
    print(f"cache_stale: {cache_states['HISTORY_STALE']}")
    print(f"cache_insufficient: {cache_states['INSUFFICIENT_ROWS']}")
    print(f"bad_ohlcv: {cache_states['BAD_OHLCV']}")
    print(f"cache_missing: {cache_states['CACHE_MISSING']}")
    print(f"network_required: {sum(readiness_by_symbol[s] != 'HISTORY_READY' for s in work)}")
    print(f"Processing {len(work)} selected symbols; fresh caches are inspected as unchanged cache hits.")
    service = HistoryService(provider_list, master=master)
    started, results, interrupted, processed = time.monotonic(), [], False, 0
    try:
        with ThreadPoolExecutor(max_workers=min(args.workers, len(work) or 1)) as pool:
            future_map = {pool.submit(service.repair, symbol): symbol for symbol in work}
            for number, future in enumerate(as_completed(future_map), 1):
                record = future.result().report()
                processed += 1
                completed[future_map[future]] = record
                results.append(record)
                saved = atomic_json(args.checkpoint, {"master_refreshed_at": master.refreshed_at,
                    "provider": args.provider, "completed": completed})
                if not saved:
                    print("CHECKPOINT_WRITE_DEFERRED", file=sys.stderr, flush=True)
                attempted = ",".join(a["provider"] for a in record["provider_attempts"]) or "NONE"
                print(f"{record['symbol']} | attempted={attempted} | selected={record['provider_selected'] or 'NONE'} | {record['failure_category'] or record['status']}")
                if number % 25 == 0 or number == len(work):
                    print(f"{number} / {len(work)} complete", flush=True)
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupted; completed checkpoint and caches are safe.", file=sys.stderr)
    elapsed = time.monotonic() - started
    failures = Counter(r.get("failure_category") for r in results if r.get("failure_category"))
    attempt_counts = Counter((a["provider"], a["status"])
                             for r in results for a in r.get("provider_attempts", []))
    attempt_counts.update(preflight_counts)
    remaining = max(0, len(work) - processed)
    summary = {"master_count": len(symbols), "selected_count": len(selected),
        "cache_files_present": cache_states["CACHE_FILES_PRESENT"],
        "cache_ready_and_fresh": cache_states["HISTORY_READY"],
        "cache_stale": cache_states["HISTORY_STALE"],
        "cache_insufficient": cache_states["INSUFFICIENT_ROWS"],
        "bad_ohlcv": cache_states["BAD_OHLCV"], "cache_missing": cache_states["CACHE_MISSING"],
        "network_required": sum(not r.get("cache_hit") for r in results),
        "processed_this_run": processed,
        "successful_updates": sum(bool(r.get("rows_written")) for r in results),
        "unchanged_cache_hits": sum(bool(r.get("cache_hit")) for r in results),
        "failures": sum(bool(r.get("failure_category")) for r in results), "remaining": remaining,
        "cache_hits": sum(bool(r.get("cache_hit")) for r in results),
        "incremental_updates": sum(bool(r.get("incremental_update")) for r in results),
        "full_bootstraps": sum(bool(r.get("full_bootstrap")) for r in results),
        "successes": sum(r.get("status") == "HISTORY_READY" for r in results),
        "provider_failures_by_category": dict(failures),
        "provider_summary": {"UPSTOX_successes": attempt_counts[("UPSTOX", "SUCCESS")],
            "UPSTOX_auth_failures": attempt_counts[("UPSTOX", "AUTH_FAILURE")],
            "UPSTOX_rate_limits": attempt_counts[("UPSTOX", "RATE_LIMIT")],
            "YAHOO_fallback_successes": sum(r.get("provider_selected") == "YAHOO" for r in results),
            "YAHOO_no_data": attempt_counts[("YAHOO", "NO_DATA")],
            "other_provider_failures": sum(n for (p, s), n in attempt_counts.items()
                if s not in {"SUCCESS", "AUTH_FAILURE", "RATE_LIMIT", "NO_DATA"})},
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
