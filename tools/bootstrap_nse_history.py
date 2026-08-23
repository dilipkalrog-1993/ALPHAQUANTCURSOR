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


class CoordinatorLock:
    """Prevent concurrent processes from coordinating the same checkpoint."""
    def __init__(self, checkpoint: Path):
        self.path = checkpoint.with_suffix(checkpoint.suffix + ".lock")
        self.fd: int | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self.fd, str(os.getpid()).encode())
        except FileExistsError as exc:
            raise RuntimeError(f"checkpoint coordinator already running: {self.path}") from exc
        return self

    def __exit__(self, *_):
        if self.fd is not None:
            os.close(self.fd)
        try:
            self.path.unlink()
        except OSError:
            pass


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


def history_inventory(symbols: list[str]) -> tuple[dict[str, str], dict[str, int]]:
    """Inspect each selected cache once and return mutually exclusive counts."""
    readiness = {symbol: cache_readiness(read_cache(symbol)) for symbol in symbols}
    counts = Counter(readiness.values())
    classified = sum(counts[key] for key in ("HISTORY_READY", "HISTORY_STALE",
                                              "INSUFFICIENT_ROWS"))
    return readiness, {
        "history_ready_fresh": counts["HISTORY_READY"],
        "history_stale": counts["HISTORY_STALE"],
        "history_insufficient": counts["INSUFFICIENT_ROWS"],
        "history_missing": len(symbols) - classified,
    }


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
    all_present = sum((DEFAULT_CACHE_DIR / f"{master.normalize_symbol(s).replace('.NS', '')}.pkl").exists() for s in symbols)
    readiness, inventory = history_inventory(selected)
    fresh = {s for s, status in readiness.items() if status == "HISTORY_READY"}
    stale = {s for s, status in readiness.items() if status == "HISTORY_STALE"}
    insufficient = {s for s, status in readiness.items() if status == "INSUFFICIENT_ROWS"}
    missing = set(selected) - fresh - stale - insufficient
    work = [s for s in selected if s not in fresh]
    token_available = bool(_token())
    yahoo_enabled = args.provider in {"auto", "yahoo"}
    print(f"UPSTOX HISTORICAL: {'AVAILABLE' if token_available else 'AUTH NOT AVAILABLE'}")
    print(f"YAHOO FALLBACK: {'ENABLED' if yahoo_enabled else 'DISABLED'}")
    print(f"master_count: {len(symbols)}")
    print(f"selected_count: {len(selected)}")
    print(f"cache_files_present: {all_present}")
    print(f"history_ready_fresh: {len(fresh)}")
    print(f"history_stale: {len(stale)}")
    print(f"history_insufficient: {len(insufficient)}")
    print(f"history_missing: {len(missing)}")
    print(f"Symbols requiring freshness inspection: {len(selected)}")
    print(f"network_required: {len(work)}")
    configured_providers = providers(args.provider)
    service = HistoryService(configured_providers, master=master)
    started, results, interrupted, processed = time.monotonic(), [], False, 0
    checkpoint_deferred = False
    try:
        with CoordinatorLock(args.checkpoint), \
                ThreadPoolExecutor(max_workers=min(args.workers, len(work) or 1)) as pool:
            future_map = {pool.submit(service.repair, symbol): symbol for symbol in work}
            for number, future in enumerate(as_completed(future_map), 1):
                record = future.result().report()
                processed += 1
                completed[future_map[future]] = record
                results.append(record)
                if record.get("status") != "HISTORY_READY":
                    last = (record.get("provider_attempts") or [{}])[-1]
                    print(f"{future_map[future]} | {last.get('provider', 'NONE')} | "
                          f"{last.get('status', record.get('failure_category', 'OTHER_FAILURE'))}")
                if not atomic_json(args.checkpoint, {"master_refreshed_at": master.refreshed_at,
                        "provider": args.provider, "completed": completed}):
                    checkpoint_deferred = True
                    print("CHECKPOINT_WRITE_DEFERRED", file=sys.stderr, flush=True)
                if number % 25 == 0 or number == len(work):
                    print(f"processed_this_run: {number} / {len(work)}", flush=True)
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupted; completed checkpoint and caches are safe.", file=sys.stderr)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    elapsed = time.monotonic() - started
    failures = Counter(r.get("failure_category") for r in results if r.get("failure_category"))
    attempts = [a for r in results for a in r.get("provider_attempts", [])]
    def attempt_count(provider, status=None, *, other=False):
        rows = [a for a in attempts if a.get("provider") == provider]
        if other:
            reported = {"SUCCESS", "AUTH_FAILURE", "RATE_LIMIT"}
            if provider == "YAHOO":
                reported.add("NO_DATA")
            return sum(a.get("status") not in reported for a in rows)
        return sum(a.get("status") == status for a in rows)
    summary = {"master_count": len(symbols), "selected_count": len(selected),
        "cache_files_present": all_present, "history_ready_fresh": len(fresh),
        "history_stale": len(stale), "history_insufficient": len(insufficient),
        "history_missing": len(missing), "network_required": len(work),
        "processed_this_run": processed, "cache_only_completed": len(fresh),
        "incremental_updates": sum(bool(r.get("incremental_update")) for r in results),
        "full_bootstraps": sum(bool(r.get("full_bootstrap")) for r in results),
        "provider_failures": sum(r.get("status") != "HISTORY_READY" for r in results),
        "remaining": max(0, len(work) - processed),
        "upstox_successes": attempt_count("UPSTOX", "SUCCESS"),
        "upstox_auth_failures": attempt_count("UPSTOX", "AUTH_FAILURE"),
        "upstox_rate_limits": attempt_count("UPSTOX", "RATE_LIMIT"),
        "upstox_other_failures": attempt_count("UPSTOX", other=True),
        "yahoo_fallback_successes": attempt_count("YAHOO", "SUCCESS"),
        "yahoo_no_data": attempt_count("YAHOO", "NO_DATA"),
        "yahoo_auth_failures": attempt_count("YAHOO", "AUTH_FAILURE"),
        "other_failures": sum(attempt_count(p, other=True) for p in ("UPSTOX", "YAHOO")),
        "provider_failures_by_category": dict(failures),
        "invalid_symbols": failures.get("INVALID_SYMBOL", 0),
        "confirmed_delisted": failures.get("DELISTED_CONFIRMED", 0),
        "rows_written": sum(int(r.get("rows_written") or 0) for r in results),
        "elapsed_seconds": round(elapsed, 3),
        "average_symbols_per_second": round(processed / elapsed, 3) if elapsed else 0,
        "interrupted": interrupted, "checkpoint": str(args.checkpoint),
        "checkpoint_write_deferred": checkpoint_deferred,
        "instrument_master_status": state["status"]}
    print(json.dumps(summary, indent=2, default=str))
    return 130 if interrupted else 0


if __name__ == "__main__":
    raise SystemExit(main())
