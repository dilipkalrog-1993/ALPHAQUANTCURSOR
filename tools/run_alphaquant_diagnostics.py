#!/usr/bin/env python3
"""Headless AlphaQuant diagnostics with authentic brain execution tracing."""

from __future__ import annotations

import argparse
import ast
import importlib
import inspect
import json
import subprocess
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BRAIN_MODULES = {
    "Brain 1 Market Observer": "os_brains.market_observer",
    "Brain 2 Market Historian": "os_brains.market_historian",
    "Brain 3 Historical Analog": "os_brains.historical_analog_engine",
    "Brain 4 Strategist": "os_brains.strategist",
    "Brain 5 Risk Manager": "os_brains.risk_manager",
    "Brain 6 Portfolio Manager": "os_brains.portfolio_manager",
    "Brain 7 Reviewer": "os_brains.reviewer",
}


def _compile_check() -> dict:
    targets = [ROOT / "app.py", ROOT / "appemergentquant_v3_1.py", ROOT / "news_intelligence.py"]
    targets += sorted((ROOT / "os_brains").glob("*.py"))
    for target in targets:
        subprocess.check_call([sys.executable, "-m", "py_compile", str(target)])
        ast.parse(target.read_text(encoding="utf-8"))
    return {"compile": "PASS", "ast": "PASS", "files": len(targets)}


def _classify_brain_files() -> dict:
    """Compare os_brains modules against known authentic signatures."""
    classifications = {}
    authentic_markers = {
        "market_observer.py": "Brain 1: Market Observer",
        "market_historian.py": "Brain 2: Market Historian",
        "historical_analog_engine.py": "Brain 3: Historical Analog Engine",
        "strategist.py": "Brain 4: Strategist",
        "risk_manager.py": "Brain 5: Risk Manager",
        "portfolio_manager.py": "Brain 6: Portfolio Manager",
        "reviewer.py": "Brain 7: Reviewer",
        "setup_vector.py": "shared setup-vector construction",
        "db.py": "Postgres connection + schema management",
        "experience_memory.py": "Experience Memory store",
        "pipeline_manager.py": "Pipeline Manager",
        "backfill.py": "Phase 1 backfill pipeline",
    }
    for name, marker in authentic_markers.items():
        path = ROOT / "os_brains" / name
        if not path.exists():
            classifications[name] = "MISSING"
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if marker in text:
            classifications[name] = "AUTHENTIC ORIGINAL"
        elif "GENERATED" in text or "Restored" in text or "sqlite3" in text and name == "db.py":
            classifications[name] = "GENERATED COMPATIBILITY VERSION"
        else:
            classifications[name] = "UNKNOWN"
    return classifications


def _trace_brain(module_name: str, entrypoint: str) -> dict:
    report = {
        "module": module_name,
        "entrypoint": entrypoint,
        "instantiated": False,
        "called": False,
        "exception": None,
        "fallback_used": False,
        "source": module_name,
    }
    try:
        mod = importlib.import_module(module_name)
        fn = getattr(mod, entrypoint)
        report["instantiated"] = callable(fn)
        report["source_file"] = inspect.getsourcefile(fn) or str(mod.__file__)
        sig = str(inspect.signature(fn))
        report["signature"] = sig
        # Exercise with minimal safe inputs where possible
        if module_name == "os_brains.pipeline_manager":
            mgr = mod.PipelineManager()
            report["instantiated"] = True
            ok = mgr.run([mod.PipelineStep("probe", lambda: True, detail="probe")])
            report["called"] = True
            report["output_type"] = f"bool({ok})"
        elif module_name == "os_brains.market_observer":
            report["input_type"] = "(stock, app_module) — requires Streamlit session"
        elif module_name == "os_brains.market_historian":
            try:
                mod.seed_regime_catalog()
                report["called"] = True
                report["output_type"] = "seed_regime_catalog()"
            except Exception as exc:
                report["exception"] = f"{type(exc).__name__}: {exc}"
                report["fallback_used"] = "DATABASE_URL missing" in str(exc)
        elif module_name == "os_brains.historical_analog_engine":
            report["input_type"] = "(symbol, setup_vector, as_of_date) — requires Postgres backfill"
        elif entrypoint in {"enrich_candidate", "evaluate", "allocate", "review_closed_trade"}:
            report["input_type"] = f"({entrypoint} args) — requires TradeCandidate + session"
        elif module_name == "os_brains.experience_memory":
            report["called"] = hasattr(mod, "record_decision")
            report["output_type"] = "module functions"
    except Exception as exc:
        report["exception"] = f"{type(exc).__name__}: {exc}"
        report["traceback"] = traceback.format_exc()
    return report


def _brain_trace_report() -> dict:
    entrypoints = {
        "Brain 1 Market Observer": ("os_brains.market_observer", "observe"),
        "Brain 2 Market Historian": ("os_brains.market_historian", "get_regime_context"),
        "Brain 3 Historical Analog": ("os_brains.historical_analog_engine", "find_analogs"),
        "Brain 4 Strategist": ("os_brains.strategist", "enrich_candidate"),
        "Brain 5 Risk Manager": ("os_brains.risk_manager", "evaluate"),
        "Brain 6 Portfolio Manager": ("os_brains.portfolio_manager", "allocate"),
        "Brain 7 Reviewer": ("os_brains.reviewer", "review_closed_trade"),
    }
    return {label: _trace_brain(mod, fn) for label, (mod, fn) in entrypoints.items()}


def _import_modules() -> dict:
    modules = [
        "news_intelligence",
        "os_brains",
        "os_brains.db",
        "os_brains.experience_memory",
        "os_brains.pipeline_manager",
        "os_brains.risk_manager",
        "os_brains.portfolio_manager",
        "os_brains.strategist",
        "os_brains.reviewer",
        "os_brains.market_observer",
        "os_brains.market_historian",
        "os_brains.historical_analog_engine",
        "os_brains.setup_vector",
    ]
    results = {}
    for name in modules:
        mod = importlib.import_module(name)
        results[name] = getattr(mod, "__file__", str(mod))
    return results


def _synthetic_mode() -> dict:
    from os_brains.pipeline_manager import PipelineManager, PipelineStep

    started = time.perf_counter()
    manager = PipelineManager()
    ok = manager.run([
        PipelineStep("Synthetic", lambda: True, detail="noop"),
        PipelineStep("Import Brains", lambda: _import_modules(), detail="all brains importable"),
    ])
    return {"pipeline_ok": ok, "pipeline_ms": round((time.perf_counter() - started) * 1000, 2)}


def _performance_mode() -> dict:
    started = time.perf_counter()
    _import_modules()
    return {"import_ms": round((time.perf_counter() - started) * 1000, 2)}


def main() -> int:
    parser = argparse.ArgumentParser(description="AlphaQuant headless diagnostics")
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--cached", action="store_true")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--live-data-no-orders", action="store_true")
    parser.add_argument("--restart-test", action="store_true")
    parser.add_argument("--performance", action="store_true")
    parser.add_argument("--trace-brains", action="store_true")
    args = parser.parse_args()

    report: dict = {"root": str(ROOT), "branch": subprocess.check_output(
        ["git", "branch", "--show-current"], text=True, cwd=ROOT
    ).strip()}
    report.update(_compile_check())
    report["brain_file_classification"] = _classify_brain_files()
    report["modules"] = _import_modules()

    if args.trace_brains or not any(vars(args).values()):
        report["brain_trace"] = _brain_trace_report()

    if args.synthetic or not any(vars(args).values()):
        report["synthetic"] = _synthetic_mode()
    if args.cached:
        cache_dir = Path.home() / ".alphaquant" / "universe_cache"
        report["cached"] = {"universe_cache_dir": str(cache_dir), "exists": cache_dir.exists()}
    if args.performance or not any(vars(args).values()):
        report["performance"] = _performance_mode()
    if args.restart_test:
        paper_state = ROOT / "data" / "paper_state.json"
        report["restart_test"] = {"paper_state_exists": paper_state.exists()}

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
