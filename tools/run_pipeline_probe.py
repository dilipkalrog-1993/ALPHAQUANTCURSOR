#!/usr/bin/env python3
"""Headless pipeline probe and paper lifecycle checks using authentic brains."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run_paper_validation(state_dir: Path) -> dict:
    """Run built-in paper validation lifecycle with isolated state directory."""
    os.environ["ALPHAQUANT_DATA_DIR"] = str(state_dir)
    # Import after path setup; appemergentquant uses _APP_DIR for data paths
    import appemergentquant_v3_1 as aq

    # Mock streamlit session minimally
    if not hasattr(aq.st.session_state, "_state"):
        aq.st.session_state._state = {}
    ss = aq.st.session_state._state
    ss.clear()
    ss["paper_broker"] = {
        "connected": False, "cash": 500_000.0, "starting_capital": 500_000.0,
        "positions": {}, "orders": {}, "trade_history": [], "realized_pnl": 0.0, "risk": {},
    }
    ss["paper_positions"] = {}
    ss["paper_history"] = []
    ss["paper_capital"] = 500_000.0
    ss["_paper_state_restored"] = False
    aq.WORKSPACE.preferences["execution_mode"] = "PAPER"
    aq.PAPER_STATE_PATH = state_dir / "paper_state.json"

    result = aq._paper_validation_lifecycle(quantity=1)
    return result


def _run_restart_test(state_dir: Path) -> dict:
    """Process A: persist; Process B: restore via subprocess."""
    state_dir.mkdir(parents=True, exist_ok=True)
    paper_path = state_dir / "paper_state.json"

    # Process A
    val = _run_paper_validation(state_dir)
    if not val.get("passed"):
        return {"passed": False, "stage": "paper_validation", "detail": val}

    # Simulate Process B in fresh interpreter
    script = f"""
import json, sys
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
import appemergentquant_v3_1 as aq
aq.st.session_state._state = {{}}
aq.st.session_state._state["_paper_state_restored"] = False
aq.PAPER_STATE_PATH = Path({str(paper_path)!r})
aq.restore_trading_state_once()
positions = aq.st.session_state.get("paper_positions", {{}})
history = aq.st.session_state.get("paper_history", [])
broker = aq.st.session_state.get("paper_broker", {{}})
print(json.dumps({{
    "open_positions": len(positions),
    "closed_trades": len(history),
    "cash": broker.get("cash"),
    "realized_pnl": broker.get("realized_pnl"),
    "orders": len(broker.get("orders", {{}})),
}}))
"""
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, cwd=ROOT)
    if proc.returncode != 0:
        return {"passed": False, "stage": "restore_subprocess", "stderr": proc.stderr}
    restored = json.loads(proc.stdout.strip())
    passed = (
        restored["closed_trades"] >= 1
        and restored["orders"] >= 1
        and restored["cash"] is not None
    )
    return {"passed": passed, "restored": restored, "validation": val}


def _trace_brain_modules() -> dict:
    import importlib
    brains = {
        "Brain 1": "os_brains.market_observer.observe",
        "Brain 2": "os_brains.market_historian.get_regime_context",
        "Brain 3": "os_brains.historical_analog_engine.find_analogs",
        "Brain 4": "os_brains.strategist.enrich_candidate",
        "Brain 5": "os_brains.risk_manager.evaluate",
        "Brain 6": "os_brains.portfolio_manager.allocate",
        "Brain 7": "os_brains.reviewer.review_closed_trade",
    }
    out = {}
    for label, path in brains.items():
        mod_name, attr = path.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, attr)
        out[label] = {"module": mod_name, "file": mod.__file__, "callable": callable(fn)}
    return out


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="alphaquant_test_") as tmp:
        state_dir = Path(tmp)
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "brain_modules": _trace_brain_modules(),
            "paper_validation": _run_paper_validation(state_dir),
            "restart_test": _run_restart_test(Path(tmp) / "restart"),
        }
        print(json.dumps(report, indent=2, default=str))
        return 0 if report["paper_validation"].get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
