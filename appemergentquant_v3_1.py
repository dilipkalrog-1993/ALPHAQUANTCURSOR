"""
=========================================================
AlphaQuant Professional
Version : 3.0.0
Status  : Production Build
Author  : Dilip + ChatGPT

Build Date : 2026

Current Phase
-------------
✓ Foundation
✓ Startup Engine
✓ Download Engine
✓ Indicator Engine
✓ AI Engine
✓ Portfolio Engine
✓ Paper Trading

Development Mode : TRUE
=========================================================
"""

# =====================================================
# IMPORTS
# =====================================================

import os
import sys
import logging
import traceback
import warnings
import time
import uuid
import hashlib
import json
import math
import random
import tempfile
import threading
import platform
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta, timezone
from abc import ABC, abstractmethod
from pathlib import Path
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable

try:
    import psutil
except ImportError:  # Optional: trading remains available without diagnostics.
    psutil = None

try:
    import resource as _unix_resource
except ImportError:  # Windows does not provide the Unix resource module.
    _unix_resource = None

# Make the `os_brains` package importable regardless of the process's
# working directory. app.py uses absolute imports like
# `from os_brains import experience_memory` throughout, which only resolve
# automatically when Python's cwd/script-dir happens to be this file's own
# directory (e.g. `cd alphaquant && streamlit run app.py`). Explicitly
# putting this file's directory on sys.path makes those imports work no
# matter where the process is launched from -- the repo root, an extracted
# export run from any starting directory, etc. -- without changing any
# trading logic.
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
import requests
from news_intelligence import NewsManager
from broker.connection_manager import BrokerConnectionManager
from market.market_state import get_market_state
from market.snapshots import (
    get_broker_summary,
    get_market_snapshot,
    get_opportunity_snapshot,
    get_portfolio_snapshot,
    get_report_snapshot,
)
from market.feed_worker import UpstoxFeedWorker
from market.upstox_v3_feed import UpstoxV3FeedManager
from market.analytics import refresh_market_analytics, record_entry_evaluation_latency
from core.streamlit_production_adapter import run_streamlit_discovery
from core.scoring_profiles import COMPONENTS, DEFAULT_WEIGHTS, SAFE_BOUNDS, ScoringProfileStore, simulate
from streamlit.components.v1 import html as component_html
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore")


def get_process_metrics() -> dict[str, Any]:
    """Return process diagnostics without making platform support a startup gate."""
    metrics = {"platform": platform.system(), "memory_mb": None,
        "cpu_percent": None, "thread_count": None, "process_id": os.getpid(),
        "available": False, "source": "fallback", "error": None}
    if psutil is not None:
        try:
            process = psutil.Process(os.getpid())
            metrics.update(memory_mb=round(process.memory_info().rss / (1024 ** 2), 1),
                cpu_percent=float(process.cpu_percent(interval=None)),
                thread_count=int(process.num_threads()), available=True, source="psutil")
            return metrics
        except Exception as exc:
            metrics["error"] = f"{type(exc).__name__}: {exc}"
    if _unix_resource is not None:
        try:
            maximum = _unix_resource.getrusage(_unix_resource.RUSAGE_SELF).ru_maxrss
            # macOS reports bytes; Linux and other common Unix systems report KiB.
            divisor = 1024 ** 2 if platform.system() == "Darwin" else 1024
            metrics.update(memory_mb=round(maximum / divisor, 1),
                thread_count=threading.active_count(), available=True, source="resource",
                error=None)
        except Exception as exc:
            metrics["error"] = f"{type(exc).__name__}: {exc}"
    return metrics


class AlphaQuantCoreRuntime:
    """Process-wide control plane that is deliberately independent of a UI run.

    Streamlit sessions are short-lived script executions.  This object is cached
    as a process resource and owns the long-lived heartbeat, run identity and an
    atomic recovery snapshot.  Trading objects remain in the existing engines;
    the core receives portable snapshots so it never calls Streamlit from its
    worker thread (which would bind it to a stale ScriptRunContext).
    """

    VERSION = 1

    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.stop_event = threading.Event()
        self.state = self._load()
        self.thread = threading.Thread(target=self._worker, name="alphaquant-core", daemon=True)
        self.thread.start()

    def _load(self):
        default = {"version": self.VERSION, "status": "STOPPED", "run_id": None,
                   "heartbeat": None, "started_at": None, "pipeline": {},
                   "positions": {}, "orders": {}, "candidates": [], "workers": {},
                   "queue_depths": {}, "restart_count": 0, "last_successful_cycle": None,
                   "last_error": None, "memory_warning": False}
        try:
            saved = json.loads(self.storage_path.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                default.update(saved)
        except (OSError, ValueError, TypeError):
            pass
        # A process restart restores an armed engine without pretending an old
        # worker is still alive.
        if default["status"] in {"STARTING", "RUNNING", "MONITORING"}:
            default["status"] = "RESTORED"
        return default

    def _persist(self):
        temp = self.storage_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self.state, default=str, indent=2), encoding="utf-8")
        temp.replace(self.storage_path)

    def start(self, configuration: dict[str, Any]):
        with self.lock:
            if self.state.get("status") in {"STARTING", "RUNNING", "MONITORING"}:
                return self.state["run_id"], False
            now = datetime.now(timezone.utc).isoformat()
            self.state.update(status="STARTING", run_id=uuid.uuid4().hex,
                              started_at=now, heartbeat=now, configuration=configuration)
            self._persist()
        self.wake.set()
        return self.state["run_id"], True

    def stop(self):
        with self.lock:
            self.state.update(status="STOPPED", stopped_at=datetime.now(timezone.utc).isoformat())
            self._persist()
        self.wake.set()

    def publish(self, **snapshot):
        with self.lock:
            self.state.update(snapshot)
            if self.state.get("status") in {"STARTING", "RUNNING", "RESTORED"}:
                self.state["status"] = "MONITORING"
            self._persist()
        self.wake.set()

    def heartbeat(self, worker: str, *, queue_depth: int = 0, error: Exception | None = None):
        """Record bounded worker health without allowing worker exceptions into the UI."""
        now = datetime.now(timezone.utc).isoformat()
        with self.lock:
            workers = self.state.setdefault("workers", {})
            item = workers.setdefault(worker, {"restart_count": 0})
            item.update(heartbeat=now, status="ERROR" if error else "HEALTHY")
            self.state.setdefault("queue_depths", {})[worker] = max(0, int(queue_depth))
            if error:
                item["last_error"] = f"{type(error).__name__}: {error}"
                self.state["last_error"] = {"worker": worker, "error": item["last_error"], "time": now}
            else:
                item["last_successful_cycle"] = now
                self.state["last_successful_cycle"] = now
            metrics = get_process_metrics()
            self.state["process_metrics"] = metrics
            self.state["memory_mb"] = metrics["memory_mb"]
            self.state["memory_warning"] = bool(metrics["memory_mb"] and metrics["memory_mb"] >= 1024)

    def snapshot(self):
        with self.lock:
            return dict(self.state)

    def _worker(self):
        while not self.stop_event.is_set():
            self.wake.wait(timeout=2.0)
            self.wake.clear()
            with self.lock:
                if self.state.get("status") in {"STARTING", "RUNNING", "MONITORING", "RESTORED"}:
                    self.state["heartbeat"] = datetime.now(timezone.utc).isoformat()
                    if self.state["status"] == "STARTING":
                        self.state["status"] = "RUNNING"
                    self._persist()


@st.cache_resource(show_spinner=False)
def get_core_runtime():
    """Return the sole daemon control plane for this Python process."""
    return AlphaQuantCoreRuntime(str(Path(_APP_DIR) / "data" / "runtime_state.json"))

# NSE cash-session policy. Keep this as the single market-hours helper so a
# holiday calendar can be injected later without changing UI or provider code.
IST = ZoneInfo("Asia/Kolkata")
NSE_MARKET_OPEN = (9, 15)
NSE_MARKET_CLOSE = (15, 30)
NSE_HOLIDAY_CALENDAR = None

def is_market_open(now=None) -> bool:
    """Return whether *now* falls in the regular NSE cash session (IST)."""
    current = now or datetime.now(IST)
    if current.tzinfo is None:
        current = current.replace(tzinfo=IST)
    else:
        current = current.astimezone(IST)
    if current.weekday() >= 5:
        return False
    if NSE_HOLIDAY_CALENDAR is not None and NSE_HOLIDAY_CALENDAR(current.date()):
        return False
    opened = current.replace(hour=NSE_MARKET_OPEN[0], minute=NSE_MARKET_OPEN[1], second=0, microsecond=0)
    closed = current.replace(hour=NSE_MARKET_CLOSE[0], minute=NSE_MARKET_CLOSE[1], second=0, microsecond=0)
    return opened <= current <= closed

def market_status(now=None) -> str:
    """Fail-safe display wrapper; market-status failures never break a page."""
    try:
        return "OPEN" if is_market_open(now) else "CLOSED"
    except Exception:
        logging.exception("NSE market-status evaluation failed")
        return "UNKNOWN"

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="AlphaQuant Professional",
    page_icon="📈",
    layout="wide"
)

# =====================================================
# APPEMERGENTQUANTFINAL - v1.0.0 (2026-07-18 13:32:14 UTC)
# Production build entry point.
#   * One-click end-to-end pipeline (RUN ALPHAQUANT primary button)
#   * Universe Engine v3.0.2 (nsearchives primary + fallbacks + disk cache)
#   * All existing engines preserved and reused
#   * Unified professional dark theme (CSS injected exactly once)
# =====================================================

# -------- Stable institutional terminal theme (applied before visible UI) --------
# This is deliberately unconditional: Streamlit rebuilds the DOM on every rerun.
# A session-only injection flag caused widgets to fall back to the light theme after RUN.
st.markdown("""
<style>
:root { color-scheme: dark; --aq-bg:#070b12; --aq-panel:#0d1420; --aq-line:#263142;
 --aq-text:#d9e2ef; --aq-muted:#7f8da3; --aq-accent:#f2a900; --aq-green:#24c78e; --aq-red:#ef5b64; }
html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"] { background:#070b12 !important; color:var(--aq-text)!important; }
[data-testid="stAppViewContainer"] > .main .block-container { max-width:100%!important; padding:2.7rem 1rem 1rem!important; }
header[data-testid="stHeader"] { background:#070b12!important; height:2.25rem!important; }
section[data-testid="stSidebar"] { display:none!important; }
*, h1,h2,h3,p,label { font-family:Inter,Segoe UI,sans-serif!important; }
h1 {font-size:1.25rem!important} h2 {font-size:1.05rem!important} h3 {font-size:.92rem!important}
h1,h2,h3 {margin:.25rem 0!important;color:#eef4fc!important} p {margin:.15rem 0!important}
[data-testid="stVerticalBlock"] {gap:.45rem!important}
.aq-brand {display:flex;align-items:center;justify-content:space-between;border:1px solid var(--aq-line);border-left:3px solid var(--aq-accent);background:#0a101a;padding:6px 10px;margin-bottom:5px}
.aq-brand b{font-size:14px;letter-spacing:.08em}.aq-brand span{font-size:10px;color:var(--aq-muted)}
.aq-panel-title{font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#aebbd0;border-bottom:1px solid var(--aq-line);padding:5px 7px;background:#111927}
.aq-ticker{display:grid;grid-template-columns:repeat(6,minmax(125px,1fr));border:1px solid var(--aq-line);background:#0a1019;margin:3px 0}
.aq-tick{padding:5px 8px;border-right:1px solid var(--aq-line);font-size:10px}.aq-tick:last-child{border:0}.aq-tick strong{display:block;color:#eef4fc;font-size:11px}.aq-up{color:var(--aq-green)}.aq-down{color:var(--aq-red)}
.aq-status{display:flex;gap:16px;flex-wrap:wrap;background:#0d1420;border:1px solid var(--aq-line);padding:4px 8px;font-size:10px;color:var(--aq-muted)}
div[data-testid="stMetric"]{background:var(--aq-panel)!important;border:1px solid var(--aq-line)!important;border-radius:2px!important;padding:6px 8px!important} [data-testid="stMetricLabel"]{font-size:10px!important;color:var(--aq-muted)!important}[data-testid="stMetricValue"]{font-size:16px!important;color:var(--aq-text)!important}
.stButton button,.stDownloadButton button{min-height:30px!important;padding:3px 10px!important;border-radius:2px!important;border:1px solid #344258!important;background:#141d2b!important;color:var(--aq-text)!important;font-size:11px!important;font-weight:650!important;box-shadow:none!important}
.stButton button:hover,.stDownloadButton button:hover{border-color:var(--aq-accent)!important;color:#fff!important}.stButton button[kind="primary"]{background:var(--aq-accent)!important;color:#080b10!important;border-color:var(--aq-accent)!important}
[data-baseweb="input"],[data-baseweb="select"]>div,[data-testid="stNumberInput"] input{background:#0c1320!important;color:var(--aq-text)!important;border-color:var(--aq-line)!important;border-radius:2px!important}
[data-testid="stDataFrame"], [data-testid="stTable"]{border:1px solid var(--aq-line)!important} [data-testid="stExpander"]{background:var(--aq-panel)!important;border:1px solid var(--aq-line)!important;border-radius:2px!important}
[data-baseweb="tab-list"]{background:#0a1019!important;border-bottom:1px solid var(--aq-line)!important;gap:0!important}[data-baseweb="tab"]{padding:5px 12px!important;color:var(--aq-muted)!important}[data-baseweb="tab"][aria-selected="true"]{color:var(--aq-accent)!important;border-bottom-color:var(--aq-accent)!important}
/* One high-contrast dark treatment for every native and BaseWeb control. */
[data-baseweb="input"], [data-baseweb="textarea"], [data-baseweb="select"] > div,
[data-testid="stNumberInput"] > div, [data-testid="stDateInput"] > div,
[data-testid="stTimeInput"] > div, input, textarea {
 background:#101a29!important; color:#f4f7fb!important; border-color:#52657d!important;
 -webkit-text-fill-color:#f4f7fb!important; caret-color:#f2a900!important;
}
input::placeholder, textarea::placeholder {color:#91a0b5!important;-webkit-text-fill-color:#91a0b5!important;opacity:1!important}
input:disabled, textarea:disabled, [aria-disabled="true"] {color:#b8c3d2!important;-webkit-text-fill-color:#b8c3d2!important;opacity:.78!important}
[data-baseweb="select"] *, [role="listbox"], [role="option"], [data-baseweb="popover"] * {background-color:#101a29!important;color:#f4f7fb!important}
[data-baseweb="select"]:focus-within > div, [data-baseweb="input"]:focus-within, [data-baseweb="textarea"]:focus-within {border-color:#f2a900!important;box-shadow:0 0 0 1px #f2a900!important}
[data-testid="stWidgetLabel"] p, [data-testid="stRadio"] label, [data-testid="stCheckbox"] label,
[data-testid="stToggle"] label, [data-testid="stSlider"] label {color:#d9e2ef!important}
[data-testid="stAlert"] {background:#111c2b!important;color:#eef4fc!important;border:1px solid #41536b!important}
[data-testid="stAlert"] * {color:#eef4fc!important}
[data-testid="stDataFrame"], [data-testid="stDataEditor"], [data-testid="stTable"] {background:#0d1420!important;color:#eef4fc!important;border-color:#2a3547!important}
[data-testid="stDataFrame"] canvas, [data-testid="stDataEditor"] canvas {background:#0d1420!important;color:#eef4fc!important}
[data-testid="stDataFrame"] [role="grid"], [data-testid="stDataEditor"] [role="grid"], [role="columnheader"], [role="gridcell"] {background:#0d1420!important;color:#eef4fc!important;border-color:#2a3547!important}
[role="row"][aria-selected="true"] [role="gridcell"] {background:#20334b!important;color:#fff!important}
.aq-empty{background:#0d1420;border:1px solid #2a3547;color:#aebbd0;padding:22px;text-align:center;font-size:12px}
.aq-status-card{display:grid;grid-template-columns:minmax(150px,.7fr) 1.3fr;background:#0d1420;border:1px solid #2a3547;padding:10px;gap:7px 18px;font-size:12px}.aq-status-card b{color:#aebbd0;text-transform:uppercase;font-size:10px}.aq-status-card span{color:#eef4fc}
[data-testid="stExpander"] summary, [data-testid="stTooltipIcon"], [data-testid="stMarkdownContainer"] {color:#d9e2ef!important}
[role="radiogroup"] label, [data-testid="stDownloadButton"] button {color:#eef4fc!important}
[data-testid="stNumberInput"] button, [data-testid="stDateInput"] button, [data-testid="stTimeInput"] button {color:#f4f7fb!important;background:#182437!important}
input:-webkit-autofill, textarea:-webkit-autofill, select:-webkit-autofill {-webkit-box-shadow:0 0 0 1000px #101a29 inset!important;-webkit-text-fill-color:#f4f7fb!important}
hr{margin:.35rem 0!important;border-color:var(--aq-line)!important} #MainMenu,footer{visibility:hidden}
@media(max-width:900px){.aq-ticker{grid-template-columns:repeat(2,1fr)}[data-testid="column"]{min-width:100%!important}}
</style>
""", unsafe_allow_html=True)

st.markdown(f'<div class="aq-brand"><b>ALPHAQUANT TERMINAL</b><span>MARKETS · RESEARCH · EXECUTION · RISK · <b>{st.session_state.get("pipeline_state", "STOPPED")}</b></span></div>', unsafe_allow_html=True)

# The normal product shell has exactly four stable destinations.  Profile and
# developer controls are intentionally outside product navigation.
_PRODUCT_PAGES = ["Market", "Configuration", "Trading", "Reports"]
if st.session_state.get("_page") not in _PRODUCT_PAGES:
    st.session_state["_page"] = "Market"
_nav_cols = st.columns([1, 1, 1, 1, .42])
for _column, _pname in zip(_nav_cols[:4], _PRODUCT_PAGES):
    with _column:
        if st.button(_pname.upper(), key=f"_nav_btn_{_pname}", width="stretch",
                     type="primary" if st.session_state["_page"] == _pname else "secondary"):
            st.session_state["_page"] = _pname
            st.rerun()
with _nav_cols[4]:
    if st.button("USER", key="_profile_control", width="stretch", help="Open user profile"):
        st.session_state["_profile_open"] = True
        st.rerun()

def _P(*pages) -> bool:
    return st.session_state.get("_page", "Market") in pages

# =====================================================
# APPLICATION CONFIGURATION
# =====================================================

CONFIG = {

    "VERSION": "3.0.0",

    "SCAN_MODE": "FULL_NSE",

    "LONG_ONLY": True,

    "MAX_WORKERS": 8,

    "DOWNLOAD_BATCH": 50,

    "DOWNLOAD_PERIOD": "1y",

    "DOWNLOAD_INTERVAL": "1d",

    "CACHE_MINUTES": 30,

    "MIN_PRICE": 20,

    "MIN_AVG_VOLUME": 100000,

    "MIN_AVG_TURNOVER": 10000000,

    "MAX_OPEN_POSITIONS": 10,

    "RISK_PER_TRADE": 1.0

}


class WorkspaceManager:
    """Own the stable user workspace independently of individual widgets.

    Streamlit removes widget state when a widget is not rendered.  The
    workspace therefore keeps durable, versioned preferences in a small JSON
    document and mirrors them into session state.  Secrets and broker tokens
    are deliberately excluded from this store.
    """

    VERSION = 2
    STORAGE_PATH = Path(_APP_DIR) / "data" / "workspace.json"
    DEFAULTS = {
        "watchlist": [],
        "watchlists": {"Default": []},
        "default_watchlist": "Default",
        "history_period": "1y",
        "candle_interval": "1d",
        "minimum_price": 20,
        "minimum_volume": 100000,
        "maximum_positions": 10,
        "theme": "terminal-dark",
        "dashboard_density": "Compact",
        "opportunity_limit": 8,
        "minimum_confidence": 70,
        "ai_profile": "Balanced",
        "developer_mode": False,
        "column_order": {},
        "filters": {},
        "panel_sizes": {},
        "universe_source": "",
        "universe_filters": {},
        "universe_presets": {},
        "operating_mode": "Fast Scan",
        "execution_mode": "PAPER",
        "default_broker": "YFinance (default, no login)",
        "selected_watchlist": "Default",
        "chart_timeframe": "1d",
        "risk_preferences": {"risk_per_trade": 1.0, "maximum_daily_loss": 3.0, "maximum_position_size": 10.0, "maximum_portfolio_exposure": 80.0, "maximum_sector_exposure": 25.0, "minimum_cash_reserve": 20.0},
        "paper_trading_capital": 500000.0,
        "paper_capital_pending": None,
        "setup_complete": False,
        "historical_provider": "Yahoo Finance",
        "fast_track_execution": True,
        "minimum_fast_ai_score": 70,
        "scoring_engine_version": "V2",
        "require_deep_ai_before_entry": False,
        "signal_expiry_minutes": 30,
        "discovery_focus_limit_fast": 28,
        "discovery_focus_limit_normal": 75,
        "discovery_focus_limit_full": 9999,
        "discovery_focus_limit_watchlist": 50,
        "discovery_min_opportunity_score": 8.0,
        "discovery_worker_count": 0,
        "news_settings": {"enabled": False, "provider": "RSS", "fetch_interval_minutes": 15,
            "market_wide": True, "portfolio": True, "watchlist": True, "candidates": True,
            "earnings": True, "regulatory": True, "corporate_actions": True,
            "spoken_briefing": False, "auto_speak_critical": False, "voice": "",
            "speech_rate": 1.0, "speech_pitch": 1.0, "speech_volume": 1.0,
            "quiet_hours_start": "22:00", "quiet_hours_end": "07:00"},
        "report_filters": {},
    }

    def __init__(self) -> None:
        self.STORAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.preferences = self._load()
        st.session_state.setdefault("workspace_preferences", self.preferences)
        st.session_state.setdefault("watchlist", list(self.preferences["watchlist"]))

    def _load(self) -> dict[str, Any]:
        saved: dict[str, Any] = {}
        if self.STORAGE_PATH.exists():
            try:
                payload = json.loads(self.STORAGE_PATH.read_text(encoding="utf-8"))
                if isinstance(payload.get("preferences"), dict):
                    saved = payload.get("preferences", {})
            except (OSError, ValueError, TypeError) as exc:
                logging.warning("Workspace preferences could not be loaded: %s", exc)
        return {**self.DEFAULTS, **saved}

    def save(self, **changes: Any) -> dict[str, Any]:
        allowed = self.DEFAULTS.keys()
        self.preferences.update({key: value for key, value in changes.items() if key in allowed})
        self.preferences["watchlist"] = list(st.session_state.get("watchlist", []))
        payload = {"version": self.VERSION, "preferences": self.preferences}
        temp_path = self.STORAGE_PATH.with_suffix(".tmp")
        temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp_path.replace(self.STORAGE_PATH)
        st.session_state["workspace_preferences"] = self.preferences
        return self.preferences


WORKSPACE = WorkspaceManager()


@st.cache_resource(show_spinner=False)
def get_news_manager():
    """Return the process-wide news service without making it a startup gate.

    The module is a shipped, required source file, but the service itself is an
    optional product feature.  Cache corruption, an unwritable data directory,
    or any other initialization failure therefore disables News rather than
    preventing the trading application from loading.
    """
    try:
        return NewsManager(Path(_APP_DIR) / "data" / "news_cache.json")
    except Exception as exc:
        logging.exception("News Intelligence initialization failed; News is disabled")
        return DisabledNewsManager(exc)


class DisabledNewsManager:
    """Stable no-op interface used when the optional news service is unavailable."""

    def __init__(self, error: Exception):
        self.error = f"{type(error).__name__}: {error}"

    def configure(self, **_changes: Any) -> None:
        return None

    def candidate_effect(self, _symbol: str) -> dict[str, Any]:
        return {
            "news_status": "DISABLED", "news_relevance": 0,
            "news_sentiment": 0, "news_risk": 0, "news_summary": "",
            "news_timestamp": None, "news_sources": [],
            "news_effect_on_confidence": 0, "news_veto_reason": None,
        }

    def briefing(self, kind: str = "INTRADAY") -> str:
        return f"{kind.upper()} BRIEF. News Intelligence is currently unavailable."

    def claim_critical_alert(self, cooldown_minutes: int = 30) -> None:
        return None

    def snapshot(self) -> dict[str, Any]:
        return {
            "articles": [], "clusters": [], "briefing_history": [],
            "provider_status": "DISABLED", "last_error": self.error,
            "stale": True,
        }


def configure_news_manager():
    settings = dict(WORKSPACE.preferences.get("news_settings", {}))
    # Credentials are memory/environment-only and never enter workspace persistence.
    api_key = st.session_state.get("news_api_key") or os.environ.get("NEWSAPI_KEY", "")
    get_news_manager().configure(**settings, api_key=api_key)
_wp = WORKSPACE.preferences
CONFIG.update({
    "DOWNLOAD_PERIOD": _wp["history_period"], "DOWNLOAD_INTERVAL": _wp["candle_interval"],
    "MIN_PRICE": _wp["minimum_price"], "MIN_AVG_VOLUME": _wp["minimum_volume"],
    "MAX_OPEN_POSITIONS": _wp["maximum_positions"],
})

# =====================================================
# SECTOR MAP
# =====================================================
# Defined here (ahead of both the sector-scoring engine further down and
# the Scan Manager's Sector filter) so any code that needs it - however
# early in the script - can rely on it already existing.

STOCK_SECTOR_MAP = {
    "HDFCBANK": "NIFTY BANK", "ICICIBANK": "NIFTY BANK", "SBIN": "NIFTY BANK",
    "KOTAKBANK": "NIFTY BANK", "AXISBANK": "NIFTY BANK", "INDUSINDBK": "NIFTY BANK",
    "BANKBARODA": "NIFTY BANK", "PNB": "NIFTY BANK", "IDFCFIRSTB": "NIFTY BANK",
    "FEDERALBNK": "NIFTY BANK", "AUBANK": "NIFTY BANK", "BANDHANBNK": "NIFTY BANK",

    "TCS": "NIFTY IT", "INFY": "NIFTY IT", "WIPRO": "NIFTY IT", "HCLTECH": "NIFTY IT",
    "TECHM": "NIFTY IT", "LTIM": "NIFTY IT", "MPHASIS": "NIFTY IT", "COFORGE": "NIFTY IT",
    "PERSISTENT": "NIFTY IT", "LTTS": "NIFTY IT",

    "MARUTI": "NIFTY AUTO", "TATAMOTORS": "NIFTY AUTO", "M&M": "NIFTY AUTO",
    "BAJAJ-AUTO": "NIFTY AUTO", "HEROMOTOCO": "NIFTY AUTO", "EICHERMOT": "NIFTY AUTO",
    "TVSMOTOR": "NIFTY AUTO", "ASHOKLEY": "NIFTY AUTO", "BALKRISIND": "NIFTY AUTO",
    "BOSCHLTD": "NIFTY AUTO", "MOTHERSON": "NIFTY AUTO",

    "HINDUNILVR": "NIFTY FMCG", "ITC": "NIFTY FMCG", "NESTLEIND": "NIFTY FMCG",
    "BRITANNIA": "NIFTY FMCG", "TATACONSUM": "NIFTY FMCG", "DABUR": "NIFTY FMCG",
    "MARICO": "NIFTY FMCG", "GODREJCP": "NIFTY FMCG", "COLPAL": "NIFTY FMCG",

    "SUNPHARMA": "NIFTY PHARMA", "DRREDDY": "NIFTY PHARMA", "CIPLA": "NIFTY PHARMA",
    "DIVISLAB": "NIFTY PHARMA", "AUROPHARMA": "NIFTY PHARMA", "LUPIN": "NIFTY PHARMA",
    "BIOCON": "NIFTY PHARMA", "TORNTPHARM": "NIFTY PHARMA", "ALKEM": "NIFTY PHARMA",

    "TATASTEEL": "NIFTY METAL", "JSWSTEEL": "NIFTY METAL", "HINDALCO": "NIFTY METAL",
    "VEDL": "NIFTY METAL", "SAIL": "NIFTY METAL", "JINDALSTEL": "NIFTY METAL",
    "NMDC": "NIFTY METAL", "NATIONALUM": "NIFTY METAL", "HINDCOPPER": "NIFTY METAL",

    "RELIANCE": "NIFTY ENERGY", "ONGC": "NIFTY ENERGY", "NTPC": "NIFTY ENERGY",
    "POWERGRID": "NIFTY ENERGY", "BPCL": "NIFTY ENERGY", "IOC": "NIFTY ENERGY",
    "GAIL": "NIFTY ENERGY", "TATAPOWER": "NIFTY ENERGY", "ADANIGREEN": "NIFTY ENERGY",
    "ADANIPOWER": "NIFTY ENERGY", "COALINDIA": "NIFTY ENERGY",
}


# =====================================================
# LOGGING
# =====================================================

LOG_FOLDER = os.path.join(_APP_DIR, "logs")

os.makedirs(LOG_FOLDER, exist_ok=True)

logging.basicConfig(

    filename=os.path.join(LOG_FOLDER, "alphaquant.log"),

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s"

)

logging.info("AlphaQuant Started")

# =====================================================
# SESSION STATE
# =====================================================

if "portfolio" not in st.session_state:

    st.session_state.portfolio = []

if "watchlist" not in st.session_state:

    st.session_state.watchlist = []

if "audit_log" not in st.session_state:

    st.session_state.audit_log = []

if "selected_portfolio" not in st.session_state:

    st.session_state.selected_portfolio = []

if "market_data" not in st.session_state:

    st.session_state.market_data = {}


if "closed_positions" not in st.session_state:
    st.session_state.closed_positions = []

if "trade_journal" not in st.session_state:
    st.session_state.trade_journal = []

if "run_complete_scan_requested" not in st.session_state:
    st.session_state.run_complete_scan_requested = False

if "last_cycle_time" not in st.session_state:
    st.session_state.last_cycle_time = None

if "last_cycle_trigger" not in st.session_state:
    st.session_state.last_cycle_trigger = None

if "last_cycle_message" not in st.session_state:
    st.session_state.last_cycle_message = ""

if "autonomous_active" not in st.session_state:
    st.session_state.autonomous_active = False

if "pipeline_events" not in st.session_state:
    st.session_state.pipeline_events = []

if "brain_status" not in st.session_state:
    st.session_state.brain_status = {}

if "decision_funnel" not in st.session_state:
    st.session_state.decision_funnel = []

if "no_trade_explanation" not in st.session_state:
    st.session_state.no_trade_explanation = []

if "startup_health" not in st.session_state:
    st.session_state.startup_health = []

if "pipeline_phases" not in st.session_state:
    st.session_state.pipeline_phases = {}

if "pipeline_errors" not in st.session_state:
    st.session_state.pipeline_errors = []

if "system_health" not in st.session_state:
    st.session_state.system_health = {"startup_time": datetime.now()}

if "pipeline_monitor" not in st.session_state:
    st.session_state.pipeline_monitor = {}

if "paper_broker" not in st.session_state:
    st.session_state.paper_broker = {"connected": False, "cash": float(WORKSPACE.preferences["paper_trading_capital"]), "starting_capital": float(WORKSPACE.preferences["paper_trading_capital"]), "positions": {}, "orders": {}, "trade_history": [], "realized_pnl": 0.0, "risk": {}}


@dataclass
class PipelinePhase:
    phase_name: str
    status: str = "PENDING"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration: float = 0.0
    progress_percent: float = 0.0
    message: str = ""


@dataclass
class PipelineError:
    timestamp: datetime
    phase: str
    component: str
    severity: str
    error_type: str
    message: str
    traceback: str
    resolved: bool = False


class CentralErrorManager:
    session_key = "pipeline_errors"

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger(__name__)
        st.session_state.setdefault(self.session_key, [])

    def record_error(self, phase: str, component: str, error: Exception | str, severity: str = "ERROR") -> PipelineError:
        err = PipelineError(
            timestamp=datetime.now(),
            phase=phase,
            component=component,
            severity=severity,
            error_type=type(error).__name__ if isinstance(error, Exception) else "RuntimeError",
            message=str(error),
            traceback=traceback.format_exc() if isinstance(error, Exception) else "",
            resolved=False,
        )
        st.session_state.setdefault(self.session_key, []).append(err)
        self.logger.error(
            "Pipeline error | phase=%s | component=%s | severity=%s | type=%s | message=%s",
            err.phase, err.component, err.severity, err.error_type, err.message,
        )
        return err

    def resolve_error(self, index: int) -> bool:
        errors = st.session_state.setdefault(self.session_key, [])
        if 0 <= index < len(errors):
            errors[index].resolved = True
            self.logger.info("Pipeline error resolved | index=%s", index)
            return True
        return False

    def get_errors(self, include_resolved: bool = True) -> list[PipelineError]:
        errors = st.session_state.setdefault(self.session_key, [])
        return list(errors if include_resolved else [e for e in errors if not e.resolved])

    def clear_resolved(self) -> int:
        errors = st.session_state.setdefault(self.session_key, [])
        before = len(errors)
        st.session_state[self.session_key] = [e for e in errors if not e.resolved]
        cleared = before - len(st.session_state[self.session_key])
        self.logger.info("Cleared resolved pipeline errors | count=%s", cleared)
        return cleared


class SystemHealthEngine:
    session_key = "system_health"

    def __init__(self):
        st.session_state.setdefault(self.session_key, {"startup_time": datetime.now()})

    def update(self, **kwargs) -> dict[str, Any]:
        health = st.session_state.setdefault(self.session_key, {"startup_time": datetime.now()})
        health.setdefault("startup_time", datetime.now())
        health["cpu_percent"] = self._cpu_percent()
        health["memory_percent"] = self._memory_percent()
        health["pipeline_runtime"] = self._runtime_seconds(st.session_state.get("pipeline_started_at"))
        health.update(kwargs)
        return health

    def mark(self, key: str) -> dict[str, Any]:
        return self.update(**{key: datetime.now()})

    def _cpu_percent(self) -> float | None:
        try:
            psutil = __import__("psutil")
            return float(psutil.cpu_percent(interval=0.0))
        except Exception:
            return None

    def _memory_percent(self) -> float | None:
        try:
            psutil = __import__("psutil")
            return float(psutil.virtual_memory().percent)
        except Exception:
            return None

    @staticmethod
    def _runtime_seconds(started_at) -> float:
        if not started_at:
            return 0.0
        return round((datetime.now() - started_at).total_seconds(), 2)


class PipelineMonitor:
    session_key = "pipeline_monitor"

    def __init__(self):
        st.session_state.setdefault(self.session_key, {})

    def update(self) -> dict[str, Any]:
        phases = st.session_state.setdefault("pipeline_phases", {})
        ordered = list(phases.values())
        completed = [p.phase_name for p in ordered if p.status == "COMPLETED"]
        failed = [p.phase_name for p in ordered if p.status == "FAILED"]
        active = next((p for p in ordered if p.status == "RUNNING"), None)
        pending = [p.phase_name for p in ordered if p.status == "PENDING"]
        started = st.session_state.get("pipeline_started_at")
        runtime = SystemHealthEngine._runtime_seconds(started)
        avg = runtime / max(1, len(completed)) if completed else 0
        estimate = datetime.now() + timedelta(seconds=avg * len(pending)) if pending and avg else None
        monitor = {
            "current_phase": active.phase_name if active else ("FAILED" if failed else "IDLE"),
            "completed_phases": completed,
            "failed_phases": failed,
            "remaining_phases": pending,
            "estimated_completion": estimate,
            "pipeline_runtime": runtime,
        }
        st.session_state[self.session_key] = monitor
        return monitor


@dataclass(slots=True)
class PipelineDiagnostics:
    """Exception-safe diagnostics recorder for non-trading pipeline stages."""

    session_key: str = "pipeline_diagnostics"
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    def reset(self) -> None:
        """Reset diagnostics for the current application run."""
        st.session_state[self.session_key] = []

    def record(
        self,
        stage: str,
        status: str,
        message: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a diagnostics event without interrupting pipeline execution."""
        try:
            events = st.session_state.setdefault(self.session_key, [])
            events.append({
                "Time": datetime.now().strftime("%H:%M:%S"),
                "Stage": stage,
                "Status": status,
                "Message": message,
                "Metadata": metadata or {},
            })
            self.logger.info(
                "Pipeline diagnostics | stage=%s | status=%s | message=%s",
                stage,
                status,
                message,
            )
        except Exception as exc:
            self.logger.warning("Pipeline diagnostics recording failed: %s", exc)

    def register_phase(self, phase_name: str, message: str = "") -> PipelinePhase:
        phases = st.session_state.setdefault("pipeline_phases", {})
        phase = PipelinePhase(phase_name=phase_name, status="RUNNING", started_at=datetime.now(), progress_percent=0.0, message=message)
        phases[phase_name] = phase
        self.record(phase_name, "RUNNING", message)
        PipelineMonitor().update()
        return phase

    def update_progress(self, phase_name: str, progress_percent: float, message: str = "") -> PipelinePhase:
        phases = st.session_state.setdefault("pipeline_phases", {})
        phase = phases.get(phase_name) or self.register_phase(phase_name, message)
        phase.progress_percent = max(0.0, min(100.0, float(progress_percent)))
        if message:
            phase.message = message
        self.record(phase_name, "PROGRESS", phase.message, {"progress_percent": phase.progress_percent})
        PipelineMonitor().update()
        return phase

    def complete_phase(self, phase_name: str, message: str = "Completed") -> PipelinePhase:
        phases = st.session_state.setdefault("pipeline_phases", {})
        phase = phases.get(phase_name) or self.register_phase(phase_name, message)
        phase.status = "COMPLETED"
        phase.completed_at = datetime.now()
        phase.duration = round((phase.completed_at - (phase.started_at or phase.completed_at)).total_seconds(), 2)
        phase.progress_percent = 100.0
        phase.message = message
        self.record(phase_name, "COMPLETED", message, {"duration": phase.duration})
        PipelineMonitor().update()
        return phase

    def fail_phase(self, phase_name: str, message: str = "Failed") -> PipelinePhase:
        phases = st.session_state.setdefault("pipeline_phases", {})
        phase = phases.get(phase_name) or self.register_phase(phase_name, message)
        phase.status = "FAILED"
        phase.completed_at = datetime.now()
        phase.duration = round((phase.completed_at - (phase.started_at or phase.completed_at)).total_seconds(), 2)
        phase.message = message
        self.record(phase_name, "FAILED", message, {"duration": phase.duration})
        PipelineMonitor().update()
        return phase

    def record_startup_health(self, checks: list[dict[str, Any]]) -> None:
        """Capture startup health summary diagnostics only."""
        total = len(checks)
        unhealthy = sum(1 for check in checks if check.get("Status") != "OK")
        status = "OK" if unhealthy == 0 else "DEGRADED"
        self.record(
            "Startup Health Check",
            status,
            f"{total - unhealthy}/{total} startup checks passed",
            {"total": total, "unhealthy": unhealthy},
        )


# =====================================================
# STARTUP HEALTH CHECK
# =====================================================

def run_startup_health_check():
    checks = []

    def add(name, ok, message=""):
        checks.append({"Component": name, "Status": "OK" if ok else "MISSING/ERROR", "Message": message})

    add("Python", True, sys.version.split()[0])
    for module_name in ["streamlit", "pandas", "numpy", "psycopg2"]:
        try:
            __import__(module_name)
            add(module_name, True, "importable")
        except Exception as exc:
            add(module_name, False, str(exc))

    for module_name in [
        "os_brains.db", "os_brains.experience_memory", "os_brains.historical_analog_engine",
        "os_brains.strategist", "os_brains.risk_manager", "os_brains.portfolio_manager",
        "os_brains.reviewer", "os_brains.pipeline_manager",
    ]:
        try:
            __import__(module_name, fromlist=["*"])
            add(module_name, True, "importable")
        except Exception as exc:
            add(module_name, False, str(exc))

    try:
        apply_schema = __import__("os_brains.db", fromlist=["apply_schema"]).apply_schema
        apply_schema()
        add("Database", True, "schema reachable")
    except Exception as exc:
        add("Database", False, str(exc))

    add("Configuration", bool(CONFIG), f"{len(CONFIG)} settings loaded")
    st.session_state.startup_health = checks
    PipelineDiagnostics().record_startup_health(checks)
    return checks


def show_startup_health_check():
    if not st.session_state.startup_health:
        run_startup_health_check()
    unhealthy = [c for c in st.session_state.startup_health if c["Status"] != "OK"]
    with st.expander("Startup Health Check", expanded=bool(unhealthy)):
        st.dataframe(pd.DataFrame(st.session_state.startup_health), width="stretch")
        if unhealthy:
            st.error("AlphaQuant can start, but one or more services need attention before the full pipeline can complete.")
        else:
            st.success("All startup checks passed.")

# =====================================================
# UNIVERSE ENGINE
# VERSION 3.0.0 - Production Rebuild (Emergent)
# =====================================================
#
# The four brittle helpers that used to live here
# (fetch_complete_nse_universe, fetch_index_constituents,
# _clean_and_suffix_symbols, and their duplicated blacklist blocks)
# have been extracted into a single hardened module,
# `universe_engine.py`, which lives next to this file.
#
# The module exposes the SAME public signatures the rest of this app
# already imports/calls, so nothing else in the file needs to change:
#
#     NSE_INDEX_SOURCES                       - unchanged keys
#     fetch_complete_nse_universe() -> list   - '.NS'-suffixed uniques
#     fetch_index_constituents(url) -> set    - raw base symbols
#     _clean_and_suffix_symbols(iter) -> list - legacy alias
#
# The module handles: local disk cache (~/.alphaquant/universe_cache/*.csv,
# 24h TTL, stale-cache-wins), NSE archive fetch with cookie priming, public
# mirrors (nseindia.com/api + community GitHub CSVs), yfinance verification
# fallback for critical indices, urllib3 Retry with exponential backoff,
# rotating User-Agents, and a single canonical blacklist/suffix helper.
#
# @st.cache_data(ttl=86400, show_spinner=False) is re-applied inside the
# module, so the Streamlit-cache behaviour of this section is preserved.

# -------- BEGIN inlined universe_engine.py (v3.0.2, generated 2026-07-18 12:47:36 UTC) --------
# This block replaces the legacy Universe Engine (fetch_complete_nse_universe,
# fetch_index_constituents, and the duplicated blacklist code) with a hardened
# multi-tier fallback implementation:
#   Tier 1 - Local disk cache (~/.alphaquant/universe_cache/*.csv), 24h TTL
#   Tier 2 - nsearchives.nseindia.com (real NSE archives, primary)
#   Tier 3 - archives.nseindia.com + www.nseindia.com/api/... mirrors
#   Tier 4 - yfinance verification probe (Nifty50 seed fallback)
# Public API preserved: NSE_INDEX_SOURCES, fetch_complete_nse_universe(),
# fetch_index_constituents(url), _clean_and_suffix_symbols(iter).
# =========================================================
# IMPORTS
# =========================================================



log = logging.getLogger("alphaquant.universe")

# ---------------------------------------------------------
# Optional streamlit cache decorator (no-op when not on Streamlit)
# ---------------------------------------------------------
def _st_cache(func):
    return st.cache_data(ttl=86400, show_spinner=False)(func)

# ---------------------------------------------------------
# Optional yfinance import (only used by the tier-4 probe)
# ---------------------------------------------------------

# =========================================================
# CONSTANTS
# =========================================================

def _resolve_cache_dir() -> Path:
    """Prefer ~/.alphaquant/universe_cache, fall back to tempdir if not writable."""
    primary = Path.home() / ".alphaquant" / "universe_cache"
    try:
        primary.mkdir(parents=True, exist_ok=True)
        probe = primary / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return primary
    except Exception:
        fallback = Path(tempfile.gettempdir()) / "alphaquant_universe_cache"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


UNIVERSE_CACHE_DIR: Path = _resolve_cache_dir()
UNIVERSE_CACHE_META: Path = UNIVERSE_CACHE_DIR / "_meta.json"
UNIVERSE_CACHE_TTL_SEC: int = 86_400  # 24h

HTTP_TIMEOUT: tuple[int, int] = (5, 8)  # (connect, read) - fail fast on unreachable hosts
RETRY_TOTAL: int = 2
RETRY_BACKOFF: float = 0.3
RETRY_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})
# Note: 403 is INTENTIONALLY not in RETRY_STATUS. NSE returns 403 when the
# session cookies aren't primed - retrying without new cookies just wastes
# time. We handle 403 by cookie-priming once, then treating a second 403 as
# a hard failure and moving to the next URL/tier.
PER_URL_BUDGET_SEC: float = 25.0  # hard ceiling per URL incl. retries+backoff

USER_AGENTS: tuple[str, ...] = (
    # Realistic desktop browsers, rotated per session
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

# ---------------------------------------------------------
# NSE index sources (KEYS MUST NOT CHANGE - the rest of the app reads these)
#
# Primary uses `nsearchives.nseindia.com` (the newer NSE archive host that
# consistently returns 200 with browser-realistic UA + Referer). The legacy
# `archives.nseindia.com` host started returning 503/timeout in 2025 which
# is what triggered the "Universe Loading Failed" errors that motivated
# this rebuild.
# ---------------------------------------------------------
NSE_INDEX_SOURCES: dict[str, str] = {
    "Nifty50":        "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "Nifty Next 50":  "https://nsearchives.nseindia.com/content/indices/ind_niftynext50list.csv",
    "NSE500":         "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "Midcap":         "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
    "Smallcap":       "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    "Nifty100":       "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv",
    "Nifty200":       "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv",
    "BankNifty":      "https://nsearchives.nseindia.com/content/indices/ind_niftybanklist.csv",
    "FinNifty":       "https://nsearchives.nseindia.com/content/indices/ind_niftyfinancelist.csv",
}

# Alternative hosts tried in order after the primary fails. Only real,
# publicly-reachable endpoints are listed - no placeholder GitHub URLs.
NSE_INDEX_MIRRORS: dict[str, tuple[str, ...]] = {
    "Nifty50": (
        "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
    ),
    "Nifty Next 50": (
        "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv",
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050",
    ),
    "NSE500": (
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
    ),
    "Midcap": (
        "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%20150",
    ),
    "Smallcap": (
        "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20SMALLCAP%20250",
    ),
    # LargeMidcap250 is merged into fetch_complete_nse_universe.
    "LargeMidcap250": (
        "https://nsearchives.nseindia.com/content/indices/ind_niftylargemidcap250list.csv",
        "https://archives.nseindia.com/content/indices/ind_niftylargemidcap250list.csv",
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20LARGEMIDCAP%20250",
    ),
}

# The master equity list is the "everything on NSE" source.
EQUITY_MASTER_SOURCES: tuple[str, ...] = (
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
)

# Extra URLs merged into `fetch_complete_nse_universe` on top of the master.
_UNIVERSE_MERGE_KEYS: tuple[str, ...] = ("NSE500", "LargeMidcap250", "Smallcap")

# ---------------------------------------------------------
# Blacklists  (single source of truth - deduplicated from 3 places)
# ---------------------------------------------------------
SYMBOL_BLACKLIST: frozenset[str] = frozenset({
    "NIFTYBEES", "BANKBEES", "GOLDBEES", "LIQUIDBEES", "SILVERBEES",
    "JUNIORBEES", "CPSEETF", "PSUBNKBEES", "ITBEES",
})

SYMBOL_SUBSTRING_BLACKLIST: tuple[str, ...] = (
    "ETF", "BEES", "FUND", "GILT", "LIQUID", "NIFTY",
)


# =========================================================
# CACHE MANAGER
# =========================================================

def _cache_path(key: str) -> Path:
    """Deterministic per-key CSV cache file."""
    safe = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return UNIVERSE_CACHE_DIR / f"{safe}.csv"


def _meta_read() -> dict:
    try:
        return json.loads(UNIVERSE_CACHE_META.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _meta_write(meta: dict) -> None:
    try:
        UNIVERSE_CACHE_META.write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )
    except Exception as exc:
        log.warning("Universe: could not write cache meta: %s", exc)


def _cache_read(key: str) -> tuple[list[str] | None, float | None]:
    """Return (symbols, fetched_at_epoch) or (None, None) if no cache."""
    path = _cache_path(key)
    if not path.exists():
        return None, None
    try:
        symbols = [
            line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        meta = _meta_read().get(key, {})
        fetched_at = float(meta.get("fetched_at", path.stat().st_mtime))
        return symbols, fetched_at
    except Exception as exc:
        log.warning("Universe: cache read failed for %s: %s", key, exc)
        return None, None


def _cache_write(key: str, symbols: list[str]) -> None:
    if not symbols:
        return
    path = _cache_path(key)
    try:
        path.write_text("\n".join(symbols), encoding="utf-8")
        meta = _meta_read()
        meta[key] = {"fetched_at": time.time(), "count": len(symbols)}
        _meta_write(meta)
    except Exception as exc:
        log.warning("Universe: cache write failed for %s: %s", key, exc)


def _is_fresh(fetched_at: float | None) -> bool:
    if not fetched_at:
        return False
    return (time.time() - fetched_at) < UNIVERSE_CACHE_TTL_SEC


def _fmt_age(fetched_at: float) -> str:
    delta = int(max(0, time.time() - fetched_at))
    h, rem = divmod(delta, 3600)
    m, _ = divmod(rem, 60)
    return f"{h}h {m:02d}m"


# =========================================================
# HTTP LAYER
# =========================================================

def _build_session() -> requests.Session:
    """
    One reusable session with:
      * rotating browser-realistic UA
      * urllib3 Retry (exponential backoff, 403/429/5xx)
      * HTTPAdapter mounted on http:// and https://
    """
    s = requests.Session()

    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        status=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=tuple(RETRY_STATUS),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=16)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/csv,application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/",
        "Upgrade-Insecure-Requests": "1",
    })
    return s


def _prime_nse_cookies(session: requests.Session) -> None:
    """
    NSE returns 403 to any request that doesn't already carry its session
    cookies. Hitting the home page first can get us `nsit`/`nseappid`
    cookies which unlock the `www.nseindia.com` API endpoints.

    Best-effort: uses a plain requests call with NO retries so a hostile
    network can't stall this warm-up for tens of seconds. Note that the
    primary source `nsearchives.nseindia.com` does NOT require cookies -
    it serves 200 with just UA + Referer - so a failed prime here is
    non-fatal: the real fetches will still succeed.
    """
    for warmup in ("https://www.nseindia.com/",
                   "https://www.nseindia.com/market-data/live-equity-market"):
        try:
            # Bypass the retry-mounted adapters by using a bare request.
            r = requests.get(
                warmup,
                headers={
                    "User-Agent": session.headers.get("User-Agent", USER_AGENTS[0]),
                    "Accept": "text/html,application/xhtml+xml,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                },
                timeout=(3, 4),
            )
            # Copy any cookies back into our session
            session.cookies.update(r.cookies)
        except Exception as exc:
            log.debug("Universe: NSE cookie prime hit %s failed: %s", warmup, exc)


def _fetch_csv(session: requests.Session, url: str) -> pd.DataFrame | None:
    """
    Best-effort CSV/JSON fetch. Returns a DataFrame with normalised
    (uppercased, stripped) column names, or None on failure. The
    `nseindia.com/api/...` mirrors return JSON, which is transparently
    reshaped into a DataFrame with a `SYMBOL` column.

    Has a hard PER_URL_BUDGET_SEC wall-clock ceiling to prevent unreachable
    hosts from stalling the whole universe load.
    """
    start = time.monotonic()
    try:
        resp = session.get(url, timeout=HTTP_TIMEOUT)
    except Exception as exc:
        log.warning("Universe: request error for %s: %s", url, exc)
        return None

    if time.monotonic() - start > PER_URL_BUDGET_SEC:
        log.warning("Universe: exceeded %ss budget on %s", PER_URL_BUDGET_SEC, url)
        return None

    if resp.status_code != 200 or not resp.content:
        log.warning("Universe: HTTP %s from %s", resp.status_code, url)
        return None

    ctype = (resp.headers.get("Content-Type") or "").lower()
    body = resp.text

    try:
        if "json" in ctype or body.lstrip().startswith("{"):
            payload = resp.json()
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not rows:
                return None
            df = pd.DataFrame(rows)
        else:
            df = pd.read_csv(StringIO(body))
    except Exception as exc:
        log.warning("Universe: parse error for %s: %s", url, exc)
        return None

    if df is None or df.empty:
        return None

    df.columns = [str(c).upper().strip() for c in df.columns]
    return df


def _extract_symbol_column(df: pd.DataFrame) -> list[str]:
    """Pick the first plausible symbol column and return uppercased values."""
    for candidate in ("SYMBOL", "SYMBOL ", "TICKER", "SCRIP", "SCRIPCODE", "NSE_SYMBOL"):
        if candidate in df.columns:
            return (
                df[candidate]
                .astype(str)
                .str.upper()
                .str.strip()
                .replace({"NAN": ""})
                .tolist()
            )
    return []


# =========================================================
# NORMALISATION HELPER (single source of truth)
# =========================================================

def _normalize_symbols(raw: Iterable[str], suffix: bool = True) -> list[str]:
    """
    Uppercase + strip + dedupe + sort, apply blacklist, optionally append '.NS'.
    Empty/short/non-alnum entries are dropped. This is the ONE place blacklist
    rules live - any changes go here, nowhere else.
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in raw:
        s = str(item).upper().strip()
        # Strip any pre-existing '.NS' or '.BO' so blacklist matches the base.
        if s.endswith(".NS") or s.endswith(".BO"):
            s = s.rsplit(".", 1)[0]
        if len(s) < 2:
            continue
        if not s.replace("-", "").replace("&", "").isalnum():
            continue
        if s in SYMBOL_BLACKLIST:
            continue
        if any(bad in s for bad in SYMBOL_SUBSTRING_BLACKLIST):
            continue
        final = f"{s}.NS" if suffix else s
        if final in seen:
            continue
        seen.add(final)
        out.append(final)
    out.sort()
    return out


# =========================================================
# TIER-4  yfinance verification fallback
# =========================================================

# Small, hard-coded seed of Nifty50 constituents (Jan 2026 membership) used
# only when EVERY http tier and every mirror has failed for the critical
# Nifty50/NSE500 paths. yfinance is then used to VERIFY these tickers are
# alive on Yahoo - anything that fails to resolve is dropped. This is not
# meant to replace the primary source, only to keep the app usable when NSE
# and every mirror are simultaneously unreachable.
_NIFTY50_SEED: tuple[str, ...] = (
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
    "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JSWSTEEL", "KOTAKBANK",
    "LT", "LTIM", "M&M", "MARUTI", "NESTLEIND",
    "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATAMOTORS",
    "TATASTEEL", "TCS", "TECHM", "TITAN", "TRENT",
    "ULTRACEMCO", "UPL", "WIPRO",
)


def _yfinance_probe(symbols: Iterable[str]) -> list[str]:
    """
    Return the subset of `symbols` (bare, no suffix) for which yfinance
    has non-empty history. Fast: uses a single batched download.
    """
    if yf is None:
        return []
    tickers = [f"{s}.NS" for s in symbols]
    try:
        data = yf.download(
            tickers=" ".join(tickers),
            period="5d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception as exc:
        log.warning("Universe: yfinance probe failed: %s", exc)
        return []

    alive: list[str] = []
    if hasattr(data, "columns") and getattr(data.columns, "nlevels", 1) > 1:
        for t in tickers:
            try:
                sub = data[t]
                if not sub.dropna(how="all").empty:
                    alive.append(t.rsplit(".", 1)[0])
            except Exception:
                continue
    else:
        # single-ticker degenerate case
        if not data.dropna(how="all").empty and tickers:
            alive.append(tickers[0].rsplit(".", 1)[0])
    return alive


# =========================================================
# INTERNAL FETCH ORCHESTRATOR
# =========================================================

def _fetch_from_urls(
    session: requests.Session,
    urls: Iterable[str],
    label: str,
) -> list[str]:
    """
    Try each URL in order, return the first non-empty symbol list. Emits the
    exact log line style required ("Universe: NSE archives unavailable ...").
    """
    urls = list(urls)
    if not urls:
        return []

    for idx, url in enumerate(urls):
        tier = "NSE archives" if idx == 0 else f"mirror #{idx}"
        df = _fetch_csv(session, url)
        if df is None:
            log.warning(
                "Universe: %s unavailable for %s, trying %s",
                tier, label, f"mirror #{idx + 1}" if idx + 1 < len(urls) else "next tier",
            )
            continue
        symbols = _extract_symbol_column(df)
        if not symbols:
            log.warning("Universe: %s returned no SYMBOL column for %s", tier, label)
            continue
        log.info("Universe: %s served %s (%d rows)", tier, label, len(symbols))
        return symbols

    return []


# =========================================================
# PUBLIC API
# =========================================================

@_st_cache
def fetch_complete_nse_universe() -> list[str]:
    """
    Return the full NSE equity universe as sorted, unique, '.NS'-suffixed
    tickers. Implements the 4-tier fallback described in the module docstring.
    Never raises. Empty list is only returned if EVERY tier fails AND no
    cache has ever existed.
    """
    cache_key = "complete_nse_universe"
    cached, fetched_at = _cache_read(cache_key)

    if cached and _is_fresh(fetched_at):
        log.info(
            "Universe: loaded cache (%d symbols, age %s)",
            len(cached), _fmt_age(fetched_at),
        )
        return cached

    if cached:
        log.info("Universe: refreshing (cache expired)")
    else:
        log.info("Universe: no cache found, cold fetch")

    session = _build_session()
    _prime_nse_cookies(session)

    # --- Tier 2/3: master equity list + index constituent merges ---
    master = _fetch_from_urls(session, EQUITY_MASTER_SOURCES, "EQUITY_L")
    merged: set[str] = set(master)

    for key in _UNIVERSE_MERGE_KEYS:
        urls: list[str] = []
        primary = NSE_INDEX_SOURCES.get(key)
        if primary:
            urls.append(primary)
        urls.extend(NSE_INDEX_MIRRORS.get(key, ()))
        merged.update(_fetch_from_urls(session, urls, key))

    # --- Tier 4: yfinance probe as a last resort for the critical path ---
    if not merged:
        log.warning("Universe: all HTTP tiers failed, probing yfinance (Nifty50 seed)")
        probed = _yfinance_probe(_NIFTY50_SEED)
        merged.update(probed)

    final = _normalize_symbols(merged, suffix=True)

    if final:
        _cache_write(cache_key, final)
        log.info("Universe Loaded: %d symbols", len(final))
        return final

    # --- Stale-cache-wins ---
    if cached:
        log.warning("Universe: using cached universe (last refresh failed)")
        return cached

    log.error("Universe: every source failed and no cache exists - returning empty list")
    return []


@_st_cache
def fetch_index_constituents(url: str) -> set[str]:
    """
    Generic NSE index-constituent fetcher. Returns a set of RAW base symbols
    (no '.NS' suffix) - unchanged contract vs. the legacy implementation so
    the downstream `_clean_and_suffix_symbols` keeps working.

    Fallback chain per URL:
      1. Disk cache (< 24h)
      2. Primary URL (typically archives.nseindia.com)
      3. Mirrors mapped by the URL's index-key, if known
      4. Stale-cache-wins
    """
    if not url:
        return set()

    cache_key = f"index::{hashlib.sha1(url.encode('utf-8')).hexdigest()[:16]}"
    cached, fetched_at = _cache_read(cache_key)

    if cached and _is_fresh(fetched_at):
        log.info(
            "Universe: loaded cache for %s (%d symbols, age %s)",
            url, len(cached), _fmt_age(fetched_at),
        )
        return set(cached)

    if cached:
        log.info("Universe: refreshing index %s (cache expired)", url)

    # Discover which named index this URL corresponds to, if any, so we can
    # append its known mirrors.
    index_key: str | None = None
    for key, src in NSE_INDEX_SOURCES.items():
        if src == url:
            index_key = key
            break

    session = _build_session()
    _prime_nse_cookies(session)

    urls: list[str] = [url]
    if index_key:
        urls.extend(NSE_INDEX_MIRRORS.get(index_key, ()))

    symbols = _fetch_from_urls(session, urls, index_key or url)

    # For the critical Nifty50 path, use yfinance as a last resort.
    if not symbols and index_key in {"Nifty50", "NSE500"}:
        log.warning("Universe: yfinance fallback for %s", index_key)
        symbols = _yfinance_probe(_NIFTY50_SEED)

    # Store the CLEANED (blacklist-applied) base symbols, but return an unsuffixed
    # set to preserve the legacy public contract.
    cleaned = _normalize_symbols(symbols, suffix=False)

    if cleaned:
        _cache_write(cache_key, cleaned)
        log.info("Universe Loaded: %d symbols for %s", len(cleaned), index_key or url)
        return set(cleaned)

    if cached:
        log.warning("Universe: using cached universe for %s (last refresh failed)", url)
        return set(cached)

    log.error("Universe: index %s exhausted every source and has no cache", url)
    return set()


# =========================================================
# BACKWARD-COMPAT SHIM
# =========================================================
# The legacy file defined `_clean_and_suffix_symbols` inline. Downstream
# call-sites (`get_named_universe`, `get_cap_bucket_symbols`) still use it,
# so we re-export a thin wrapper that delegates to `_normalize_symbols`.

def _clean_and_suffix_symbols(base_symbols: Iterable[str]) -> list[str]:
    """Legacy alias - kept so downstream call-sites keep working unchanged."""
    return _normalize_symbols(base_symbols, suffix=True)
# -------- END inlined universe_engine.py --------

# =====================================================
# LOAD UNIVERSE
# =====================================================

ALL_SYMBOLS = fetch_complete_nse_universe()

if _P("Developer Mode"):
    if len(ALL_SYMBOLS) == 0:
        st.error("Universe loading failed")
    else:
        st.success(f"Universe loaded: {len(ALL_SYMBOLS)} stocks")

if _P("Scanner"):
    with st.expander("Universe Preview"):
        st.dataframe(
            pd.DataFrame({"Symbol": ALL_SYMBOLS}).head(100)
        )

# =====================================================
# SCAN MANAGER
# =====================================================
#
# Everything above this point (fetch_complete_nse_universe / ALL_SYMBOLS)
# is one raw data source: "the entire NSE, minus obvious ETFs". The Scan
# Manager below turns that - plus a few more NSE index-constituent
# sources - into the actual, final list of symbols a scan will run
# against, chosen and filtered by the user BEFORE any market data is
# downloaded or any strategy runs. This keeps a "RUN ALPHAQUANT" (or,
# later, an automated scan) from blindly crawling the whole exchange
# every time.
#
# It produces exactly one thing that matters downstream:
# `st.session_state.scan_universe` - the symbol list the Download/Scan
# buttons below now consume instead of the raw ALL_SYMBOLS. Nothing about
# how strategies, AI Consensus, Risk Manager, Portfolio Manager, or paper
# trading work is changed by this section.
#
# NSE_INDEX_SOURCES + fetch_index_constituents + _clean_and_suffix_symbols
# are re-exported from universe_engine (see block above), so the rest of
# this Scan Manager works unchanged.


SCAN_UNIVERSE_CHOICES = [
    "NSE All", "NSE500", "Nifty50", "Nifty Next 50",
    "Midcap", "Smallcap", "Watchlist",
]


def get_named_universe(choice):
    """
    Resolves one of the Scan Manager's universe choices to a final,
    ".NS"-suffixed, blacklist-cleaned symbol list.

    "NSE All" reuses fetch_complete_nse_universe()'s output as-is (already
    cleaned/suffixed). Every named index reuses the same NSE archive
    source pattern via fetch_index_constituents(). "Watchlist" reads the
    user's own session watchlist (managed in the Watchlist panel below).
    """

    if choice == "NSE All":
        return list(ALL_SYMBOLS)

    if choice == "Watchlist":
        return _clean_and_suffix_symbols(
            s.replace(".NS", "") for s in st.session_state.watchlist
        )

    url = NSE_INDEX_SOURCES.get(choice)

    if url is None:
        return []

    return _clean_and_suffix_symbols(fetch_index_constituents(url))


def get_cap_bucket_symbols(bucket):
    """
    "Market Cap" filter data source. There is no free, bulk market-cap
    number available without a per-symbol lookup across the whole
    exchange, so this reuses NSE's own index membership as the lightest
    reasonable proxy: Nifty50 + Nifty Next 50 = Large Cap, Nifty Midcap
    150 = Mid Cap, Nifty Smallcap 250 = Small Cap (NSE's own index
    methodology already buckets the exchange by free-float market cap).
    Returns None for "Any" (no filtering), or an empty set if every
    underlying source failed to fetch (degrades to "no cap filtering"
    rather than zeroing out the universe on a network hiccup).
    """

    if bucket == "Large Cap":
        base = (
            fetch_index_constituents(NSE_INDEX_SOURCES["Nifty50"])
            | fetch_index_constituents(NSE_INDEX_SOURCES["Nifty Next 50"])
        )
    elif bucket == "Mid Cap":
        base = fetch_index_constituents(NSE_INDEX_SOURCES["Midcap"])
    elif bucket == "Small Cap":
        base = fetch_index_constituents(NSE_INDEX_SOURCES["Smallcap"])
    else:
        return None

    return set(_clean_and_suffix_symbols(base))


SCAN_STYLE_STRATEGY_MAP = {

    "Breakout": {"BREAKOUT"},

    "Momentum": {"VCP", "PRICE SQUEEZE"},

    "Swing": {"DEMAND & SUPPLY", "MARKET REGIME"},

    "Intraday": {"ORDER BLOCK", "FVG"},

}


# =====================================================
# SCANNER FILTER PERSISTENCE
# =====================================================
# Streamlit widget keys are ephemeral: when the Scanner tab isn't rendered
# (user switched to Dashboard/AI/etc.), the `scan_manager_*` widget keys
# are garbage-collected from session_state. To keep filter selections
# stable across tab switches, we mirror every widget value into a
# `*_saved` shadow key that survives the widget's lifecycle. The pipeline
# and build_default_scan_universe_for_pipeline() read exclusively from
# the saved keys.

_SCAN_FILTER_DEFAULTS = {
    "scan_manager_universe_choice_saved": SCAN_UNIVERSE_CHOICES[0],
    "scan_manager_cap_filter_saved": "Any",
    "scan_manager_price_range_saved": (20, 20000),
    "scan_manager_min_volume_saved": CONFIG["MIN_AVG_VOLUME"],
    "scan_manager_min_turnover_saved": CONFIG["MIN_AVG_TURNOVER"],
    "scan_manager_sector_filter_saved": [],
    "scan_manager_style_filter_saved": [],
}
for _k, _v in _SCAN_FILTER_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v



def apply_style_selection_to_strategies(selected_styles):
    """
    Applies the Scan Manager's Style filter by enabling/disabling entries
    already sitting in st.session_state.strategy_registry - it does not
    change how the pipeline runs registered strategies (run_all_strategies
    is untouched). No style selected = no restriction, which is also the
    app's long-standing default (every registered strategy enabled).
    """

    if not selected_styles:

        for strategy in st.session_state.strategy_registry:
            strategy.enabled = True

        return

    active_names = set()

    for style in selected_styles:
        active_names |= SCAN_STYLE_STRATEGY_MAP.get(style, set())

    for strategy in st.session_state.strategy_registry:
        strategy.enabled = strategy.name in active_names


def fetch_quote_snapshot(symbols):
    """
    Cheap pre-scan snapshot used only to filter the universe by price /
    volume / liquidity before the real (period=1y+) download happens.
    Intentionally independent of download_batch/download_market_data:
    those are tuned for the full history download every stock needs once
    it's actually being scanned, while this only needs ~5 days of data
    per symbol to answer "is this even in range", so it stays a small,
    separate helper rather than overloading the main download path.
    """

    snapshot = {}

    def _fetch_one(symbol):

        try:

            df = yf.download(
                symbol,
                period="5d",
                interval="1d",
                progress=False,
                auto_adjust=True,
                threads=False,
            )

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df is None or len(df) == 0:
                return None

            closes = df["Close"].dropna()
            volumes = df["Volume"].dropna()

            if len(closes) == 0 or len(volumes) == 0:
                return None

            last_close = float(closes.iloc[-1])
            avg_volume = float(volumes.tail(5).mean())

            return symbol, {
                "price": last_close,
                "avg_volume": avg_volume,
                "avg_turnover": last_close * avg_volume,
            }

        except Exception as e:

            logging.warning(f"quote snapshot {symbol} : {e}")
            return None

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:

        for result in executor.map(_fetch_one, symbols):

            if result is not None:
                symbol, data = result
                snapshot[symbol] = data

    return snapshot


def build_scan_universe(
    universe_choice,
    cap_filter="Any",
    price_range=(0, 100000),
    min_volume=0,
    min_turnover=0,
    sectors=None,
    styles=None,
):
    """
    The Scan Manager's single public entry point: given a universe choice
    and the pre-scan filters, returns the final symbol list a scan should
    run against. Both the manual "Preview Scan List" button below and any
    future automated scan trigger call this same function, so there is
    exactly one place universe + filter logic lives.
    """

    symbols = get_named_universe(universe_choice)

    if cap_filter != "Any":
        cap_set = get_cap_bucket_symbols(cap_filter)
        if cap_set:
            symbols = [s for s in symbols if s in cap_set]

    if sectors:
        allowed = set(sectors)
        symbols = [
            s for s in symbols
            if STOCK_SECTOR_MAP.get(s.replace(".NS", ""), "UNKNOWN") in allowed
        ]

    price_min, price_max = price_range

    needs_quote_filter = (
        price_min > 0 or price_max < 100000 or min_volume > 0 or min_turnover > 0
    )

    if needs_quote_filter and symbols:

        snapshot = fetch_quote_snapshot(symbols)

        filtered = []

        for s in symbols:

            q = snapshot.get(s)

            if q is None:
                continue

            if not (price_min <= q["price"] <= price_max):
                continue

            if q["avg_volume"] < min_volume:
                continue

            if q["avg_turnover"] < min_turnover:
                continue

            filtered.append(s)

        symbols = filtered

    apply_style_selection_to_strategies(styles or [])

    return symbols


# =====================================================
# SCAN MANAGER UI
# =====================================================

if _P("Scanner"):

    st.divider()

    st.subheader("Scan Manager")

    st.caption(
        "Choose exactly what to scan before downloading any data or running "
        "any strategy - the Scan Manager builds the final symbol list once, "
        "up front."
    )

    if "scan_universe" not in st.session_state:
        st.session_state.scan_universe = []

    with st.expander("Watchlist"):

        watchlist_cols = st.columns([3, 1])

        new_watch_symbol = watchlist_cols[0].text_input(
            "Add symbol (e.g. RELIANCE)",
            key="watchlist_add_input",
        )

        if watchlist_cols[1].button("Add", key="watchlist_add_button"):

            cleaned = new_watch_symbol.strip().upper().replace(".NS", "")

            if cleaned and cleaned not in st.session_state.watchlist:
                st.session_state.watchlist.append(cleaned)

        if st.session_state.watchlist:

            st.write(", ".join(st.session_state.watchlist))

            remove_choice = st.selectbox(
                "Remove from watchlist",
                ["-"] + st.session_state.watchlist,
                key="watchlist_remove_select",
            )

            if remove_choice != "-" and st.button("Remove", key="watchlist_remove_button"):
                st.session_state.watchlist.remove(remove_choice)

        else:

            st.caption("Watchlist is empty.")

    scan_manager_cols = st.columns(2)

    scan_universe_choice = scan_manager_cols[0].selectbox(
        "Universe",
        SCAN_UNIVERSE_CHOICES,
        key="scan_manager_universe_choice",
    )
    st.session_state["scan_manager_universe_choice_saved"] = scan_universe_choice

    scan_cap_filter = scan_manager_cols[1].selectbox(
        "Market Cap",
        ["Any", "Large Cap", "Mid Cap", "Small Cap"],
        key="scan_manager_cap_filter",
    )
    st.session_state["scan_manager_cap_filter_saved"] = scan_cap_filter

    scan_price_range = st.slider(
        "Price Range (Rs)",
        min_value=0,
        max_value=20000,
        value=(20, 20000),
        key="scan_manager_price_range",
    )
    st.session_state["scan_manager_price_range_saved"] = scan_price_range

    scan_min_volume = st.number_input(
        "Minimum Average Volume (5-day)",
        min_value=0,
        value=CONFIG["MIN_AVG_VOLUME"],
        step=1000,
        key="scan_manager_min_volume",
    )
    st.session_state["scan_manager_min_volume_saved"] = scan_min_volume

    scan_min_turnover = st.number_input(
        "Liquidity - Minimum Average Turnover (Rs, 5-day)",
        min_value=0,
        value=CONFIG["MIN_AVG_TURNOVER"],
        step=1000000,
        key="scan_manager_min_turnover",
    )
    st.session_state["scan_manager_min_turnover_saved"] = scan_min_turnover

    scan_sector_filter = st.multiselect(
        "Sector (known-sector map only; leave empty for no sector restriction)",
        sorted(set(STOCK_SECTOR_MAP.values())),
        key="scan_manager_sector_filter",
    )
    st.session_state["scan_manager_sector_filter_saved"] = scan_sector_filter

    scan_style_filter = st.multiselect(
        "Style (which strategy types this scan should look for; leave empty for all)",
        sorted(SCAN_STYLE_STRATEGY_MAP.keys()),
        key="scan_manager_style_filter",
    )
    st.session_state["scan_manager_style_filter_saved"] = scan_style_filter

    st.checkbox(
        "Long Only",
        value=True,
        key="scan_manager_long_only",
        disabled=True,
        help="AlphaQuant is a long-only platform end-to-end -- there is no "
             "short-selling logic to disable.",
    )

    if st.button("Preview Scan List", key="scan_manager_build_button"):

        with st.spinner("Building scan universe..."):

            st.session_state.scan_universe = build_scan_universe(
                scan_universe_choice,
                cap_filter=scan_cap_filter,
                price_range=scan_price_range,
                min_volume=scan_min_volume,
                min_turnover=scan_min_turnover,
                sectors=scan_sector_filter,
                styles=scan_style_filter,
            )

            st.session_state.scan_manager_active_styles = scan_style_filter

    if st.session_state.scan_universe:

        st.success(
            f"Scan list ready: {len(st.session_state.scan_universe)} symbols "
            f"selected for the next scan."
        )

        with st.expander("Scan List Preview"):
            st.dataframe(
                pd.DataFrame(
                    {"Symbol": st.session_state.scan_universe}
                ).head(200)
            )

    else:

        st.info("Build a scan list above before downloading data or running a scan.")


# =====================================================
# PRODUCTION DOWNLOAD MANAGER
# VERSION 2.1C
# =====================================================

DOWNLOAD_STATUS = st.empty()

DOWNLOAD_PROGRESS = st.progress(0)


def split_into_batches(symbols, batch_size):

    return [

        symbols[i:i + batch_size]

        for i in range(

            0,

            len(symbols),

            batch_size

        )

    ]


def download_batch(batch):

    """
    Downloads one batch.

    Returns

    {
        symbol:dataframe
    }

    """

    results = {}

    for symbol in batch:

        try:

            df = yf.download(

                symbol,

                period=CONFIG["DOWNLOAD_PERIOD"],

                interval=CONFIG["DOWNLOAD_INTERVAL"],

                progress=False,

                auto_adjust=True,

                threads=False

            )

            # yfinance returns MultiIndex columns (Price, Ticker) even for a
            # single symbol. Flatten to plain OHLCV column names so every
            # downstream df["Close"]/df["High"]/etc access returns a Series,
            # not a nested DataFrame.
            if isinstance(df.columns, pd.MultiIndex):

                df.columns = df.columns.get_level_values(0)

            # Drop the most recent row if it is still an incomplete/unsettled
            # session (all price fields NaN), which yfinance can include for
            # the current trading day.
            if len(df) and df["Close"].isna().iloc[-1]:

                df = df.iloc[:-1]

            if len(df) > 50:

                results[symbol] = df

        except Exception as e:

            logging.warning(

                f"{symbol} : {e}"

            )

    return results


def download_market_data(symbols):

    logging.info(

        "Starting Production Download"

    )

    batches = split_into_batches(

        symbols,

        CONFIG["DOWNLOAD_BATCH"]

    )

    complete = {}

    total = len(batches)

    start = time.time()

    with ThreadPoolExecutor(

        max_workers=CONFIG["MAX_WORKERS"]

    ) as executor:

        futures = [

            executor.submit(

                download_batch,

                batch

            )

            for batch in batches

        ]

        for idx, future in enumerate(futures):

            data = future.result()

            complete.update(data)

            DOWNLOAD_PROGRESS.progress(

                (idx + 1) / total

            )

            DOWNLOAD_STATUS.write(

                f"Completed Batch {idx+1} / {total}"

            )

    elapsed = round(

        time.time() - start,

        2

    )

    logging.info(

        f"{len(complete)} datasets downloaded"

    )

    st.success(

        f"{len(complete)} symbols downloaded in {elapsed} sec"

    )

    return complete


# =====================================================
# MARKET DATA ENGINE STATUS
# =====================================================

if _P("Developer Mode"):
    st.divider()
    st.subheader("Market Data Engine")
    st.caption("Market data downloads automatically when RUN ALPHAQUANT is pressed.")
    if st.session_state.market_data:
        st.success(f"{len(st.session_state.market_data)} symbol datasets are loaded for the latest run.")
    else:
        st.info("No market data loaded yet.")
# =====================================================
# CONFIGURATION CONTROLS
# User preferences render in Profile; engineering tuning renders only in Developer.
# No normal-workspace sidebar is created.
# =====================================================
if _P("Developer Mode"):
    st.markdown('<div class="aq-panel-title">Download Engine & Performance</div>', unsafe_allow_html=True)
    d1, d2 = st.columns(2)
    CONFIG["DOWNLOAD_BATCH"] = d1.slider("Batch size", 10, 100, CONFIG["DOWNLOAD_BATCH"], 10, key="data_download_batch_size")
    CONFIG["MAX_WORKERS"] = d2.slider("Parallel workers", 2, 16, CONFIG["MAX_WORKERS"], key="data_download_parallel_workers")
    st.caption("Diagnostics, provider status and pipeline tuning are isolated in Developer mode.")
# =====================================================
# INDICATOR ENGINE
# VERSION 2.2A
# =====================================================

try:
    import pandas_ta as ta
except Exception as e:
    logging.exception("pandas_ta failed to import")
    st.error(
        "pandas-ta failed to import:\n\n" + str(e)
    )
    st.code(traceback.format_exc())
    st.stop()


def calculate_indicators(df):
    """
    Calculates all core indicators once.

    Returns dataframe with indicators added.
    """

    # Default download period is "1y", which yfinance realistically
    # returns as ~249 rows (calendar year minus weekends/holidays), never
    # a full 250. The old ">= 250" bar was unreachable with the default
    # config, silently rejecting every stock. 200 rows is still enough
    # history for the longest lookback actually used below (EMA200).
    if len(df) < 200:
        return None

    try:

        # ==========================
        # EMA
        # ==========================

        df["EMA20"] = ta.ema(
            df["Close"],
            length=20
        )

        df["EMA50"] = ta.ema(
            df["Close"],
            length=50
        )

        df["EMA100"] = ta.ema(
            df["Close"],
            length=100
        )

        df["EMA200"] = ta.ema(
            df["Close"],
            length=200
        )

        # ==========================
        # RSI
        # ==========================

        df["RSI"] = ta.rsi(
            df["Close"],
            length=14
        )

        # ==========================
        # ATR
        # ==========================

        df["ATR"] = ta.atr(

            df["High"],

            df["Low"],

            df["Close"],

            length=14

        )

        # ==========================
        # ADX
        # ==========================

        adx = ta.adx(

            df["High"],

            df["Low"],

            df["Close"]

        )

        df["ADX"] = adx["ADX_14"]

        # ==========================
        # MACD
        # ==========================

        macd = ta.macd(

            df["Close"]

        )

        df["MACD"] = macd["MACD_12_26_9"]

        df["MACD_SIGNAL"] = macd["MACDs_12_26_9"]

        # ==========================
        # Bollinger
        # ==========================

        # length=20 must be explicit: this pandas_ta version defaults
        # bbands() to a 5-period window, not 20, when length is omitted.
        # Column names also vary by pandas_ta version (e.g. "BBU_20_2.0"
        # vs "BBU_20_2.0_2.0"), so select by prefix instead of an exact
        # hardcoded name.
        bb = ta.bbands(df["Close"], length=20)

        bb_upper_col = [c for c in bb.columns if c.startswith("BBU_20")][0]

        bb_middle_col = [c for c in bb.columns if c.startswith("BBM_20")][0]

        bb_lower_col = [c for c in bb.columns if c.startswith("BBL_20")][0]

        df["BB_UPPER"] = bb[bb_upper_col]

        df["BB_MIDDLE"] = bb[bb_middle_col]

        df["BB_LOWER"] = bb[bb_lower_col]

        # ==========================
        # VWAP
        # ==========================

        df["VWAP"] = ta.vwap(

            df["High"],

            df["Low"],

            df["Close"],

            df["Volume"]

        )

        # ==========================
        # Average Volume
        # ==========================

        df["AVG_VOLUME20"] = (

            df["Volume"]

            .rolling(20)

            .mean()

        )

        # ==========================
        # Relative Volume
        # ==========================

        df["RVOL"] = (

            df["Volume"]

            /

            df["AVG_VOLUME20"]

        )

        # ==========================
        # 52 Week High Low
        # ==========================

        df["HIGH52"] = (

            df["High"]

            .rolling(252)

            .max()

        )

        df["LOW52"] = (

            df["Low"]

            .rolling(252)

            .min()

        )

        return df

    except Exception as e:

        st.error("===================================")
        st.error("Indicator failed")
        st.error(str(e))
        st.code(traceback.format_exc())

        return None
# =====================================================
# STOCK INTELLIGENCE OBJECT
# VERSION 2.2B
# =====================================================

class StockObject:
    """
    Master object for every stock.

    Every engine writes into this object.

    Indicators

    Patterns

    Risk

    News

    Portfolio

    AI Decision

    """

    def __init__(self, symbol):

        self.symbol = symbol

        self.data = None

        self.indicators = {}

        self.patterns = {}

        self.risk = {}

        self.market = {}

        self.news = {}

        self.portfolio = {}

        self.decision = {}

        self.score = {

            "trend": 0,

            "momentum": 0,

            "volume": 0,

            "volatility": 0,

            "pattern": 0,

            "market": 0,

            "sector": 0,

            "news": 0,

            "risk": 0,

            "quality": 0

        }

        self.state = "DISCOVERED"

        self.reason = []

    def set_dataframe(self, df):

        self.data = df

    def add_indicator(self, name, value):

        self.indicators[name] = value

    def add_pattern(self, name, value):

        self.patterns[name] = value

    def add_news(self, name, value):

        self.news[name] = value

    def add_market(self, name, value):

        self.market[name] = value

    def add_risk(self, name, value):

        self.risk[name] = value

    def add_reason(self, text):

        self.reason.append(text)

    def set_state(self, state):

        self.state = state

    def add_score(self, category, points):

        if category in self.score:

            self.score[category] += points

    def calculate_trade_quality(self):

        total = 0

        for key in self.score:

            if key != "quality":

                total += self.score[key]

        self.score["quality"] = round(total, 2)

        return self.score["quality"]

    def summary(self):

        return {

            "Symbol": self.symbol,

            "State": self.state,

            "Trade Quality": self.score["quality"],

            "Reasons": self.reason

        }


# =====================================================
# STOCK OBJECT STORAGE
# =====================================================

if "stock_objects" not in st.session_state:

    st.session_state.stock_objects = {}


def get_stock(symbol):

    if symbol not in st.session_state.stock_objects:

        st.session_state.stock_objects[symbol] = StockObject(symbol)

    return st.session_state.stock_objects[symbol]


if _P("Developer Mode"):
    st.success("Stock Intelligence Engine loaded")
# =====================================================
# TRADE QUALITY ENGINE
# VERSION 2.2C
# =====================================================

MAX_SCORE = {

    "trend":20,

    "momentum":15,

    "volume":15,

    "volatility":10,

    "pattern":15,

    "market":5,

    "sector":5,

    "news":5,

    "risk":10

}


def score_trend(stock):

    df = stock.data

    if df is None:
        return

    last = df.iloc[-1]

    if last["EMA20"] > last["EMA50"]:

        stock.add_score("trend",5)

        stock.add_reason("EMA20 above EMA50")

    if last["EMA50"] > last["EMA100"]:

        stock.add_score("trend",5)

        stock.add_reason("EMA50 above EMA100")

    if last["EMA100"] > last["EMA200"]:

        stock.add_score("trend",5)

        stock.add_reason("EMA100 above EMA200")

    if last["Close"] > last["EMA20"]:

        stock.add_score("trend",5)

        stock.add_reason("Price above EMA20")


def score_momentum(stock):

    df = stock.data

    if df is None:
        return

    last = df.iloc[-1]

    rsi = last["RSI"]

    if 55 <= rsi <= 70:

        stock.add_score("momentum",8)

        stock.add_reason(f"Healthy RSI {round(rsi,1)}")

    elif rsi > 70:

        stock.add_score("momentum",4)

        stock.add_reason("RSI Overbought")

    macd = last["MACD"]

    signal = last["MACD_SIGNAL"]

    if macd > signal:

        stock.add_score("momentum",7)

        stock.add_reason("MACD Bullish")


def score_volume(stock):

    df = stock.data

    if df is None:
        return

    last = df.iloc[-1]

    rvol = last["RVOL"]

    if rvol >= 2:

        stock.add_score("volume",15)

        stock.add_reason("Very High Relative Volume")

    elif rvol >= 1.5:

        stock.add_score("volume",10)

        stock.add_reason("High Relative Volume")

    elif rvol >= 1.2:

        stock.add_score("volume",5)

        stock.add_reason("Moderate Relative Volume")


def score_volatility(stock):

    df = stock.data

    if df is None:
        return

    last = df.iloc[-1]

    atr = last["ATR"]

    price = last["Close"]

    atr_percent = (atr / price) * 100

    stock.add_indicator(

        "ATR_PERCENT",

        round(atr_percent,2)

    )

    if 1 <= atr_percent <= 5:

        stock.add_score(

            "volatility",

            10

        )

        stock.add_reason(

            "Healthy ATR"

        )

    elif atr_percent < 1:

        stock.add_score(

            "volatility",

            5

        )


def score_risk(stock):

    df = stock.data

    if df is None:
        return

    last = df.iloc[-1]

    stop = last["Close"] - (2 * last["ATR"])

    stock.add_risk(

        "INITIAL_STOP",

        round(stop,2)

    )

    stock.add_score(

        "risk",

        10

    )

    stock.add_reason(

        "ATR Risk Calculated"

    )


def calculate_trade_quality(stock):

    stock.score = {

        "trend":0,

        "momentum":0,

        "volume":0,

        "volatility":0,

        "pattern":0,

        "market":0,

        "sector":0,

        "news":0,

        "risk":0,

        "quality":0

    }

    stock.reason = []

    score_trend(stock)

    score_momentum(stock)

    score_volume(stock)

    score_volatility(stock)

    score_risk(stock)

    quality = stock.calculate_trade_quality()

    if quality >= 90:

        stock.set_state("HIGH CONVICTION BUY")

    elif quality >= 80:

        stock.set_state("BUY")

    elif quality >= 70:

        stock.set_state("READY")

    elif quality >= 60:

        stock.set_state("WATCHLIST")

    else:

        stock.set_state("REJECT")

    stock.decision["QUALITY"] = quality

    stock.decision["STATE"] = stock.state

    return stock


# =====================================================
# QUALITY PREVIEW
# =====================================================
# The old "Test Trade Quality" manual dev button has been removed - this
# is not skipped work, calculate_trade_quality() already runs for every
# symbol inside execute_scan_pipeline() automatically, so a separate
# manual preview button was a dev-only leftover that duplicated the same
# call.

# =====================================================
# MARKET STRUCTURE ENGINE
# VERSION 2.2D
# =====================================================

STRUCTURE_LOOKBACK = 5


def detect_swings(df, lookback=STRUCTURE_LOOKBACK):

    df = df.copy()

    df["SWING_HIGH"] = False
    df["SWING_LOW"] = False

    for i in range(lookback, len(df)-lookback):

        high = df["High"].iloc[i]

        if high == max(
            df["High"].iloc[
                i-lookback:i+lookback+1
            ]
        ):
            df.loc[df.index[i], "SWING_HIGH"] = True

        low = df["Low"].iloc[i]

        if low == min(
            df["Low"].iloc[
                i-lookback:i+lookback+1
            ]
        ):
            df.loc[df.index[i], "SWING_LOW"] = True

    return df


def classify_market_structure(stock):

    df = stock.data

    if df is None:
        return

    df = detect_swings(df)

    stock.data = df

    highs = df[df["SWING_HIGH"]]

    lows = df[df["SWING_LOW"]]

    if len(highs) < 2 or len(lows) < 2:
        return

    last_high = highs["High"].iloc[-1]
    prev_high = highs["High"].iloc[-2]

    last_low = lows["Low"].iloc[-1]
    prev_low = lows["Low"].iloc[-2]

    trend = "SIDEWAYS"

    if last_high > prev_high and last_low > prev_low:

        trend = "UPTREND"

        stock.add_score("trend",5)

        stock.add_reason(
            "Higher High + Higher Low"
        )

    elif last_high < prev_high and last_low < prev_low:

        trend = "DOWNTREND"

        stock.add_reason(
            "Lower High + Lower Low"
        )

    elif last_high > prev_high:

        trend = "POSSIBLE_BREAKOUT"

        stock.add_reason(
            "Higher High Formed"
        )

    elif last_low < prev_low:

        trend = "WEAKNESS"

        stock.add_reason(
            "Lower Low Formed"
        )

    stock.market["TREND"] = trend

    stock.market["LAST_SWING_HIGH"] = round(
        last_high,
        2
    )

    stock.market["LAST_SWING_LOW"] = round(
        last_low,
        2
    )


def detect_break_of_structure(stock):

    df = stock.data

    if df is None:
        return

    last_close = df["Close"].iloc[-1]

    if "LAST_SWING_HIGH" in stock.market:

        if last_close > stock.market["LAST_SWING_HIGH"]:

            stock.add_pattern(
                "BOS",
                True
            )

            stock.add_score(
                "pattern",
                8
            )

            stock.add_reason(
                "Break of Structure"
            )

    if "LAST_SWING_LOW" in stock.market:

        if last_close < stock.market["LAST_SWING_LOW"]:

            stock.add_pattern(
                "BEARISH_BOS",
                True
            )


def detect_change_of_character(stock):

    trend = stock.market.get(
        "TREND",
        ""
    )

    if trend == "POSSIBLE_BREAKOUT":

        stock.add_pattern(
            "CHOCH",
            True
        )

        stock.add_score(
            "pattern",
            5
        )

        stock.add_reason(
            "Possible Change of Character"
        )


def update_market_structure(stock):

    classify_market_structure(stock)

    detect_break_of_structure(stock)

    detect_change_of_character(stock)

    return stock


# =====================================================
# MARKET STRUCTURE TEST
# =====================================================
# The old "Test Market Structure" manual dev button has been removed -
# update_market_structure() already runs for every symbol inside
# execute_scan_pipeline() automatically.

# =====================================================
# TRADE CANDIDATE ENGINE
# VERSION 2.2E
# =====================================================

class TradeCandidate:

    def __init__(self, symbol, strategy):

        self.symbol = symbol
        self.strategy = strategy

        self.state = "DISCOVERED"

        self.entry = None
        self.stop = None

        self.target1 = None
        self.target2 = None
        self.target3 = None

        self.risk = None
        self.reward = None
        self.risk_reward = 0

        self.confidence = 0

        self.position_size = 0
        self.capital_required = 0
        self.maximum_loss = 0
        self.maximum_profit = 0
        self.portfolio_weight = 0
        self.ai_score = 0
        self.strategy_count = 0
        self.direction = "LONG"

        self.reasons = []

        self.missing_triggers = []

        self.warnings = []

        self.timestamp = datetime.now()

    def add_reason(self, text):

        self.reasons.append(text)

    def add_trigger(self, text):

        self.missing_triggers.append(text)

    def add_warning(self, text):

        self.warnings.append(text)

    def set_entry(self, value):

        self.entry = round(value,2)

    def set_stop(self, value):

        self.stop = round(value,2)

    def set_target1(self, value):

        self.target1 = round(value,2)

    def set_target2(self, value):

        self.target2 = round(value,2)

    def set_target3(self, value):

        self.target3 = round(value,2)

    def calculate_rr(self):

        if self.entry is None:
            return

        if self.stop is None:
            return

        if self.target1 is None:
            return

        risk = self.entry-self.stop

        reward=self.target1-self.entry

        if risk<=0:

            return

        self.risk=risk

        self.reward=reward

        self.risk_reward=round(

            reward/risk,

            2

        )

    def summary(self):

        return {

            "Symbol":self.symbol,

            "Strategy":self.strategy,

            "State":self.state,

            "Entry":self.entry,

            "Stop":self.stop,

            "Target":self.target1,

            "RR":self.risk_reward,

            "Confidence":self.confidence,

            "Reasons":" | ".join(self.reasons),

            "Pending":" | ".join(self.missing_triggers)

        }


# =====================================================
# TRADE STORAGE
# =====================================================

if "trade_candidates" not in st.session_state:

    st.session_state.trade_candidates={}


def normalize_strategy_confidence(raw, *, explicit_zero: bool = False) -> dict[str, Any]:
    """Canonical strategy confidence contract: float 0–100 with explicit missing handling."""
    if raw is None:
        return {
            "strategy_confidence": None,
            "raw_strategy_score": None,
            "normalized_strategy_confidence": None,
            "missing": True,
            "reason": "MISSING_INPUT",
        }
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return {
            "strategy_confidence": None,
            "raw_strategy_score": raw,
            "normalized_strategy_confidence": None,
            "missing": True,
            "reason": "INVALID_INPUT",
        }
    normalized = val * 100.0 if 0.0 < val <= 1.0 else max(0.0, min(100.0, val))
    if normalized == 0.0 and not explicit_zero:
        return {
            "strategy_confidence": 0.0,
            "raw_strategy_score": val,
            "normalized_strategy_confidence": 0.0,
            "missing": False,
            "reason": "EXPLICIT_ZERO" if val == 0 else "NORMALIZED_ZERO",
        }
    return {
        "strategy_confidence": normalized,
        "raw_strategy_score": val,
        "normalized_strategy_confidence": normalized,
        "missing": False,
        "reason": "OK",
    }


def apply_strategy_confidence_contract(candidate) -> None:
    contract = normalize_strategy_confidence(
        getattr(candidate, "confidence", None),
        explicit_zero=getattr(candidate, "confidence_explicit_zero", False),
    )
    candidate.strategy_confidence_contract = contract
    if contract["normalized_strategy_confidence"] is not None:
        candidate.confidence = contract["normalized_strategy_confidence"]
    if not hasattr(candidate, "signal_time") or candidate.signal_time is None:
        candidate.signal_time = getattr(candidate, "timestamp", datetime.now(timezone.utc))


def compute_ai_score_breakdown(candidate, signal_stock, *, fast_threshold: float) -> dict[str, Any]:
    """Expose every AI score component; mark missing inputs explicitly."""
    def _num(value, name: str):
        if value is None:
            return None, f"MISSING:{name}"
        try:
            return float(value), None
        except (TypeError, ValueError):
            return None, f"INVALID:{name}"

    strategy_conf, miss1 = _num(getattr(candidate, "confidence", None), "strategy_confidence")
    count, miss2 = _num(getattr(candidate, "strategy_count", None), "strategy_count")
    rr, miss3 = _num(getattr(candidate, "risk_reward", None), "risk_reward")
    batch1, miss4 = _num(signal_stock.score.get("batch1_bonus"), "batch1_bonus")
    batch2, miss5 = _num(signal_stock.score.get("batch2_bonus"), "batch2_bonus")
    analog_adj, miss6 = _num(getattr(candidate, "analog_score_adjustment", None), "analog_adjustment")
    news_adj, miss7 = _num(getattr(candidate, "news_effect_on_confidence", None), "news_contribution")
    missing = [m for m in (miss1, miss2, miss3, miss4, miss5, miss6, miss7) if m]

    count_bonus = (count or 0) * 5
    rr_bonus = (rr or 0) * 5
    raw_total = sum(x or 0 for x in (strategy_conf, count_bonus, rr_bonus, batch1, batch2))
    final = raw_total + (analog_adj or 0) + (news_adj or 0)
    decision = "APPROVED" if final >= fast_threshold else "REJECTED"
    return {
        "strategy_confidence": strategy_conf,
        "strategy_count_bonus": count_bonus,
        "risk_reward_contribution": rr_bonus,
        "batch1_contribution": batch1,
        "batch2_contribution": batch2,
        "historical_analog_contribution": analog_adj,
        "strategist_contribution": getattr(candidate, "strategist_contribution", None),
        "news_contribution": news_adj,
        "raw_ai_score": raw_total,
        "final_ai_score": final,
        "threshold": fast_threshold,
        "decision": decision,
        "missing_inputs": missing,
        "deep_ai_path": getattr(candidate, "deep_ai_status", "FAST_PATH"),
    }


def record_candidate_terminal_state(candidate, *, stage: str, blocker: str, detail: str = "") -> None:
    candidate.current_stage = stage
    candidate.primary_blocker = blocker
    candidate.detailed_reason = detail or blocker
    candidate.last_evaluated = datetime.now(timezone.utc)
    if not getattr(candidate, "next_evaluation", None):
        candidate.next_evaluation = candidate.last_evaluated + timedelta(seconds=20)


def save_trade_candidate(candidate):

    apply_strategy_confidence_contract(candidate)

    key=f"{candidate.symbol}_{candidate.strategy}"

    st.session_state.trade_candidates[key]=candidate


def get_trade_candidates():

    rows=[]

    for trade in st.session_state.trade_candidates.values():

        rows.append(

            trade.summary()

        )

    return pd.DataFrame(rows)


# =====================================================
# PREVIEW PANEL
# =====================================================

if _P("AI"):

    st.divider()

    st.subheader("Trade Candidate Engine")

    if st.button("Show Trade Candidates", key="developer_show_trade_candidates"):

        df=get_trade_candidates()

        if len(df)==0:

            st.info(

                "No Trade Candidates Yet"

            )

        else:

            st.dataframe(

                df,

                width="stretch"

            )
    # =====================================================
    # BATCH 1 - ADVANCED SIGNALS PANEL
    # =====================================================

    def get_batch1_signals_dataframe():

        rows = []

        for symbol, stock in st.session_state.stock_objects.items():

            if stock.data is None:
                continue

            rows.append({
                "Symbol": symbol,
                "Daily Trend": stock.indicators.get("MTF_DAILY_TREND"),
                "1H Trend": stock.indicators.get("MTF_1H_TREND"),
                "15M Trend": stock.indicators.get("MTF_15M_TREND"),
                "MTF Alignment": stock.score.get("mtf_alignment"),
                "RS vs NIFTY": stock.indicators.get("RS_SCORE"),
                "Sector": stock.indicators.get("SECTOR_NAME"),
                "Sector Score": stock.indicators.get("SECTOR_SCORE"),
                "Volume POC": stock.indicators.get("VOLUME_POC"),
                "Vol. Profile Position": stock.indicators.get("VOLUME_PROFILE_POSITION"),
                "Demand Zones": len(stock.patterns.get("FRESH_DEMAND", [])),
                "Supply Zones": len(stock.patterns.get("FRESH_SUPPLY", [])),
                "Batch1 Bonus": stock.score.get("batch1_bonus"),
                "Reasons": " | ".join(stock.reason[-4:]) if stock.reason else "",
            })

        return pd.DataFrame(rows)


    st.divider()

    st.subheader("Batch 1 - Advanced Signals")

    st.caption(
        "Multi-Timeframe Alignment, Relative Strength vs NIFTY, Sector Ranking, "
        "Volume Profile and Demand/Supply Zones - feeding into AI Consensus."
    )

    if st.button("Show Advanced Signals", key="developer_show_advanced_signals"):

        signals_df = get_batch1_signals_dataframe()

        if len(signals_df) == 0:

            st.info("No Signals Yet - Run a Scan First")

        else:

            st.dataframe(
                signals_df,
                width="stretch"
            )


    def get_batch2_signals_dataframe():

        rows = []

        for symbol, stock in st.session_state.stock_objects.items():

            if stock.data is None:
                continue

            rows.append({
                "Symbol": symbol,
                "AI Confidence": stock.indicators.get("AI_CONFIDENCE"),
                "Batch2 Bonus": stock.indicators.get("BATCH2_CONFIDENCE_BONUS"),
                "False Breakout": stock.patterns.get("FALSE_BREAKOUT", False),
                "Exhaustion": stock.patterns.get("BREAKOUT_EXHAUSTION", False),
                "Smart Money Score": stock.indicators.get("SMART_MONEY_SCORE"),
                "BOS": stock.patterns.get("BOS", False),
                "CHOCH": stock.patterns.get("CHOCH", False),
                "Bullish OB": len(stock.patterns.get("BULLISH_ORDER_BLOCKS", [])),
                "Bullish FVG": len(stock.patterns.get("BULLISH_FVG", [])),
                "Liquidity Sweep": stock.patterns.get("BULLISH_SWEEP", False),
                "OBV Trend": stock.indicators.get("OBV_TREND"),
                "A/D Trend": stock.indicators.get("ADL_TREND"),
                "Volume Z-Score": stock.indicators.get("VOLUME_ZSCORE"),
                "Absorption Day": stock.indicators.get("ABSORPTION_DAY"),
                "Days To Earnings": stock.indicators.get("DAYS_TO_EARNINGS"),
                "Recent News": stock.indicators.get("RECENT_NEWS_COUNT"),
                "News Notes": " | ".join(list(stock.news.values())[-2:]) if stock.news else "",
                "Reasons": " | ".join(stock.reason[-4:]) if stock.reason else "",
            })

        return pd.DataFrame(rows)


    st.divider()

    st.subheader("Batch 2 - False Breakout, Smart Money, Institutional & News/Earnings")

    st.caption(
        "False Breakout Detection, Smart Money Concepts (BOS/CHOCH/Order Blocks/"
        "Liquidity Sweeps/FVG), Institutional Activity (volume-based proxy - "
        "NSE delivery % is not available from this data source), and the News/"
        "Earnings Filter - combined into the AI Confidence Engine feeding AI Consensus."
        )

    if st.button("Show Batch 2 Signals", key="developer_show_batch_2_signals"):

        b2_df = get_batch2_signals_dataframe()

        if len(b2_df) == 0:

            st.info("No Signals Yet - Run a Scan First")

        else:

            st.dataframe(
                b2_df,
                width="stretch"
            )

# =====================================================
# PRICE SQUEEZE ENGINE - PART 1
# VERSION 3.1A
# =====================================================

def calculate_price_squeeze(stock):

    """
    Detects volatility contraction.

    This is only the CORE engine.

    Trade generation happens later.
    """

    df = stock.data

    if df is None:

        return

    if len(df) < 50:

        return

    try:

        # -----------------------------
        # Bollinger Width
        # -----------------------------

        df["BB_WIDTH"] = (

            (df["BB_UPPER"] - df["BB_LOWER"])

            /

            df["BB_MIDDLE"]

        ) * 100

        # -----------------------------
        # Keltner Channel
        # -----------------------------

        ema20 = ta.ema(

            df["Close"],

            length=20

        )

        atr20 = ta.atr(

            df["High"],

            df["Low"],

            df["Close"],

            length=20

        )

        df["KC_UPPER"] = ema20 + (2 * atr20)

        df["KC_LOWER"] = ema20 - (2 * atr20)

        # -----------------------------
        # TTM Squeeze
        # -----------------------------

        df["TTM_SQUEEZE"] = (

            (df["BB_LOWER"] > df["KC_LOWER"])

            &

            (df["BB_UPPER"] < df["KC_UPPER"])

        )

        # -----------------------------
        # Width Percentile
        # -----------------------------

        width = df["BB_WIDTH"]

        current_width = width.iloc[-1]

        percentile = (

            width.rank(pct=True)

            .iloc[-1]

        )

        stock.add_indicator(

            "BB_WIDTH",

            round(current_width,2)

        )

        stock.add_indicator(

            "BB_PERCENTILE",

            round(percentile*100,2)

        )

        # -----------------------------
        # Core Squeeze Score
        # -----------------------------

        squeeze_score = 0

        if percentile <= 0.10:

            squeeze_score += 40

            stock.add_reason(

                "Extremely Tight Bollinger Width"

            )

        elif percentile <= 0.20:

            squeeze_score += 30

            stock.add_reason(

                "Very Tight Bollinger Width"

            )

        elif percentile <= 0.30:

            squeeze_score += 20

        elif percentile <= 0.40:

            squeeze_score += 10

        if df["TTM_SQUEEZE"].iloc[-1]:

            squeeze_score += 40

            stock.add_reason(

                "TTM Squeeze Active"

            )

        stock.patterns["PRICE_SQUEEZE"] = squeeze_score

        stock.add_score(

            "pattern",

            min(

                15,

                squeeze_score/5

            )

        )

        stock.data = df

        return

    except Exception:

        logging.exception(
            f"Price Squeeze Error: {stock.symbol}"
        )

        return

# =====================================================
# PRICE SQUEEZE ENGINE - PART 2
# VERSION 3.1B
# =====================================================

def detect_nr4(df):

    df["NR4"] = False

    ranges = df["High"] - df["Low"]

    for i in range(3, len(df)):

        current = ranges.iloc[i]

        previous = ranges.iloc[i-3:i]

        if current <= previous.min():

            df.loc[df.index[i], "NR4"] = True

    return df


def detect_nr7(df):

    df["NR7"] = False

    ranges = df["High"] - df["Low"]

    for i in range(6, len(df)):

        current = ranges.iloc[i]

        previous = ranges.iloc[i-6:i]

        if current <= previous.min():

            df.loc[df.index[i], "NR7"] = True

    return df


def detect_inside_bar(df):

    df["INSIDE_BAR"] = False

    for i in range(1, len(df)):

        if (

            df["High"].iloc[i] < df["High"].iloc[i-1]

            and

            df["Low"].iloc[i] > df["Low"].iloc[i-1]

        ):

            df.loc[df.index[i], "INSIDE_BAR"] = True

    return df


def detect_atr_contraction(stock):

    df = stock.data

    if df is None:

        return

    atr_avg = df["ATR"].rolling(20).mean()

    latest = df["ATR"].iloc[-1]

    average = atr_avg.iloc[-1]

    if pd.isna(average):

        return

    contraction = latest / average

    stock.add_indicator(

        "ATR_CONTRACTION",

        round(contraction,2)

    )

    if contraction < 0.80:

        stock.patterns["ATR_CONTRACTION"] = True

        stock.add_score("pattern",3)

        stock.add_reason("ATR Contracting")


def detect_volume_dryup(stock):

    df = stock.data

    if df is None:

        return

    latest = df["Volume"].iloc[-1]

    average = df["AVG_VOLUME20"].iloc[-1]

    if pd.isna(average):

        return

    ratio = latest / average

    stock.add_indicator(

        "VOLUME_RATIO",

        round(ratio,2)

    )

    if ratio < 0.70:

        stock.patterns["VOLUME_DRYUP"] = True

        stock.add_score("pattern",4)

        stock.add_reason("Volume Dry-Up")


def update_volatility_patterns(stock):

    df = stock.data

    if df is None:

        return

    df = detect_nr4(df)

    df = detect_nr7(df)

    df = detect_inside_bar(df)

    stock.data = df

    last = df.iloc[-1]

    if last["NR4"]:

        stock.patterns["NR4"] = True

        stock.add_score("pattern",2)

        stock.add_reason("NR4")

    if last["NR7"]:

        stock.patterns["NR7"] = True

        stock.add_score("pattern",3)

        stock.add_reason("NR7")

    if last["INSIDE_BAR"]:

        stock.patterns["INSIDE_BAR"] = True

        stock.add_score("pattern",3)

        stock.add_reason("Inside Bar")

    detect_atr_contraction(stock)

    detect_volume_dryup(stock)
# =====================================================
# PRICE SQUEEZE ENGINE - PART 3
# TRADE GENERATION
# VERSION 3.1C
# =====================================================

MIN_RISK_REWARD = 2.5
MIN_CONFIDENCE = 70


def create_price_squeeze_candidate(stock):

    df = stock.data

    if df is None:
        return None

    if len(df) < 60:
        return None

    last = df.iloc[-1]

    candidate = TradeCandidate(

        stock.symbol,

        "PRICE SQUEEZE"

    )

    confidence = 0

    # ==========================================
    # Trend Filter
    # ==========================================

    if stock.market.get("TREND") == "UPTREND":

        confidence += 20

        candidate.add_reason(

            "Strong Uptrend"

        )

    else:

        candidate.add_trigger(

            "Trend must become UPTREND"

        )

    # ==========================================
    # EMA Alignment
    # ==========================================

    if (

        last["EMA20"] >

        last["EMA50"] >

        last["EMA100"] >

        last["EMA200"]

    ):

        confidence += 20

        candidate.add_reason(

            "EMA Alignment"

        )

    else:

        candidate.add_trigger(

            "EMA Alignment Pending"

        )

    # ==========================================
    # Price Squeeze
    # ==========================================

    squeeze = stock.patterns.get(

        "PRICE_SQUEEZE",

        0

    )

    if squeeze >= 50:

        confidence += 20

        candidate.add_reason(

            "Strong Price Squeeze"

        )

    else:

        candidate.add_trigger(

            "Better Volatility Compression"

        )

    # ==========================================
    # Volume Dry-Up
    # ==========================================

    if stock.patterns.get(

        "VOLUME_DRYUP",

        False

    ):

        confidence += 10

        candidate.add_reason(

            "Volume Dry-Up"

        )

    # ==========================================
    # NR7
    # ==========================================

    if stock.patterns.get(

        "NR7",

        False

    ):

        confidence += 10

        candidate.add_reason(

            "NR7 Pattern"

        )

    # ==========================================
    # Inside Bar
    # ==========================================

    if stock.patterns.get(

        "INSIDE_BAR",

        False

    ):

        confidence += 10

        candidate.add_reason(

            "Inside Bar"

        )

    # ==========================================
    # ATR Contraction
    # ==========================================

    if stock.patterns.get(

        "ATR_CONTRACTION",

        False

    ):

        confidence += 10

        candidate.add_reason(

            "ATR Contracting"

        )

    candidate.confidence = confidence

    # ==========================================
    # Entry Trigger
    # ==========================================

    entry = round(

        last["High"] * 1.001,

        2

    )

    candidate.set_entry(entry)

    candidate.add_trigger(

        f"Close Above {entry}"

    )

    # ==========================================
    # Initial Stop
    # ==========================================

    stop = min(

        last["Low"],

        last["Close"] -

        (2 * last["ATR"])

    )

    candidate.set_stop(stop)

    # ==========================================
    # First Target
    # ==========================================

    risk = entry - stop

    target = entry + (risk * 3)

    candidate.set_target1(target)

    candidate.calculate_rr()

    # ==========================================
    # State
    # ==========================================

    if (

        candidate.risk_reward >= MIN_RISK_REWARD

        and

        confidence >= MIN_CONFIDENCE

    ):

        candidate.state = "READY"

    else:

        candidate.state = "WATCHLIST"

    save_trade_candidate(

        candidate

    )

    return candidate
# =====================================================
# PRICE SQUEEZE ENGINE - PART 4
# BREAKOUT CONFIRMATION ENGINE
# VERSION 3.1D
# =====================================================

MIN_BREAKOUT_RVOL = 1.80

def confirm_price_squeeze_breakout(stock):

    df = stock.data

    if df is None:
        return None

    key = f"{stock.symbol}_PRICE SQUEEZE"

    if key not in st.session_state.trade_candidates:
        return None

    trade = st.session_state.trade_candidates[key]

    last = df.iloc[-1]

    breakout = True

    # ----------------------------------
    # Price Breakout
    # ----------------------------------

    if last["Close"] < trade.entry:

        breakout = False

        trade.add_trigger(

            f"Close Above {trade.entry}"

        )

    else:

        trade.add_reason(

            "Breakout Confirmed"

        )

    # ----------------------------------
    # Relative Volume
    # ----------------------------------

    rvol = last["RVOL"]

    if rvol >= MIN_BREAKOUT_RVOL:

        trade.add_reason(

            f"RVOL {round(rvol,2)}"

        )

        trade.confidence += 10

    else:

        breakout = False

        trade.add_trigger(

            "RVOL >= 1.80"

        )

    # ----------------------------------
    # MACD
    # ----------------------------------

    if last["MACD"] > last["MACD_SIGNAL"]:

        trade.add_reason(

            "MACD Bullish"

        )

        trade.confidence += 5

    else:

        trade.add_trigger(

            "MACD Bullish Cross"

        )

    # ----------------------------------
    # RSI
    # ----------------------------------

    if 55 <= last["RSI"] <= 70:

        trade.add_reason(

            "Healthy RSI"

        )

        trade.confidence += 5

    else:

        trade.add_trigger(

            "RSI 55-70"

        )

    # ----------------------------------
    # Final Decision
    # ----------------------------------

    trade.calculate_rr()

    if (

        breakout

        and

        trade.risk_reward >= MIN_RISK_REWARD

        and

        trade.confidence >= MIN_CONFIDENCE

    ):

        trade.state = "BUY"

    elif trade.confidence >= 60:

        trade.state = "READY"

    else:

        trade.state = "WATCHLIST"

    save_trade_candidate(trade)

    return trade


# =====================================================
# RUN COMPLETE PRICE SQUEEZE STRATEGY
# =====================================================

def run_price_squeeze_strategy(stock):

    calculate_price_squeeze(stock)

    update_volatility_patterns(stock)

    create_price_squeeze_candidate(stock)

    confirm_price_squeeze_breakout(stock)


# =====================================================
# COMPLETE SCAN ENGINE
# =====================================================

def legacy_run_complete_scan():

    st.session_state.trade_candidates = {}

    total = len(st.session_state.market_data)

    progress = st.progress(0)

    status = st.empty()

    completed = 0

    for symbol, df in st.session_state.market_data.items():

        completed += 1

        progress.progress(completed / total)

        status.write(f"Scanning {symbol}")

        df = calculate_indicators(df)

        if df is None:
            continue

        stock = get_stock(symbol)

        stock.set_dataframe(df)

        calculate_trade_quality(stock)

        update_market_structure(stock)

        run_price_squeeze_strategy(stock)

    status.success("Scan Complete")


# =====================================================
# SCAN BUTTON
# =====================================================



def _pipeline_event(event):
    st.session_state.pipeline_events.append({
        "Time": event.timestamp.strftime("%H:%M:%S"),
        "Stage": event.step,
        "Status": event.status,
        "Message": event.message,
    })
    st.session_state.brain_status[event.step] = event.status
    diagnostics = PipelineDiagnostics()
    status = str(event.status).upper()
    if status in ("STARTED", "RUNNING"):
        diagnostics.register_phase(event.step, event.message)
    elif status in ("COMPLETED", "SUCCESS", "OK"):
        diagnostics.complete_phase(event.step, event.message or "Completed")
    elif status in ("FAILED", "ERROR"):
        diagnostics.fail_phase(event.step, event.message or "Failed")
    else:
        diagnostics.update_progress(event.step, 50.0, event.message)


def build_default_scan_universe_for_pipeline():
    """Resolve persisted pre-run choices before any historical download."""
    prefs = WORKSPACE.preferences
    filters = dict(st.session_state.get("active_prerun_filters") or prefs.get("universe_filters", {}))
    source = st.session_state.get("active_universe_source", prefs.get("universe_source", "NIFTY 50"))
    mode = st.session_state.get("active_operating_mode", prefs.get("operating_mode", "Fast Scan"))
    if mode == "Custom Universe" or source == "Custom symbol list":
        symbols = _normalise_custom_symbols(filters.get("custom_symbols", ""))
    elif mode == "Watchlist Only" or source == "User watchlist" or filters.get("watchlist_only"):
        symbols = _clean_and_suffix_symbols(st.session_state.get("watchlist", []))
    else:
        symbols = build_scan_universe(
            _source_to_engine(source), cap_filter=filters.get("market_cap", "Any"),
            price_range=(float(filters.get("minimum_price", 0)), float(filters.get("maximum_price", 100000))),
            min_volume=int(filters.get("minimum_volume", 0)),
            min_turnover=int(filters.get("minimum_turnover", 0)),
            sectors=filters.get("sector", []), styles=filters.get("enabled_strategies", []),
        )
    if mode == "Fast Scan":
        priority = _clean_and_suffix_symbols(st.session_state.get("watchlist", []))
        symbols = list(dict.fromkeys(priority + symbols))[:200]
    st.session_state.scan_universe = symbols
    st.session_state.scan_manager_active_styles = filters.get("enabled_strategies", [])
    st.session_state["pipeline_state"] = "PREPARING"
    return f"{len(symbols)} symbols ({mode})"



def get_trading_workflow() -> str:
    mode = st.session_state.get("execution_mode", "PAPER")
    interval = CONFIG.get("DOWNLOAD_INTERVAL", "1d")
    if mode == "PAPER":
        return "PAPER_TRADING"
    return "INTRADAY" if interval not in ("1d", "1wk", "1mo") else "POSITIONAL"


def execute_arbitrage_pipeline():
    diagnostics = PipelineDiagnostics()
    diagnostics.register_phase("Arbitrage Pipeline", "Arbitrage execution foundation initialized")
    st.session_state["arbitrage_pipeline"] = {
        "status": "READY",
        "last_checked": datetime.now(),
        "message": "Arbitrage strategy logic not implemented yet",
    }
    diagnostics.complete_phase("Arbitrage Pipeline", "Arbitrage foundation ready")
    return st.session_state["arbitrage_pipeline"]


def run_alphaquant(trigger="MANUAL"):
    """One professional workflow entry point for the RUN ALPHAQUANT button."""
    PipelineManager, PipelineStep = (lambda m: (m.PipelineManager, m.PipelineStep))(__import__("os_brains.pipeline_manager", fromlist=["PipelineManager", "PipelineStep"]))

    diagnostics = PipelineDiagnostics()
    errors = CentralErrorManager()
    health = SystemHealthEngine()
    if "PaperBroker" in globals():
        PaperBroker().connect()
    health.mark("last_broker_sync")
    st.session_state["trading_workflow"] = get_trading_workflow()
    st.session_state.pipeline_started_at = datetime.now()
    st.session_state.pipeline_phases = {}
    st.session_state.pipeline_events = []
    st.session_state.brain_status = {}
    st.session_state.decision_funnel = []
    st.session_state.no_trade_explanation = []

    manager = PipelineManager(on_event=_pipeline_event)

    def download_stage():
        if not st.session_state.scan_universe:
            return False
        data_manager = MarketDataManager()
        manager_data = data_manager.get_history(
            st.session_state.scan_universe,
            CONFIG.get("DOWNLOAD_INTERVAL", "1d"),
            CONFIG.get("DOWNLOAD_PERIOD", "1y"),
        )
        manager_data = data_manager.update_history(st.session_state.scan_universe, CONFIG.get("DOWNLOAD_INTERVAL", "1d")) or manager_data
        st.session_state.market_data = manager_data
        return f"{len(st.session_state.market_data)} datasets" if st.session_state.market_data else False

    try:
        ok = manager.run([
            PipelineStep("Build Universe", build_default_scan_universe_for_pipeline, "Building selected universe"),
            PipelineStep("Download Data", download_stage, "Downloading OHLCV data"),
        ])
    except Exception as exc:
        errors.record_error("Run AlphaQuant", "run_alphaquant", exc)
        diagnostics.fail_phase("Run AlphaQuant", str(exc))
        return False, "AlphaQuant stopped after an unexpected error. See Mission Control errors."

    if not ok:
        return False, "AlphaQuant stopped before scan execution. See Mission Control logs."

    st.session_state.run_complete_scan_requested = True
    st.session_state.last_cycle_time = datetime.now()
    st.session_state.last_cycle_trigger = trigger
    health.mark("last_scan")
    PipelineMonitor().update()
    return True, f"RUN ALPHAQUANT queued: {len(st.session_state.market_data)} symbols."



# =====================================================
# TRADE VALIDATOR ENGINE
# =====================================================
# TRADE VALIDATOR ENGINE
# VERSION 3.2A
# =====================================================

MIN_TRADE_QUALITY = 70
MIN_RR = 2.5

def validate_trade_candidate(stock, trade):

    if trade is None:
        return None

    valid = True

    # Strategy engines add pattern/structure points after the initial quality
    # pass in scan_stage — refresh the aggregate before gating.
    if hasattr(stock, "calculate_trade_quality"):
        stock.calculate_trade_quality()

    # -------------------------------
    # Trade Quality
    # -------------------------------

    tqi = stock.score.get("quality", 0)

    if tqi < MIN_TRADE_QUALITY:

        valid = False

        trade.add_trigger(
            f"TQI >= {MIN_TRADE_QUALITY}"
        )

    # -------------------------------
    # Risk Reward
    # -------------------------------

    trade.calculate_rr()

    if trade.risk_reward < MIN_RR:

        valid = False

        trade.add_trigger(
            f"RR >= {MIN_RR}"
        )

    # -------------------------------
    # Trend Filter
    # -------------------------------

    if stock.market.get("TREND") != "UPTREND":

        valid = False

        trade.add_trigger(
            "Primary Trend Up"
        )

    # -------------------------------
    # Price Filter
    # -------------------------------

    last = stock.data.iloc[-1]

    if last["Close"] < CONFIG["MIN_PRICE"]:

        valid = False

        trade.add_trigger(
            "Price Filter"
        )

    # -------------------------------
    # Volume Filter
    # -------------------------------

    if last["AVG_VOLUME20"] < CONFIG["MIN_AVG_VOLUME"]:

        valid = False

        trade.add_trigger(
            "Average Volume Filter"
        )

    # -------------------------------
    # Long Only
    # -------------------------------

    trade.direction = "LONG"

    if valid:

        trade.state = "READY"

    else:

        trade.state = "WATCHLIST"

    return trade
# =====================================================
# POSITION SIZING ENGINE
# VERSION 3.2B
# =====================================================

DEFAULT_CAPITAL = float(WORKSPACE.preferences["paper_trading_capital"])
MAX_CAPITAL_PER_TRADE = 0.10

if "paper_capital" not in st.session_state:
    st.session_state.paper_capital = DEFAULT_CAPITAL


def calculate_position_size(trade):

    if trade is None:
        return None

    if trade.entry is None:
        return trade

    if trade.stop is None:
        return trade

    risk = trade.entry - trade.stop

    if risk <= 0:
        return trade

    capital = st.session_state.paper_capital

    risk_amount = capital * (CONFIG["RISK_PER_TRADE"] / 100)

    quantity = int(risk_amount / risk)

    max_quantity = int(
        (capital * MAX_CAPITAL_PER_TRADE) / trade.entry
    )

    quantity = min(quantity, max_quantity)

    multiplier = float(getattr(trade, "position_size_multiplier", 1.0) or 1.0)
    rv = getattr(trade, "risk_verdict", {}) or {}
    multiplier = min(multiplier, float(rv.get("position_size_multiplier", multiplier) or multiplier))
    if multiplier < 1.0 and quantity > 0:
        quantity = max(1, int(quantity * multiplier))

    quantity = max(quantity, 0)

    trade.position_size = quantity

    trade.capital_required = round(
        quantity * trade.entry,
        2
    )

    trade.maximum_loss = round(
        quantity * risk,
        2
    )

    if trade.target1 is not None:

        trade.maximum_profit = round(
            quantity *
            (trade.target1 - trade.entry),
            2
        )

    else:

        trade.maximum_profit = 0

    return trade


def update_trade_position_sizes():

    for trade in st.session_state.trade_candidates.values():

        calculate_position_size(trade)
# =====================================================
# PAPER TRADING ENGINE
# VERSION 3.2C
# =====================================================

if "paper_positions" not in st.session_state:
    st.session_state.paper_positions = {}

if "paper_history" not in st.session_state:
    st.session_state.paper_history = []


def open_paper_trade(trade):

    if trade.position_size <= 0:
        return

    if trade.state != "BUY":
        return

    if trade.symbol in st.session_state.paper_positions:
        return

    position = PaperPosition(
        symbol=trade.symbol,
        strategy=trade.strategy,
        qty=trade.position_size,
        entry=trade.entry or 0,
        stop=trade.stop or 0,
        target1=trade.target1 or 0,
        target2=trade.target2 or 0,
        target3=trade.target3 or 0,
        confidence=trade.confidence,
        ai_score=getattr(trade, "ai_score", 0),
        decision_id=getattr(trade, "decision_id", None)
    )

    if hasattr(position, "initialise"):

        position.initialise()

    st.session_state.paper_positions[trade.symbol] = position

    if position.decision_id is not None:
        try:
            experience_memory = __import__("os_brains", fromlist=["experience_memory"]).experience_memory
            experience_memory.mark_open(position.decision_id)
        except Exception as e:
            logging.warning(f"OPEN_PAPER_TRADE experience_memory.mark_open failed symbol={trade.symbol}: {e}")

    logging.info(

        f"Paper BUY : {trade.symbol}"

    )


# =====================================================
# PAPER PORTFOLIO DASHBOARD
# VERSION 3.2D
# =====================================================

def paper_portfolio_summary():

    open_positions = len(st.session_state.paper_positions)
    closed_positions = len(st.session_state.paper_history)

    total_pnl = 0

    winners = 0
    losers = 0

    for trade in st.session_state.paper_history:

        total_pnl += trade.pnl

        if trade.pnl >= 0:
            winners += 1
        else:
            losers += 1

    if closed_positions > 0:
        win_rate = round(
            winners * 100 / closed_positions,
            2
        )
    else:
        win_rate = 0

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Open Positions",
        open_positions
    )

    c2.metric(
        "Closed Trades",
        closed_positions
    )

    c3.metric(
        "Net P&L",
        f"₹ {round(total_pnl,2)}"
    )

    c4.metric(
        "Win Rate",
        f"{win_rate}%"
    )


def show_open_positions():

    rows = []

    for position in st.session_state.paper_positions.values():

        rows.append({

            "Symbol": position.symbol,

            "Strategy": position.strategy,

            "Entry": position.entry,

            "Stop": position.stop,

            "Target": position.target1,

            "Qty": position.quantity,

            "Status": position.status

        })

    if len(rows):

        st.dataframe(

            pd.DataFrame(rows),

            width="stretch"

        )

    else:

        st.info("No Open Paper Positions")


def show_trade_history():

    rows = []

    for trade in st.session_state.paper_history:

        rows.append({

            "Symbol": trade.symbol,

            "Strategy": trade.strategy,

            "Entry": trade.entry,

            "Exit": trade.exit_price,

            "Qty": trade.quantity,

            "PnL": trade.pnl,

            "Reason": trade.exit_reason

        })

    if len(rows):

        st.dataframe(

            pd.DataFrame(rows),

            width="stretch"

        )

    else:

        st.info("No Closed Trades")


if _P("Paper Trading"):
    st.divider()

    st.subheader("Paper Trading Dashboard")

    paper_portfolio_summary()

    with st.expander("Open Positions", expanded=True):

        show_open_positions()

    with st.expander("Trade History"):

        show_trade_history()
# =====================================================
# STRATEGY REGISTRY
# VERSION 3.3A
# =====================================================

if "strategy_registry" not in st.session_state:

    st.session_state.strategy_registry = []


class Strategy:

    def __init__(

        self,

        name,

        function,

        enabled=True,

        priority=100

    ):

        self.name = name

        self.function = function

        self.enabled = enabled

        self.priority = priority


def register_strategy(

    name,

    function,

    priority=100,

    enabled=True

):

    for strategy in st.session_state.strategy_registry:

        if strategy.name == name:

            return

    st.session_state.strategy_registry.append(

        Strategy(

            name,

            function,

            enabled,

            priority

        )

    )

    st.session_state.strategy_registry.sort(

        key=lambda x: x.priority

    )


def run_all_strategies(stock):

    for strategy in st.session_state.strategy_registry:

        if strategy.enabled:
            try:
                from discovery.strategy_gates import should_run_strategy
                if not should_run_strategy(strategy.name, stock):
                    continue
                strategy.function(stock)

            except Exception:

                logging.exception(

                    f"{strategy.name} : {stock.symbol}"

                )


def show_registered_strategies():

    rows = []

    for strategy in st.session_state.strategy_registry:

        rows.append({

            "Priority": strategy.priority,

            "Strategy": strategy.name,

            "Enabled": strategy.enabled

        })

    if len(rows):

        st.dataframe(

            pd.DataFrame(rows),

            width="stretch"

        )


# ==========================================
# Strategy Dashboard
# ==========================================

if _P("Developer Mode"):
    st.divider()

    st.subheader("Strategy Registry")

    show_registered_strategies()
# =====================================================
# DEMAND & SUPPLY ENGINE
# PART 1
# VERSION 3.3B
# =====================================================

BASE_LOOKBACK = 20
IMPULSE_MULTIPLIER = 2.0


def candle_body(candle):

    return abs(

        candle["Close"]

        - candle["Open"]

    )


def detect_impulse_candle(df, idx):

    candle = df.iloc[idx]

    avg_body = (

        abs(

            df["Close"]

            - df["Open"]

        )

        .rolling(20)

        .mean()

        .iloc[idx]

    )

    if pd.isna(avg_body):

        return False

    return candle_body(candle) >= (

        avg_body *

        IMPULSE_MULTIPLIER

    )


def detect_base(df, idx):

    if idx < 2:

        return False

    c1 = df.iloc[idx]

    c2 = df.iloc[idx-1]

    c3 = df.iloc[idx-2]

    avg_range = (

        (df["High"]-df["Low"])

        .rolling(20)

        .mean()

        .iloc[idx]

    )

    if pd.isna(avg_range):

        return False

    r1 = c1["High"]-c1["Low"]

    r2 = c2["High"]-c2["Low"]

    r3 = c3["High"]-c3["Low"]

    return (

        r1 < avg_range

        and

        r2 < avg_range

        and

        r3 < avg_range

    )


def detect_demand_supply(stock):

    df = stock.data

    if df is None:

        return

    demand = []

    supply = []

    for i in range(

        25,

        len(df)-3

    ):

        if detect_base(df, i):

            before = df.iloc[i-1]

            after = df.iloc[i+1]

            if detect_impulse_candle(df, i+1):

                if after["Close"] > before["Close"]:

                    demand.append({

                        "Low": round(

                            df.iloc[i]["Low"],

                            2

                        ),

                        "High": round(

                            df.iloc[i]["High"],

                            2

                        ),

                        "Index": i

                    })

                else:

                    supply.append({

                        "Low": round(

                            df.iloc[i]["Low"],

                            2

                        ),

                        "High": round(

                            df.iloc[i]["High"],

                            2

                        ),

                        "Index": i

                    })

    stock.patterns["DEMAND_ZONES"] = demand

    stock.patterns["SUPPLY_ZONES"] = supply

    stock.add_indicator(

        "DEMAND_COUNT",

        len(demand)

    )

    stock.add_indicator(

        "SUPPLY_COUNT",

        len(supply)

    )
# =====================================================
# DEMAND & SUPPLY ENGINE
# PART 2 - FRESH ZONES
# VERSION 3.3C
# =====================================================

def classify_fresh_zones(stock):

    df = stock.data

    if df is None:
        return

    demand = stock.patterns.get("DEMAND_ZONES", [])
    supply = stock.patterns.get("SUPPLY_ZONES", [])

    fresh_demand = []
    tested_demand = []

    for zone in demand:

        tested = False

        for j in range(zone["Index"] + 1, len(df)):

            low = df["Low"].iloc[j]

            if low <= zone["High"]:

                tested = True
                break

        if tested:

            tested_demand.append(zone)

        else:

            zone["Fresh"] = True
            fresh_demand.append(zone)

    fresh_supply = []
    tested_supply = []

    for zone in supply:

        tested = False

        for j in range(zone["Index"] + 1, len(df)):

            high = df["High"].iloc[j]

            if high >= zone["Low"]:

                tested = True
                break

        if tested:

            tested_supply.append(zone)

        else:

            zone["Fresh"] = True
            fresh_supply.append(zone)

    stock.patterns["FRESH_DEMAND"] = fresh_demand
    stock.patterns["TESTED_DEMAND"] = tested_demand

    stock.patterns["FRESH_SUPPLY"] = fresh_supply
    stock.patterns["TESTED_SUPPLY"] = tested_supply

    stock.add_indicator(
        "FRESH_DEMAND",
        len(fresh_demand)
    )

    stock.add_indicator(
        "FRESH_SUPPLY",
        len(fresh_supply)
    )

    if len(fresh_demand):

        stock.add_score(
            "pattern",
            min(
                len(fresh_demand) * 2,
                10
            )
        )

        stock.add_reason(
            f"{len(fresh_demand)} Fresh Demand Zones"
        )
# =====================================================
# DEMAND & SUPPLY ENGINE
# PART 3 - ZONE STRENGTH
# VERSION 3.3D
# =====================================================

def calculate_zone_strength(stock):

    demand = stock.patterns.get("FRESH_DEMAND", [])
    supply = stock.patterns.get("FRESH_SUPPLY", [])

    for zone in demand:

        score = 50

        width = zone["High"] - zone["Low"]

        if width > 0:
            score += 10

        score += 15

        zone["Strength"] = min(score, 100)

    for zone in supply:

        score = 50

        width = zone["High"] - zone["Low"]

        if width > 0:
            score += 10

        score += 15

        zone["Strength"] = min(score, 100)

    stock.patterns["FRESH_DEMAND"] = demand
    stock.patterns["FRESH_SUPPLY"] = supply


def get_best_demand_zone(stock):

    zones = stock.patterns.get("FRESH_DEMAND", [])

    if len(zones) == 0:
        return None

    return sorted(

        zones,

        key=lambda x: x["Strength"],

        reverse=True

    )[0]


def get_best_supply_zone(stock):

    zones = stock.patterns.get("FRESH_SUPPLY", [])

    if len(zones) == 0:
        return None

    return sorted(

        zones,

        key=lambda x: x["Strength"],

        reverse=True

    )[0]
# =====================================================
# DEMAND & SUPPLY ENGINE
# PART 4 - TRADE GENERATION
# VERSION 3.3E
# =====================================================

MIN_DS_CONFIDENCE = 70


def create_demand_supply_candidate(stock):

    calculate_zone_strength(stock)

    zone = get_best_demand_zone(stock)

    if zone is None:
        return

    df = stock.data

    last = df.iloc[-1]

    trade = TradeCandidate(

        stock.symbol,

        "DEMAND_SUPPLY"

    )

    entry = round(zone["High"], 2)

    stop = round(zone["Low"] - last["ATR"], 2)

    risk = entry - stop

    target = round(entry + (risk * 3), 2)

    trade.set_entry(entry)
    trade.set_stop(stop)
    trade.set_target1(target)

    confidence = 0

    if stock.market.get("TREND") == "UPTREND":

        confidence += 20

        trade.add_reason("Primary Uptrend")

    if stock.score["quality"] >= 70:

        confidence += 20

        trade.add_reason("High Trade Quality")

    if zone["Strength"] >= 70:

        confidence += 20

        trade.add_reason("Strong Institutional Demand")

    if len(stock.patterns.get("FRESH_DEMAND", [])):

        confidence += 20

        trade.add_reason("Fresh Demand Zone")

    if last["RVOL"] >= 1.2:

        confidence += 10

        trade.add_reason("Volume Confirmation")

    if last["MACD"] > last["MACD_SIGNAL"]:

        confidence += 10

        trade.add_reason("Bullish MACD")

    trade.confidence = confidence

    trade.calculate_rr()

    if (

        confidence >= MIN_DS_CONFIDENCE

        and

        trade.risk_reward >= 2.5

    ):

        trade.state = "READY"

    else:

        trade.state = "WATCHLIST"

    save_trade_candidate(trade)

    return trade


def run_demand_supply_strategy(stock):

    detect_demand_supply(stock)

    classify_fresh_zones(stock)

    calculate_zone_strength(stock)

    trade = create_demand_supply_candidate(stock)

    logging.info(
        f"DEMAND_SUPPLY symbol={stock.symbol} "
        f"fresh_demand={len(stock.patterns.get('FRESH_DEMAND', []))} "
        f"fresh_supply={len(stock.patterns.get('FRESH_SUPPLY', []))} "
        f"confidence={getattr(trade, 'confidence', None)} "
        f"state={getattr(trade, 'state', None)}"
    )
# =====================================================
# AI CONSENSUS ENGINE
# VERSION 3.4A
# =====================================================

if "final_trade_list" not in st.session_state:
    st.session_state.final_trade_list = []


def get_scoring_engine_version() -> str:
    """V2 for both PAPER and LIVE CASH GUARDED. V1 preserved for explicit rollback only."""
    # V2 is the only normal production path. A stale saved V1 preference must
    # not silently split the Streamlit and headless pipelines again.
    return "V2"


def build_ai_consensus():
    """
    AI Decision Engine orchestrator: Brain 4 (Strategist) produces one
    candidate per symbol enriched with historical evidence, Brain 5 (Risk
    Manager) reviews every candidate and can veto it. Vetoed candidates
    stay in final_trade_list (with their RiskVerdict attached) instead of
    being dropped - "no trade" is a first-class, visible outcome, not a
    silent absence. Brain 6 (Portfolio Manager) runs later, in
    allocate_portfolio(), once every candidate has a verdict.
    """
    from os_brains.strategist import enrich_candidate
    from os_brains.risk_manager import evaluate as risk_evaluate, build_portfolio_state
    experience_memory = __import__("os_brains", fromlist=["experience_memory"]).experience_memory

    app_module = sys.modules[__name__]

    grouped = {}

    for trade in st.session_state.trade_candidates.values():

        symbol = trade.symbol

        if symbol not in grouped:

            grouped[symbol] = []

        grouped[symbol].append(trade)

    portfolio_state = build_portfolio_state(app_module)

    final_list = []

    for symbol, trades in grouped.items():

        # ---- Brain 4 (Strategist): pick the best raw candidate for this
        # symbol from everything the strategy registry + Batch 1/2 signal
        # engines produced, then enrich it with historical evidence.
        best = max(

            trades,

            key=lambda x: (
                x.confidence,
                x.risk_reward
            )

        )

        best.strategy_count = len(trades)
        all_strategy_names = [getattr(t, "strategy", "") for t in trades]

        signal_stock = get_stock(symbol)
        scoring_ver = get_scoring_engine_version()
        best.score_version = scoring_ver
        fast_threshold = float(WORKSPACE.preferences.get("minimum_fast_ai_score", 70))

        if scoring_ver == "V2":
            from scoring_engine_v2 import compute_trade_score_v2, apply_volatility_risk_hint

            best.regime_context, best.analog_report, best.evidence_summary, best.expected_value = None, None, [], 0.0
            require_deep = bool(WORKSPACE.preferences.get("require_deep_ai_before_entry", False))
            if require_deep:
                try:
                    enrich_candidate(signal_stock, best, app_module)
                    best.deep_ai_status = "ENRICHED"
                except Exception as e:
                    logging.warning(f"AI_CONSENSUS V2 enrichment failed symbol={symbol}: {type(e).__name__}")
                    best.deep_ai_status = "DEEP_AI_UNAVAILABLE" if "DATABASE_URL" in str(e) else "ERROR"
            else:
                best.deep_ai_status = "V2_FAST_PATH"

            news_effect = get_news_manager().candidate_effect(symbol)
            for news_field, news_value in news_effect.items():
                setattr(best, news_field, news_value)

            v2 = compute_trade_score_v2(
                signal_stock,
                best,
                all_strategies=all_strategy_names,
                analog_report=getattr(best, "analog_report", None),
                news_payload=news_effect,
                gate_threshold=fast_threshold,
                scoring_profile=ScoringProfileStore(Path(_APP_DIR) / "data" / "scoring_profiles.json").active(),
            )
            best.trade_score_v2 = v2
            best.scoring_profile = v2.scoring_profile
            best.scoring_version = v2.scoring_version
            best.scoring_weights_snapshot = dict(v2.scoring_weights_snapshot)
            best.ai_score = v2.trade_confidence
            best.ai_score_breakdown = v2.to_gate_breakdown()
            best.ai_confidence = v2.trade_confidence
            best.positive_reasons_v2 = v2.positive_reasons
            best.watch_items_v2 = v2.watch_items
            best.fast_ai_status = v2.gate_decision
            best.fast_ai_reason = (
                "V2 trade confidence passed" if v2.gate_decision == "APPROVED"
                else "V2 trade confidence below threshold"
            )
            best.add_reason(
                f"[V2] Trade Confidence={v2.trade_confidence} "
                f"(Structure {v2.structure.weighted_contribution:.0f}/{v2.structure.weight:.0f}, "
                f"Participation {v2.participation.weighted_contribution:.0f}/{v2.participation.weight:.0f}, "
                f"Momentum {v2.momentum.weighted_contribution:.0f}/{v2.momentum.weight:.0f})"
            )
        else:
            batch1_bonus = signal_stock.score.get("batch1_bonus", 0)
            batch2_bonus = signal_stock.score.get("batch2_bonus", 0)

            breakdown = compute_ai_score_breakdown(
                best, signal_stock, fast_threshold=fast_threshold
            )
            best.ai_score_breakdown = breakdown
            best.ai_score = breakdown["final_ai_score"]
            best.ai_confidence = signal_stock.score.get("ai_confidence", 50)

            best.add_reason(
                f"Batch1 signals bonus={batch1_bonus} "
                f"(MTF={signal_stock.score.get('mtf_alignment')}, "
                f"RS={signal_stock.score.get('relative_strength')}, "
                f"Sector={signal_stock.score.get('sector')}, "
                f"VolProfile={signal_stock.score.get('volume_profile')})"
            )

            best.add_reason(
                f"Batch2 signals bonus={batch2_bonus} ai_confidence={best.ai_confidence} "
                f"(SmartMoney={signal_stock.score.get('smart_money')}, "
                f"Institutional={signal_stock.score.get('institutional')}, "
                f"FalseBreakoutPenalty={signal_stock.score.get('false_breakout_penalty')}, "
                f"NewsEarningsPenalty={signal_stock.score.get('news_earnings')})"
            )

            best.fast_ai_status = "APPROVED" if best.ai_score >= fast_threshold else "REJECTED"
            best.fast_ai_reason = (
                "Fast local score passed" if best.fast_ai_status == "APPROVED"
                else "AI confidence below threshold"
            )
            best.regime_context, best.analog_report, best.evidence_summary, best.expected_value = None, None, [], 0.0
            require_deep = bool(WORKSPACE.preferences.get("require_deep_ai_before_entry", False))
            if best.fast_ai_status == "APPROVED" and require_deep:
                try:
                    enrich_candidate(signal_stock, best, app_module)
                    best.deep_ai_status = "ENRICHED"
                except Exception as e:
                    logging.warning(f"AI_CONSENSUS strategist enrichment failed symbol={symbol}: {type(e).__name__}")
                    best.deep_ai_status = "DEEP_AI_UNAVAILABLE" if "DATABASE_URL" in str(e) else "ERROR"
            elif best.fast_ai_status == "APPROVED":
                best.deep_ai_status = "FAST_PATH"
            if hasattr(best, "ai_score_breakdown"):
                best.ai_score_breakdown["deep_ai_path"] = getattr(best, "deep_ai_status", "FAST_PATH")
                if getattr(best, "analog_score_adjustment", None) is not None:
                    best.ai_score_breakdown["historical_analog_contribution"] = best.analog_score_adjustment
                    best.ai_score = round(best.ai_score_breakdown["raw_ai_score"] + best.analog_score_adjustment, 2)
                    best.ai_score_breakdown["final_ai_score"] = best.ai_score
                    best.ai_score_breakdown["decision"] = (
                        "APPROVED" if best.ai_score >= best.ai_score_breakdown["threshold"] else "REJECTED"
                    )
                    best.fast_ai_status = best.ai_score_breakdown["decision"]

            news_effect = get_news_manager().candidate_effect(symbol)
            for news_field, news_value in news_effect.items():
                setattr(best, news_field, news_value)
            if news_effect["news_effect_on_confidence"]:
                best.add_reason(f"[News] {news_effect['news_status']}: relevance "
                    f"{news_effect['news_relevance']}, risk {news_effect['news_risk']}, "
                    f"confidence effect {news_effect['news_effect_on_confidence']:+d}")

        if best.fast_ai_status == "REJECTED":
            best.risk_verdict = {"candidate_symbol":symbol, "verdict":"NOT_EVALUATED",
                "vetoed_by":["FAST_AI_GATE"], "reason":best.fast_ai_reason}
            best.state = "AI_REJECTED"
            best.add_reason(f"[FastAIGate] REJECTED - {best.fast_ai_reason}")
            record_candidate_terminal_state(best, stage="AI_REJECTED", blocker="AI_REJECTED", detail=best.fast_ai_reason)
            final_list.append(best)
            continue

        # Brain 5 runs only after the local gate; rejected candidates remain in
        # final_list with an explicit NOT_EVALUATED risk state.
        try:
            risk_verdict = risk_evaluate(
                best, signal_stock, best.regime_context, portfolio_state, app_module
            )
        except Exception as e:
            logging.warning(f"AI_CONSENSUS risk evaluation failed symbol={symbol}: {e}")
            risk_verdict = {
                "candidate_symbol": symbol, "verdict": "VETOED",
                "vetoed_by": ["RISK_MANAGER_ERROR"], "reason": str(e),
            }

        best.risk_verdict = risk_verdict

        if scoring_ver == "V2":
            from scoring_engine_v2 import apply_volatility_risk_hint
            risk_verdict = apply_volatility_risk_hint(signal_stock, risk_verdict)
            best.risk_verdict = risk_verdict

        if news_effect.get("news_veto_reason"):
            best.risk_verdict = {**risk_verdict, "verdict":"VETOED",
                "reason":news_effect["news_veto_reason"], "news_veto":True}
            risk_verdict = best.risk_verdict

        if risk_verdict["verdict"] in ("VETOED",):
            best.state = "VETOED"
            best.add_reason(f"[RiskManager] VETOED - {risk_verdict['reason']}")
            record_candidate_terminal_state(best, stage="RISK_VETO", blocker="RISK_VETO", detail=risk_verdict.get("reason", ""))
        elif risk_verdict.get("verdict") == "APPROVED_REDUCED_SIZE":
            best.state = "READY"
            best.add_reason(f"[RiskManager] APPROVED (reduced size) - {risk_verdict['reason']}")
            record_candidate_terminal_state(best, stage="RISK_APPROVED", blocker="", detail=risk_verdict.get("reason", ""))
        else:
            best.add_reason(f"[RiskManager] APPROVED - {risk_verdict['reason']}")
            record_candidate_terminal_state(best, stage="RISK_APPROVED", blocker="", detail=risk_verdict.get("reason", ""))

        # ---- Brain 7 (Reviewer) support: record this decision in Experience
        # Memory now, vetoed or not, so a later close/review can be linked
        # back to exactly the evidence Brain 4/5 acted on. Never blocks the
        # pipeline - decision_id stays None on any DB failure.
        try:
            best.decision_id = experience_memory.record_decision(
                best, best.regime_context, best.analog_report, risk_verdict
            )
        except Exception as e:
            logging.warning(f"AI_CONSENSUS experience_memory.record_decision failed symbol={symbol}: {e}")
            best.decision_id = None

        final_list.append(best)

    # Approved candidates first (ranked by ai_score), then vetoed ones -
    # vetoes stay visible at the bottom rather than being filtered out.
    final_list.sort(

        key=lambda x: (_is_risk_approved(getattr(x, "risk_verdict", {})), x.ai_score),

        reverse=True

    )

    st.session_state.final_trade_list = final_list

    # Persist the complete current-run population, including strategy ideas
    # superseded during per-symbol ranking.  They remain auditable rather than
    # disappearing merely because only the strongest idea advances.
    archived = st.session_state.setdefault("candidate_archive", [])
    advanced_ids = {id(t) for t in final_list}
    for trade in st.session_state.trade_candidates.values():
        verdict = getattr(trade, "risk_verdict", {}) or {}
        archived.append({"Observed At":datetime.now(timezone.utc).isoformat(), "Symbol":trade.symbol,
            "Side":getattr(trade,"side","BUY"), "Strategy":getattr(trade,"strategy",""),
            "Stage":getattr(trade,"state","STRATEGY_CANDIDATE") if id(trade) in advanced_ids else "SUPERSEDED_BY_STRONGER_SIGNAL",
            "Status":verdict.get("verdict","NOT_EVALUATED"),
            "Reason":verdict.get("reason","A stronger strategy candidate for this symbol advanced."),
            "Strategy Score":getattr(trade,"confidence",0), "AI Score":getattr(trade,"ai_score",None),
            "Entry":getattr(trade,"entry",None), "Stop":getattr(trade,"stop",None),
            "Target":getattr(trade,"target1",None), "Risk Reward":getattr(trade,"risk_reward",None)})
    if len(archived) > 10000: del archived[:-10000]

    approved_count = sum(1 for t in final_list if _is_risk_approved(getattr(t, "risk_verdict", {})))
    vetoed_count = len(final_list) - approved_count

    logging.info(

        f"AI_CONSENSUS candidates_in={len(st.session_state.trade_candidates)} "

        f"symbols_grouped={len(grouped)} final_ranked={len(final_list)} "

        f"approved={approved_count} vetoed={vetoed_count} "

        f"symbols={[t.symbol for t in final_list]}"

    )

    return final_list


def get_final_trade_dataframe():

    rows = []

    for trade in st.session_state.final_trade_list:

        verdict = getattr(trade, "risk_verdict", None) or {}

        rows.append({

            "Symbol": trade.symbol,

            "Best Strategy": trade.strategy,

            "Verdict": verdict.get("verdict", "PENDING"),

            "AI Score": round(trade.ai_score, 2),

            "AI Confidence": getattr(trade, "ai_confidence", None),

            "Expected Value": getattr(trade, "expected_value", None),

            "Confidence": trade.confidence,

            "RR": trade.risk_reward,

            "Entry": trade.entry,

            "Stop": trade.stop,

            "Target": trade.target1,

            "Signals": trade.strategy_count,

            "State": trade.state,

            "Reason": verdict.get("reason", "")

        })

    if len(rows):

        return pd.DataFrame(rows)

    return pd.DataFrame()


def show_ai_consensus():
    # NOTE: DO NOT call build_ai_consensus() here. This is a DISPLAY function.
    # execute_scan_pipeline() already ran the full AI Consensus + Risk Manager
    # pass and populated st.session_state.final_trade_list. Calling it a second
    # time here re-runs the entire Risk Manager against paper_positions that
    # the pipeline just opened, so every candidate gets vetoed with
    # `EXPOSURE 10/10` and the original approvals are silently overwritten.
    # This function must ONLY read state, never re-run the pipeline.

    df = get_final_trade_dataframe()

    approved = [
        t for t in st.session_state.final_trade_list
        if _is_risk_approved(getattr(t, "risk_verdict", {}))
    ]

    if len(df):

        st.subheader("AI Consensus Ranking")

        st.dataframe(

            df,

            width="stretch"

        )

        if not approved:

            st.info(
                "No Trade - every candidate this scan was vetoed by the Risk "
                "Manager. See the Verdict/Reason columns above for why."
            )

    else:

        st.info("No Trade - no candidates were generated this scan.")
# =====================================================
# CAPITAL ALLOCATION ENGINE
# VERSION 3.4B
# =====================================================

def _is_risk_approved(verdict: dict | None) -> bool:
    v = (verdict or {}).get("verdict")
    return v in ("APPROVED", "APPROVED_REDUCED_SIZE")


def allocate_portfolio():
    """
    Brain 6 (Portfolio Manager): sizes and allocates capital across every
    candidate Brain 5 approved this scan - ranked by expected_value
    (Brain 4's historical-analog-informed estimate), respecting sector
    concentration caps and the portfolio's open-position limit. Vetoed
    candidates never reach this function; candidates that are approved
    but get no capital still get a state/rationale (APPROVED_NO_CAPITAL)
    so they stay visible rather than silently disappearing.
    """
    build_portfolio_state = __import__("os_brains.risk_manager", fromlist=["build_portfolio_state"]).build_portfolio_state
    from os_brains.portfolio_manager import allocate as portfolio_allocate
    experience_memory = __import__("os_brains", fromlist=["experience_memory"]).experience_memory

    if len(st.session_state.final_trade_list) == 0:
        st.session_state.selected_portfolio = []
        return []

    app_module = sys.modules[__name__]

    approved = [
        t for t in st.session_state.final_trade_list
        if _is_risk_approved(getattr(t, "risk_verdict", {}))
    ]

    if not approved:
        st.session_state.selected_portfolio = []
        logging.info(
            f"PORTFOLIO_ALLOCATION final_trades_in={len(st.session_state.final_trade_list)} "
            f"approved=0 selected=[] - no trade this scan"
        )
        return []

    portfolio_state = build_portfolio_state(app_module)

    try:
        portfolio_allocate(approved, portfolio_state, app_module)
    except Exception as e:
        logging.warning(f"PORTFOLIO_ALLOCATION Brain 6 allocation failed: {e}")

    # Brain 7 support: every approved candidate now has a final capital
    # fate (ALLOCATED or APPROVED_NO_CAPITAL) - mirror it onto its
    # Experience Memory decision row so a trade that never actually opened
    # is recorded as NO_TRADE rather than sitting at PENDING forever.
    for candidate in approved:
        decision_id = getattr(candidate, "decision_id", None)
        if decision_id is None:
            continue
        allocation_decision = {
            "state": candidate.state,
            "position_size": getattr(candidate, "position_size", 0),
            "capital_required": getattr(candidate, "capital_required", 0),
            "portfolio_weight": getattr(candidate, "portfolio_weight", 0),
            "rationale": getattr(candidate, "allocation_rationale", ""),
        }
        outcome_state = "PENDING" if candidate.state == "ALLOCATED" else "NO_TRADE"
        try:
            experience_memory.update_allocation(decision_id, allocation_decision, outcome_state)
        except Exception as e:
            logging.warning(f"PORTFOLIO_ALLOCATION experience_memory.update_allocation failed symbol={candidate.symbol}: {e}")

    selected = [t for t in approved if t.state == "ALLOCATED"]

    st.session_state.selected_portfolio = selected

    logging.info(

        f"PORTFOLIO_ALLOCATION final_trades_in={len(st.session_state.final_trade_list)} "

        f"approved={len(approved)} selected={[t.symbol for t in selected]} "

        f"remaining_capital={selected[-1].cash_reserved if selected else portfolio_state['capital']}"

    )

    return selected


def portfolio_dataframe():

    rows = []

    for trade in st.session_state.selected_portfolio:

        rows.append({

            "Symbol": trade.symbol,

            "Strategy": trade.strategy,

            "AI Score": round(trade.ai_score,2),

            "AI Confidence": getattr(trade, "ai_confidence", None),

            "Expected Value": getattr(trade, "expected_value", None),

            "Confidence": trade.confidence,

            "Qty": trade.position_size,

            "Capital": round(trade.capital_required,2),

            "Weight %": trade.portfolio_weight,

            "Entry": trade.entry,

            "Stop": trade.stop,

            "Target": trade.target1,

            "RR": trade.risk_reward,

            "Rationale": getattr(trade, "allocation_rationale", "")

        })

    return pd.DataFrame(rows)


def show_allocated_portfolio():

    allocate_portfolio()

    df = portfolio_dataframe()

    if len(df):

        st.subheader("Allocated Portfolio")

        st.dataframe(

            df,

            width="stretch"

        )
# =====================================================
# REAL TIME TRADE MONITOR
# VERSION 3.5A
# =====================================================


if "live_monitor_running" not in st.session_state:
    st.session_state.live_monitor_running = False


# =====================================================
# EXECUTION PIPELINE
# VERSION 3.5B
# =====================================================
def execute_selected_portfolio():
    # Position size/capital here were already set by Brain 6
    # (portfolio_manager.allocate) - recalculating with the flat
    # per-trade sizer would silently overwrite its sector-cap trimming
    # and ranking, so this only flips state to BUY and opens the trade.

    for trade in st.session_state.selected_portfolio:

        trade.state = "BUY"

        get_execution_engine().open_trade(trade)

def _append_funnel(stage, entered, exited, rejected=0, reasons=None):
    reasons = reasons or []
    st.session_state.decision_funnel.append({
        "Stage": stage,
        "Entered": int(entered or 0),
        "Exited": int(exited or 0),
        "Rejected": int(rejected or 0),
        "Rejection Reasons": " | ".join(reasons),
    })


def _collect_no_trade_explanation():
    reasons = []
    veto_counts = {}
    for trade in st.session_state.final_trade_list:
        verdict = getattr(trade, "risk_verdict", {}) or {}
        if verdict.get("verdict") == "VETOED":
            labels = verdict.get("vetoed_by") or ["Risk Manager veto"]
            for label in labels:
                veto_counts[label] = veto_counts.get(label, 0) + 1
    for label, count in sorted(veto_counts.items()):
        reasons.append(f"Risk Manager rejected {count} candidate(s): {label}.")
    no_capital = [t for t in st.session_state.final_trade_list if getattr(t, "state", "") == "APPROVED_NO_CAPITAL"]
    if no_capital:
        reasons.append(f"Portfolio Exposure rejected {len(no_capital)} approved candidate(s).")
    weak_analog = [
        t for t in st.session_state.final_trade_list
        if (getattr(t, "analog_report", {}) or {}).get("sample_confidence") == "LOW"
    ]
    if weak_analog:
        reasons.append(f"Historical Analog evidence was weak for {len(weak_analog)} candidate(s).")
    negative_ev = [t for t in st.session_state.final_trade_list if (getattr(t, "expected_value", 0) or 0) < 0]
    if negative_ev:
        reasons.append(f"Expected Value was negative for {len(negative_ev)} candidate(s).")
    if not reasons:
        reasons.append("No stock satisfied the minimum evidence threshold across strategy, risk, portfolio and consensus stages.")
    st.session_state.no_trade_explanation = reasons
    return reasons


def entry_trigger_status(trade):
    """Return a transparent READY-FOR-ENTRY gate using only cached market data."""
    symbol = getattr(trade, "symbol", "")
    created = getattr(trade, "signal_time", getattr(trade, "created_at", None))
    if created:
        try:
            created_at = pd.to_datetime(created, utc=True).to_pydatetime()
            if datetime.now(timezone.utc) - created_at > timedelta(minutes=int(WORKSPACE.preferences.get("signal_expiry_minutes", 30))):
                trade.entry_status = "EXPIRED"; trade.entry_reason = "Signal expiry time exceeded"
                return False, "EXPIRED: signal expiry time exceeded"
        except (TypeError, ValueError):
            pass
    frame = st.session_state.get("market_data", {}).get(symbol)
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return False, "Waiting for a cached completed candle"
    last = frame.iloc[-1]
    price = float(last.get("Close", 0) or 0)
    entry = float(getattr(trade, "entry", 0) or 0)
    volume = float(last.get("Volume", 0) or 0)
    average_volume = float(pd.to_numeric(frame.get("Volume"), errors="coerce").tail(20).mean() or 0)
    vwap = float(last.get("VWAP", price) or price)
    confidence = float(getattr(trade, "ai_score", getattr(trade, "confidence", 0)) or 0)
    discovery = (WORKSPACE.preferences.get("execution_mode","PAPER") == "PAPER" and
        WORKSPACE.preferences.get("paper_preset","PAPER NORMAL") == "PAPER DISCOVERY")
    configured_confidence = float(WORKSPACE.preferences.get("minimum_confidence", 70))
    paper_confidence = max(50.0, configured_confidence - 10.0) if discovery else configured_confidence
    requirements = {
        "entry price not reached": price >= entry if entry else False,
        "price below VWAP": price >= vwap,
        "volume confirmation missing": volume >= average_volume if average_volume else True,
        "AI score below configured threshold": confidence >= paper_confidence,
    }
    failed = [reason for reason, passed in requirements.items() if not passed]
    if not failed: trade.entry_status = "READY"
    elif "entry price not reached" in failed: trade.entry_status = "WAITING_PRICE"
    elif "volume confirmation missing" in failed: trade.entry_status = "WAITING_VOLUME"
    elif "price below VWAP" in failed: trade.entry_status = "WAITING_VWAP"
    else: trade.entry_status = "WAITING_AI_SCORE"
    trade.entry_reason = "Entry trigger confirmed" if not failed else "; ".join(failed)
    trade.last_evaluated = datetime.now(timezone.utc)
    trade.next_evaluation = trade.last_evaluated + timedelta(seconds=20)
    return not failed, "Entry trigger confirmed" if not failed else "; ".join(failed)


def execute_scan_pipeline():
    PipelineManager, PipelineStep = (lambda m: (m.PipelineManager, m.PipelineStep))(__import__("os_brains.pipeline_manager", fromlist=["PipelineManager", "PipelineStep"]))

    diagnostics = PipelineDiagnostics()
    errors = CentralErrorManager()
    health = SystemHealthEngine()
    st.session_state.pipeline_started_at = st.session_state.get("pipeline_started_at") or datetime.now()
    manager = PipelineManager(on_event=_pipeline_event)
    rejection_codes = {"NO_HISTORY","INSUFFICIENT_CANDLES","STALE_DATA","FILTERED_PRICE",
        "FILTERED_VOLUME","FILTERED_MARKET_CAP","FILTERED_FNO","MARKET_SESSION_BLOCK",
        "NO_STRATEGY_SIGNAL","STRATEGY_ERROR","SIGNAL_EXPIRED","OTHER"}
    rejections, stage_counts, timings = {}, {"Universe selected":0,"Data available":0,
        "Passed fast screen":0,"Strategy evaluated":0,"Strategy signalled":0,"AI approved":0,
        "Risk approved":0,"Waiting for entry":0,"Executed":0}, {}
    st.session_state["candidate_rejections"] = rejections
    st.session_state["pipeline_stage_counts"] = stage_counts
    st.session_state["pipeline_timings"] = timings

    def reject(symbol, code, detail):
        if symbol not in rejections:
            rejections[symbol] = {"Symbol":symbol,"First Removal Stage":code if code in rejection_codes else "OTHER","Detail":detail}

    def initialize_stage():
        started = time.perf_counter()
        # Preserve the complete decision trail before beginning a new cycle.
        # Rejected ideas are evidence, not disposable UI rows.
        archive = st.session_state.setdefault("candidate_archive", [])
        for trade in st.session_state.get("final_trade_list", []):
            row = _trade_row(trade) if "_trade_row" in globals() else _portable_value(trade)
            if isinstance(row, dict):
                verdict = getattr(trade, "risk_verdict", {}) or {}
                row.update({"Observed At": datetime.now(timezone.utc).isoformat(),
                            "Stage": getattr(trade, "state", "CANDIDATE"),
                            "Status": verdict.get("verdict", getattr(trade, "state", "PENDING")),
                            "Reason": verdict.get("reason", "; ".join(getattr(trade, "reasons", []) or []))})
                archive.append(row)
        if len(archive) > 10000:
            del archive[:-10000]
        st.session_state.trade_candidates = {}
        st.session_state.final_trade_list = []
        st.session_state.selected_portfolio = []
        st.session_state.decision_funnel = []
        calculate_sector_strength()
        fetch_nifty_benchmark()
        prefetch_news_earnings(list(st.session_state.market_data.keys()))
        _append_funnel("Universe", len(st.session_state.market_data), len(st.session_state.market_data), 0, [])
        stage_counts["Universe selected"] = len(st.session_state.market_data)
        timings["data readiness"] = round(time.perf_counter()-started, 4)
        return f"{len(st.session_state.market_data)} symbols initialized"

    def scan_stage():
        screen_started = time.perf_counter()
        total = len(st.session_state.market_data)
        progress = st.progress(0) if total else None
        status = st.empty()
        status.write("Running eligibility filter and opportunity ranking…")

        discovery = run_streamlit_discovery(
            sys.modules[__name__],
            reject_fn=reject,
            stage_counts=stage_counts,
        )
        stage_counts["Data available"] = discovery.history_ready_count
        st.session_state["discovery_last_audit"] = discovery.eligibility_audit.to_dict()
        st.session_state["discovery_last_focus"] = discovery.focus_meta
        st.session_state["discovery_last_timings"] = discovery.timings.as_dict()

        raw_symbols = len({t.symbol for t in st.session_state.trade_candidates.values()})
        timings["eligibility"] = discovery.timings.eligibility
        timings["opportunity ranking"] = discovery.timings.opportunity_ranking
        timings["fast screen"] = round(time.perf_counter() - screen_started, 4)
        timings["indicator update"] = discovery.timings.indicator_preparation
        timings["strategy evaluation"] = discovery.timings.full_strategies
        _append_funnel("Eligibility", total, discovery.eligible_count, total - discovery.eligible_count, [])
        _append_funnel("Focus Universe", discovery.eligible_count, discovery.focus_count,
                       max(0, discovery.eligible_count - discovery.focus_count), [])
        _append_funnel("Strategist", discovery.strategy_evaluated, raw_symbols,
                       max(0, discovery.strategy_evaluated - raw_symbols), [])
        if progress:
            progress.progress(1.0)
        status.write(f"Focus {discovery.focus_count}/{discovery.eligible_count} — {len(st.session_state.trade_candidates)} raw candidates")
        return f"{len(st.session_state.trade_candidates)} raw candidates ({discovery.focus_count} focus)"

    def consensus_stage():
        started = time.perf_counter()
        before = len(st.session_state.trade_candidates)
        final_list = build_ai_consensus()
        analog_reject = [t.symbol for t in final_list if (getattr(t, "analog_report", {}) or {}).get("sample_confidence") == "LOW"]
        _append_funnel("Historical Analog", before, len(final_list) - len(analog_reject), len(analog_reject), [f"{s}: Historical Analog too weak" for s in analog_reject[:25]])
        approved = [t for t in final_list if _is_risk_approved(getattr(t, "risk_verdict", {}))]
        stage_counts["AI approved"] = len(final_list); stage_counts["Risk approved"] = len(approved)
        risk_reasons = []
        for t in final_list:
            verdict = getattr(t, "risk_verdict", {}) or {}
            if verdict.get("verdict") == "VETOED":
                risk_reasons.append(f"{t.symbol}: {verdict.get('reason', 'Risk Manager veto')}")
        _append_funnel("Risk Manager", len(final_list), len(approved), len(final_list) - len(approved), risk_reasons[:25])
        _append_funnel("AI Consensus", before, len(final_list), max(0, before - len(final_list)), [])
        timings["AI"] = round(time.perf_counter()-started,4); timings["risk"] = timings["AI"]
        return f"{len(final_list)} consensus candidates; {len(approved)} approved"

    def portfolio_stage():
        approved = [t for t in st.session_state.final_trade_list if _is_risk_approved(getattr(t, "risk_verdict", {}))]
        selected = allocate_portfolio()
        rejected = [t for t in approved if t.state != "ALLOCATED"]
        _append_funnel("Portfolio Manager", len(approved), len(selected), len(rejected), [f"{t.symbol}: {getattr(t, 'allocation_rationale', 'Portfolio correlation/exposure')}" for t in rejected[:25]])
        _append_funnel("Final Trades", len(st.session_state.final_trade_list), len(selected), len(st.session_state.final_trade_list) - len(selected), [])
        return f"{len(selected)} allocated trades"

    def paper_stage():
        started = time.perf_counter()
        waiting = st.session_state.setdefault("waiting_entry", {})
        triggered = 0
        for trade in st.session_state.get("selected_portfolio", []):
            valid, reason = entry_trigger_status(trade)
            waiting[trade.symbol] = {"trade": trade, "reason": reason, "updated_at": datetime.now()}
            trade.state = "READY_FOR_ENTRY" if not valid else "ENTRY_TRIGGERED"
            # A closed-session scan remains useful historical analysis, but it
            # cannot arm a stale signal for a future live fill.
            if valid and not is_market_open():
                trade.state = "HISTORICAL_NON_LIVE"
                trade.entry_status = "MARKET_CLOSED_REVALIDATION_REQUIRED"
                waiting[trade.symbol]["reason"] = "Historical signal; revalidate at market open"
            elif valid and trade.symbol not in st.session_state.paper_positions:
                position, _ = create_atomic_paper_trade(trade)
                if position is not None:
                    waiting.pop(trade.symbol, None); triggered += 1
        monitor_open_positions()
        stage_counts["Waiting for entry"] = len(waiting); stage_counts["Executed"] = triggered
        timings["entry monitoring"] = round(time.perf_counter()-started,4); timings["execution"] = timings["entry monitoring"]
        return f"{len(waiting)} waiting for entry; {triggered} triggered; {len(st.session_state.paper_positions)} open"

    def reviewer_memory_stage():
        reviewed = len(st.session_state.get("closed_positions", []))
        _collect_no_trade_explanation()
        return f"Experience Memory updated; reviewer has {reviewed} closed position(s) available"

    try:
        ok = manager.run([
            PipelineStep("Market Observer", initialize_stage, "Preparing market context"),
            PipelineStep("Market Historian", lambda: "Regime catalog/context available", "Historian ready"),
            PipelineStep("Trade Candidate Engine", scan_stage, "Running indicators, Batch 1, Batch 2 and strategies"),
            PipelineStep("Historical Analog Engine", consensus_stage, "Strategist, analog evidence, risk and consensus"),
            PipelineStep("Strategist", lambda: "Strategist evidence included in consensus", "Completed"),
            PipelineStep("Risk Manager", lambda: "Risk verdicts captured", "Completed"),
            PipelineStep("Portfolio Manager", portfolio_stage, "Allocating approved trades"),
            PipelineStep("AI Consensus", lambda: "Consensus ranking refreshed", "Completed"),
            PipelineStep("Paper Trading Engine", paper_stage, "Opening/monitoring paper trades"),
            PipelineStep("Reviewer", reviewer_memory_stage, "Recording learning updates"),
            PipelineStep("Experience Memory", lambda: "Decision memory synchronized", "Completed"),
            PipelineStep("Dashboard Refresh", lambda: "Mission Control refreshed", "Completed"),
        ])
    except Exception as exc:
        errors.record_error("Scan Pipeline", "execute_scan_pipeline", exc)
        diagnostics.fail_phase("Scan Pipeline", str(exc))
        st.error("Pipeline stopped after an unexpected error. See Mission Control errors.")
        return

    health.mark("last_strategy_run")
    execute_arbitrage_pipeline()
    PipelineMonitor().update()
    if ok:
        persist_trading_state()
        st.success("Pipeline Completed Successfully")
    else:
        st.error("Pipeline stopped. See Mission Control logs.")
# =====================================================
# VCP ENGINE
# VERSION 3.6A
# =====================================================

def detect_vcp(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < 120:
        return

    highs = df["High"].rolling(20).max()
    lows = df["Low"].rolling(20).min()

    swings = []

    for i in range(20, len(df), 10):

        high = highs.iloc[i]
        low = lows.iloc[i]

        contraction = ((high - low) / high) * 100

        swings.append(contraction)

    if len(swings) < 4:
        return

    last_four = swings[-4:]

    decreasing = all(
        last_four[i] > last_four[i + 1]
        for i in range(3)
    )

    if decreasing:

        stock.patterns["VCP"] = True

        stock.add_score("pattern", 15)

        stock.add_reason("Volatility Contraction Pattern")

        stock.add_indicator(
            "VCP_CONTRACTIONS",
            len(last_four)
        )
def create_vcp_candidate(stock):

    if not stock.patterns.get("VCP", False):
        return

    last = stock.data.iloc[-1]

    trade = TradeCandidate(
        stock.symbol,
        "VCP"
    )

    trade.set_entry(round(last["High"] * 1.002, 2))

    stop = min(
        last["Low"],
        last["EMA20"]
    )

    trade.set_stop(round(stop, 2))

    if trade.entry is None or trade.stop is None:
        return

    risk = trade.entry - trade.stop

    trade.set_target1(
        round(trade.entry + (risk * 3), 2)
    )

    confidence = 75

    if stock.market.get("TREND") == "UPTREND":
        confidence += 10

    if last["RVOL"] > 1.2:
        confidence += 10

    if last["MACD"] > last["MACD_SIGNAL"]:
        confidence += 5

    trade.confidence = min(confidence, 100)

    trade.calculate_rr()

    trade.state = "READY"

    save_trade_candidate(trade)
def run_vcp_strategy(stock):

    detect_vcp(stock)

    create_vcp_candidate(stock)


# ==========================================
# REGISTER STRATEGIES
# ==========================================

register_strategy(
    "PRICE SQUEEZE",
    run_price_squeeze_strategy,
    priority=10
)

register_strategy(
    "DEMAND & SUPPLY",
    run_demand_supply_strategy,
    priority=20
)

register_strategy(
    "VCP",
    run_vcp_strategy,
    priority=30
)


# =====================================================
# BREAKOUT ENGINE
# VERSION 3.7A
# =====================================================

BREAKOUT_LOOKBACK = 20
BREAKOUT_BUFFER = 0.002


def detect_breakout(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < BREAKOUT_LOOKBACK + 5:
        return

    last = df.iloc[-1]

    resistance = df["High"].iloc[-BREAKOUT_LOOKBACK-1:-1].max()

    breakout_price = round(
        resistance * (1 + BREAKOUT_BUFFER),
        2
    )

    stock.add_indicator(
        "BREAKOUT_LEVEL",
        breakout_price
    )

    breakout = False

    if last["Close"] >= breakout_price:

        breakout = True

        stock.patterns["BREAKOUT"] = True

        stock.add_score(
            "pattern",
            12
        )

        stock.add_reason(
            "Resistance Breakout"
        )

    if last["RVOL"] >= 1.50:

        stock.add_score(
            "volume",
            5
        )

        stock.add_reason(
            "Volume Confirmation"
        )

    if last["MACD"] > last["MACD_SIGNAL"]:

        stock.add_score(
            "momentum",
            5
        )

        stock.add_reason(
            "Bullish MACD"
        )

    stock.patterns["BREAKOUT_READY"] = breakout
# =====================================================
# BREAKOUT ENGINE
# PART 2
# VERSION 3.7B
# =====================================================

def create_breakout_candidate(stock):

    if not stock.patterns.get(

        "BREAKOUT_READY",

        False

    ):

        return

    last = stock.data.iloc[-1]

    trade = TradeCandidate(

        stock.symbol,

        "BREAKOUT"

    )

    entry = stock.indicators["BREAKOUT_LEVEL"]

    stop = round(

        min(

            last["EMA20"],

            last["Low"]

        ),

        2

    )

    risk = entry - stop

    if risk <= 0:

        return

    trade.set_entry(entry)

    trade.set_stop(stop)

    trade.set_target1(

        round(

            entry + (risk * 3),

            2

        )

    )

    confidence = 70

    if stock.market.get("TREND") == "UPTREND":

        confidence += 10

        trade.add_reason(

            "Primary Uptrend"

        )

    if last["RVOL"] >= 1.5:

        confidence += 10

        trade.add_reason(

            "Volume Confirmation"

        )

    if last["MACD"] > last["MACD_SIGNAL"]:

        confidence += 10

        trade.add_reason(

            "Bullish MACD"

        )

    trade.confidence = min(

        confidence,

        100

    )

    trade.calculate_rr()

    if trade.risk_reward >= 2.5:

        trade.state = "READY"

    else:

        trade.state = "WATCHLIST"

    save_trade_candidate(trade)


def run_breakout_strategy(stock):

    detect_breakout(stock)

    create_breakout_candidate(stock)

register_strategy(
    "BREAKOUT",
    run_breakout_strategy,
    priority=40
)
# =====================================================
# ORDER BLOCK ENGINE
# VERSION 3.8A
# =====================================================

ORDER_BLOCK_LOOKBACK = 40


def detect_order_blocks(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < ORDER_BLOCK_LOOKBACK:
        return

    bullish_blocks = []
    bearish_blocks = []

    for i in range(5, len(df)-3):

        candle = df.iloc[i]

        next1 = df.iloc[i+1]
        next2 = df.iloc[i+2]

        body = abs(
            candle["Close"] - candle["Open"]
        )

        rng = candle["High"] - candle["Low"]

        if rng == 0:
            continue

        body_percent = body / rng

        # Bullish Order Block

        if candle["Close"] < candle["Open"]:

            if (

                next1["Close"] > next1["Open"]

                and

                next2["Close"] > next2["Open"]

                and

                next2["Close"] > candle["High"]

            ):

                bullish_blocks.append({

                    "Low": candle["Low"],

                    "High": candle["High"],

                    "Index": i

                })

        # Bearish Order Block

        if candle["Close"] > candle["Open"]:

            if (

                next1["Close"] < next1["Open"]

                and

                next2["Close"] < next2["Open"]

                and

                next2["Close"] < candle["Low"]

            ):

                bearish_blocks.append({

                    "Low": candle["Low"],

                    "High": candle["High"],

                    "Index": i

                })

    stock.patterns["BULLISH_ORDER_BLOCKS"] = bullish_blocks

    stock.patterns["BEARISH_ORDER_BLOCKS"] = bearish_blocks

    stock.add_indicator(

        "ORDER_BLOCKS",

        len(bullish_blocks)

    )

    if len(bullish_blocks):

        stock.add_score(

            "pattern",

            min(len(bullish_blocks),5)

        )

        stock.add_reason(

            f"{len(bullish_blocks)} Bullish Order Blocks"

        )

def nearest_order_block(stock):

    zones = stock.patterns.get(
        "BULLISH_ORDER_BLOCKS",
        []
    )

    if not zones:
        return None

    price = stock.data.iloc[-1]["Close"]

    return min(
        zones,
        key=lambda z: abs(
            price - z["High"]
        )
    )


def create_order_block_candidate(stock):

    zone = nearest_order_block(stock)

    if zone is None:
        return

    last = stock.data.iloc[-1]

    trade = TradeCandidate(
        stock.symbol,
        "ORDER_BLOCK"
    )

    trade.set_entry(round(zone["High"], 2))
    trade.set_stop(round(zone["Low"], 2))

    if trade.entry is None or trade.stop is None:
        return

    risk = trade.entry - trade.stop

    if risk <= 0:
        return

    trade.set_target1(
        round(trade.entry + risk * 3, 2)
    )

    confidence = 70

    if stock.market.get("TREND") == "UPTREND":
        confidence += 10

    if last["RVOL"] >= 1.2:
        confidence += 10

    if stock.score["quality"] >= 80:
        confidence += 10

    trade.confidence = min(confidence, 100)

    trade.calculate_rr()

    trade.state = "READY"

    save_trade_candidate(trade)


def run_order_block_strategy(stock):

    detect_order_blocks(stock)

    create_order_block_candidate(stock)

register_strategy(
    "ORDER BLOCK",
    run_order_block_strategy,
    priority=50
)
# ==========================================================
# MODULE A - PART 1
# PROFESSIONAL POSITION OBJECT
# ==========================================================





class BrokerBase(ABC):
    @abstractmethod
    def connect(self): ...
    @abstractmethod
    def disconnect(self): ...
    @abstractmethod
    def place_order(self, order): ...
    @abstractmethod
    def modify_order(self, order_id, **changes): ...
    @abstractmethod
    def cancel_order(self, order_id): ...
    @abstractmethod
    def positions(self): ...
    @abstractmethod
    def holdings(self): ...
    @abstractmethod
    def orders(self): ...
    @abstractmethod
    def funds(self): ...
    @abstractmethod
    def margin(self): ...


class PaperBroker(BrokerBase):
    def __init__(self, initial_cash: float | None = None):
        initial_cash = float(initial_cash or WORKSPACE.preferences["paper_trading_capital"])
        self.state = st.session_state.setdefault("paper_broker", {
            "connected": False, "cash": initial_cash, "positions": {},
            "orders": {}, "trade_history": [], "realized_pnl": 0.0, "risk": {},
        })

    def connect(self):
        self.state["connected"] = True
        logging.info("PaperBroker connected")
        return True

    def disconnect(self):
        self.state["connected"] = False
        logging.info("PaperBroker disconnected")
        return True

    def place_order(self, order):
        order = dict(order)
        order_id = order.get("order_id") or str(uuid.uuid4())
        order.update({"order_id": order_id, "status": "OPEN", "timestamp": datetime.now()})
        self.state["orders"][order_id] = order
        if order.get("order_type", "MARKET").upper() == "MARKET":
            self._execute_order(order_id, float(order.get("price") or order.get("ltp") or 0))
        logging.info("PaperBroker order placed | order_id=%s | symbol=%s", order_id, order.get("symbol"))
        return order

    def modify_order(self, order_id, **changes):
        order = self.state["orders"].get(order_id)
        if not order or order.get("status") != "OPEN":
            return None
        order.update(changes)
        order["modified_at"] = datetime.now()
        logging.info("PaperBroker order modified | order_id=%s", order_id)
        return order

    def cancel_order(self, order_id):
        order = self.state["orders"].get(order_id)
        if order and order.get("status") == "OPEN":
            order["status"] = "CANCELLED"
            order["completed_at"] = datetime.now()
            logging.info("PaperBroker order cancelled | order_id=%s", order_id)
            return True
        return False

    def partial_exit(self, symbol: str, qty: int, price: float):
        return self.place_order({"symbol": symbol, "side": "SELL", "qty": qty, "price": price, "order_type": "MARKET", "tag": "PARTIAL_EXIT"})

    def _execute_order(self, order_id: str, price: float):
        order = self.state["orders"][order_id]
        qty = int(order.get("qty") or order.get("quantity") or 0)
        symbol = order.get("symbol")
        side = order.get("side", "BUY").upper()
        if not symbol or qty <= 0 or price <= 0:
            order["status"] = "REJECTED"
            return order
        pos = self.state["positions"].setdefault(symbol, {"symbol": symbol, "qty": 0, "avg_price": 0.0, "realized_pnl": 0.0})
        if side == "BUY":
            cost = qty * price
            new_qty = pos["qty"] + qty
            pos["avg_price"] = ((pos["avg_price"] * pos["qty"]) + cost) / new_qty
            pos["qty"] = new_qty
            self.state["cash"] -= cost
        else:
            exit_qty = min(qty, pos["qty"])
            pnl = (price - pos["avg_price"]) * exit_qty
            pos["qty"] -= exit_qty
            pos["realized_pnl"] += pnl
            self.state["realized_pnl"] += pnl
            self.state["cash"] += exit_qty * price
            if pos["qty"] == 0:
                self.state["positions"].pop(symbol, None)
        order.update({"status": "COMPLETE", "execution_price": price, "completed_at": datetime.now()})
        self.state["trade_history"].append(order.copy())
        self._update_risk()
        return order

    def mark_to_market(self, prices: dict[str, float]):
        unrealized = 0.0
        for sym, pos in self.state["positions"].items():
            ltp = float(prices.get(sym, pos["avg_price"]))
            pos["ltp"] = ltp
            pos["unrealized_pnl"] = (ltp - pos["avg_price"]) * pos["qty"]
            unrealized += pos["unrealized_pnl"]
        self.state["unrealized_pnl"] = unrealized
        self._update_risk()
        return unrealized

    def _update_risk(self):
        exposure = sum(p["qty"] * p.get("ltp", p["avg_price"]) for p in self.state["positions"].values())
        equity = self.state["cash"] + exposure
        self.state["risk"] = {"exposure": exposure, "equity": equity, "open_positions": len(self.state["positions"])}

    def positions(self): return list(self.state["positions"].values())
    def holdings(self): return self.positions()
    def orders(self): return list(self.state["orders"].values())
    def funds(self): return {"cash": self.state["cash"], "realized_pnl": self.state.get("realized_pnl", 0.0), "unrealized_pnl": self.state.get("unrealized_pnl", 0.0)}
    def margin(self): return self.state.get("risk", {})

# ==========================================================
# EXECUTION ENGINE (Paper / Simulation / future Live)
# ==========================================================
# Owns "how a decision to open/close a trade becomes a recorded trade".
# Everything upstream (Brains, AI Consensus, Risk Manager, Portfolio
# Manager) only ever decides WHAT to trade - it hands a TradeCandidate or
# an open PaperPosition to get_execution_engine() and never touches
# st.session_state.paper_positions or PaperPosition.close_trade() itself.
# That means a future real broker only has to implement this interface;
# no decision-making code changes when the mode changes.

class ExecutionEngine:

    mode = "ABSTRACT"

    def open_trade(self, trade):
        raise NotImplementedError

    def close_trade(self, position, reason, price):
        raise NotImplementedError


class PaperExecutionEngine(ExecutionEngine):
    """
    Today's default and only fully "real" mode: records trades as
    PaperPosition objects in st.session_state, exactly as the app already
    did before this abstraction existed.
    """

    mode = "PAPER"

    def open_trade(self, trade):
        open_paper_trade(trade)
        return st.session_state.paper_positions.get(trade.symbol)

    def close_trade(self, position, reason, price):
        position.close_trade(reason, price)
        return position


class SimulationExecutionEngine(PaperExecutionEngine):
    """
    Identical mechanics to Paper mode - same PaperPosition bookkeeping,
    same Brain 7 / Experience Memory feedback on close. Kept as a
    distinct, explicitly-labelled mode so a run made against non-live or
    backfilled data is never confused with real-time paper trading in the
    trade journal / audit log, without needing a second trade-recording
    implementation.
    """

    mode = "SIMULATION"


class LiveExecutionEngine(ExecutionEngine):
    """Live mode routes the same trade decisions to the selected broker."""

    mode = "LIVE"

    def _broker(self):
        return st.session_state.get("active_broker_client") or PaperBroker()

    def open_trade(self, trade):
        order = {
            "symbol": trade.symbol, "side": "BUY",
            "qty": getattr(trade, "quantity", getattr(trade, "qty", 1)),
            "price": getattr(trade, "entry", getattr(trade, "current_price", 0)),
            "order_type": "MARKET", "tag": "ALPHAQUANT_LIVE",
        }
        return self._broker().place_order(order)

    def close_trade(self, position, reason, price):
        order = {
            "symbol": position.symbol, "side": "SELL",
            "qty": getattr(position, "quantity", getattr(position, "qty", 1)),
            "price": price, "order_type": "MARKET", "tag": reason,
        }
        return self._broker().place_order(order)


EXECUTION_ENGINES = {
    "PAPER": PaperExecutionEngine(),
    "SIMULATION": SimulationExecutionEngine(),
    "LIVE": LiveExecutionEngine(),
}

if "execution_mode" not in st.session_state:
    st.session_state.execution_mode = "PAPER"


def get_execution_engine():
    return EXECUTION_ENGINES.get(
        st.session_state.execution_mode,
        EXECUTION_ENGINES["PAPER"],
    )


@dataclass
class PaperPosition:

    symbol: str
    strategy: str

    qty: int = 0

    entry: float = 0.0

    stop: float = 0.0

    target1: float = 0.0
    target2: float = 0.0
    target3: float = 0.0

    current_price: float = 0.0

    highest_price: float = 0.0
    lowest_price: float = 0.0

    trailing_stop: float = 0.0

    invested: float = 0.0

    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0

    max_profit: float = 0.0
    max_drawdown: float = 0.0

    rr: float = 0.0

    confidence: int = 0

    ai_score: int = 0

    score_version: str = "V1"
    trade_confidence: float = 0.0

    status: str = "OPEN"

    target1_hit: bool = False
    target2_hit: bool = False
    target3_hit: bool = False

    break_even: bool = False

    partial_exit_done: bool = False

    remaining_qty: int = 0

    entry_time: datetime = field(
        default_factory=datetime.now
    )

    exit_time: datetime | None = None

    exit_reason: str = ""

    history: list = field(default_factory=list)

    decision_id: int | None = None

    # Provenance is persisted with the position.  Validation records are
    # deliberately distinguishable from normal paper performance.
    source: str = "PAPER"
    idempotency_key: str | None = None

    # -----------------------------------------------------

    def initialise(self):

        self.current_price = self.entry

        self.highest_price = self.entry

        self.lowest_price = self.entry

        self.trailing_stop = self.stop

        self.remaining_qty = self.qty

        self.invested = self.qty * self.entry

    # -----------------------------------------------------

    def update_price(self, price):

        self.current_price = float(price)

        if price > self.highest_price:
            self.highest_price = price

        if price < self.lowest_price:
            self.lowest_price = price

        self.unrealized_pnl = round(
            (price - self.entry) * self.remaining_qty,
            2
        )

        self.max_profit = max(
            self.max_profit,
            self.unrealized_pnl
        )

        self.max_drawdown = min(
            self.max_drawdown,
            self.unrealized_pnl
        )

    # -----------------------------------------------------

    def move_stop(self, new_stop):

        if new_stop > self.trailing_stop:

            self.trailing_stop = round(
                new_stop,
                2
            )

    # -----------------------------------------------------

    def activate_break_even(self):

        if self.break_even:
            return

        self.break_even = True

        self.trailing_stop = self.entry

    # -----------------------------------------------------

    def close_partial(self, qty, price):

        if qty <= 0:
            return

        qty = min(
            qty,
            self.remaining_qty
        )

        pnl = (
            price - self.entry
        ) * qty

        self.realized_pnl += pnl

        self.remaining_qty -= qty

        _sync_paper_broker_on_exit(self.symbol, qty, price, pnl)

        self.history.append({

            "time": datetime.now(),

            "type": "PARTIAL",

            "qty": qty,

            "price": price,

            "pnl": pnl

        })

    # -----------------------------------------------------

    def close_trade(self, reason, price):

        pnl = (
            price - self.entry
        ) * self.remaining_qty

        self.realized_pnl += pnl

        exit_qty = self.remaining_qty
        self.remaining_qty = 0

        self.status = "CLOSED"

        self.exit_reason = reason

        self.exit_time = datetime.now()

        _sync_paper_broker_on_exit(self.symbol, exit_qty, price, pnl)

        self.history.append({

            "time": self.exit_time,

            "type": "EXIT",

            "price": price,

            "reason": reason,

            "pnl": pnl

        })

        # Brain 7 (Reviewer): every close path funnels through this method,
        # so this is the one place a completed trade can be reviewed and fed
        # back into Experience Memory regardless of which trade-management
        # path (legacy mark_closed or the newer check_stop_loss/
        # check_target3) actually triggered the close. Never raises - a
        # review failure must not prevent a trade from closing.
        try:
            from os_brains.reviewer import review_closed_trade
            review_closed_trade(self)
        except Exception as e:
            logging.warning(f"CLOSE_TRADE reviewer.review_closed_trade failed symbol={self.symbol}: {e}")

    # -----------------------------------------------------

    def total_pnl(self):

        return round(

            self.realized_pnl +

            self.unrealized_pnl,

            2

        )

    # -----------------------------------------------------
    # Backward-compatible aliases: earlier pipeline code (Trade History,
    # Open/Live Positions panels) reads .quantity / .pnl / .mark_closed(),
    # while this dataclass uses .qty / realized+unrealized pnl / close_trade().
    # These properties bridge the two without touching either call site.

    @property
    def quantity(self):
        return self.qty

    @property
    def pnl(self):
        return self.total_pnl()

    @property
    def exit_price(self):
        return self.current_price

    def mark_closed(self, price, reason):
        self.close_trade(reason, price)

    # -----------------------------------------------------

    def age_minutes(self):

        return int(

            (

                datetime.now()

                -

                self.entry_time

            ).total_seconds()

            / 60

        )


PAPER_STATE_PATH = Path(_APP_DIR) / "data" / "paper_state.json"


def _sync_paper_broker_on_exit(symbol: str, qty: int, price: float, pnl: float) -> None:
    """Keep paper_broker cash and realized P&L aligned with PaperPosition exits."""
    if qty <= 0 or price <= 0:
        return
    ledger = st.session_state.setdefault("paper_broker", {})
    ledger["cash"] = float(ledger.get("cash", 0)) + (price * qty)
    ledger["realized_pnl"] = float(ledger.get("realized_pnl", 0)) + float(pnl)
    broker_pos = ledger.setdefault("positions", {}).get(symbol)
    if isinstance(broker_pos, dict):
        broker_pos["qty"] = max(0, int(broker_pos.get("qty", 0)) - qty)
        if broker_pos["qty"] <= 0:
            ledger["positions"].pop(symbol, None)


def _portable_value(value):
    """Convert trading state to deterministic JSON without losing timestamps."""
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _portable_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_portable_value(v) for v in value]
    if hasattr(value, "__dict__"):
        return _portable_value(vars(value))
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    return value


def persist_trading_state():
    """Atomically checkpoint paper ledger, positions and opportunity history."""
    PAPER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "paper_broker": _portable_value(st.session_state.get("paper_broker", {})),
        "paper_positions": _portable_value(st.session_state.get("paper_positions", {})),
        "paper_history": _portable_value(st.session_state.get("paper_history", [])),
        "candidate_archive": _portable_value(st.session_state.get("candidate_archive", [])),
        "decision_funnel": _portable_value(st.session_state.get("decision_funnel", [])),
    }
    temp = PAPER_STATE_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(PAPER_STATE_PATH)
    return payload


def restore_trading_state_once():
    """Restore open paper positions before rendering any trading workspace."""
    if st.session_state.get("_paper_state_restored"):
        return
    st.session_state["_paper_state_restored"] = True
    if not PAPER_STATE_PATH.exists():
        return
    try:
        payload = json.loads(PAPER_STATE_PATH.read_text(encoding="utf-8"))
        allowed = {f.name for f in fields(PaperPosition)}
        positions = {}
        for symbol, raw in (payload.get("paper_positions") or {}).items():
            values = {k: v for k, v in raw.items() if k in allowed}
            for key in ("entry_time", "exit_time"):
                if values.get(key):
                    values[key] = datetime.fromisoformat(values[key])
            positions[symbol] = PaperPosition(**values)
        history = []
        for raw in payload.get("paper_history") or []:
            if not isinstance(raw, dict):
                continue
            values = {k: v for k, v in raw.items() if k in allowed}
            for key in ("entry_time", "exit_time"):
                if values.get(key):
                    values[key] = datetime.fromisoformat(values[key])
            history.append(PaperPosition(**values))
        st.session_state.paper_positions = positions
        st.session_state.paper_history = history
        if isinstance(payload.get("paper_broker"), dict):
            st.session_state.paper_broker = payload["paper_broker"]
            st.session_state.paper_broker["connected"] = False
        st.session_state.candidate_archive = payload.get("candidate_archive", [])
        st.session_state.decision_funnel = payload.get("decision_funnel", [])
        st.session_state["restored_position_count"] = len(positions)
    except (OSError, ValueError, TypeError) as exc:
        logging.warning("Paper-state restore failed: %s", exc)
# ==========================================================
# MODULE A - PART 2
# POSITION MANAGER
# ==========================================================

def get_open_position(symbol):

    return st.session_state.paper_positions.get(symbol)


def add_open_position(position):

    st.session_state.paper_positions[position.symbol] = position


def remove_open_position(symbol):

    if symbol in st.session_state.paper_positions:

        del st.session_state.paper_positions[symbol]

       # ==========================================================
# MODULE A - PART 3
# TRADE MANAGEMENT ENGINE
# ==========================================================

TRAIL_ATR_MULTIPLIER = 1.5


def update_position_price(position, latest_price):

    position.update_price(latest_price)


def check_stop_loss(position):

    if position.status != "OPEN":
        return False

    if position.current_price <= position.trailing_stop:

        get_execution_engine().close_trade(
            position,
            "STOP LOSS",
            position.current_price
        )

        return True

    return False


def check_target1(position):

    if position.target1_hit:
        return

    if position.current_price >= position.target1:

        qty = max(
            1,
            int(position.qty * 0.25)
        )

        position.close_partial(
            qty,
            position.current_price
        )

        position.target1_hit = True

        position.activate_break_even()


def check_target2(position):

    if position.target2_hit:
        return

    if position.target2 == 0:
        return

    if position.current_price >= position.target2:

        qty = max(
            1,
            int(position.qty * 0.35)
        )

        position.close_partial(
            qty,
            position.current_price
        )

        position.target2_hit = True


def check_target3(position):

    if position.target3_hit:
        return

    if position.target3 == 0:
        return

    if position.current_price >= position.target3:

        position.target3_hit = True

        get_execution_engine().close_trade(
            position,
            "TARGET 3",
            position.current_price
        )


def update_trailing_stop(position, stock):

    df = stock.data

    if df is None:
        return

    if len(df) < 20:
        return

    ema20 = df.iloc[-1]["EMA20"]

    atr = df.iloc[-1]["ATR"]

    new_stop = max(

        ema20,

        position.current_price -
        (atr * TRAIL_ATR_MULTIPLIER)

    )

    position.move_stop(new_stop)


def manage_position(stock, position):

    last_price = stock.data.iloc[-1]["Close"]

    update_position_price(
        position,
        last_price
    )

    if check_stop_loss(position):

        return

    check_target1(position)

    check_target2(position)

    check_target3(position)

    update_trailing_stop(
        position,
        stock
    )
# ==========================================================
# MODULE A - PART 4
# LIVE POSITION MONITOR
# ==========================================================

def update_position_statistics(position):

    if position.unrealized_pnl > position.max_profit:

        position.max_profit = position.unrealized_pnl

    if position.unrealized_pnl < position.max_drawdown:

        position.max_drawdown = position.unrealized_pnl


def archive_closed_position(position):

    st.session_state.setdefault("closed_positions", []).append(position)

    st.session_state.setdefault("paper_history", []).append(position)

    st.session_state.setdefault("trade_journal", []).append({
        "Symbol": position.symbol,

        "Strategy": position.strategy,

        "Entry": position.entry,

        "Exit": position.current_price,

        "PnL": round(position.realized_pnl,2),

        "Reason": position.exit_reason,

        "Minutes": position.age_minutes()

    })


def monitor_open_positions():

    if len(st.session_state.paper_positions) == 0:

        return

    remove_list = []

    for symbol, position in list(

        st.session_state.paper_positions.items()

    ):

        stock = get_stock(symbol)

        if stock is None:

            continue

        if stock.data is None:

            continue

        manage_position(

            stock,

            position

        )

        update_position_statistics(position)

        if position.status == "CLOSED":

            archive_closed_position(position)

            remove_list.append(symbol)

    for symbol in remove_list:

        del st.session_state.paper_positions[symbol]
# ==========================================================
# MODULE A - PART 5
# DASHBOARD & ANALYTICS
# ==========================================================

def portfolio_statistics():

    open_positions = list(
        st.session_state.paper_positions.values()
    )

    closed_positions = st.session_state.closed_positions

    open_pnl = round(
        sum(p.unrealized_pnl for p in open_positions),
        2
    )

    closed_pnl = round(
        sum(p.realized_pnl for p in closed_positions),
        2
    )

    invested = round(
        sum(p.invested for p in open_positions),
        2
    )

    wins = len(
        [
            p for p in closed_positions
            if p.realized_pnl > 0
        ]
    )

    losses = len(
        [
            p for p in closed_positions
            if p.realized_pnl <= 0
        ]
    )

    total = wins + losses

    if total == 0:

        win_rate = 0

    else:

        win_rate = round(
            wins * 100 / total,
            2
        )

    return {

        "OpenPnL": open_pnl,

        "ClosedPnL": closed_pnl,

        "Invested": invested,

        "Wins": wins,

        "Losses": losses,

        "WinRate": win_rate,

        "OpenTrades": len(open_positions),

        "ClosedTrades": len(closed_positions)

    }


# ----------------------------------------------------------


def show_portfolio_dashboard():

    stats = portfolio_statistics()

    st.markdown(
        "## 📊 Portfolio Monitor"
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Open P&L",
        f"₹{stats['OpenPnL']:.2f}"
    )

    c2.metric(
        "Closed P&L",
        f"₹{stats['ClosedPnL']:.2f}"
    )

    c3.metric(
        "Win Rate",
        f"{stats['WinRate']}%"
    )

    c4.metric(
        "Open Trades",
        stats["OpenTrades"]
    )

    st.divider()

    st.subheader(
        "Open Positions"
    )

    rows = []

    for p in st.session_state.paper_positions.values():

        rows.append({

            "Symbol": p.symbol,

            "Strategy": p.strategy,

            "Qty": p.remaining_qty,

            "Entry": p.entry,

            "LTP": round(
                p.current_price,
                2
            ),

            "Stop": round(
                p.trailing_stop,
                2
            ),

            "PnL": round(
                p.unrealized_pnl,
                2
            ),

            "Age(Min)": p.age_minutes()

        })

    if len(rows):

        st.dataframe(
            rows,
            width="stretch"
        )

    st.divider()

    st.subheader(
        "Closed Trades"
    )

    closed = []

    for p in st.session_state.closed_positions:

        closed.append({

            "Symbol": p.symbol,

            "Strategy": p.strategy,

            "PnL": round(
                p.realized_pnl,
                2
            ),

            "Reason": p.exit_reason

        })

    if len(closed):

        st.dataframe(

            closed,

            width="stretch"

        )
# =====================================================
# FAIR VALUE GAP ENGINE
# MODULE B - PART 1
# =====================================================

FVG_MIN_GAP_PERCENT = 0.30


def detect_fair_value_gaps(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < 10:
        return

    bullish = []
    bearish = []

    for i in range(2, len(df)):

        c1 = df.iloc[i-2]
        c2 = df.iloc[i-1]
        c3 = df.iloc[i]

        # Bullish FVG

        if c3["Low"] > c1["High"]:

            gap = (

                (c3["Low"] - c1["High"])

                / c1["High"]

            ) * 100

            if gap >= FVG_MIN_GAP_PERCENT:

                bullish.append({

                    "Low": round(c1["High"],2),

                    "High": round(c3["Low"],2),

                    "Gap": round(gap,2),

                    "Index": i

                })

        # Bearish FVG

        if c3["High"] < c1["Low"]:

            gap = (

                (c1["Low"] - c3["High"])

                / c1["Low"]

            ) * 100

            if gap >= FVG_MIN_GAP_PERCENT:

                bearish.append({

                    "Low": round(c3["High"],2),

                    "High": round(c1["Low"],2),

                    "Gap": round(gap,2),

                    "Index": i

                })

    stock.patterns["BULLISH_FVG"] = bullish
    stock.patterns["BEARISH_FVG"] = bearish

    if bullish:

        stock.add_score("pattern", 8)

        stock.add_reason(

            f"{len(bullish)} Bullish FVG"

        )


def nearest_fvg(stock):

    zones = stock.patterns.get(

        "BULLISH_FVG",

        []

    )

    if not zones:

        return None

    price = stock.data.iloc[-1]["Close"]

    return min(

        zones,

        key=lambda x: abs(

            price -

            x["High"]

        )

    )


def create_fvg_candidate(stock):

    zone = nearest_fvg(stock)

    if zone is None:
        return

    last = stock.data.iloc[-1]

    trade = TradeCandidate(

        stock.symbol,

        "FVG"

    )

    trade.set_entry(

        round(zone["High"],2)

    )

    trade.set_stop(

        round(

            zone["Low"] - last["ATR"],

            2

        )

    )

    if trade.entry is None or trade.stop is None:
        return

    risk = trade.entry - trade.stop

    if risk <= 0:
        return

    trade.set_target1(

        round(

            trade.entry + risk * 3,

            2

        )

    )

    confidence = 70

    if stock.market.get(

        "TREND"

    ) == "UPTREND":

        confidence += 10

    if last["RVOL"] >= 1.20:

        confidence += 10

    if last["MACD"] > last["MACD_SIGNAL"]:

        confidence += 10

    trade.confidence = min(

        confidence,

        100

    )

    trade.calculate_rr()

    trade.state = "READY"

    save_trade_candidate(trade)


def run_fvg_strategy(stock):

    detect_fair_value_gaps(stock)

    create_fvg_candidate(stock)

register_strategy(
    "FVG",
    run_fvg_strategy,
    priority=60
)
# =====================================================
# LIQUIDITY SWEEP ENGINE
# MODULE B - PART 2
# =====================================================

LIQUIDITY_LOOKBACK = 15


def detect_liquidity_sweep(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < LIQUIDITY_LOOKBACK + 5:
        return

    last = df.iloc[-1]

    previous_low = (

        df["Low"]

        .iloc[-LIQUIDITY_LOOKBACK-1:-1]

        .min()

    )

    previous_high = (

        df["High"]

        .iloc[-LIQUIDITY_LOOKBACK-1:-1]

        .max()

    )

    bullish = False

    bearish = False

    # ==========================================
    # Bullish Liquidity Sweep
    # ==========================================

    if (

        last["Low"] < previous_low

        and

        last["Close"] > previous_low

    ):

        bullish = True

        stock.patterns["BULLISH_SWEEP"] = True

        stock.add_score(

            "pattern",

            12

        )

        stock.add_reason(

            "Bullish Liquidity Sweep"

        )

    # ==========================================
    # Bearish Liquidity Sweep
    # ==========================================

    if (

        last["High"] > previous_high

        and

        last["Close"] < previous_high

    ):

        bearish = True

        stock.patterns["BEARISH_SWEEP"] = True

    stock.add_indicator(

        "LIQUIDITY_SWEEP",

        bullish

    )


def create_liquidity_candidate(stock):

    if not stock.patterns.get(

        "BULLISH_SWEEP",

        False

    ):

        return

    last = stock.data.iloc[-1]

    trade = TradeCandidate(

        stock.symbol,

        "LIQUIDITY_SWEEP"

    )

    entry = round(

        last["High"] * 1.001,

        2

    )

    stop = round(

        last["Low"],

        2

    )

    risk = entry - stop

    if risk <= 0:

        return

    trade.set_entry(entry)

    trade.set_stop(stop)

    trade.set_target1(

        round(

            entry + (risk * 3),

            2

        )

    )

    confidence = 75

    # ==========================================
    # Volume Confirmation
    # ==========================================

    if last["RVOL"] >= 1.5:

        confidence += 10

        trade.add_reason(

            "High Relative Volume"

        )

    # ==========================================
    # MACD
    # ==========================================

    if last["MACD"] > last["MACD_SIGNAL"]:

        confidence += 5

        trade.add_reason(

            "Bullish MACD"

        )

    # ==========================================
    # Trend
    # ==========================================

    if stock.market.get(

        "TREND"

    ) == "UPTREND":

        confidence += 10

        trade.add_reason(

            "Primary Uptrend"

        )

    trade.confidence = min(

        confidence,

        100

    )

    trade.calculate_rr()

    trade.state = "READY"

    save_trade_candidate(

        trade

    )


def run_liquidity_strategy(stock):

    detect_liquidity_sweep(stock)

    create_liquidity_candidate(stock)
# =====================================================
# ==========================================================
# MARKET REGIME ENGINE
# MODULE B - PART 3 (FINAL)
# ==========================================================

ADX_TREND = 25
RVOL_HIGH = 1.50


def detect_market_regime(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < 60:
        return

    last = df.iloc[-1]
    prev = df.iloc[-2]

    regime = "SIDEWAYS"
    strength = 50

    # ------------------------------------------------------
    # Trend Detection
    # ------------------------------------------------------

    if (

        last["EMA20"] >

        last["EMA50"] >

        last["EMA200"]

    ):

        regime = "TRENDING_BULL"

        strength += 20

    elif (

        last["EMA20"] <

        last["EMA50"] <

        last["EMA200"]

    ):

        regime = "TRENDING_BEAR"

        strength += 20

    # ------------------------------------------------------
    # ADX
    # ------------------------------------------------------

    if "ADX" in df.columns:

        if last["ADX"] >= ADX_TREND:

            strength += 10

    # ------------------------------------------------------
    # Relative Volume
    # ------------------------------------------------------

    if "RVOL" in df.columns:

        if last["RVOL"] >= RVOL_HIGH:

            strength += 10

    # ------------------------------------------------------
    # ATR Expansion
    # ------------------------------------------------------

    if "ATR" in df.columns:

        atr_avg = df["ATR"].tail(20).mean()

        if last["ATR"] > atr_avg:

            strength += 10

    # ------------------------------------------------------
    # Gap Detection
    # ------------------------------------------------------

    gap_up = False
    gap_down = False

    if last["Open"] > prev["High"]:

        gap_up = True

        strength += 5

    elif last["Open"] < prev["Low"]:

        gap_down = True

        strength += 5

    # ------------------------------------------------------
    # EMA Slope
    # ------------------------------------------------------

    ema20_prev = df.iloc[-5]["EMA20"]

    if last["EMA20"] > ema20_prev:

        strength += 5

    # ------------------------------------------------------
    # Save Results
    # ------------------------------------------------------

    strength = min(

        strength,

        100

    )

    stock.market["REGIME"] = regime

    stock.market["MARKET_STRENGTH"] = strength

    stock.market["GAP_UP"] = gap_up

    stock.market["GAP_DOWN"] = gap_down

    stock.add_indicator(

        "MARKET_REGIME",

        regime

    )

    stock.add_indicator(

        "MARKET_STRENGTH",

        strength

    )

    stock.patterns["TRENDING_BULL"] = regime == "TRENDING_BULL"

    stock.patterns["TRENDING_BEAR"] = regime == "TRENDING_BEAR"

    if regime == "TRENDING_BULL":

        stock.add_score(

            "trend",

            10

        )

        stock.add_reason(

            "Bull Market"

        )

    elif regime == "TRENDING_BEAR":

        stock.add_reason(

            "Bear Market"

        )

    else:

        stock.add_reason(

            "Sideways Market"

        )


# ==========================================================
# MARKET REGIME AI BONUS
# ==========================================================

def apply_market_regime_bonus(stock, trade):

    regime = stock.market.get(

        "REGIME",

        "SIDEWAYS"

    )

    strength = stock.market.get(

        "MARKET_STRENGTH",

        50

    )

    if regime == "TRENDING_BULL":

        trade.confidence += 10

        trade.add_reason(

            "Bull Market"

        )

    elif regime == "TRENDING_BEAR":

        trade.confidence -= 15

        trade.add_reason(

            "Bear Market"

        )

    else:

        trade.confidence -= 5

        trade.add_reason(

            "Sideways Market"

        )

    if strength >= 80:

        trade.confidence += 5

        trade.add_reason(

            "Strong Trend"

        )

    trade.confidence = max(

        0,

        min(

            trade.confidence,

            100

        )

    )


# ==========================================================
# RUNNER
# ==========================================================

def run_market_regime(stock):

    detect_market_regime(stock)

register_strategy(

    "MARKET REGIME",

    run_market_regime,

    priority=5

)


# =====================================================
# BREAK OF STRUCTURE (BOS)
# CHANGE OF CHARACTER (CHOCH)
# MODULE B - PART 4
# =====================================================

BOS_LOOKBACK = 20


def detect_market_structure_shift(stock):

    df = stock.data

    if df is None:
        return

    if len(df) < BOS_LOOKBACK + 10:
        return

    last = df.iloc[-1]

    previous_high = df["High"].iloc[
        -BOS_LOOKBACK-1:-1
    ].max()

    previous_low = df["Low"].iloc[
        -BOS_LOOKBACK-1:-1
    ].min()

    bos = False

    choch = False

    direction = "NONE"

    # ===========================
    # Bullish BOS
    # ===========================

    if last["Close"] > previous_high:

        bos = True

        direction = "UP"

        stock.add_reason(
            "Bullish BOS"
        )

        stock.add_score(
            "pattern",
            15
        )

    # ===========================
    # Bearish BOS
    # ===========================

    elif last["Close"] < previous_low:

        direction = "DOWN"

    # ===========================
    # CHOCH
    # ===========================

    ema20 = last["EMA20"]

    ema50 = last["EMA50"]

    if (

        ema20 > ema50

        and

        direction == "UP"

    ):

        choch = True

        stock.add_reason(
            "CHOCH"
        )

        stock.add_score(
            "pattern",
            8
        )

    stock.patterns["BOS"] = bos

    stock.patterns["CHOCH"] = choch

    stock.market["STRUCTURE"] = direction

    stock.add_indicator(
        "BOS",
        bos
    )

    stock.add_indicator(
        "CHOCH",
        choch
    )
# =====================================================
# MULTI TIMEFRAME ENGINE
# MODULE B - PART 5
# =====================================================

TIMEFRAME_SCORE = {

    "MONTHLY":20,

    "WEEKLY":20,

    "DAILY":20

}


def evaluate_timeframe(df):

    if len(df) < 200:

        return None

    last = df.iloc[-1]

    if (

        last["EMA20"] >

        last["EMA50"] >

        last["EMA200"]

    ):

        return "UP"

    elif (

        last["EMA20"] <

        last["EMA50"] <

        last["EMA200"]

    ):

        return "DOWN"

    return "SIDEWAYS"


def detect_multi_timeframe(stock):

    if stock.data is None:

        return

    daily = stock.data.copy()

    weekly = (

        daily

        .resample("W-FRI")

        .agg({

            "Open":"first",

            "High":"max",

            "Low":"min",

            "Close":"last",

            "Volume":"sum"

        })

        .dropna()

    )

    monthly = (

        daily

        .resample("ME")

        .agg({

            "Open":"first",

            "High":"max",

            "Low":"min",

            "Close":"last",

            "Volume":"sum"

        })

        .dropna()

    )

    weekly = calculate_indicators(weekly)

    monthly = calculate_indicators(monthly)

    if weekly is None or monthly is None:

        return

    daily_trend = evaluate_timeframe(daily)

    weekly_trend = evaluate_timeframe(weekly)

    monthly_trend = evaluate_timeframe(monthly)

    stock.market["DAILY_TREND"] = daily_trend
    stock.market["WEEKLY_TREND"] = weekly_trend
    stock.market["MONTHLY_TREND"] = monthly_trend

    alignment = 0

    if daily_trend == "UP":

        alignment += 20

    if weekly_trend == "UP":

        alignment += 20

    if monthly_trend == "UP":

        alignment += 20

    stock.market["MTF_SCORE"] = alignment

    stock.add_indicator(

        "MTF_SCORE",

        alignment

    )

    if alignment >= 60:

        stock.add_reason(

            "Multi Timeframe Bullish"

        )

        stock.add_score(

            "trend",

            10

        )
# ==========================================================
# SECTOR STRENGTH ENGINE
# MODULE B - PART 6
# ==========================================================

SECTOR_ETFS = {

    "NIFTY BANK":"^NSEBANK",

    "NIFTY IT":"^CNXIT",

    "NIFTY AUTO":"^CNXAUTO",

    "NIFTY FMCG":"^CNXFMCG",

    "NIFTY PHARMA":"^CNXPHARMA",

    "NIFTY METAL":"^CNXMETAL",

    "NIFTY ENERGY":"^CNXENERGY"

}

sector_strength = {}


def calculate_sector_strength():

    global sector_strength

    sector_strength = {}

    for sector in SECTOR_ETFS:

        try:

            ticker = yf.Ticker(

                SECTOR_ETFS[sector]

            )

            df = ticker.history(

                period="2y"

            )

            if len(df) < 200:

                continue

            df = calculate_indicators(df)

            if df is None:

                continue

            last = df.iloc[-1]

            score = 0

            if last["EMA20"] > last["EMA50"]:

                score += 30

            if last["EMA50"] > last["EMA200"]:

                score += 30

            if last["MACD"] > last["MACD_SIGNAL"]:

                score += 20

            if last["RSI"] > 55:

                score += 20

            sector_strength[sector] = score

        except Exception as e:

            logging.warning(f"calculate_sector_strength failed for {sector}: {e}")

            continue


def get_sector_score(sector):

    return sector_strength.get(

        sector,

        50

    )
def apply_sector_bonus(

    stock,

    trade

):

    sector = getattr(

        stock,

        "sector",

        None

    )

    if sector is None:

        return

    score = get_sector_score(

        sector

    )

    if score >= 80:

        trade.confidence += 10

        trade.add_reason(

            "Strong Sector"

        )

    elif score >= 60:

        trade.confidence += 5

        trade.add_reason(

            "Positive Sector"

        )

    elif score <= 40:

        trade.confidence -= 5

        trade.add_reason(

            "Weak Sector"

        )

    trade.confidence = min(

        trade.confidence,

        100

    )
# ==========================================================
# BATCH 1 - ADVANCED SIGNAL ENGINES
# MULTI-TIMEFRAME / RELATIVE STRENGTH / SECTOR / VOLUME PROFILE
# ==========================================================

NIFTY_INDEX_SYMBOL = "^NSEI"

nifty_benchmark_df = None

def assign_sector(stock):
    """
    Maps a stock to a known NSE sector bucket using STOCK_SECTOR_MAP.
    Falls back to a neutral "UNKNOWN" sector when no mapping exists,
    which get_sector_score() treats as a neutral 50 score.
    """
    base = stock.symbol.replace(".NS", "")
    sector = STOCK_SECTOR_MAP.get(base, "UNKNOWN")
    stock.sector = sector
    score = get_sector_score(sector) if sector != "UNKNOWN" else 50
    stock.score["sector"] = score
    stock.add_indicator("SECTOR_NAME", sector)
    stock.add_indicator("SECTOR_SCORE", score)
    if score >= 80:
        stock.add_reason(f"Sector {sector} strong ({score}/100)")
    elif score <= 40:
        stock.add_reason(f"Sector {sector} weak ({score}/100)")
    logging.info(
        f"SECTOR symbol={stock.symbol} sector={sector} score={score}"
    )
    return sector, score


def fetch_nifty_benchmark():
    """
    Downloads the NIFTY 50 index once per scan and caches it globally,
    so per-symbol relative strength calculations don't re-download it.
    """
    global nifty_benchmark_df
    try:
        df = yf.download(
            NIFTY_INDEX_SYMBOL,
            period=CONFIG["DOWNLOAD_PERIOD"],
            interval="1d",
            progress=False,
            auto_adjust=True,
            threads=False,
        )
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if df is None or len(df) < 30:
            logging.warning("NIFTY benchmark download returned insufficient data")
            nifty_benchmark_df = None
            return None
        nifty_benchmark_df = df
        logging.info(f"NIFTY_BENCHMARK loaded rows={len(df)}")
        return df
    except Exception as e:
        logging.exception(f"fetch_nifty_benchmark failed: {e}")
        nifty_benchmark_df = None
        return None


def calculate_relative_strength(stock, lookback=63):
    """
    Relative Strength vs NIFTY: compares the stock's trailing return over
    `lookback` trading days (default ~3 months) against the index's return
    over the same window. Score is centered at 50 (in-line with the index),
    above 50 = outperforming, below 50 = underperforming.
    """
    df = stock.data
    if df is None or len(df) <= lookback:
        return None
    if nifty_benchmark_df is None or len(nifty_benchmark_df) <= lookback:
        return None
    try:
        stock_return = (
            df["Close"].iloc[-1] / df["Close"].iloc[-lookback] - 1
        ) * 100
        nifty_return = (
            nifty_benchmark_df["Close"].iloc[-1]
            / nifty_benchmark_df["Close"].iloc[-lookback]
            - 1
        ) * 100
        rs_delta = stock_return - nifty_return
        rs_score = max(0, min(100, 50 + (rs_delta * 2)))
        stock.add_indicator("RS_STOCK_RETURN_PCT", round(stock_return, 2))
        stock.add_indicator("RS_NIFTY_RETURN_PCT", round(nifty_return, 2))
        stock.add_indicator("RS_SCORE", round(rs_score, 2))
        stock.score["relative_strength"] = round(rs_score, 2)
        if rs_delta > 0:
            stock.add_reason(
                f"Outperforming NIFTY by {round(rs_delta, 2)}% over {lookback}d"
            )
        else:
            stock.add_reason(
                f"Underperforming NIFTY by {round(abs(rs_delta), 2)}% over {lookback}d"
            )
        logging.info(
            f"RELATIVE_STRENGTH symbol={stock.symbol} stock_return={round(stock_return,2)} "
            f"nifty_return={round(nifty_return,2)} rs_score={round(rs_score,2)}"
        )
        return rs_score
    except Exception as e:
        logging.exception(f"calculate_relative_strength failed for {stock.symbol}: {e}")
        return None


def _mtf_trend_label(df):
    """Shared trend classifier used for 1H / 15M timeframes."""
    if df is None or len(df) < 25:
        return "UNKNOWN"
    close = df["Close"]
    ema_fast = close.ewm(span=9).mean().iloc[-1]
    ema_slow = close.ewm(span=21).mean().iloc[-1]
    if ema_fast > ema_slow * 1.001:
        return "UPTREND"
    if ema_fast < ema_slow * 0.999:
        return "DOWNTREND"
    return "SIDEWAYS"


def analyze_multi_timeframe(stock):
    """
    Multi-Timeframe Analysis: pulls intraday 1H and 15M data for the symbol
    and classifies trend on each timeframe, then measures alignment against
    the already-computed Daily trend (stock.market["TREND"]).

    yfinance intraday limits (60m ~730d, 15m ~60d) are respected via the
    request periods below; failures are logged, never swallowed silently,
    and simply leave that timeframe as UNKNOWN rather than crashing the scan.
    """
    import pickle
    from pathlib import Path

    cache_dir = Path(__file__).resolve().parent / "data" / "mtf_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{stock.symbol.replace('.NS', '')}.pkl"
    cache_ttl = 3600
    if cache_file.exists() and (time.time() - cache_file.stat().st_mtime) < cache_ttl:
        try:
            cached = pickle.loads(cache_file.read_bytes())
            if isinstance(cached, dict) and cached.get("symbol") == stock.symbol:
                stock.add_indicator("MTF_DAILY_TREND", cached.get("daily_trend", "UNKNOWN"))
                stock.add_indicator("MTF_1H_TREND", cached.get("trend_1h", "UNKNOWN"))
                stock.add_indicator("MTF_15M_TREND", cached.get("trend_15m", "UNKNOWN"))
                stock.score["mtf_alignment"] = cached.get("alignment_score", 30)
                return cached.get("alignment_score", 30)
        except Exception:
            pass

    daily_trend = stock.market.get("TREND", "UNKNOWN")

    trend_1h = "UNKNOWN"
    trend_15m = "UNKNOWN"

    try:
        df_1h = yf.download(
            stock.symbol,
            period="60d",
            interval="60m",
            progress=False,
            auto_adjust=True,
            threads=False,
        )
        if isinstance(df_1h.columns, pd.MultiIndex):
            df_1h.columns = df_1h.columns.get_level_values(0)
        trend_1h = _mtf_trend_label(df_1h)
    except Exception as e:
        logging.warning(f"analyze_multi_timeframe 1H failed for {stock.symbol}: {e}")

    try:
        df_15m = yf.download(
            stock.symbol,
            period="5d",
            interval="15m",
            progress=False,
            auto_adjust=True,
            threads=False,
        )
        if isinstance(df_15m.columns, pd.MultiIndex):
            df_15m.columns = df_15m.columns.get_level_values(0)
        trend_15m = _mtf_trend_label(df_15m)
    except Exception as e:
        logging.warning(f"analyze_multi_timeframe 15M failed for {stock.symbol}: {e}")

    trends = [daily_trend, trend_1h, trend_15m]
    known = [t for t in trends if t != "UNKNOWN"]
    agree = len(known) > 0 and len(set(known)) == 1
    alignment_score = 100 if agree and len(known) == 3 else (
        60 if agree else 30
    )

    stock.add_indicator("MTF_DAILY_TREND", daily_trend)
    stock.add_indicator("MTF_1H_TREND", trend_1h)
    stock.add_indicator("MTF_15M_TREND", trend_15m)
    stock.score["mtf_alignment"] = alignment_score

    if agree and len(known) == 3:
        stock.add_reason("All timeframes aligned (Daily/1H/15M)")
    elif not agree:
        stock.add_reason("Timeframes conflicting (Daily/1H/15M)")

    logging.info(
        f"MULTI_TIMEFRAME symbol={stock.symbol} daily={daily_trend} "
        f"1h={trend_1h} 15m={trend_15m} alignment={alignment_score}"
    )

    try:
        cache_file.write_bytes(pickle.dumps({
            "symbol": stock.symbol,
            "daily_trend": daily_trend,
            "trend_1h": trend_1h,
            "trend_15m": trend_15m,
            "alignment_score": alignment_score,
        }))
    except Exception:
        pass

    return alignment_score


def calculate_volume_profile(stock, lookback=120, bins=20):
    """
    Volume Profile Analysis: bins Close price over the lookback window and
    sums traded Volume per bin to find the Point of Control (POC) - the
    price level with the heaviest traded volume - plus any other
    High Volume Nodes (HVN, >=70% of the POC's volume).
    """
    df = stock.data
    if df is None or len(df) < 30:
        return None
    try:
        window = df.tail(lookback)
        price_min = window["Low"].min()
        price_max = window["High"].max()
        if price_max <= price_min:
            return None

        bin_edges = np.linspace(price_min, price_max, bins + 1)
        bin_volume = np.zeros(bins)

        for _, row in window.iterrows():
            mid_price = (row["High"] + row["Low"]) / 2
            idx = np.searchsorted(bin_edges, mid_price) - 1
            idx = max(0, min(bins - 1, idx))
            bin_volume[idx] += row["Volume"]

        poc_idx = int(np.argmax(bin_volume))
        poc_price = round((bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2, 2)
        poc_volume = bin_volume[poc_idx]

        hvn_threshold = poc_volume * 0.7
        hvn_count = int(np.sum(bin_volume >= hvn_threshold))

        last_close = df["Close"].iloc[-1]
        position = "ABOVE_POC" if last_close > poc_price else (
            "BELOW_POC" if last_close < poc_price else "AT_POC"
        )

        stock.add_indicator("VOLUME_POC", poc_price)
        stock.add_indicator("VOLUME_HVN_COUNT", hvn_count)
        stock.add_indicator("VOLUME_PROFILE_POSITION", position)
        stock.score["volume_profile"] = 70 if position == "ABOVE_POC" else (
            50 if position == "AT_POC" else 30
        )

        if position == "ABOVE_POC":
            stock.add_reason(f"Price trading above Volume POC ({poc_price})")
        elif position == "BELOW_POC":
            stock.add_reason(f"Price trading below Volume POC ({poc_price})")

        logging.info(
            f"VOLUME_PROFILE symbol={stock.symbol} poc={poc_price} "
            f"hvn_count={hvn_count} position={position}"
        )

        return {
            "poc": poc_price,
            "hvn_count": hvn_count,
            "position": position,
        }
    except Exception as e:
        logging.exception(f"calculate_volume_profile failed for {stock.symbol}: {e}")
        return None


def run_batch1_signal_engines(stock):
    """
    Runs all five Batch 1 signal engines for a single stock and returns a
    combined confidence bonus (0-25) intended to be added on top of a trade
    candidate's existing confidence score inside build_ai_consensus().
    Demand & Supply zones already run as part of the strategy registry
    (run_demand_supply_strategy); this function only adds the sector bonus
    for that engine via apply_sector_bonus, called per-candidate below.
    """
    assign_sector(stock)
    analyze_multi_timeframe(stock)
    calculate_relative_strength(stock)
    calculate_volume_profile(stock)

    bonus = 0
    if stock.score.get("mtf_alignment", 0) >= 100:
        bonus += 8
    elif stock.score.get("mtf_alignment", 0) >= 60:
        bonus += 4

    rs = stock.score.get("relative_strength", 50)
    if rs >= 70:
        bonus += 6
    elif rs <= 30:
        bonus -= 4

    sector_score = stock.score.get("sector", 50)
    if sector_score >= 80:
        bonus += 5
    elif sector_score <= 40:
        bonus -= 3

    vp_score = stock.score.get("volume_profile", 50)
    if vp_score >= 70:
        bonus += 4

    bonus = max(-10, min(25, bonus))
    stock.add_indicator("BATCH1_CONFIDENCE_BONUS", bonus)
    stock.score["batch1_bonus"] = bonus
    return bonus


# =====================================================
# PHASE 2 - BATCH 2
# False Breakout Detection / Smart Money Concepts /
# Institutional Activity / News & Earnings Filter /
# AI Confidence Engine
# =====================================================

# Configurable weights: how much each Batch 2 engine moves the final
# AI Confidence score. Tune these without touching engine logic.
BATCH2_WEIGHTS = {
    "smart_money": 1.0,
    "institutional": 0.4,
    "false_breakout": 1.0,
    "news_earnings": 1.0,
}

NEWS_EARNINGS_WARNING_DAYS = 5

# Populated once per scan by prefetch_news_earnings(); avoids one
# network round-trip per symbol inside the hot per-symbol loop.
news_earnings_cache = {}

# ----------------------------------------------------------------
# News/Earnings prefetch throttling layer
# ----------------------------------------------------------------
# Yahoo Finance rate-limits aggressively at scale (observed during the
# 800-symbol Batch 2 stress test). This layer adds a disk-backed TTL
# cache (so a rescan within the TTL window makes zero network calls),
# a global request-pacing gate shared across worker threads (so we
# never exceed a fixed requests/second ceiling regardless of
# MAX_WORKERS), and per-request exponential backoff with jitter on
# failure/rate-limit. Every outcome (cache hit, retry, rate-limit,
# permanent failure) is counted and logged; nothing here is allowed to
# raise back into the scan pipeline - on total failure a symbol simply
# gets a neutral {"days_to_earnings": None, "recent_headlines": []}.


NEWS_EARNINGS_CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".cache", "news_earnings_cache.json"
)

NEWS_EARNINGS_CACHE_TTL_SECONDS = 3600  # 1 hour - balances freshness vs. request volume

NEWS_EARNINGS_MAX_REQUESTS_PER_SECOND = 4  # global ceiling, independent of MAX_WORKERS

NEWS_EARNINGS_MAX_RETRIES = 3

NEWS_EARNINGS_BACKOFF_BASE_SECONDS = 1.0

_news_earnings_rate_lock = threading.Lock()
_news_earnings_last_request_time = [0.0]

_news_earnings_stats_lock = threading.Lock()


def _news_earnings_rate_gate():
    # Global pacing gate: blocks the calling thread until issuing another
    # request would not exceed NEWS_EARNINGS_MAX_REQUESTS_PER_SECOND,
    # regardless of how many worker threads are running concurrently.
    min_interval = 1.0 / NEWS_EARNINGS_MAX_REQUESTS_PER_SECOND

    with _news_earnings_rate_lock:

        now = time.time()

        elapsed = now - _news_earnings_last_request_time[0]

        wait = min_interval - elapsed

        if wait > 0:
            time.sleep(wait)

        _news_earnings_last_request_time[0] = time.time()


def _news_earnings_is_rate_limit_error(exc):

    msg = str(exc).lower()

    return "429" in msg or "too many requests" in msg or "rate limit" in msg


def _news_earnings_load_disk_cache():

    try:
        if os.path.exists(NEWS_EARNINGS_CACHE_FILE):

            with open(NEWS_EARNINGS_CACHE_FILE, "r") as f:
                return json.load(f)

    except Exception as e:
        logging.warning(f"NEWS_EARNINGS_CACHE load failed err={e}")

    return {}


def _news_earnings_save_disk_cache(cache):

    try:
        os.makedirs(os.path.dirname(NEWS_EARNINGS_CACHE_FILE), exist_ok=True)

        with open(NEWS_EARNINGS_CACHE_FILE, "w") as f:
            json.dump(cache, f)

    except Exception as e:
        logging.warning(f"NEWS_EARNINGS_CACHE save failed err={e}")


def detect_false_breakout(stock, check_window=5):
    """
    Flags breakouts that already failed (price broke the resistance
    level in the recent past, then closed back below it), plus
    exhaustion on the CURRENT breakout candle (weak close, low volume).
    Returns a confidence penalty (<= 0).
    """
    df = stock.data

    if df is None or len(df) < BREAKOUT_LOOKBACK + check_window + 5:
        stock.patterns["FALSE_BREAKOUT"] = False
        return 0

    false_breakout = False

    n = len(df)

    start = max(BREAKOUT_LOOKBACK + 1, n - check_window - 5)

    for idx in range(start, n):

        resistance = df["High"].iloc[idx - BREAKOUT_LOOKBACK:idx].max()

        breakout_price = resistance * (1 + BREAKOUT_BUFFER)

        candle = df.iloc[idx]

        if candle["Close"] >= breakout_price:

            after = df.iloc[idx + 1: idx + 1 + check_window]

            if len(after) and (after["Close"] < resistance).any():
                false_breakout = True
                break

    last = df.iloc[-1]

    exhaustion = False

    if stock.patterns.get("BREAKOUT_READY"):

        candle_range = last["High"] - last["Low"]

        if candle_range > 0:

            close_position = (last["Close"] - last["Low"]) / candle_range

            if close_position <= 0.3 and last.get("RVOL", 1) < 1.2:
                exhaustion = True

    penalty = 0

    if false_breakout:
        penalty -= 15
        stock.add_reason(
            "[FalseBreakout] Prior breakout failed - price reclaimed below resistance"
        )

    if exhaustion:
        penalty -= 8
        stock.add_reason(
            "[FalseBreakout] Exhaustion candle on breakout - weak close, low volume"
        )

    stock.patterns["FALSE_BREAKOUT"] = false_breakout
    stock.patterns["BREAKOUT_EXHAUSTION"] = exhaustion
    stock.score["false_breakout_penalty"] = penalty

    logging.info(
        f"FALSE_BREAKOUT symbol={stock.symbol} false_breakout={false_breakout} "
        f"exhaustion={exhaustion} penalty={penalty}"
    )

    return penalty


def analyze_smart_money_concepts(stock):
    """
    Aggregates the Smart Money Concept patterns that already run earlier
    in the pipeline (BOS/CHOCH via update_market_structure(), Order
    Blocks / Liquidity Sweeps / Fair Value Gaps via their strategy
    registry entries) into a single explainable score. Must be called
    AFTER run_all_strategies(stock) so those patterns are populated.
    """
    score = 0

    if stock.patterns.get("BOS"):
        score += 15
        stock.add_reason("[SmartMoney] Break of Structure (bullish)")

    if stock.patterns.get("BEARISH_BOS"):
        score -= 10
        stock.add_reason("[SmartMoney] Bearish Break of Structure")

    if stock.patterns.get("CHOCH"):
        score += 8
        stock.add_reason("[SmartMoney] Change of Character (possible trend shift)")

    bullish_ob = len(stock.patterns.get("BULLISH_ORDER_BLOCKS", []))
    bearish_ob = len(stock.patterns.get("BEARISH_ORDER_BLOCKS", []))

    if bullish_ob:
        score += min(bullish_ob * 3, 9)
        stock.add_reason(
            f"[SmartMoney] {bullish_ob} Bullish Order Block(s) identified"
        )

    if bearish_ob > bullish_ob:
        score -= 5
        stock.add_reason("[SmartMoney] Bearish Order Blocks dominate")

    if stock.patterns.get("BULLISH_SWEEP"):
        score += 10
        stock.add_reason("[SmartMoney] Bullish Liquidity Sweep (stop hunt reversal)")

    if stock.patterns.get("BEARISH_SWEEP"):
        score -= 6
        stock.add_reason("[SmartMoney] Bearish Liquidity Sweep")

    bullish_fvg = len(stock.patterns.get("BULLISH_FVG", []))
    bearish_fvg = len(stock.patterns.get("BEARISH_FVG", []))

    if bullish_fvg:
        score += min(bullish_fvg * 2, 6)
        stock.add_reason(
            f"[SmartMoney] {bullish_fvg} Bullish Fair Value Gap(s) unfilled"
        )

    if bearish_fvg > bullish_fvg:
        score -= 3

    score = max(-15, min(30, score))

    stock.add_indicator("SMART_MONEY_SCORE", score)
    stock.score["smart_money"] = score

    logging.info(
        f"SMART_MONEY symbol={stock.symbol} score={score} "
        f"bos={stock.patterns.get('BOS', False)} choch={stock.patterns.get('CHOCH', False)} "
        f"bullish_ob={bullish_ob} bearish_ob={bearish_ob} "
        f"bullish_sweep={stock.patterns.get('BULLISH_SWEEP', False)} "
        f"bearish_sweep={stock.patterns.get('BEARISH_SWEEP', False)} "
        f"bullish_fvg={bullish_fvg} bearish_fvg={bearish_fvg}"
    )

    return score


def analyze_institutional_activity(stock, lookback=20):
    """
    NSE delivery-percentage / bulk-and-block-deal data is not available
    through the yfinance data source this app uses, so institutional
    activity is approximated from price/volume behaviour instead:
    On-Balance Volume trend, a Chaikin-style Accumulation/Distribution
    Line, volume Z-score anomalies, and high-volume narrow-range
    'absorption' candles. This is a documented proxy, not delivery %.
    """
    df = stock.data

    if df is None or len(df) < lookback + 5:
        stock.score["institutional"] = 50
        return 50

    recent = df.iloc[-(lookback + 1):]

    close = recent["Close"]
    volume = recent["Volume"]
    high = recent["High"]
    low = recent["Low"]

    obv = [0.0]

    for i in range(1, len(recent)):

        if close.iloc[i] > close.iloc[i - 1]:
            obv.append(obv[-1] + volume.iloc[i])
        elif close.iloc[i] < close.iloc[i - 1]:
            obv.append(obv[-1] - volume.iloc[i])
        else:
            obv.append(obv[-1])

    obv_rising = (obv[-1] - obv[0]) > 0

    price_range = (high - low).replace(0, np.nan)

    mfm = ((close - low) - (high - close)) / price_range
    mfm = mfm.fillna(0)

    adl = (mfm * volume).cumsum()

    adl_trend = float(adl.iloc[-1] - adl.iloc[0])

    vol_mean = volume.iloc[:-1].mean()
    vol_std = volume.iloc[:-1].std()

    if vol_std and vol_std > 0:
        vol_z = float((volume.iloc[-1] - vol_mean) / vol_std)
    else:
        vol_z = 0.0

    last_range = float(high.iloc[-1] - low.iloc[-1])
    avg_range = float((high - low).iloc[:-1].mean())

    absorption = False

    if vol_z >= 2 and avg_range > 0 and last_range <= avg_range * 0.7:

        close_pos = (
            (close.iloc[-1] - low.iloc[-1]) / last_range
            if last_range > 0 else 0.5
        )

        if close_pos >= 0.6:
            absorption = True

    score = 50

    if obv_rising and adl_trend > 0:
        score += 15
        stock.add_reason(
            "[Institutional] OBV and Accumulation/Distribution both rising (buying pressure)"
        )
    elif (not obv_rising) and adl_trend < 0:
        score -= 15
        stock.add_reason(
            "[Institutional] OBV and Accumulation/Distribution both falling (distribution pressure)"
        )
    elif obv_rising or adl_trend > 0:
        score += 6
    else:
        score -= 6

    if vol_z >= 2:
        score += 8
        stock.add_reason(
            f"[Institutional] Unusual volume spike (z={round(vol_z, 1)}) - possible institutional activity"
        )

    if absorption:
        score += 10
        stock.add_reason(
            "[Institutional] High-volume narrow-range absorption candle detected"
        )

    score = max(0, min(100, score))

    stock.add_indicator("OBV_TREND", "RISING" if obv_rising else "FALLING")
    stock.add_indicator("ADL_TREND", round(adl_trend, 2))
    stock.add_indicator("VOLUME_ZSCORE", round(vol_z, 2))
    stock.add_indicator("ABSORPTION_DAY", absorption)
    stock.score["institutional"] = score

    logging.info(
        f"INSTITUTIONAL_ACTIVITY symbol={stock.symbol} score={score} "
        f"obv_trend={'RISING' if obv_rising else 'FALLING'} "
        f"adl_trend={round(adl_trend, 2)} vol_z={round(vol_z, 2)} absorption={absorption}"
    )

    return score


def prefetch_news_earnings(symbols):
    """
    Fetches earnings-calendar and recent-news data for every symbol in
    the scan ONCE, concurrently, before the per-symbol loop runs -
    exactly like fetch_nifty_benchmark() does for the RS benchmark.
    check_news_earnings_filter() then reads from this cache instead of
    making a network call per symbol inside the hot loop.

    Throttling layer (Batch 2 completion):
      - Disk-backed TTL cache: a symbol fetched within the last
        NEWS_EARNINGS_CACHE_TTL_SECONDS is served from disk with ZERO
        network calls.
      - Global rate gate: all worker threads share one pacing gate
        capping requests to NEWS_EARNINGS_MAX_REQUESTS_PER_SECOND,
        independent of CONFIG["MAX_WORKERS"].
      - Exponential backoff with jitter: on failure (rate-limit or
        transient error) a request is retried up to
        NEWS_EARNINGS_MAX_RETRIES times with delay
        base * 2**attempt + random jitter.
      - Total failure after retries never raises - the symbol gets a
        neutral entry ({"days_to_earnings": None, "recent_headlines": []})
        and the pipeline continues.
    """
    global news_earnings_cache

    disk_cache = _news_earnings_load_disk_cache()

    now_ts = time.time()

    stats = {
        "cache_hits": 0,
        "cache_misses": 0,
        "requests_attempted": 0,
        "retries": 0,
        "rate_limited": 0,
        "permanent_failures": 0,
    }

    fresh_cache = {}
    to_fetch = []

    for symbol in symbols:

        entry = disk_cache.get(symbol)

        if entry and (now_ts - entry.get("fetched_at", 0)) < NEWS_EARNINGS_CACHE_TTL_SECONDS:
            fresh_cache[symbol] = entry["data"]
            stats["cache_hits"] += 1
        else:
            to_fetch.append(symbol)
            stats["cache_misses"] += 1

    def _fetch_one(symbol):

        from datetime import date

        last_exc = None

        for attempt in range(NEWS_EARNINGS_MAX_RETRIES + 1):

            try:
                _news_earnings_rate_gate()

                with _news_earnings_stats_lock:
                    stats["requests_attempted"] += 1

                ticker = yf.Ticker(symbol)

                days_to_earnings = None

                try:
                    cal = ticker.calendar

                    earnings_dates = cal.get("Earnings Date") if isinstance(cal, dict) else None

                    if earnings_dates:

                        if not isinstance(earnings_dates, list):
                            earnings_dates = [earnings_dates]

                        today = date.today()

                        upcoming = [d for d in earnings_dates if d >= today]

                        if upcoming:
                            days_to_earnings = (min(upcoming) - today).days

                except Exception as e:
                    logging.warning(f"NEWS_EARNINGS calendar fetch failed symbol={symbol} err={e}")

                recent_headlines = []

                try:
                    news_items = ticker.news or []

                    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

                    for item in news_items[:10]:

                        content = item.get('content', item)

                        pub = content.get('pubDate')
                        title = content.get('title')

                        if not pub or not title:
                            continue

                        try:
                            pub_dt = datetime.fromisoformat(pub.replace('Z', '+00:00'))
                        except Exception:
                            continue

                        if pub_dt >= cutoff:
                            recent_headlines.append(title)

                except Exception as e:
                    logging.warning(f"NEWS_EARNINGS news fetch failed symbol={symbol} err={e}")

                return symbol, {
                    "days_to_earnings": days_to_earnings,
                    "recent_headlines": recent_headlines,
                }

            except Exception as e:

                last_exc = e

                is_rate_limit = _news_earnings_is_rate_limit_error(e)

                with _news_earnings_stats_lock:
                    if is_rate_limit:
                        stats["rate_limited"] += 1

                if attempt < NEWS_EARNINGS_MAX_RETRIES:

                    with _news_earnings_stats_lock:
                        stats["retries"] += 1

                    delay = NEWS_EARNINGS_BACKOFF_BASE_SECONDS * (2 ** attempt) + random.uniform(0, 0.5)

                    logging.warning(
                        f"NEWS_EARNINGS retry symbol={symbol} attempt={attempt + 1} "
                        f"rate_limited={is_rate_limit} delay={round(delay, 2)}s err={e}"
                    )

                    time.sleep(delay)

                    continue

        with _news_earnings_stats_lock:
            stats["permanent_failures"] += 1

        logging.warning(
            f"NEWS_EARNINGS prefetch permanently failed symbol={symbol} "
            f"after {NEWS_EARNINGS_MAX_RETRIES} retries err={last_exc}"
        )

        return symbol, {"days_to_earnings": None, "recent_headlines": []}

    fetched_results = {}

    if to_fetch:

        fetch_workers = min(CONFIG["MAX_WORKERS"], NEWS_EARNINGS_MAX_REQUESTS_PER_SECOND * 2, len(to_fetch))
        fetch_workers = max(fetch_workers, 1)

        with ThreadPoolExecutor(max_workers=fetch_workers) as executor:

            for symbol, result in executor.map(_fetch_one, to_fetch):
                fetched_results[symbol] = result

    news_earnings_cache = {}
    news_earnings_cache.update(fresh_cache)
    news_earnings_cache.update(fetched_results)

    fetched_at = time.time()

    for symbol, result in fetched_results.items():
        disk_cache[symbol] = {"fetched_at": fetched_at, "data": result}

    stale_cutoff = fetched_at - (NEWS_EARNINGS_CACHE_TTL_SECONDS * 24)

    disk_cache = {
        sym: entry for sym, entry in disk_cache.items()
        if entry.get("fetched_at", 0) >= stale_cutoff
    }

    _news_earnings_save_disk_cache(disk_cache)

    cache_hit_ratio = (
        round(stats["cache_hits"] / len(symbols) * 100, 1) if symbols else 0.0
    )

    logging.info(
        f"NEWS_EARNINGS_PREFETCH symbols={len(symbols)} "
        f"cached={len(news_earnings_cache)} "
        f"cache_hits={stats['cache_hits']} cache_misses={stats['cache_misses']} "
        f"cache_hit_ratio={cache_hit_ratio}% "
        f"requests_attempted={stats['requests_attempted']} "
        f"retries={stats['retries']} rate_limited={stats['rate_limited']} "
        f"permanent_failures={stats['permanent_failures']}"
    )

    return stats
def check_news_earnings_filter(stock):
    """
    Reads the pre-fetched earnings/news cache for this symbol and turns
    it into a confidence penalty: heavy penalty for entries right before
    an earnings release, a lighter caution penalty for an unusually
    busy news cycle (higher volatility risk).
    """
    cached = news_earnings_cache.get(stock.symbol, {})

    days_to_earnings = cached.get("days_to_earnings")
    recent_headlines = cached.get("recent_headlines", [])

    stock.add_indicator("DAYS_TO_EARNINGS", days_to_earnings)
    stock.add_indicator("RECENT_NEWS_COUNT", len(recent_headlines))

    penalty = 0

    if days_to_earnings is not None and days_to_earnings <= NEWS_EARNINGS_WARNING_DAYS:
        penalty -= 15
        stock.add_news(
            "EARNINGS_WARNING",
            f"Earnings in {days_to_earnings} day(s) - avoid new entry"
        )
        stock.add_reason(
            f"[News/Earnings] Earnings in {days_to_earnings}d - confidence reduced, avoid fresh entries"
        )

    if len(recent_headlines) >= 3:
        penalty -= 3
        stock.add_news(
            "NEWS_VOLUME_WARNING",
            f"{len(recent_headlines)} news items in last 48h - elevated volatility risk"
        )
        stock.add_reason(
            f"[News/Earnings] {len(recent_headlines)} recent headlines - caution advised"
        )

    for i, headline in enumerate(recent_headlines[:3]):
        stock.add_news(f"HEADLINE_{i}", headline)

    penalty = max(-20, penalty)

    stock.score["news_earnings"] = penalty

    logging.info(
        f"NEWS_EARNINGS symbol={stock.symbol} days_to_earnings={days_to_earnings} "
        f"recent_news={len(recent_headlines)} penalty={penalty}"
    )

    return penalty


def run_batch2_signal_engines(stock):
    """
    Runs False Breakout Detection, Smart Money Concepts aggregation,
    Institutional Activity Analysis and the News/Earnings Filter for a
    single stock, combines them with BATCH2_WEIGHTS into one AI
    Confidence score (0-100) and a matching bonus/penalty intended to
    be added onto a trade candidate's ai_score inside
    build_ai_consensus() - exactly like run_batch1_signal_engines().
    Must be called AFTER run_all_strategies(stock) so BOS/CHOCH/Order
    Block/Liquidity Sweep/FVG patterns are already populated.
    """
    fb_penalty = detect_false_breakout(stock)
    sm_score = analyze_smart_money_concepts(stock)
    inst_score = analyze_institutional_activity(stock)
    news_penalty = check_news_earnings_filter(stock)

    bonus = 0.0

    bonus += sm_score * BATCH2_WEIGHTS["smart_money"]
    bonus += (inst_score - 50) * BATCH2_WEIGHTS["institutional"]
    bonus += fb_penalty * BATCH2_WEIGHTS["false_breakout"]
    bonus += news_penalty * BATCH2_WEIGHTS["news_earnings"]

    bonus = max(-30, min(30, round(bonus, 1)))

    ai_confidence = max(0, min(100, round(50 + bonus, 1)))

    stock.add_indicator("BATCH2_CONFIDENCE_BONUS", bonus)
    stock.add_indicator("AI_CONFIDENCE", ai_confidence)
    stock.score["batch2_bonus"] = bonus
    stock.score["ai_confidence"] = ai_confidence

    stock.add_reason(
        f"[AI Confidence Engine] {ai_confidence}/100 "
        f"(SmartMoney={sm_score}, Institutional={inst_score}, "
        f"FalseBreakout={fb_penalty}, News/Earnings={news_penalty})"
    )

    logging.info(
        f"AI_CONFIDENCE symbol={stock.symbol} ai_confidence={ai_confidence} "
        f"bonus={bonus} smart_money={sm_score} institutional={inst_score} "
        f"false_breakout_penalty={fb_penalty} news_earnings_penalty={news_penalty}"
    )

    return bonus


# ==========================================================
# EXECUTIVE MARKET DASHBOARD
# MODULE C - PART 1
# ==========================================================

def show_market_dashboard():

    st.divider()

    st.header("📊 AlphaQuant Executive Dashboard")

    total_scanned = len(st.session_state.market_data)

    qualified = len(st.session_state.trade_candidates)

    selected = len(st.session_state.selected_portfolio)

    open_positions = len(st.session_state.paper_positions)

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Stocks Scanned",
        total_scanned
    )

    c2.metric(
        "Trade Candidates",
        qualified
    )

    c3.metric(
        "Portfolio",
        selected
    )

    c4.metric(
        "Live Positions",
        open_positions
    )

    st.divider()
def show_ai_summary():

    if len(st.session_state.final_trade_list)==0:

        return

    st.subheader("Top AI Opportunities")

    rows=[]

    for trade in st.session_state.final_trade_list[:10]:

        rows.append({

            "Rank":len(rows)+1,

            "Stock":trade.symbol,

            "Strategy":trade.strategy,

            "AI Score":round(trade.ai_score,2),

            "Confidence":trade.confidence,

            "RR":trade.risk_reward,

            "State":trade.state

        })

    st.dataframe(

        pd.DataFrame(rows),

        width="stretch",

        hide_index=True

    )
def show_portfolio_summary():

    st.subheader("Portfolio Allocation")

    if len(st.session_state.selected_portfolio)==0:

        st.info("No Portfolio")

        return

    rows=[]

    for trade in st.session_state.selected_portfolio:

        rows.append({

            "Stock":trade.symbol,

            "Weight %":trade.portfolio_weight,

            "Capital":trade.capital_required,

            "Entry":trade.entry,

            "Target":trade.target1,

            "RR":trade.risk_reward

        })

    st.dataframe(

        pd.DataFrame(rows),

        width="stretch",

        hide_index=True

    )
def show_live_positions():

    st.subheader("Live Positions")

    rows=[]

    for p in st.session_state.paper_positions.values():

        rows.append({

            "Stock":p.symbol,

            "Strategy":p.strategy,

            "Entry":p.entry,

            "Qty":p.quantity,

            "Status":p.status

        })

    if len(rows):

        st.dataframe(

            pd.DataFrame(rows),

            width="stretch",

            hide_index=True

        )

    else:

        st.info(

            "No Open Positions"

        )
# =====================================================
# ALPHAQUANT OS - PHASE 1 (READ-ONLY)
# =====================================================
# Brains 1-3 (Market Observer, Market Historian, Historical Analog Engine)
# are surfaced here purely as read-only context for a human to inspect.
# This panel never calls build_ai_consensus, never mutates scoring/ranking,
# and never gates or auto-vetoes a trade candidate - it only reports what
# AlphaQuant OS already knows. Any failure here is caught and shown as a
# warning so it can never break the existing scan/consensus workflow above.


def show_alphaquant_os_panel():

    with st.expander("AlphaQuant OS - Historical Context (Phase 1, read-only)"):

        try:
            apply_schema = __import__("os_brains.db", fromlist=["apply_schema"]).apply_schema
            from os_brains.market_historian import get_regime_context, seed_regime_catalog
            from os_brains.market_observer import observe
            from os_brains.historical_analog_engine import find_analogs
            from os_brains.setup_vector import (
                build_setup_vector_row,
                compute_pattern_flag_series,
                compute_relative_strength_series,
            )
        except Exception as e:
            st.info(f"AlphaQuant OS modules unavailable: {e}")
            return

        # Idempotent bootstrap so this panel works on a clean environment
        # with zero manual DB prep - safe to call every render
        # (CREATE ... IF NOT EXISTS / ON CONFLICT DO UPDATE), gated on a
        # session flag so it only actually runs once per session.
        if not st.session_state.get("alphaquant_os_bootstrapped", False):
            try:
                apply_schema()
                seed_regime_catalog()
                st.session_state["alphaquant_os_bootstrapped"] = True
            except Exception as e:
                st.warning(f"AlphaQuant OS database bootstrap failed: {e}")
                return

        stocks = st.session_state.get("stock_objects", {})
        symbols_with_data = [
            s for s, obj in stocks.items() if obj.data is not None and len(obj.data)
        ]

        if not symbols_with_data:
            st.caption(
                "Run a scan first, then pick a scanned symbol here to see its "
                "regime context and closest historical analogs."
            )
            return

        symbol = st.selectbox(
            "Symbol",
            sorted(symbols_with_data),
            key="alphaquant_os_symbol_select",
        )

        stock = stocks[symbol]

        try:
            if "REGIME" not in stock.market:
                detect_market_regime(stock)

            regime_ctx = get_regime_context(stock)

            st.markdown(f"**Current regime:** {regime_ctx['current_regime']} "
                        f"(strength {regime_ctx['current_regime_strength']})")

            for entry in regime_ctx["nearest_historical_regimes"]:
                st.write(
                    f"- {entry['regime_name']} ({entry['date_range']}) - "
                    f"similarity {entry['similarity_score']} - {entry['how_it_resolved']}"
                )
        except Exception as e:
            st.warning(f"Market Historian unavailable: {e}")

        try:
            observation = observe(stock, sys.modules[__name__])
            st.caption(
                f"Sector: {observation['sector']['name']} | "
                f"Relative strength vs NIFTY: {observation['relative_strength']}"
            )
        except Exception as e:
            st.warning(f"Market Observer unavailable: {e}")

        try:
            nifty_df = nifty_benchmark_df
            if nifty_df is None:
                fetch_nifty_benchmark()
                nifty_df = nifty_benchmark_df

            if nifty_df is not None:
                flags = compute_pattern_flag_series(stock.data)
                rs_series = compute_relative_strength_series(stock.data, nifty_df)
                vector = build_setup_vector_row(
                    stock.data, len(stock.data) - 1, flags, rs_series
                )
                if vector is not None:
                    report = find_analogs(
                        symbol, vector, as_of_date=stock.data.index[-1].date()
                    )
                    if report["matched_analogs_count"] > 0:
                        st.markdown(
                            f"**Closest historical analogs:** "
                            f"{report['matched_analogs_count']} matches "
                            f"({report['sample_confidence']} confidence)"
                        )
                        st.write(
                            f"- Typical holding period: {report['typical_holding_period_days']}d | "
                            f"Win rate: {round(report['win_rate'] * 100, 1)}% | "
                            f"Expected return: {round(report['expected_return'] * 100, 2)}% | "
                            f"Expected drawdown: {round(report['expected_drawdown'] * 100, 2)}%"
                        )
                        st.caption(
                            "For context only - Phase 1 does not feed this into "
                            "trade scoring, ranking, or vetoes."
                        )
                    else:
                        st.caption(
                            "No sufficiently similar historical setups found yet "
                            "(backfilled dataset is still small in Phase 1)."
                        )
        except Exception as e:
            st.warning(f"Historical Analog Engine unavailable: {e}")


def show_mission_control():
    st.divider()
    st.header("Mission Control")

    if st.session_state.get("last_cycle_time"):
        st.caption(
            f"Last run: {st.session_state.last_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"({st.session_state.get('last_cycle_trigger', 'UNKNOWN')})"
        )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Universe", len(st.session_state.get("scan_universe", [])))
    c2.metric("Downloaded", len(st.session_state.get("market_data", {})))
    c3.metric("Candidates", len(st.session_state.get("trade_candidates", {})))
    c4.metric("Final Trades", len(st.session_state.get("selected_portfolio", [])))

    tab_progress, tab_funnel, tab_trades, tab_learning, tab_portfolio, tab_health, tab_errors = st.tabs([
        "Live Progress", "Decision Funnel", "Final Trades", "Learning & Reviewer", "Portfolio", "System Health", "Errors"
    ])

    with tab_progress:
        if st.session_state.get("brain_status"):
            st.dataframe(
                pd.DataFrame([
                    {"Brain/Stage": k, "Status": v}
                    for k, v in st.session_state.brain_status.items()
                ]),
                width="stretch",
            )
        monitor = PipelineMonitor().update()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Current Phase", monitor.get("current_phase", "IDLE"))
        m2.metric("Completed", len(monitor.get("completed_phases", [])))
        m3.metric("Failed", len(monitor.get("failed_phases", [])))
        m4.metric("Runtime (sec)", monitor.get("pipeline_runtime", 0.0))
        if monitor.get("estimated_completion"):
            st.caption(f"Estimated completion: {monitor['estimated_completion'].strftime('%Y-%m-%d %H:%M:%S')}")
        phases = st.session_state.get("pipeline_phases", {})
        if phases:
            phase_rows = []
            for phase in phases.values():
                phase_rows.append({
                    "Phase": phase.phase_name,
                    "Status": phase.status,
                    "Progress %": phase.progress_percent,
                    "Started": phase.started_at.strftime("%H:%M:%S") if phase.started_at else "",
                    "Completed": phase.completed_at.strftime("%H:%M:%S") if phase.completed_at else "",
                    "Duration": phase.duration,
                    "Message": phase.message,
                })
                st.progress(int(phase.progress_percent), text=f"{phase.phase_name}: {phase.status}")
            st.dataframe(pd.DataFrame(phase_rows), width="stretch")
        if st.session_state.get("pipeline_events"):
            st.dataframe(pd.DataFrame(st.session_state.pipeline_events), width="stretch")
        else:
            st.info("Press RUN ALPHAQUANT to start the complete workflow.")

    with tab_funnel:
        if st.session_state.get("decision_funnel"):
            st.dataframe(pd.DataFrame(st.session_state.decision_funnel), width="stretch")
        else:
            st.info("Decision funnel will populate after RUN ALPHAQUANT.")
        if not st.session_state.get("selected_portfolio") and st.session_state.get("no_trade_explanation"):
            st.warning("No trade today.")
            for reason in st.session_state.no_trade_explanation:
                st.write(f"- {reason}")

    with tab_trades:
        final_df = get_final_trade_dataframe()
        if len(final_df):
            st.dataframe(final_df, width="stretch")
        elif st.session_state.get("no_trade_explanation"):
            st.warning("No trade today.")
            for reason in st.session_state.no_trade_explanation:
                st.write(f"- {reason}")
        else:
            st.info("Final trades will appear after the pipeline completes.")

    with tab_learning:
        st.subheader("Learning Updates")
        if st.session_state.get("no_trade_explanation"):
            for reason in st.session_state.no_trade_explanation:
                st.write(f"- {reason}")
        st.subheader("Reviewer Output")
        if st.session_state.get("closed_positions"):
            st.write(f"Closed positions available for reviewer: {len(st.session_state.closed_positions)}")
        else:
            st.caption("No closed paper trades have reached reviewer yet.")

    with tab_portfolio:
        pdf = portfolio_dataframe()
        if len(pdf):
            st.dataframe(pdf, width="stretch")
        else:
            st.caption("No allocated portfolio positions from the latest run.")

    with tab_health:
        health = SystemHealthEngine().update()
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("CPU", "N/A" if health.get("cpu_percent") is None else f"{health['cpu_percent']}%")
        h2.metric("Memory", "N/A" if health.get("memory_percent") is None else f"{health['memory_percent']}%")
        h3.metric("Pipeline Runtime", health.get("pipeline_runtime", 0.0))
        h4.metric("Startup Time", health.get("startup_time").strftime("%Y-%m-%d %H:%M:%S") if health.get("startup_time") else "")
        st.dataframe(pd.DataFrame([
            {"Area": "Broker Status", "Value": "Connected" if st.session_state.get("paper_broker", {}).get("connected") else "Disconnected"},
            {"Area": "Live Data Status", "Value": f"{sum(len(v) for v in st.session_state.get('market_data_manager', {}).get('live_ticks', {}).values())} cached ticks"},
            {"Area": "Historical Data Status", "Value": f"{len(st.session_state.get('market_data_manager', {}).get('history', {}))} cached symbols"},
            {"Area": "Paper Trading Status", "Value": f"{len(st.session_state.get('paper_positions', {}))} open positions"},
            {"Area": "Open Positions", "Value": len(st.session_state.get('paper_positions', {}))},
            {"Area": "PnL", "Value": st.session_state.get('paper_broker', {}).get('realized_pnl', 0.0)},
            {"Area": "Risk", "Value": st.session_state.get('paper_broker', {}).get('risk', {})},
            {"Area": "Last Scan", "Value": health.get("last_scan")},
            {"Area": "Last Strategy Run", "Value": health.get("last_strategy_run")},
            {"Area": "Last Broker Sync", "Value": health.get("last_broker_sync")},
            {"Area": "Last Live Tick", "Value": health.get("last_live_tick")},
        ]), width="stretch")

    with tab_errors:
        errors = CentralErrorManager().get_errors()
        if errors:
            st.dataframe(pd.DataFrame([{
                "Timestamp": e.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "Phase": e.phase,
                "Severity": e.severity,
                "Message": e.message,
                "Status": "Resolved" if e.resolved else "Open",
            } for e in errors]), width="stretch")
        else:
            st.success("No pipeline errors recorded.")


if _P("Developer Mode"):
    show_mission_control()


# =====================================================
# COMPACT TERMINAL WORKSPACES
# =====================================================
_INDEX_TICKERS = {
    "NIFTY 50": "^NSEI", "BANK NIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
    "NIFTY MIDCAP": "^NSEMDCP50", "NIFTY SMALLCAP": "^CNXSC", "INDIA VIX": "^INDIAVIX",
}

def _quote_from_frame(df):
    if df is None or df.empty or "Close" not in df: return None
    close = pd.to_numeric(df["Close"], errors="coerce").dropna()
    if len(close) < 1: return None
    value=float(close.iloc[-1]); previous=float(close.iloc[-2]) if len(close)>1 else value
    change=value-previous
    return {"value":value,"change":change,"pct":change/previous*100 if previous else 0.0}

def refresh_terminal_quotes(force=False):
    ms = get_market_state().snapshot()
    if ms.get("quotes") and ms.get("data_source") in {"BROKER_LIVE", "BROKER_SNAPSHOT"}:
        return ms["quotes"]
    shared = get_broker_state().snapshot()
    if shared["quotes"] and shared["data_source"] in {"BROKER_LIVE", "BROKER_SNAPSHOT"}:
        return shared["quotes"]
    if st.session_state.get("terminal_quotes") and not force:
        return st.session_state.terminal_quotes
    quotes={}; source="YFINANCE_INTRADAY_FALLBACK"
    try:
        raw=yf.download(list(_INDEX_TICKERS.values()), period="1d", interval="5m", group_by="ticker", progress=False, threads=True, auto_adjust=False)
        for name,ticker in _INDEX_TICKERS.items():
            frame = raw[ticker] if isinstance(raw.columns,pd.MultiIndex) and ticker in raw.columns.get_level_values(0) else raw
            q=_quote_from_frame(frame)
            if q:
                stamp = frame.index[-1] if len(frame.index) else datetime.now(timezone.utc)
                quotes[name]={"symbol":name,"instrument_key":ticker,"ltp":q["value"],"value":q["value"],
                    "open":None,"high":None,"low":None,"previous_close":q["value"]-q["change"],
                    "change":q["change"],"change_percent":q["pct"],"pct":q["pct"],"volume":None,
                    "timestamp":stamp,"received_at":datetime.now(timezone.utc),"source":source,"is_stale":False}
    except Exception as exc:
        logging.warning("Terminal index strip refresh failed: %s", exc)
    st.session_state.terminal_quotes=quotes
    st.session_state.terminal_quote_time=datetime.now()
    st.session_state.terminal_quote_source=source
    if quotes: get_broker_state().publish_quotes(quotes, source)
    return quotes

def render_ticker_strip():
    """Render the same compact, product-safe market strip below every route."""
    quotes=refresh_terminal_quotes(False); broker_state=get_broker_state().snapshot()
    source=broker_state.get("data_source","UNAVAILABLE")
    cells=[]; ages=[]; stamps=[]
    for name in _INDEX_TICKERS:
        q=quotes.get(name); raw_time=q.get("received_at") if q else None
        if isinstance(raw_time,str): raw_time=pd.to_datetime(raw_time,utc=True,errors="coerce")
        if isinstance(raw_time,pd.Timestamp): raw_time=raw_time.to_pydatetime()
        if isinstance(raw_time,datetime):
            if raw_time.tzinfo is None: raw_time=raw_time.replace(tzinfo=timezone.utc)
            ages.append(max(0,(datetime.now(timezone.utc)-raw_time).total_seconds())); stamps.append(raw_time)
        value_raw=q.get("ltp",q.get("value")) if q else None
        value=f"{value_raw:,.2f}" if value_raw is not None else "—"
        if q and q.get("change") is not None and q.get("change_percent") is not None:
            change=float(q.get("change") or 0); pct=float(q.get("change_percent") or 0)
            change_text=f"{change:+.2f} ({pct:+.2f}%)"
        else:
            change_text="N/A"
        cells.append(f'<div class="aq-tick"><strong>{name}</strong>{value} <span>{change_text}</span></div>')
    st.markdown('<div class="aq-ticker">'+''.join(cells)+'</div>',unsafe_allow_html=True)
    age=max(ages) if ages else None
    data_status=("OFFLINE" if not quotes else "DELAYED" if source=="YFINANCE_INTRADAY_FALLBACK" else
        "STALE" if age is not None and age>(10 if source=="BROKER_LIVE" else 30) else
        "LIVE" if source=="BROKER_LIVE" else "NEAR LIVE" if source=="BROKER_SNAPSHOT" else "STALE")
    label={"BROKER_LIVE":"UPSTOX","BROKER_SNAPSHOT":"UPSTOX SNAPSHOT","YFINANCE_INTRADAY_FALLBACK":"YAHOO FINANCE FALLBACK","HISTORICAL_CACHE":"HISTORICAL CACHE","UNAVAILABLE":"UNAVAILABLE"}.get(source,"UNAVAILABLE")
    last=max(stamps).strftime("%H:%M:%S") if stamps else "NEVER"; age_text=f"{age:.0f}s" if age is not None else "N/A"
    st.markdown(f'<div class="aq-status"><b>MARKET {market_status()}</b><span>DATA {data_status}</span><span>SOURCE {label}</span><span>LAST UPDATE {last}</span><span>QUOTE AGE {age_text}</span></div>',unsafe_allow_html=True)

# Fragment reruns only this lightweight snapshot renderer; it never invokes the
# strategy pipeline or constructs a second quote worker.
if hasattr(st, "fragment"):
    render_ticker_strip = st.fragment(run_every="3s")(render_ticker_strip)

def _trade_row(t):
    entry=float(getattr(t,"entry",0) or 0); target=float(getattr(t,"target1",0) or 0); verdict=getattr(t,"risk_verdict",{}) or {}
    scoring_ver = getattr(t, "score_version", get_scoring_engine_version())
    ai_score=float(getattr(t,"ai_score",getattr(t,"confidence",0)) or 0); threshold=float(WORKSPACE.preferences.get("minimum_fast_ai_score",70))
    ai_ok=ai_score>=threshold; risk_status=verdict.get("verdict","PENDING")
    waiting=st.session_state.get("waiting_entry",{}).get(getattr(t,"symbol",""),{})
    trade_conf = ai_score if scoring_ver == "V2" else getattr(t, "confidence", 0)
    return {"Symbol":getattr(t,"symbol",""),"Side":getattr(t,"side","BUY"),"Strategy":getattr(t,"strategy",""),
        "Signal Time":getattr(t,"signal_time",getattr(t,"created_at",None)),"Strategy Score":getattr(t,"confidence",0),
        "Score Version": scoring_ver,
        "Trade Confidence": round(ai_score, 1) if scoring_ver == "V2" else round(trade_conf, 1),
        "AI Score":round(ai_score,2),"AI Threshold":threshold,"AI Status":"APPROVED" if ai_ok else "REJECTED",
        "AI Reason":"Trade confidence passed" if ai_ok and scoring_ver == "V2" else ("Fast local score passed" if ai_ok else "AI confidence below threshold"),
        "Risk Status":risk_status,"Risk Reason":verdict.get("reason","Risk evaluation pending"),
        "Entry Status":getattr(t,"entry_status",getattr(t,"state","PENDING")),"Entry Reason":waiting.get("reason",getattr(t,"entry_reason","Not evaluated")),
        "Execution Status":getattr(t,"execution_status","NOT_SUBMITTED"),"Current Stage":getattr(t,"state","CANDIDATE"),
        "Entry":entry,"CMP":getattr(t,"current_price",entry),"Stop":getattr(t,"stop",0),"Target":target,
        "Risk Reward":getattr(t,"risk_reward",0),"Expected Return %":round((target-entry)/entry*100,2) if entry else 0,
        "Confidence":getattr(t,"confidence",0),
        "Primary Blocker":getattr(t,"primary_blocker",""),
        "Positive Reasons": "; ".join(getattr(t, "positive_reasons_v2", [])[:3]),
        "Watch Items": "; ".join(getattr(t, "watch_items_v2", [])[:2]),
        "Quote Source":getattr(t,"quote_source",get_broker_state().snapshot()["data_source"]),
        "Quote Timestamp":getattr(t,"quote_timestamp",get_broker_state().snapshot()["last_quote_time"]),
        "Quote Age":getattr(t,"quote_age",None),"Holding Period":getattr(t,"holding_period","Swing"),"Action":"Why?"}

def opportunity_filters():
    saved=WORKSPACE.preferences.get("filters",{}); sectors=sorted(set(STOCK_SECTOR_MAP.values()))
    with st.expander("OPPORTUNITY FILTERS", expanded=False):
        a,b,c,d=st.columns(4)
        values={
          "search":a.text_input("Search symbol",saved.get("search",""),key="dashboard_opportunity_filters_search"), "watchlist_only":b.toggle("Watchlist only",saved.get("watchlist_only",False),key="dashboard_opportunity_filters_watchlist_only"),
          "exchange":c.selectbox("Exchange",["All","NSE","BSE"],index=["All","NSE","BSE"].index(saved.get("exchange","All")),key="dashboard_opportunity_filters_exchange"),
          "universe":d.selectbox("Index / universe",["All"]+SCAN_UNIVERSE_CHOICES,index=0,key="dashboard_opportunity_filters_universe"),
          "sector":a.selectbox("Sector",["All"]+sectors,index=0,key="dashboard_opportunity_filters_sector"), "side":b.selectbox("BUY / SELL",["All","BUY","SELL"],key="dashboard_opportunity_filters_side"),
          "holding":c.selectbox("Holding period",["All","Intraday","Swing","Positional"],key="dashboard_opportunity_filters_holding"),
          "minimum_confidence":d.slider("Minimum confidence",0,100,int(saved.get("minimum_confidence",0)),key="dashboard_opportunity_filters_minimum_confidence"),
          "maximum_risk":a.selectbox("Maximum risk",["Any","LOW","MEDIUM","HIGH"],key="dashboard_opportunity_filters_maximum_risk"),
          "minimum_return":b.number_input("Minimum expected return %",0.0,100.0,float(saved.get("minimum_return",0.0)),key="dashboard_opportunity_filters_minimum_return"),
          "strategy":c.text_input("Strategy",saved.get("strategy",""),key="dashboard_opportunity_filters_strategy"),
          "price_range":d.slider("Price range",0,100000,tuple(saved.get("price_range",[0,100000])),key="dashboard_opportunity_filters_price_range"),
          "volume_threshold":a.number_input("Volume threshold",0,int(1e9),int(saved.get("volume_threshold",0)),step=1000,key="dashboard_opportunity_filters_volume_threshold"),
        }
        x,y,z=st.columns(3)
        apply=x.button("Apply Filters",type="primary",width="stretch",key="dashboard_opportunity_filters_apply")
        reset=y.button("Reset Filters",width="stretch",key="dashboard_opportunity_filters_reset")
        save=z.button("Save Filter Preset",width="stretch",key="dashboard_opportunity_filters_save")
        if reset: values={}; WORKSPACE.save(filters={}); st.rerun()
        if apply or save: WORKSPACE.save(filters=values)
    return WORKSPACE.preferences.get("filters",saved)

def filtered_opportunities(filters):
    current=list(st.session_state.get("trade_candidates",{}).values())
    rows=[_trade_row(t) for t in (current or st.session_state.get("final_trade_list",[]))]
    watch=set(st.session_state.get("watchlist",[])); out=[]
    for row in rows:
        symbol=str(row["Symbol"]).replace(".NS","")
        if filters.get("search") and filters["search"].upper() not in symbol.upper(): continue
        if filters.get("watchlist_only") and symbol not in watch: continue
        if filters.get("side") not in (None,"All",row["Side"]): continue
        if float(row["Confidence"] or 0)<float(filters.get("minimum_confidence",0)): continue
        if float(row["Expected Return %"] or 0)<float(filters.get("minimum_return",0)): continue
        if filters.get("strategy") and filters["strategy"].lower() not in str(row["Strategy"]).lower(): continue
        lo,hi=filters.get("price_range",[0,100000])
        if not lo<=float(row["CMP"] or 0)<=hi: continue
        if filters.get("sector") not in (None,"All",STOCK_SECTOR_MAP.get(symbol,"UNKNOWN")): continue
        out.append(row)
    return pd.DataFrame(out)

def _normalized_confidence(raw):
    """Bound product confidence while retaining the raw engine score elsewhere."""
    try:
        score=max(0.0,float(raw or 0))
    except (TypeError,ValueError):
        return 0
    # Engine scores over 100 have diminishing product meaning; preserve ordering.
    return int(round(min(100.0, score if score <= 100 else 100*(1-math.exp(-score/83.0)))))


def _opportunity_status(row):
    ai=str(row.get("AI Status","")).upper(); risk=str(row.get("Risk Status","")).upper()
    entry=str(row.get("Entry Status","")).upper(); execution=str(row.get("Execution Status","")).upper()
    if execution in {"FILLED","EXECUTED","COMPLETE","COMPLETED"}: return "EXECUTED", "Trade was taken"
    if entry in {"EXPIRED","STALE_DATA"}: return "EXPIRED", "Signal is no longer current"
    if ai in {"REJECTED","VETOED"}: return "REJECTED", "Confidence too low"
    if risk in {"REJECTED","VETOED","NOT_EVALUATED"}: return "REJECTED", row.get("Risk Reason") or "Did not pass risk checks"
    if entry=="WAITING_VOLUME": return "WAITING FOR ENTRY", "Waiting for volume confirmation"
    if entry in {"WAITING_PRICE","WAITING_VWAP"}: return "WAITING FOR ENTRY", "Waiting for entry price"
    if risk=="APPROVED" and entry in {"READY","TRIGGERED","APPROVED"}: return "READY", "Entry conditions are met"
    return "WATCHING", "Monitoring entry conditions"


def _normal_opportunity_frame(df):
    use_v2 = get_scoring_engine_version() == "V2"
    columns=["Symbol","Side","Strategy","Current Price","Entry","Stop","Target",
             "Trade Confidence" if use_v2 else "Strategy Confidence",
             "Risk Status","Entry Status","Status","Reason","Action"]
    if not use_v2:
        columns = ["Symbol","Side","Strategy","Current Price","Entry","Stop","Target",
                     "Strategy Confidence","AI Score","Current Stage","Primary Blocker","Status","Reason","Action"]
    if df.empty: return pd.DataFrame(columns=columns)
    rows=[]
    for _,row in df.iterrows():
        status,reason=_opportunity_status(row)
        base={"Symbol":row.get("Symbol",""),"Side":row.get("Side",""),"Strategy":row.get("Strategy",""),
            "Current Price":row.get("CMP"),"Entry":row.get("Entry"),"Stop":row.get("Stop"),"Target":row.get("Target"),
            "Status":status,"Reason":reason,"Action":"Review"}
        if use_v2:
            tc = row.get("Trade Confidence", row.get("AI Score", 0))
            base.update({
                "Trade Confidence": f"{round(float(tc or 0), 1)}",
                "Risk Status": row.get("Risk Status", "PENDING"),
                "Entry Status": row.get("Entry Status", row.get("Current Stage", "")),
            })
            pos = row.get("Positive Reasons", "")
            watch = row.get("Watch Items", "")
            if pos and status == "READY":
                base["Reason"] = pos.split(";")[0] if pos else reason
            elif watch:
                base["Reason"] = f"{reason} · {watch.split(';')[0]}" if reason else watch.split(";")[0]
        else:
            base.update({
                "Strategy Confidence":f"{_normalized_confidence(row.get('Strategy Score', row.get('Confidence', 0)))}%",
                "AI Score":f"{round(float(row.get('AI Score', 0) or 0), 1)}",
                "Current Stage":row.get("Current Stage", "CANDIDATE"),
                "Primary Blocker":row.get("Primary Blocker", "") or ("AI gate" if status == "REJECTED" else ""),
            })
        rows.append(base)
    return pd.DataFrame(rows,columns=columns)


def render_opportunities():
    st.markdown('<div class="aq-panel-title">Opportunities</div>',unsafe_allow_html=True)
    final=st.session_state.get("final_trade_list",[]); potential=len(st.session_state.get("trade_candidates",{}))
    source=filtered_opportunities(opportunity_filters()); normal=_normal_opportunity_frame(source)
    backend_count=len(st.session_state.get("trade_candidates",{})); persisted_count=len(final)
    unfiltered_count=len(list(st.session_state.get("trade_candidates",{}).values()) or final)
    hidden=max(0,unfiltered_count-len(source))
    st.caption(f"Backend {backend_count} → persisted {persisted_count} → unfiltered {unfiltered_count} → displayed {len(source)}")
    if hidden:
        st.warning(f"{hidden} valid setups are hidden by your saved Opportunity Filters.")
        if st.button("Reset filters",key="opportunity_hidden_reset"):
            WORKSPACE.save(filters={}); st.rerun()
    passed_analysis=sum(_normalized_confidence(getattr(t,"ai_score",getattr(t,"confidence",0))) >= 70 for t in final)
    passed_risk=sum(_is_risk_approved(getattr(t,"risk_verdict",{}) or {}) for t in final)
    tabs=st.tabs(["ACTIONABLE","WATCHING","REJECTED","ALL"])
    filters=[normal[normal["Status"]=="READY"],normal[normal["Status"].isin(["WATCHING","WAITING FOR ENTRY"])],normal[normal["Status"]=="REJECTED"],normal]
    for i,(tab,frame) in enumerate(zip(tabs,filters)):
        with tab:
            if frame.empty:
                message="No trade is ready for execution." if i==0 else {1:"No opportunities are being watched.",2:"No rejected opportunities.",3:"No opportunities are available."}[i]
                st.markdown(f'<div class="aq-empty">{message}</div>',unsafe_allow_html=True)
                if i==0: st.caption(f"{potential} candidates reviewed · {passed_analysis} passed analysis · {passed_risk} passed risk checks · 0 met final entry conditions")
            else: st.dataframe(frame,width="stretch",hide_index=True,height=min(360,38+35*len(frame)))
    st.info("A candidate is not a position. A position exists only after an order is submitted and filled.")

def _watchlist_quotes(symbols):
    rows=[]
    for symbol in symbols:
        df=st.session_state.get("market_data",{}).get(symbol)
        if not isinstance(df, pd.DataFrame):
            df=st.session_state.get("market_data",{}).get(symbol+".NS")
        q=_quote_from_frame(df) if isinstance(df,pd.DataFrame) else None
        rows.append({"Symbol":symbol,"LTP":q["value"] if q else None,"Change":q["change"] if q else None,"Change %":q["pct"] if q else None,"Volume":float(df["Volume"].iloc[-1]) if isinstance(df,pd.DataFrame) and not df.empty and "Volume" in df else None,"Day High":float(df["High"].iloc[-1]) if isinstance(df,pd.DataFrame) and not df.empty and "High" in df else None,"Day Low":float(df["Low"].iloc[-1]) if isinstance(df,pd.DataFrame) and not df.empty and "Low" in df else None,"Signal":"WATCH","Alert":"●" if symbol in {str(getattr(t,'symbol','')).replace('.NS','') for t in st.session_state.get('final_trade_list',[])} else ""})
    return pd.DataFrame(rows)

def render_watchlist(full=False):
    st.markdown('<div class="aq-panel-title">Watchlist</div>',unsafe_allow_html=True)
    lists=WORKSPACE.preferences.get("watchlists",{"Default":st.session_state.get("watchlist",[])}) or {"Default":[]}; default=WORKSPACE.preferences.get("default_watchlist","Default")
    if default not in lists: default=next(iter(lists))
    a,b,c=st.columns([2,2,1]); active=a.selectbox("Watchlist",list(lists),index=list(lists).index(default),label_visibility="collapsed",key="watchlist_active_list"); symbol=b.text_input("Symbol",placeholder="ADD SYMBOL",label_visibility="collapsed",key="watchlist_add_symbol").strip().upper().replace(".NS","");
    if c.button("Add",width="stretch",key="watchlist_add_button_route") and symbol:
        lists[active]=sorted(set(lists[active]+[symbol])); st.session_state.watchlist=lists[active]; WORKSPACE.save(watchlists=lists,default_watchlist=active); st.rerun()
    if full:
        d,e,f=st.columns(3); rename=d.text_input("Rename watchlist",value=active,key="watchlist_rename_name"); new=e.text_input("New watchlist",key="watchlist_new_name"); make_default=f.button("Set default",key="watchlist_set_default")
        if rename!=active and rename and st.button("Rename",key="watchlist_rename_button"):
            lists[rename]=lists.pop(active); WORKSPACE.save(watchlists=lists,default_watchlist=rename); st.rerun()
        if new and st.button("Create watchlist",key="watchlist_create_button"): lists.setdefault(new,[]); WORKSPACE.save(watchlists=lists); st.rerun()
        if make_default: WORKSPACE.save(default_watchlist=active)
    search=st.text_input("Search",key=f"watch_search_{full}",label_visibility="collapsed",placeholder="SEARCH WATCHLIST")
    symbols=sorted([x for x in lists[active] if search.upper() in x.upper()]); st.session_state.watchlist=lists[active]
    df=_watchlist_quotes(symbols)
    if not df.empty: st.dataframe(df,width="stretch",hide_index=True,height=min(260,38+35*len(df)))
    if full and symbols:
        remove=st.selectbox("Remove symbol",symbols,key="watchlist_remove_symbol")
        if st.button("Remove",key="watchlist_remove_button_route"): lists[active].remove(remove); WORKSPACE.save(watchlists=lists); st.rerun()
        selected=st.selectbox("Symbol details",symbols,key="watchlist_symbol_details")
        if st.button("Open Symbol Details", key="open_symbol_details"):
            st.session_state.update(selected_symbol=selected, details_return_page="Watchlist", _page="Symbol Details")
            st.rerun()
        data=st.session_state.get("market_data",{}).get(selected+".NS")
        with st.expander(f"{selected} · AlphaQuant view",expanded=True):
            if isinstance(data,pd.DataFrame) and not data.empty:
                st.line_chart(data[["Close"]].tail(90)); st.dataframe(data.tail(10),width="stretch")
            st.write("**Strategy signal:**",next((getattr(t,"strategy","WATCH") for t in st.session_state.get("final_trade_list",[]) if selected in getattr(t,"symbol","")),"WATCH"))
            st.caption("Signal reason and relevant news appear when supplied by the strategy and news engines.")

def position_frame():
    rows=[]
    for p in st.session_state.get("paper_positions",{}).values():
        entry=float(getattr(p,"entry",0) or 0); cmp=float(getattr(p,"current_price",entry) or entry); qty=getattr(p,"quantity",getattr(p,"qty",0)); pnl=(cmp-entry)*qty
        rows.append({"Symbol":p.symbol,"Quantity":qty,"Entry":entry,"CMP":cmp,"Live P&L":pnl,"P&L %":(cmp-entry)/entry*100 if entry else 0,"Stop":getattr(p,"stop",0),"Target":getattr(p,"target1",0),"Trailing Stop":getattr(p,"trailing_stop",0),"Holding Time":str(datetime.now()-getattr(p,"entry_time",datetime.now())).split('.')[0],"Current Recommendation":"HOLD","Reason":"Risk controls active","Exit Action":"Exit"})
    return pd.DataFrame(rows)

def holdings_frame():
    rows=[]
    holdings=st.session_state.get("holdings",[]) or []
    if isinstance(holdings,dict): holdings=holdings.values()
    for h in holdings:
        get=lambda k,d=0: h.get(k,d) if isinstance(h,dict) else getattr(h,k,d); qty=get("quantity",get("qty",0)); avg=float(get("average_cost",get("entry",0)) or 0); cmp=float(get("current_price",avg) or avg)
        rows.append({"Symbol":get("symbol",""),"Quantity":qty,"Average Cost":avg,"CMP":cmp,"Invested Value":avg*qty,"Current Value":cmp*qty,"Unrealized P&L":(cmp-avg)*qty,"Allocation":get("allocation",0),"Confidence":get("confidence",0),"Why Still Holding":get("reason","Thesis intact"),"Exit Condition":get("exit_condition","Risk or target trigger")})
    return pd.DataFrame(rows)

UNIVERSE_SOURCE_OPTIONS = [
    "Entire NSE", "NIFTY 50", "NIFTY 100", "NIFTY 200", "NIFTY 500",
    "BANK NIFTY", "FINNIFTY", "Watchlist Only", "Custom Universe", "Saved Preset",
]
OPERATING_MODES = ["Full Universe", "Fast Scan", "Watchlist Only", "Custom Universe"]


def _source_to_engine(source: str) -> str:
    return {
        "Entire NSE": "NSE All", "NIFTY 50": "Nifty50", "NIFTY 100": "Nifty100",
        "NIFTY 200": "Nifty200", "NIFTY 500": "NSE500",
        "BANK NIFTY": "BankNifty", "FINNIFTY": "FinNifty",
        "Watchlist Only": "Watchlist",
    }.get(source, "NSE All")


def _normalise_custom_symbols(raw: str) -> list[str]:
    values = raw.replace("\n", ",").replace(";", ",").split(",")
    return sorted(set(_clean_and_suffix_symbols(v.strip().upper().replace(".NS", "") for v in values if v.strip())))


def render_pre_run_universe_filters():
    """Persist and present filters that are resolved before history download."""
    prefs = WORKSPACE.preferences
    saved = dict(prefs.get("universe_filters", {}))
    source_default = prefs.get("universe_source", "")
    mode_default = prefs.get("operating_mode", "Fast Scan")
    with st.expander("PRE-RUN UNIVERSE FILTER", expanded=True):
        a, b, c = st.columns(3)
        source = a.selectbox("Universe source", UNIVERSE_SOURCE_OPTIONS,
            index=UNIVERSE_SOURCE_OPTIONS.index(source_default) if source_default in UNIVERSE_SOURCE_OPTIONS else None,
            placeholder="Select and save a universe",
            key="prerun_universe_source")
        mode = b.selectbox("Operating mode", OPERATING_MODES,
            index=OPERATING_MODES.index(mode_default) if mode_default in OPERATING_MODES else 1,
            key="prerun_operating_mode")
        period = c.selectbox("History period", ["5d", "1mo", "3mo", "6mo", "1y", "2y", "5y"],
            index=["5d", "1mo", "3mo", "6mo", "1y", "2y", "5y"].index(prefs.get("history_period", "1y")), key="prerun_period")
        interval = c.selectbox("Candle interval", ["1m", "5m", "15m", "30m", "1h", "1d", "1wk"],
            index=["1m", "5m", "15m", "30m", "1h", "1d", "1wk"].index(prefs.get("candle_interval", "1d")), key="prerun_interval")
        custom = st.text_area("Custom symbol list", saved.get("custom_symbols", ""), height=68,
            placeholder="RELIANCE, TCS, INFY", key="prerun_custom_symbols",
            disabled=source != "Custom Universe" and mode != "Custom Universe")
        s1, s2, s3, s4 = st.columns(4)
        minimum_price = s1.number_input("Minimum stock price", 0.0, value=float(saved.get("minimum_price", prefs.get("minimum_price", 20))), key="dashboard_universe_minimum_price")
        maximum_price = s2.number_input("Maximum stock price", 0.0, value=float(saved.get("maximum_price", 20000)), key="dashboard_universe_maximum_price")
        minimum_volume = s3.number_input("Minimum average daily volume", 0, value=int(saved.get("minimum_volume", prefs.get("minimum_volume", 100000))), step=1000, key="dashboard_universe_minimum_volume")
        minimum_turnover = s4.number_input("Minimum traded value", 0, value=int(saved.get("minimum_turnover", CONFIG["MIN_AVG_TURNOVER"])), step=100000, key="dashboard_universe_minimum_turnover")
        t1, t2, t3, t4 = st.columns(4)
        sector = t1.multiselect("Sector", sorted(set(STOCK_SECTOR_MAP.values())), default=saved.get("sector", []), key="dashboard_universe_sector")
        market_cap = t2.selectbox("Market-cap band", ["Any", "Large Cap", "Mid Cap", "Small Cap"], index=["Any", "Large Cap", "Mid Cap", "Small Cap"].index(saved.get("market_cap", "Any")), key="dashboard_universe_market_cap")
        exchange = t3.selectbox("Exchange", ["NSE"], help="The current universe engine is NSE-only.", key="dashboard_universe_exchange")
        industry = t4.text_input("Industry", saved.get("industry", ""), help="Applied when instrument metadata is available.", key="dashboard_universe_industry")
        u1, u2, u3, u4 = st.columns(4)
        fno_only = u1.toggle("F&O only", saved.get("fno_only", False), key="dashboard_universe_fno_only")
        exclude_etfs = u2.toggle("Exclude ETFs", saved.get("exclude_etfs", True), key="dashboard_universe_exclude_etfs")
        exclude_sme = u3.toggle("Exclude SME", saved.get("exclude_sme", True), key="dashboard_universe_exclude_sme")
        exclude_illiquid = u4.toggle("Exclude illiquid/unavailable", saved.get("exclude_illiquid", True), key="dashboard_universe_exclude_illiquid")
        v1, v2, v3, v4 = st.columns(4)
        styles = v1.multiselect("Strategy horizon", ["Intraday", "Swing", "Positional"], default=saved.get("styles", ["Swing"]), key="dashboard_universe_strategy_horizon")
        direction = v2.selectbox("Direction", ["Long only", "Short enabled"], index=1 if saved.get("short_enabled") else 0, key="dashboard_universe_direction")
        confidence = v3.slider("Minimum confidence", 0, 100, int(saved.get("minimum_confidence", prefs.get("minimum_confidence", 70))), key="dashboard_universe_minimum_confidence")
        risk = v4.selectbox("Maximum risk rating", ["LOW", "MEDIUM", "HIGH"], index=["LOW", "MEDIUM", "HIGH"].index(saved.get("maximum_risk", "HIGH")), key="dashboard_universe_maximum_risk")
        expected = v1.number_input("Minimum expected return %", 0.0, 100.0, float(saved.get("minimum_return", 0.0)), key="dashboard_universe_minimum_return")
        enabled = v2.multiselect("Enabled strategies", sorted(SCAN_STYLE_STRATEGY_MAP), default=saved.get("enabled_strategies", []), key="dashboard_universe_enabled_strategies")
        watch_only = v3.toggle("Watchlist only", saved.get("watchlist_only", False), key="dashboard_universe_watchlist_only")
        filters = {"custom_symbols": custom, "minimum_price": minimum_price, "maximum_price": maximum_price,
            "minimum_volume": minimum_volume, "minimum_turnover": minimum_turnover, "sector": sector,
            "market_cap": market_cap, "industry": industry, "exchange": exchange, "fno_only": fno_only,
            "exclude_etfs": exclude_etfs, "exclude_sme": exclude_sme, "exclude_illiquid": exclude_illiquid,
            "styles": styles, "short_enabled": direction == "Short enabled", "minimum_confidence": confidence,
            "maximum_risk": risk, "minimum_return": expected, "enabled_strategies": enabled, "watchlist_only": watch_only}
        if st.button("Save Universe Configuration", key="save_prerun_filters", type="primary"):
            if not source or (source == "Custom Universe" and not _normalise_custom_symbols(custom)):
                st.error("Select a universe and complete its required fields before saving.")
            else:
                WORKSPACE.save(universe_source=source, operating_mode=mode, universe_filters=filters,
                    history_period=period, candle_interval=interval, execution_mode=st.session_state.get("execution_mode", "PAPER"))
                st.success("Universe configuration saved.")
        source_estimates = {"Entire NSE": 2200, "NIFTY 50": 50, "NIFTY 100": 100, "NIFTY 200": 200,
            "NIFTY 500": 500, "BANK NIFTY": 12, "FINNIFTY": 20}
        estimated = len(st.session_state.get("scan_universe", [])) or source_estimates.get(source, len(st.session_state.get("watchlist", []))) if source else 0
        if mode == "Fast Scan": estimated = min(estimated, 200)
        if mode in ("Watchlist Only", "Custom Universe"): estimated = len(st.session_state.get("watchlist", [])) if mode == "Watchlist Only" else len(_normalise_custom_symbols(custom))
        last_update = st.session_state.get(MarketDataManager.session_key, {}).get("last_persisted")
        m1,m2,m3,m4,m5,m6=st.columns(6)
        m1.metric("Selected Universe", source); m2.metric("Estimated Symbols", estimated)
        m3.metric("Download Workload", f"Up to {estimated} incremental")
        m4.metric("Last Data Update", str(last_update or "Not yet synced")); m5.metric("History", period); m6.metric("Interval", interval)
        now = datetime.now(IST); open_at = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if mode == "Full Universe" and (now >= open_at - timedelta(minutes=45)):
            st.info("Market opens soon or is in session. Fast Scan is recommended for quicker readiness; your selected mode was not changed.")
    st.session_state["active_prerun_filters"] = filters
    st.session_state["active_universe_source"] = source
    st.session_state["active_operating_mode"] = mode
    CONFIG["DOWNLOAD_PERIOD"], CONFIG["DOWNLOAD_INTERVAL"] = period, interval


def _normalize_candles(frame):
    if not isinstance(frame, pd.DataFrame) or frame.empty: return pd.DataFrame(columns=["Open","High","Low","Close","Volume"])
    data=frame.copy()
    if isinstance(data.columns,pd.MultiIndex): data.columns=data.columns.get_level_values(0)
    data.columns=[str(c).strip().title() for c in data.columns]
    required=["Open","High","Low","Close","Volume"]
    if not set(required).issubset(data.columns): return pd.DataFrame(columns=required)
    index=pd.to_datetime(data.index,utc=True,errors="coerce"); data=data.loc[~index.isna(),required]; data.index=index[~index.isna()]
    for column in required: data[column]=pd.to_numeric(data[column],errors="coerce")
    return data.dropna(subset=["Open","High","Low","Close"]).loc[lambda x:~x.index.duplicated(keep="last")].sort_index()


def load_chart_candles(symbol, timeframe):
    """Incremental chart path, independent from the autonomous scan."""
    manager=MarketDataManager(); ticker=symbol if str(symbol).endswith(".NS") else symbol+".NS"
    provider="persistent cache"; requested=WORKSPACE.preferences.get("history_period","1y"); rows=0
    try:
        cached=_normalize_candles(manager._load(ticker,timeframe))
        # update_history fetches only the provider's latest batch and persists
        # completed candles; unlike a quote, it never fabricates OHLC history.
        updated=manager.update_history([ticker],timeframe).get(ticker)
        fresh=_normalize_candles(updated); provider="Yahoo Finance incremental" if not fresh.empty else provider
        data=_normalize_candles(pd.concat([cached,fresh])) if not cached.empty or not fresh.empty else fresh
        rows=len(fresh)
        if data.empty and timeframe != "1d":
            daily=manager.get_history([ticker],"1d",requested).get(ticker)
            fallback=_normalize_candles(daily)
            return fallback,"daily fallback",f"{timeframe} unavailable: provider returned no normalized candles; showing 1d"
        minimum=2 if timeframe=="1d" else 20
        if len(data)<minimum:
            return data,provider,f"{timeframe} has {len(data)} candles; at least {minimum} are required"
        return data,provider,None
    except Exception as exc:
        diagnostic={"symbol":symbol,"timeframe":timeframe,"provider":provider,"requested range":requested,
            "rows returned":rows,"rows after normalization":0,"exception type":type(exc).__name__,"message":str(exc)}
        st.session_state.setdefault("chart_diagnostics",[]).append(diagnostic)
        _developer_internal_error("chart",exc)
        return pd.DataFrame(),provider,f"{timeframe} unavailable: {type(exc).__name__}: {exc}"


def render_symbol_details(symbol: str | None = None):
    """Render a non-blocking symbol detail view from persistent or fallback data."""
    candidates = sorted(set(st.session_state.get("watchlist", [])) | {str(x).replace(".NS", "") for x in st.session_state.get("market_data", {})})
    selected = symbol or st.session_state.get("selected_symbol")
    if not candidates and not selected:
        st.info("Search for a symbol or add one to a watchlist to view its chart.")
        return
    if not selected:
        st.info("Choose a symbol from Watchlist to open Symbol Details.")
        return
    st.session_state["selected_symbol"] = selected
    if st.button("← Back to previous workspace", key="details_back"):
        st.session_state["_page"] = st.session_state.get("details_return_page", "Watchlist")
        st.rerun()
    timeframe = st.selectbox("Timeframe", ["1m", "5m", "15m", "30m", "1h", "1d", "1wk"],
        index=["1m", "5m", "15m", "30m", "1h", "1d", "1wk"].index(WORKSPACE.preferences.get("chart_timeframe", "1d")), key="details_timeframe")
    if timeframe != WORKSPACE.preferences.get("chart_timeframe"):
        WORKSPACE.save(chart_timeframe=timeframe)
    data, source, fallback_reason = load_chart_candles(selected, timeframe)
    if fallback_reason: st.info(fallback_reason)
    if data is None or not isinstance(data, pd.DataFrame) or data.empty:
        st.warning("Intraday chart data is unavailable from the current provider. Try 1D or connect a broker market-data source." if timeframe != "1d" else "Daily chart data is currently unavailable for this symbol.")
        return
    data = data.tail(500).copy()
    # A snapshot may update only the display's currently forming candle.  It is
    # never appended to or persisted as historical OHLCV.
    quote=get_broker_state().snapshot().get("quotes",{}).get(selected) or get_broker_state().snapshot().get("quotes",{}).get(selected+".NS")
    if quote and timeframe != "1d" and not data.empty:
        ltp=float(quote.get("ltp",quote.get("value",0)) or 0)
        if ltp:
            data.iloc[-1,data.columns.get_loc("Close")]=ltp
            data.iloc[-1,data.columns.get_loc("High")]=max(float(data.iloc[-1]["High"]),ltp)
            data.iloc[-1,data.columns.get_loc("Low")]=min(float(data.iloc[-1]["Low"]),ltp)
    overlays = st.multiselect("Overlays", ["EMA 20", "EMA 50", "VWAP", "Bollinger Bands", "Support / Resistance", "Trade Markers"],
        default=["EMA 20", "VWAP", "Trade Markers"], key="details_chart_overlays")
    close = pd.to_numeric(data["Close"], errors="coerce")
    data["EMA 20"] = close.ewm(span=20, adjust=False).mean()
    data["EMA 50"] = close.ewm(span=50, adjust=False).mean()
    typical = (pd.to_numeric(data["High"], errors="coerce") + pd.to_numeric(data["Low"], errors="coerce") + close) / 3
    volume = pd.to_numeric(data.get("Volume", 0), errors="coerce").fillna(0)
    data["VWAP"] = (typical * volume).cumsum() / volume.cumsum().replace(0, np.nan)
    middle = close.rolling(20).mean(); deviation = close.rolling(20).std()
    data["BB Upper"], data["BB Lower"] = middle + 2 * deviation, middle - 2 * deviation
    data["Support"], data["Resistance"] = pd.to_numeric(data["Low"], errors="coerce").rolling(20).min(), pd.to_numeric(data["High"], errors="coerce").rolling(20).max()
    last=data.iloc[-1]; previous=float(data["Close"].iloc[-2]) if len(data)>1 else float(last["Close"])
    change=float(last["Close"])-previous
    cols=st.columns(7)
    for col,(label,value) in zip(cols,[("Symbol",selected),("Price",f"₹{float(last['Close']):,.2f}"),("Change",f"{change:+.2f}"),("Change %",f"{change/previous*100:+.2f}%" if previous else "—"),("Open",f"{float(last['Open']):,.2f}"),("High / Low",f"{float(last['High']):,.2f} / {float(last['Low']):,.2f}"),("Volume",f"{float(last.get('Volume',0)):,.0f}")]): col.metric(label,value)
    chart = data.reset_index().rename(columns={data.reset_index().columns[0]: "Timestamp"})
    price_layers=[
        {"mark":{"type":"rule","tooltip":True},"encoding":{"x":{"field":"Timestamp","type":"temporal"},"y":{"field":"Low","type":"quantitative","scale":{"zero":False}},"y2":{"field":"High"},"color":{"condition":{"test":"datum.Open <= datum.Close","value":"#24c78e"},"value":"#ef5b64"}}},
        {"mark":{"type":"bar","tooltip":True},"encoding":{"x":{"field":"Timestamp","type":"temporal"},"y":{"field":"Open","type":"quantitative","scale":{"zero":False}},"y2":{"field":"Close"},"color":{"condition":{"test":"datum.Open <= datum.Close","value":"#24c78e"},"value":"#ef5b64"}}},
    ]
    line_colours={"EMA 20":"#f2a900","EMA 50":"#8b7cf6","VWAP":"#38bdf8","BB Upper":"#64748b","BB Lower":"#64748b","Support":"#24c78e","Resistance":"#ef5b64"}
    selected_lines=[]
    for item in overlays:
        selected_lines += {"Bollinger Bands":["BB Upper","BB Lower"],"Support / Resistance":["Support","Resistance"]}.get(item,[item] if item in line_colours else [])
    for column in selected_lines:
        price_layers.append({"mark":{"type":"line","strokeWidth":1.2,"color":line_colours[column]},"encoding":{"x":{"field":"Timestamp","type":"temporal"},"y":{"field":column,"type":"quantitative","scale":{"zero":False}}}})
    # Entry/exit/SL/target annotations are real trade state, never synthetic.
    annotations=[]
    related=[t for t in st.session_state.get("final_trade_list",[]) if selected in str(getattr(t,"symbol",""))]
    if "Trade Markers" in overlays and related:
        trade=related[0]
        for label, attr, colour in [("ENTRY","entry","#24c78e"),("STOP","stop","#ef5b64"),("TARGET","target1","#f2a900")]:
            value=float(getattr(trade,attr,0) or 0)
            if value: annotations.append({"Label":label,"Price":value})
        if annotations:
            price_layers.append({"data":{"values":annotations},"mark":{"type":"rule","strokeDash":[5,3]},"encoding":{"y":{"field":"Price","type":"quantitative"},"color":{"field":"Label","type":"nominal","legend":{"orient":"top"}}}})
    candle_spec={"params":[{"name":"zoom","select":"interval","bind":"scales"}],"vconcat":[{"height":380,"layer":price_layers},{"height":100,"mark":"bar","encoding":{"x":{"field":"Timestamp","type":"temporal"},"y":{"field":"Volume","type":"quantitative"},"color":{"condition":{"test":"datum.Open <= datum.Close","value":"#24c78e"},"value":"#ef5b64"}}}],"resolve":{"scale":{"x":"shared"}}}
    st.vega_lite_chart(chart, candle_spec, width="stretch")
    st.caption(f"Source: {source} · Last update: {data.index[-1]} · Bid/ask and company metadata appear when supplied by the active broker. Chart supports browser zoom/pan through Vega interaction.")


def _paper_ledger_metrics() -> dict[str, float]:
    ledger = st.session_state.get("paper_broker", {})
    positions = st.session_state.get("paper_positions", {})
    unrealized = sum((float(getattr(p, "current_price", getattr(p, "entry", 0)) or 0) - float(getattr(p, "entry", 0) or 0)) * int(getattr(p, "quantity", getattr(p, "qty", 0)) or 0) for p in positions.values())
    return {"starting": float(ledger.get("starting_capital", WORKSPACE.preferences["paper_trading_capital"])), "cash": float(ledger.get("cash", 0)), "realized": float(ledger.get("realized_pnl", 0)), "unrealized": unrealized}


def render_terminal_dashboard():
    """Business-level command centre; technical inspection lives in Settings."""
    prefs=WORKSPACE.preferences; mode=prefs.get("execution_mode","PAPER")
    configured=bool(prefs.get("universe_source")); broker=get_broker_state().snapshot()
    broker_ready=bool(broker.get("authenticated") and broker.get("connected"))
    running=bool(st.session_state.get("autonomous_active"))
    delayed=broker.get("data_source")=="YFINANCE_INTRADAY_FALLBACK"
    opened=market_status()=="OPEN"; ledger=_paper_ledger_metrics()
    paper_state=("RUNNING" if running and opened else "MARKET CLOSED" if running and not opened else
        "READY WITH BROKER DATA" if broker_ready else "READY WITH DELAYED DATA" if configured else "NOT READY")
    st.markdown('<div class="aq-panel-title">System Status</div>',unsafe_allow_html=True)
    labels=[("Paper Trading",paper_state),("Broker","Connected" if broker_ready else "Not Connected"),
        ("Market Data","Delayed" if delayed else "Live" if broker.get("market_data_connected") else "Unavailable"),
        ("AlphaQuant","Running" if running else "Stopped")]
    for col,(label,value) in zip(st.columns(4),labels): col.metric(label,value)
    if mode=="PAPER" and not broker_ready and delayed:
        st.caption("Broker not connected. Yahoo Finance intraday fallback is active. Execution will be simulated.")
    if not opened:
        st.info("Market closed. AlphaQuant is armed and will resume monitoring at market open." if running else "Market closed. You can review reports, candidates, holdings, daily charts, and configure the next run.")
    blocked=not configured or (mode=="LIVE" and not broker_ready)
    if mode=="LIVE" and not broker_ready: st.error("Live execution is disabled until broker authentication and quote API tests both succeed.")
    if running:
        if st.button("STOP ALPHAQUANT",type="primary",width="stretch",key="product_stop"):
            get_core_runtime().stop(); st.session_state.update(autonomous_active=False,stop_requested=True,pipeline_state="STOPPED"); st.rerun()
    elif st.button("RUN ALPHAQUANT",type="primary",width="stretch",disabled=blocked,key="product_run"):
        run_id,started=get_core_runtime().start({"mode":mode,"universe":prefs.get("universe_source"),"interval":prefs.get("candle_interval"),"period":prefs.get("history_period")})
        st.session_state.update(alphaquant_run_pending=started,pipeline_state="STARTING",core_run_id=run_id); st.rerun()
    final=st.session_state.get("final_trade_list",[]); normal=_normal_opportunity_frame(filtered_opportunities({}))
    actionable=int((normal["Status"]=="READY").sum()) if not normal.empty else 0
    waiting=int((normal["Status"]=="WAITING FOR ENTRY").sum()) if not normal.empty else 0
    pnl=ledger["realized"]+ledger["unrealized"]
    metrics=[("Today's Candidates",len(st.session_state.get("trade_candidates",{})) or len(final)),("Actionable Trades",actionable),
        ("Waiting for Entry",waiting),("Open Positions",len(st.session_state.get("paper_positions",{}))),
        ("Today's P&L",_money(pnl)),("Available Capital",_money(ledger["cash"]) if mode=="PAPER" else "Broker managed")]
    for col,(label,value) in zip(st.columns(6),metrics): col.metric(label,value)
    render_opportunities()

# =====================================================
# AUTONOMOUS TRADING LOOP
# =====================================================
# Streamlit has no always-on background process - a script only runs
# while a session is connected, so "autonomous" here means "keeps
# scanning/monitoring on its own for as long as this AlphaQuant tab
# stays open", using st.fragment(run_every=...) (the Streamlit-native
# replacement for a manual while/sleep loop) rather than a blocking
# while True loop or an OS-level background process. That is an inherent
# constraint of Streamlit's request/response execution model, not a bug
# in this feature.

MONITOR_INTERVAL_SECONDS = 20
SCAN_INTERVAL_SECONDS = 300

def quick_refresh_open_positions():
    """
    Lightweight per-tick check, independent of a full rescan: pulls just
    the latest price for every symbol with an open paper position (via
    the same cheap fetch_quote_snapshot() the Scan Manager uses) and
    checks stop-loss/target hits against it immediately, instead of
    waiting for the next full pipeline cycle. Trailing-stop
    recalculation needs EMA20/ATR from a full indicator recompute, so
    that still only happens inside the full pipeline cycle via
    monitor_open_positions() - this only re-checks the existing
    stop/target levels against a fresh price so a hit is never missed
    for SCAN_INTERVAL_SECONDS at a time.
    """

    positions = st.session_state.paper_positions

    if not positions:
        return

    quotes = fetch_quote_snapshot(list(positions.keys()))

    for symbol, position in list(positions.items()):

        quote = quotes.get(symbol)

        if not quote or quote.get("price") is None:
            continue

        update_position_price(position, quote["price"])

        closed = check_stop_loss(position)

        if not closed:
            check_target1(position)
            check_target2(position)
            check_target3(position)

        update_position_statistics(position)

        if position.status == "CLOSED":
            archive_closed_position(position)
            remove_open_position(symbol)


@st.fragment(run_every=MONITOR_INTERVAL_SECONDS)
def autonomous_loop_fragment():

    if not st.session_state.autonomous_active:
        st.caption("Autonomous mode is stopped.")
        return

    now_ist = datetime.now(IST)

    if not is_market_open(now_ist):
        st.caption(
            f"Market closed ({now_ist.strftime('%H:%M:%S')} IST) - "
            "autonomous mode is armed but idle."
        )
        return

    quick_refresh_open_positions()

    due_for_full_cycle = (
        st.session_state.last_cycle_time is None
        or (datetime.now() - st.session_state.last_cycle_time).total_seconds()
        >= SCAN_INTERVAL_SECONDS
    )

    if due_for_full_cycle:

        ok, msg = run_alphaquant(trigger="SCHEDULED")

        # The shared run_alphaquant() path sets run_complete_scan_requested so the
        # manual button (defined earlier in the script, before
        # execute_scan_pipeline itself exists yet) can defer to the bottom
        # of the file. A fragment rerun only re-executes this function's own
        # body, not that bottom block, so the pipeline has to be triggered
        # directly here - and the flag must be cleared right after, or it
        # would linger and cause a second, redundant execute_scan_pipeline()
        # call on the next full-page rerun.
        st.session_state.run_complete_scan_requested = False

        if ok:
            execute_scan_pipeline()

            st.session_state.last_cycle_message = (
                f"{len(st.session_state.final_trade_list)} trade candidate(s), "
                f"{len(st.session_state.paper_positions)} open position(s)."
            )
        else:
            st.session_state.last_cycle_message = msg

    if st.session_state.last_cycle_time:

        st.caption(
            f"Last full cycle: "
            f"{st.session_state.last_cycle_time.strftime('%H:%M:%S')} "
            f"({st.session_state.last_cycle_trigger}) - "
            f"{len(st.session_state.paper_positions)} open position(s) "
            f"monitored every {MONITOR_INTERVAL_SECONDS}s. "
            f"{st.session_state.last_cycle_message}"
        )

    else:

        st.caption("Waiting for the first autonomous cycle...")


# =====================================================
# LIVE MARKET ENGINE
# VERSION 1.0.0
# =====================================================
# Turns AlphaQuant from a one-shot scanner into a continuous market
# monitor. Downloads history ONCE per session, then polls only the
# newest completed candle for the selected interval and re-runs the
# existing pipeline (strategies -> AI -> Risk -> Portfolio -> Paper
# Trades) on the updated cache. All existing engines are reused
# unchanged; nothing about strategy, AI, risk, portfolio or paper
# trading logic is duplicated here.
#
# Provider abstraction: MarketDataProvider is an ABC so the yfinance
# implementation can later be swapped for a broker WebSocket feed
# (Upstox, Zerodha, etc.) without touching any downstream engine.



class MarketDataProvider(ABC):
    """
    Contract every market-data source must implement. yfinance today,
    Upstox/Zerodha/etc. tomorrow - the LiveMarketEngine only knows this
    interface.
    """

    @abstractmethod
    def download_history(self, symbols, interval, period):
        """Return {symbol: DataFrame} - full history for each symbol."""

    @abstractmethod
    def fetch_latest_batch(self, symbols, interval):
        """Return {symbol: DataFrame} with just the last few candles."""

    @abstractmethod
    def is_market_open(self, now=None):
        """Return True iff the market is currently open."""


class YFinanceProvider(MarketDataProvider):
    """yfinance-backed provider. Wraps the existing download plumbing."""

    _PERIOD_FOR_INTERVAL = {
        "1m": "5d", "5m": "1mo", "15m": "1mo",
        "30m": "1mo", "1h": "3mo", "1d": "1y",
    }

    def download_history(self, symbols, interval, period=None):
        period = period or self._PERIOD_FOR_INTERVAL.get(interval, "1y")
        try:
            data = yf.download(
                tickers=" ".join(symbols) if len(symbols) > 1 else symbols[0],
                period=period,
                interval=interval,
                group_by="ticker",
                progress=False,
                threads=True,
                auto_adjust=False,
            )
        except Exception as exc:
            logging.warning("LIVE download_history failed: %s", exc)
            return {}
        out = {}
        if len(symbols) == 1:
            df = data.dropna(how="all")
            if not df.empty:
                out[symbols[0]] = df
            return out
        for sym in symbols:
            try:
                sub = data[sym].dropna(how="all")
                if not sub.empty:
                    out[sym] = sub
            except Exception:
                continue
        return out

    def fetch_latest_batch(self, symbols, interval):
        # Short-period pull is enough to grab the last N candles for each
        # symbol; we then slice to just the newest candle per symbol in
        # LiveMarketCache.append_from_batch.
        short_period = "1d" if interval in ("1m", "5m", "15m", "30m", "1h") else "5d"
        return self.download_history(symbols, interval, short_period)

    def download_missing_range(self, symbol, interval, start, end):
        """Download only an explicitly missing Yahoo Finance candle range."""
        try:
            frame = yf.download(symbol, start=pd.Timestamp(start).date(),
                end=(pd.Timestamp(end) + pd.Timedelta(days=1)).date(), interval=interval,
                progress=False, threads=False, auto_adjust=False)
            if isinstance(frame.columns, pd.MultiIndex):
                frame.columns = frame.columns.get_level_values(0)
            return frame.dropna(how="all")
        except Exception as exc:
            logging.warning("Missing-range download failed | %s | %s", symbol, exc)
            return pd.DataFrame()

    def is_market_open(self, now=None):
        return is_market_open(now)




class MarketDataManager:
    session_key = "market_data_manager"
    storage_dir = Path(_APP_DIR) / "data" / "market_cache"

    def __init__(self, provider=None):
        self.provider = provider or YFinanceProvider()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        st.session_state.setdefault(self.session_key, {"history": {}, "live_ticks": {}, "status": {}, "last_persisted": None})

    def _path(self, symbol: str, interval: str) -> Path:
        safe_symbol = str(symbol).replace("/", "_").replace("|", "_")
        return self.storage_dir / f"{safe_symbol}_{interval}.csv"

    def _backup_path(self, symbol: str, interval: str) -> Path:
        backup_dir = self.storage_dir / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir / f"{symbol}_{interval}_{datetime.now().strftime('%Y%m%d')}.csv.gz"

    def _load(self, symbol: str, interval: str):
        path = self._path(symbol, interval)
        if not path.exists():
            return None
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df.sort_index()

    def _save(self, symbol: str, interval: str, df, source="historical provider"):
        if df is not None and not df.empty:
            df = df.copy()
            index = pd.to_datetime(df.index, utc=True, errors="coerce")
            df = df.loc[~index.isna()]
            df.index = index[~index.isna()]
            df = df[~df.index.duplicated(keep="last")].sort_index()
            path = self._path(symbol, interval)
            df.to_csv(path)
            df.sort_index().to_pickle(str(path) + ".pkl.gz", compression="gzip")
            st.session_state[self.session_key]["last_persisted"] = datetime.now()
            st.session_state[self.session_key].setdefault("versions", {})[f"{symbol}:{interval}"] = {
                "rows": int(len(df)), "updated_at": datetime.now().isoformat(), "path": str(path),
                "source": source, "completed_candles_only": True,
            }

    def backup_history(self, symbol: str, interval: str = "1d"):
        cached = self._load(symbol, interval)
        if cached is None or cached.empty:
            return None
        backup = self._backup_path(symbol.replace("/", "_").replace("|", "_"), interval)
        cached.to_csv(backup, compression="gzip")
        return backup

    def detect_gaps(self, symbol: str, interval: str = "1d"):
        df = self._load(symbol, interval)
        if df is None or df.empty:
            return []
        freq = "B" if interval in ("1d", "1wk", "1mo", "1w", "1M") else None
        if not freq:
            return []
        expected = pd.date_range(df.index.min().date(), df.index.max().date(), freq=freq)
        missing = expected.difference(pd.DatetimeIndex(df.index).normalize())
        return [d.strftime("%Y-%m-%d") for d in missing]

    def morning_sync(self, symbols, intervals=("1d",)):
        results = {}
        for interval in intervals:
            results[interval] = self.update_history(symbols, interval)
            self.repair_history(symbols, interval)
        return results

    def get_history(self, symbols, interval="1d", period=None):
        history = {}
        missing = []
        for symbol in symbols:
            cached = self._load(symbol, interval)
            if cached is None or cached.empty:
                missing.append(symbol)
            else:
                history[symbol] = cached
        if missing:
            downloaded = self.provider.download_history(missing, interval, period or CONFIG.get("DOWNLOAD_PERIOD", "1y"))
            for symbol, df in downloaded.items():
                self._save(symbol, interval, df)
                history[symbol] = df
        st.session_state[self.session_key]["history"].update(history)
        SystemHealthEngine().mark("last_scan")
        logging.info("MarketDataManager history ready | symbols=%s | interval=%s", len(history), interval)
        return history

    def update_history(self, symbols, interval="1d"):
        batch = self.provider.fetch_latest_batch(symbols, interval)
        updated = {}
        for symbol in symbols:
            cached = self._load(symbol, interval)
            incoming = batch.get(symbol)
            if incoming is None or incoming.empty:
                updated[symbol] = cached
                continue
            incoming = incoming.copy()
            incoming.index = pd.to_datetime(incoming.index, utc=True, errors="coerce")
            incoming = incoming[~incoming.index.isna()]
            # Providers commonly return the candle currently forming. It may be
            # used in the live view, but is never persisted as completed history.
            now_utc = pd.Timestamp.now(tz="UTC")
            duration = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1h","1d":"1D","1wk":"7D"}.get(interval,"1D")
            completed = incoming.index + pd.to_timedelta(duration) <= now_utc
            incoming_completed = incoming.loc[completed]
            merged = incoming_completed if cached is None or cached.empty else pd.concat([cached, incoming_completed])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
            self._save(symbol, interval, merged, source="fallback provider incremental")
            updated[symbol] = merged
        st.session_state[self.session_key]["history"].update({k: v for k, v in updated.items() if v is not None})
        logging.info("MarketDataManager incremental update | symbols=%s | interval=%s", len(updated), interval)
        return updated

    def repair_history(self, symbols, interval="1d", period=None):
        repaired = self.update_history(symbols, interval)
        gaps = {}
        for symbol, df in repaired.items():
            if df is None or df.empty:
                continue
            freq = "B" if interval in ("1d", "1wk", "1mo") else None
            if freq:
                expected = pd.date_range(df.index.min().date(), df.index.max().date(), freq=freq)
                missing = expected.difference(pd.DatetimeIndex(df.index).normalize())
                if len(missing):
                    gaps[symbol] = [d.strftime("%Y-%m-%d") for d in missing]
        if gaps:
            for symbol, missing_dates in gaps.items():
                cached = repaired.get(symbol)
                if not hasattr(self.provider, "download_missing_range") or cached is None:
                    continue
                missing = pd.to_datetime(missing_dates)
                patch = self.provider.download_missing_range(symbol, interval, missing.min(), missing.max())
                if patch is not None and not patch.empty:
                    patch.index = pd.to_datetime(patch.index, utc=True, errors="coerce")
                    merged = pd.concat([cached, patch]).loc[lambda x: ~x.index.duplicated(keep="last")].sort_index()
                    self._save(symbol, interval, merged, source="Yahoo Finance missing candles")
                    repaired[symbol] = merged
        st.session_state[self.session_key]["status"] = {"gaps": gaps, "last_repair": datetime.now()}
        logging.info("MarketDataManager repair complete | symbols=%s | gaps=%s", len(symbols), len(gaps))
        return repaired

    def store_live_tick(self, symbol: str, tick: dict[str, Any]):
        ticks = st.session_state[self.session_key].setdefault("live_ticks", {}).setdefault(symbol, [])
        tick = {**tick, "timestamp": tick.get("timestamp", datetime.now())}
        ticks.append(tick)
        tick_dir = self.storage_dir / "ticks"
        tick_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([tick]).to_csv(tick_dir / f"{symbol}_{datetime.now().strftime('%Y%m%d')}.csv", mode="a", header=not (tick_dir / f"{symbol}_{datetime.now().strftime('%Y%m%d')}.csv").exists(), index=False)
        SystemHealthEngine().mark("last_live_tick")
        return tick

    def get_live_data(self, symbol: str | None = None):
        ticks = st.session_state[self.session_key].setdefault("live_ticks", {})
        return ticks.get(symbol, []) if symbol else ticks

    def ticks_to_candles(self, symbol: str, interval="1min"):
        ticks = self.get_live_data(symbol)
        if not ticks:
            return pd.DataFrame()
        df = pd.DataFrame(ticks)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
        price = df["price"].astype(float)
        return price.resample(interval).ohlc().dropna()

    def recover_after_disconnect(self, symbols, interval="1d"):
        logging.info("MarketDataManager broker reconnect recovery started")
        return self.repair_history(symbols, interval)


class LiveMarketCache:
    """
    Per-symbol OHLCV cache. `initialize()` fills it once from a
    provider.download_history(); subsequent `append_from_batch()` calls
    only add new candles (no duplicate rows, no re-download of history).
    """

    def __init__(self):
        self.data = {}
        self.last_updated = {}
        self.new_candle_symbols = set()

    def initialize(self, provider, symbols, interval, period=None):
        history = provider.download_history(symbols, interval, period)
        self.data = history
        now = datetime.now()
        self.last_updated = {sym: now for sym in history}
        self.new_candle_symbols = set(history.keys())
        return len(history)

    def append_from_batch(self, batch):
        """
        Merge new-candle rows in-place. Returns the set of symbols that
        received a truly new candle (i.e. a bar not already in cache).
        """
        self.new_candle_symbols = set()
        for sym, incoming in batch.items():
            if incoming is None or incoming.empty:
                continue
            cached = self.data.get(sym)
            if cached is None or cached.empty:
                self.data[sym] = incoming
                self.new_candle_symbols.add(sym)
                self.last_updated[sym] = datetime.now()
                continue
            last_ts = cached.index[-1]
            new_rows = incoming[incoming.index > last_ts]
            if new_rows.empty:
                continue
            self.data[sym] = pd.concat([cached, new_rows])
            self.new_candle_symbols.add(sym)
            self.last_updated[sym] = datetime.now()
        return self.new_candle_symbols

    def snapshot(self):
        return dict(self.data)


class LiveMarketEngine:
    """
    Orchestrator. `initialize_session()` downloads history once; every
    `tick()` fetches only the latest candle and re-executes the
    downstream pipeline on symbols whose cache actually advanced.
    """

    _INTERVAL_SECONDS = {
        "1m": 60, "5m": 300, "15m": 900,
        "30m": 1800, "1h": 3600, "1d": 24 * 3600,
    }

    def __init__(self, provider):
        self.provider = provider
        self.cache = LiveMarketCache()
        self.symbols = []
        self.interval = "5m"
        self.session_id = None
        self.last_tick_time = None
        self.last_tick_stats = {}
        self.total_ticks = 0

    def is_initialized(self):
        return bool(self.symbols) and bool(self.cache.data)

    def initialize_session(self, symbols, interval):
        self.symbols = list(symbols)
        self.interval = interval
        n = self.cache.initialize(self.provider, self.symbols, interval)
        # Hand the initialised cache to the rest of the app so the
        # existing pipeline (which reads st.session_state.market_data)
        # sees the same DataFrames.
        st.session_state.market_data = self.cache.snapshot()
        self.session_id = datetime.now().isoformat()
        return n

    def next_candle_seconds(self):
        """Approximate seconds until the next candle boundary."""
        sec = self._INTERVAL_SECONDS.get(self.interval, 300)
        if not self.last_tick_time:
            return sec
        elapsed = (datetime.now() - self.last_tick_time).total_seconds()
        return max(0, int(sec - elapsed))

    def tick(self):
        """
        One monitoring iteration: fetch newest candles, update cache,
        re-run the existing pipeline (execute_scan_pipeline) on the
        refreshed data so all downstream engines (strategies, AI, risk,
        portfolio, paper) run against the new bar.
        """
        if not self.is_initialized():
            return {"status": "not_initialized"}

        if not self.provider.is_market_open():
            return {"status": "market_closed"}

        batch = self.provider.fetch_latest_batch(self.symbols, self.interval)
        changed = self.cache.append_from_batch(batch)

        stats = {
            "status": "ok",
            "polled": len(batch),
            "new_candle_symbols": sorted(list(changed)),
            "candles_added": len(changed),
            "tick_time": datetime.now(),
        }

        if changed:
            # Hand the refreshed cache to the pipeline
            st.session_state.market_data = self.cache.snapshot()
            try:
                execute_scan_pipeline()
                stats["pipeline"] = "executed"
                stats["open_positions"] = len(st.session_state.paper_positions)
                stats["closed_positions"] = len(
                    st.session_state.get("paper_history", [])
                )
            except Exception as exc:
                stats["pipeline"] = f"error: {exc}"
                logging.warning("LIVE pipeline execution failed: %s", exc)
        else:
            # Even with no new candles we still trail-stop / monitor exits
            try:
                monitor_open_positions()
                stats["monitored_only"] = True
            except Exception as exc:
                logging.warning("LIVE monitor_only failed: %s", exc)

        self.last_tick_time = stats["tick_time"]
        self.total_ticks += 1
        self.last_tick_stats = stats
        return stats

    def status(self):
        realised_pnl = sum(
            (t.pnl or 0) for t in st.session_state.get("paper_history", [])
            if getattr(t, "closed_at", None)
            and getattr(t, "closed_at").date() == datetime.now().date()
        )
        unrealised_pnl = sum(
            ((p.last_price or p.entry or 0) - (p.entry or 0)) * (p.quantity or 0)
            for p in st.session_state.paper_positions.values()
        )
        return {
            "initialized": self.is_initialized(),
            "interval": self.interval,
            "symbols_monitored": len(self.symbols),
            "market_open": self.provider.is_market_open(),
            "last_tick": self.last_tick_time,
            "total_ticks": self.total_ticks,
            "next_candle_seconds": self.next_candle_seconds(),
            "open_positions": len(st.session_state.paper_positions),
            "closed_positions_today": sum(
                1 for t in st.session_state.get("paper_history", [])
                if getattr(t, "closed_at", None)
                and getattr(t, "closed_at").date() == datetime.now().date()
            ),
            "realised_pnl_today": realised_pnl,
            "unrealised_pnl": unrealised_pnl,
        }


# ------------------------ Live engine session bootstrap ------------------
if "live_engine" not in st.session_state:
    st.session_state.live_engine = LiveMarketEngine(YFinanceProvider())

if "live_enabled" not in st.session_state:
    st.session_state.live_enabled = False

if "live_interval" not in st.session_state:
    st.session_state.live_interval = "5m"

# ------------------------ Live Market UI (Developer only) -----------------
if _P("Developer Mode"):
    st.divider()
    st.subheader("Live Market Engine")

    _engine = st.session_state.live_engine
    _status = _engine.status()

    _cfg_cols = st.columns([1, 1, 1, 2])

    _cfg_cols[0].selectbox(
        "Interval",
        options=["1m", "5m", "15m", "30m", "1h", "1d"],
        key="live_interval",
    )

    _cfg_cols[1].toggle(
        "Live monitoring",
        key="live_enabled",
        help="When ON, fetches only the newest candle for the current scan "
             "universe on every tick and re-runs the full pipeline "
             "(strategies -> AI -> Risk -> Portfolio -> Paper Trades). "
             "History is downloaded ONCE per session, not on every tick.",
    )

    if _cfg_cols[2].button("Initialize session", key="live_init_btn"):
        _syms = st.session_state.get("scan_universe") or []
        if not _syms:
            st.warning(
                "Live Market Engine needs a scan list first. Build one on the "
                "Scanner tab (Preview Scan List) or run any RUN ALPHAQUANT "
                "cycle to populate the universe."
            )
        else:
            with st.spinner(f"Downloading history for {len(_syms)} symbols..."):
                _n = _engine.initialize_session(_syms, st.session_state.live_interval)
            st.success(
                f"Live session initialised: {_n} symbols on {st.session_state.live_interval} "
                f"interval. History cached; only newest candle will be fetched from here on."
            )

    _next_secs = _status["next_candle_seconds"] if _status["initialized"] else 0
    _cfg_cols[3].metric(
        "Next candle in",
        f"{_next_secs // 60:02d}:{_next_secs % 60:02d}" if _status["initialized"] else "--:--",
    )

    _st_cols = st.columns(6)
    _st_cols[0].metric(
        "Live status",
        "🟢 RUNNING" if (st.session_state.live_enabled and _status["market_open"] and _status["initialized"])
        else ("🟡 ARMED" if st.session_state.live_enabled else "⚪ OFF"),
    )
    _st_cols[1].metric("Monitored", _status["symbols_monitored"])
    _st_cols[2].metric(
        "Last candle",
        _status["last_tick"].strftime("%H:%M:%S") if _status["last_tick"] else "-",
    )
    _st_cols[3].metric("Open positions", _status["open_positions"])
    _st_cols[4].metric(
        "Realised P&L (today)",
        f"₹{_status['realised_pnl_today']:,.0f}",
    )
    _st_cols[5].metric(
        "Unrealised P&L",
        f"₹{_status['unrealised_pnl']:,.0f}",
    )

    if _engine.last_tick_stats:
        with st.expander("Last tick details"):
            _lt = _engine.last_tick_stats
            st.write(f"**Status:** `{_lt.get('status')}`")
            st.write(f"**Time:** {_lt.get('tick_time')}")
            st.write(f"**Candles added:** {_lt.get('candles_added', 0)}")
            if _lt.get("new_candle_symbols"):
                st.write(f"**New candle for:** {', '.join(_lt['new_candle_symbols'][:20])}"
                         + ("..." if len(_lt['new_candle_symbols']) > 20 else ""))
            st.write(f"**Pipeline:** {_lt.get('pipeline', '-')}")


# ------------------------ Live tick fragment ------------------------------
# Runs at the finest supported cadence (30s) but the tick() itself is a
# no-op unless the market is open, the engine is initialised, and live
# monitoring is enabled. We keep the fragment cadence FIXED and let the
# engine internally decide whether to actually do work; this is cheaper
# than re-mounting the fragment on every interval change.

_LIVE_TICK_SECONDS = 30

@st.fragment(run_every=_LIVE_TICK_SECONDS)
def live_market_tick_fragment():
    if not st.session_state.get("live_enabled"):
        return
    _engine = st.session_state.get("live_engine")
    if _engine is None or not _engine.is_initialized():
        return
    if not _engine.provider.is_market_open():
        return
    # Only tick if at least one candle-interval has elapsed since last tick
    interval_sec = LiveMarketEngine._INTERVAL_SECONDS.get(_engine.interval, 300)
    if _engine.last_tick_time:
        elapsed = (datetime.now() - _engine.last_tick_time).total_seconds()
        if elapsed < interval_sec:
            return
    _engine.tick()


live_market_tick_fragment()


# =====================================================
# BROKER PROVIDERS (Multi-broker WebSocket integration)
# VERSION 1.0.0
# =====================================================
# Concrete MarketDataProvider implementations for real Indian brokers.
# Each wraps the broker's official SDK/REST + WebSocket. Ticks are
# aggregated into candles matching the app's interval selection so ALL
# downstream engines (strategies, AI, risk, portfolio, paper) see the
# same DataFrame shape regardless of upstream provider.
#
# CREDENTIALS: no secrets are hard-coded. Users paste api_key / access_token
# on the Settings tab. Tokens live only in st.session_state for that session.
# =====================================================

INTERVAL_MINUTES_MAP = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "1d": 1440}


class CandleAggregator:
    """Rolls raw ticks into interval-bucketed OHLCV candles."""

    def __init__(self, interval: str):
        self.interval = interval
        self.current_start = None
        self.row = None
        self.prev_cum_volume = None

    def _floor_bucket(self, ts):
        mins = INTERVAL_MINUTES_MAP[self.interval]
        ts = ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=IST)
        ts = ts.astimezone(IST)
        if mins == 1440:
            return datetime(ts.year, ts.month, ts.day, tzinfo=IST)
        bucket_min = (ts.minute // mins) * mins
        return ts.replace(minute=bucket_min, second=0, microsecond=0)

    def push(self, ts, price, qty=None, cum_volume=None):
        start = self._floor_bucket(ts)
        completed = None
        if self.current_start is None:
            self.current_start = start
            self.row = {"timestamp": start, "Open": price, "High": price,
                        "Low": price, "Close": price, "Volume": 0}
        elif start != self.current_start:
            completed = self.row
            self.current_start = start
            self.row = {"timestamp": start, "Open": price, "High": price,
                        "Low": price, "Close": price, "Volume": 0}
            self.prev_cum_volume = None
        self.row["High"] = max(self.row["High"], price)
        self.row["Low"] = min(self.row["Low"], price)
        self.row["Close"] = price
        if cum_volume is not None:
            delta = cum_volume if self.prev_cum_volume is None else max(cum_volume - self.prev_cum_volume, 0)
            self.prev_cum_volume = cum_volume
            self.row["Volume"] += delta
        elif qty is not None:
            self.row["Volume"] += int(qty)
        return completed

    def snapshot(self):
        return dict(self.row) if self.row else None


def _period_to_start(period: str, now=None):
    now = (now or datetime.now(IST)).astimezone(IST)
    n = int(''.join(c for c in period if c.isdigit()) or "1")
    unit = ''.join(c for c in period if c.isalpha()).lower()
    if unit.startswith("d"): return now - timedelta(days=n)
    if unit.startswith("mo"): return now - timedelta(days=30 * n)
    if unit in ("y", "yr", "year"): return now - timedelta(days=365 * n)
    return now - timedelta(days=n)


class UpstoxProvider(MarketDataProvider):
    """
    Upstox Uplink v3 provider. Historical via REST; live ticks via
    upstox-python-sdk MarketDataStreamerV3.
    Requires: pip install upstox-python-sdk requests
    Docs: https://upstox.com/developer/api-documentation/
    """

    UPSTOX_HIST = {"1m": ("minutes", 1), "5m": ("minutes", 5), "15m": ("minutes", 15),
                   "30m": ("minutes", 30), "1h": ("hours", 1), "1d": ("days", 1)}

    def __init__(self, api_key: str, access_token: str, symbol_map: dict):
        self.api_key = api_key
        self.access_token = access_token
        self.symbol_map = symbol_map
        self.aggregators = {sym: CandleAggregator("1m") for sym in symbol_map}
        self._streamer = None

    def is_market_open(self, now=None):
        return is_market_open(now)

    def download_history(self, symbols, interval, period="1y"):
        import requests as _rq
        unit, step = self.UPSTOX_HIST.get(interval, ("days", 1))
        end = datetime.now(IST).date()
        start = _period_to_start(period).date()
        headers = {"Accept": "application/json", "Authorization": "Bearer " + self.access_token}
        out = {}
        for sym in symbols:
            instrument_key = self.symbol_map.get(sym)
            if not instrument_key:
                continue
            url = ("https://api.upstox.com/v3/historical-candle/"
                   + instrument_key + "/" + unit + "/" + str(step)
                   + "/" + str(end) + "/" + str(start))
            try:
                r = _rq.get(url, headers=headers, timeout=20)
                r.raise_for_status()
                candles = r.json().get("data", {}).get("candles", [])
                if not candles:
                    continue
                cols = ["timestamp", "Open", "High", "Low", "Close", "Volume", "OI"][:len(candles[0])]
                df = pd.DataFrame(candles, columns=cols)
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
                df = df.set_index("timestamp").sort_index()
                out[sym] = df
            except Exception as exc:
                logging.warning("Upstox history failed for %s: %s", sym, exc)
        return out

    def fetch_latest_batch(self, symbols, interval):
        out = {}
        for sym in symbols:
            agg = self.aggregators.get(sym) or CandleAggregator(interval)
            snap = agg.snapshot()
            if snap:
                idx = pd.DatetimeIndex([snap.pop("timestamp")], name="timestamp")
                out[sym] = pd.DataFrame([snap], index=idx)
        return out

    def start_stream(self, symbols):
        import upstox_client
        conf = upstox_client.Configuration()
        conf.access_token = self.access_token
        api_client = upstox_client.ApiClient(conf)
        instrument_keys = [self.symbol_map[s] for s in symbols if s in self.symbol_map]
        self._streamer = upstox_client.MarketDataStreamerV3(
            api_client, instrumentKeys=instrument_keys, mode="ltpc",
        )
        self._streamer.on("message", self._on_message)
        self._streamer.connect()

    def _on_message(self, msg):
        for instrument_key, feed in (msg or {}).get("feeds", {}).items():
            ltpc = feed.get("ltpc") or feed.get("ff", {}).get("marketFF", {}).get("ltpc")
            if not ltpc:
                continue
            price, ts_raw = ltpc.get("ltp"), ltpc.get("ltt")
            if price is None or ts_raw is None:
                continue
            ts = pd.to_datetime(int(ts_raw), unit="ms", utc=True).tz_convert(IST).to_pydatetime()
            cum_vol = feed.get("vtt") or ltpc.get("vtt")
            for sym, key in self.symbol_map.items():
                if key == instrument_key:
                    self.aggregators[sym].push(ts, float(price),
                                               cum_volume=int(cum_vol) if cum_vol is not None else None)
                    break


class ZerodhaProvider(MarketDataProvider):
    """
    Zerodha Kite Connect provider. Historical via kiteconnect REST; live
    ticks via KiteTicker WebSocket.
    Requires: pip install kiteconnect
    Docs: https://kite.trade/docs/connect/v3/
    """

    KITE_HIST = {"1m": "minute", "5m": "5minute", "15m": "15minute",
                 "30m": "30minute", "1h": "60minute", "1d": "day"}

    def __init__(self, api_key: str, access_token: str, symbol_map: dict):
        self.api_key = api_key
        self.access_token = access_token
        self.symbol_map = symbol_map
        self.token_to_symbol = {int(v): k for k, v in symbol_map.items()}
        self.aggregators = {sym: CandleAggregator("1m") for sym in symbol_map}
        self._ticker = None
        self._kite = None

    def is_market_open(self, now=None):
        return is_market_open(now)

    def _kite_client(self):
        if self._kite is None:
            from kiteconnect import KiteConnect
            self._kite = KiteConnect(api_key=self.api_key)
            self._kite.set_access_token(self.access_token)
        return self._kite

    def download_history(self, symbols, interval, period="1y"):
        kite = self._kite_client()
        end = datetime.now(IST)
        start = _period_to_start(period, end)
        kite_interval = self.KITE_HIST.get(interval, "day")
        out = {}
        for sym in symbols:
            token = self.symbol_map.get(sym)
            if not token:
                continue
            try:
                rows = kite.historical_data(int(token), start, end, kite_interval, continuous=False, oi=False)
                if not rows:
                    continue
                df = pd.DataFrame(rows).rename(columns={
                    "date": "timestamp", "open": "Open", "high": "High",
                    "low": "Low", "close": "Close", "volume": "Volume",
                })
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
                df = df.set_index("timestamp").sort_index()
                out[sym] = df
            except Exception as exc:
                logging.warning("Zerodha history failed for %s: %s", sym, exc)
        return out

    def fetch_latest_batch(self, symbols, interval):
        out = {}
        for sym in symbols:
            agg = self.aggregators.get(sym) or CandleAggregator(interval)
            snap = agg.snapshot()
            if snap:
                idx = pd.DatetimeIndex([snap.pop("timestamp")], name="timestamp")
                out[sym] = pd.DataFrame([snap], index=idx)
        return out

    def start_stream(self, symbols):
        from kiteconnect import KiteTicker
        tokens = [int(self.symbol_map[s]) for s in symbols if s in self.symbol_map]
        self._ticker = KiteTicker(self.api_key, self.access_token)
        self._ticker.on_connect = lambda ws, resp: (
            self._ticker.subscribe(tokens),
            self._ticker.set_mode(self._ticker.MODE_FULL, tokens),
        )
        self._ticker.on_ticks = self._on_ticks
        self._ticker.auto_reconnect(True, 5, 10)
        self._ticker.connect(threaded=True)

    def _on_ticks(self, ws, ticks):
        for t in ticks or []:
            sym = self.token_to_symbol.get(int(t.get("instrument_token", 0)))
            if not sym:
                continue
            price = float(t.get("last_price") or 0)
            ts = t.get("exchange_timestamp") or t.get("last_trade_time") or datetime.now(IST)
            if isinstance(ts, str):
                ts = pd.to_datetime(ts, utc=True).tz_convert(IST).to_pydatetime()
            cum_vol = t.get("volume_traded")
            qty = t.get("last_traded_quantity") or t.get("last_quantity")
            self.aggregators[sym].push(ts, price, qty=qty, cum_volume=cum_vol)


# ------------------------ Stub providers -----------------------------
class _StubBrokerProvider(MarketDataProvider):
    BROKER_NAME = "Stub"
    DOCS_URL = ""
    def __init__(self, api_key: str = "", access_token: str = "", symbol_map=None):
        self.api_key = api_key
        self.access_token = access_token
        self.symbol_map = symbol_map or {}
    def is_market_open(self, now=None): return is_market_open(now)
    def download_history(self, symbols, interval, period="1y"):
        raise NotImplementedError(self.BROKER_NAME + " provider not implemented. Docs: " + self.DOCS_URL)
    def fetch_latest_batch(self, symbols, interval):
        raise NotImplementedError(self.BROKER_NAME + " provider not implemented. Docs: " + self.DOCS_URL)


class AngelOneProvider(_StubBrokerProvider):
    BROKER_NAME = "AngelOne SmartAPI"
    DOCS_URL = "https://smartapi.angelbroking.com/docs"

class DhanProvider(_StubBrokerProvider):
    BROKER_NAME = "Dhan HQ"
    DOCS_URL = "https://dhanhq.co/docs"

class FyersProvider(_StubBrokerProvider):
    BROKER_NAME = "Fyers"
    DOCS_URL = "https://myapi.fyers.in/docsv3"

class FivePaisaProvider(_StubBrokerProvider):
    BROKER_NAME = "5paisa"
    DOCS_URL = "https://www.5paisa.com/developerapi/overview"

class IIFLProvider(_StubBrokerProvider):
    BROKER_NAME = "IIFL Markets"
    DOCS_URL = "https://www.iiflsecurities.com/traderterminal/apidocs"


BROKER_REGISTRY = {
    "YFinance (default, no login)": YFinanceProvider,
    "Upstox": UpstoxProvider,
    "Zerodha Kite": ZerodhaProvider,
    "AngelOne SmartAPI (coming soon)": AngelOneProvider,
    "Dhan HQ (coming soon)": DhanProvider,
    "Fyers (coming soon)": FyersProvider,
    "5paisa (coming soon)": FivePaisaProvider,
    "IIFL Markets (coming soon)": IIFLProvider,
}


# =====================================================
# PROFESSIONAL BROKER / DATABASE / SCHEDULER ARCHITECTURE
# =====================================================

@dataclass
class BrokerProfile:
    name: str
    broker_name: str = "Upstox"
    api_key: str = ""
    api_secret: str = ""
    access_token: str = ""
    refresh_token: str = ""
    client_id: str = ""
    user_id: str = ""
    redirect_url: str = ""
    totp: str = ""
    mode: str = "Paper"
    market_data_enabled: bool = True
    execution_enabled: bool = False
    connection_status: str = "Disconnected"
    last_login: str = ""
    auto_reconnect: bool = True


class BrokerConfigManager:
    storage_path = Path(_APP_DIR) / "data" / "broker_profiles.json"

    def __init__(self):
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        st.session_state.setdefault("broker_profiles", self.load())

    def load(self):
        if not self.storage_path.exists():
            return {}
        return json.loads(self.storage_path.read_text() or "{}")

    def save(self, profile):
        data = profile if isinstance(profile, dict) else profile.__dict__
        profiles = st.session_state.setdefault("broker_profiles", self.load())
        profiles[data["name"]] = data
        self.storage_path.write_text(json.dumps(profiles, indent=2, default=str))
        return data

    def delete(self, name):
        profiles = st.session_state.setdefault("broker_profiles", self.load())
        removed = profiles.pop(name, None)
        self.storage_path.write_text(json.dumps(profiles, indent=2, default=str))
        return removed is not None

    def validate(self, profile):
        data = profile if isinstance(profile, dict) else profile.__dict__
        return bool(data.get("name") and data.get("broker_name"))

    def connect(self, name):
        profile = st.session_state.setdefault("broker_profiles", self.load()).get(name)
        if not profile:
            return False, "Profile not found"
        broker = create_broker(profile)
        ok = broker.connect()
        profile["connection_status"] = "Connected" if ok else "Disconnected"
        profile["last_login"] = datetime.now().isoformat() if ok else profile.get("last_login", "")
        self.save(profile)
        st.session_state["active_broker_client"] = broker
        return ok, profile["connection_status"]

    def disconnect(self, name):
        broker = st.session_state.get("active_broker_client")
        if broker:
            broker.disconnect()
        profile = st.session_state.setdefault("broker_profiles", self.load()).get(name, {})
        profile["connection_status"] = "Disconnected"
        if profile:
            self.save(profile)
        return True


class UpstoxBroker(BrokerBase):
    base_url = "https://api.upstox.com/v2"
    def __init__(self, profile):
        self.profile = profile
        self.connected = False
    def _headers(self): return {"Authorization": "Bearer " + self.profile.get("access_token", ""), "Accept": "application/json"}
    def connect(self): self.connected = bool(self.profile.get("access_token") or self.profile.get("api_key")); return self.connected
    def disconnect(self): self.connected = False; return True
    def login(self): return self.connect()
    def logout(self): return self.disconnect()
    def reconnect(self): self.disconnect(); return self.connect()
    def _get(self, path):
        if not self.connected: self.connect()
        r = requests.get(self.base_url + path, headers=self._headers(), timeout=15); r.raise_for_status(); return r.json()
    def _putpost(self, method, path, payload):
        if not self.connected: self.connect()
        r = requests.request(method, self.base_url + path, headers={**self._headers(), "Content-Type": "application/json"}, json=payload, timeout=15); r.raise_for_status(); return r.json()
    def historical(self, instrument_key, interval="day", to_date=None, from_date=None): return self._get(f"/historical-candle/{instrument_key}/{interval}/{to_date or datetime.now().date()}/{from_date or datetime.now().date()}")
    def quote(self, instrument_key): return self._get("/market-quote/quotes?instrument_key=" + instrument_key)
    def websocket(self): return None
    def funds(self): return self._get("/user/get-funds-and-margin") if self.profile.get("access_token") else {}
    def margin(self): return self.funds()
    def positions(self): return self._get("/portfolio/short-term-positions") if self.profile.get("access_token") else []
    def holdings(self): return self._get("/portfolio/long-term-holdings") if self.profile.get("access_token") else []
    def orders(self): return self._get("/order/retrieve-all") if self.profile.get("access_token") else []
    def place_order(self, order): return self._putpost("POST", "/order/place", order) if self.profile.get("access_token") else {**order, "status": "DRY_RUN"}
    def cancel_order(self, order_id): return self._putpost("DELETE", "/order/cancel?order_id=" + str(order_id), {}) if self.profile.get("access_token") else True
    def modify_order(self, order_id, **changes): return self._putpost("PUT", "/order/modify", {**changes, "order_id": order_id}) if self.profile.get("access_token") else {"order_id": order_id, **changes}


class GenericBroker(UpstoxBroker):
    def place_order(self, order): return {**order, "status": "ROUTED", "broker": self.profile.get("broker_name")}


def create_broker(profile):
    if profile.get("mode") == "Paper":
        return PaperBroker()
    if profile.get("broker_name") == "Upstox":
        return UpstoxBroker(profile)
    return GenericBroker(profile)


class HistoricalDatabaseEngine:
    def __init__(self, manager=None): self.manager = manager or MarketDataManager()
    def sync(self, symbols, intervals=("1d", "1wk", "1mo")): return self.manager.morning_sync(symbols, intervals)
    def detect_gaps(self, symbol, interval="1d"): return self.manager.detect_gaps(symbol, interval)
    def repair(self, symbols, interval="1d"): return self.manager.repair_history(symbols, interval)
    def backup(self, symbols, interval="1d"): return {s: self.manager.backup_history(s, interval) for s in symbols}
    def lookup(self, symbol, interval="1d"): return self.manager._load(symbol, interval)


class TradingScheduler:
    def __init__(self): self.events = st.session_state.setdefault("trading_scheduler_events", [])
    def startup(self):
        symbols = st.session_state.get("scan_universe", [])
        if symbols: HistoricalDatabaseEngine().sync(symbols, (st.session_state.get("live_interval", "5m"), "1d"))
        active = st.session_state.get("active_broker_profile")
        if active: BrokerConfigManager().connect(active)
        st.session_state["live_enabled"] = True
        self.events.append({"event": "startup", "time": datetime.now()}); return True
    def market_close(self):
        active = st.session_state.get("active_broker_profile")
        if active: BrokerConfigManager().disconnect(active)
        self.events.append({"event": "market_close", "time": datetime.now()}); return True
    def background_cycle(self):
        if is_market_open(): return self.startup()
        return True



# =====================================================
# CENTRAL APPLICATION ORCHESTRATOR
# =====================================================

class AlphaQuantOrchestrator:
    """The sole UI-facing coordinator for the complete trading workflow."""

    def run(self, trigger: str = "MANUAL") -> tuple[bool, str]:
        ok, message = run_alphaquant(trigger=trigger)
        if not ok:
            return ok, message

        try:
            st.session_state.run_complete_scan_requested = False
            execute_scan_pipeline()
            monitor_open_positions()
            SystemHealthEngine().mark("last_live_tick")
            st.session_state.last_cycle_message = (
                f"{len(st.session_state.final_trade_list)} trade candidate(s), "
                f"{len(st.session_state.paper_positions)} open position(s)."
            )
            st.session_state["autonomous_active"] = True
            st.session_state["stop_requested"] = False
            st.session_state["pipeline_state"] = "MONITORING"
            WORKSPACE.save()
            return True, "AlphaQuant workflow completed; recurring monitoring is active. " + st.session_state.last_cycle_message
        except Exception as exc:
            CentralErrorManager().record_error("Run AlphaQuant", "orchestrator", exc)
            PipelineDiagnostics().fail_phase("Run AlphaQuant", str(exc))
            logging.exception("AlphaQuant orchestration failed")
            return False, f"AlphaQuant stopped: {exc}"


def _money(value):
    if value is None: return "N/A"
    try: return f"₹{float(value):,.2f}"
    except (TypeError, ValueError): return "N/A"


def render_profile():
    prefs=WORKSPACE.preferences
    st.subheader("Profile / Trading Preferences")
    st.markdown('<div class="aq-panel-title">Capital and Execution Settings</div>',unsafe_allow_html=True)
    risk=dict(prefs.get("risk_preferences",{})); c1,c2=st.columns(2)
    execution=c1.radio("Execution Mode",["PAPER","LIVE"],index=0 if prefs.get("execution_mode","PAPER")=="PAPER" else 1,horizontal=True,key="profile_execution_mode")
    paper_mode = execution == "PAPER"
    capital=(c2.number_input("Paper Trading Starting Capital (₹) · simulated",min_value=1.0,value=float(prefs["paper_trading_capital"]),step=10000.0,key="profile_paper_starting_capital")
        if paper_mode else float(prefs["paper_trading_capital"]))
    risk_per=c1.number_input("Risk Per Trade (%)",0.01,100.0,float(risk.get("risk_per_trade",1.0)),key="profile_risk_per_trade")
    daily=c2.number_input("Maximum Daily Loss (%)",0.01,100.0,float(risk.get("maximum_daily_loss",3.0)),key="profile_maximum_daily_loss")
    maxpos=c1.number_input("Maximum Open Positions",1,100,int(prefs.get("maximum_positions",10)),key="profile_maximum_open_positions")
    position=c2.number_input("Maximum Position Size (%)",0.01,100.0,float(risk.get("maximum_position_size",10.0)),key="profile_maximum_position_size")
    portfolio=c1.number_input("Maximum Portfolio Exposure (%)",0.01,100.0,float(risk.get("maximum_portfolio_exposure",80.0)),key="profile_maximum_portfolio_exposure")
    sector=c2.number_input("Maximum Sector Exposure (%)",0.01,100.0,float(risk.get("maximum_sector_exposure",25.0)),key="profile_maximum_sector_exposure")
    reserve=c1.number_input("Minimum Cash Reserve (%)",0.0,99.99,float(risk.get("minimum_cash_reserve",20.0)),key="profile_minimum_cash_reserve")
    changed=capital != float(prefs["paper_trading_capital"])
    has_trades=bool(st.session_state.get("paper_history") or st.session_state.get("paper_positions"))
    if changed and has_trades: st.warning("Existing paper trades detected. New capital will apply from the next clean session; historical P&L will not be rewritten.")
    if st.button("Save Capital and Risk Settings",type="primary",key="profile_save_capital_risk"):
        newrisk={"risk_per_trade":risk_per,"maximum_daily_loss":daily,"maximum_position_size":position,"maximum_portfolio_exposure":portfolio,"maximum_sector_exposure":sector,"minimum_cash_reserve":reserve}
        WORKSPACE.save(execution_mode=execution,maximum_positions=maxpos,risk_preferences=newrisk,paper_trading_capital=float(prefs["paper_trading_capital"]) if changed and has_trades else capital,paper_capital_pending=capital if changed and has_trades else None)
        if not has_trades: st.session_state.paper_capital=capital; st.session_state.paper_broker.update(cash=capital,starting_capital=capital)
        st.success("Settings saved." if not (changed and has_trades) else "Capital scheduled for the next paper-account reset.")
    if paper_mode: st.markdown("#### Reset Paper Account")
    confirm=st.checkbox("I understand that open simulated positions will be closed and the current ledger archived.",key="confirm_paper_reset") if paper_mode else False
    if paper_mode and st.button("Reset Paper Account",disabled=not confirm,key="profile_reset_paper_account"):
        archive=Path(_APP_DIR)/"data"/"paper_archives"; archive.mkdir(parents=True,exist_ok=True)
        snapshot={"archived_at":datetime.now().isoformat(),"ledger":st.session_state.get("paper_broker",{}),"history":[getattr(x,"__dict__",str(x)) for x in st.session_state.get("paper_history",[])],"positions":{k:getattr(v,"__dict__",str(v)) for k,v in st.session_state.get("paper_positions",{}).items()}}
        (archive/f"paper_account_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json").write_text(json.dumps(snapshot,default=str,indent=2))
        amount=float(prefs.get("paper_capital_pending") or capital); WORKSPACE.save(paper_trading_capital=amount,paper_capital_pending=None)
        st.session_state.paper_broker={"connected":False,"cash":amount,"starting_capital":amount,"positions":{},"orders":{},"trade_history":[],"realized_pnl":0.0,"risk":{}}
        st.session_state.paper_positions={}; st.session_state.paper_history=[]; st.session_state.paper_capital=amount
        st.success("Previous paper ledger archived and simulated account reset."); st.rerun()
    st.markdown('<div class="aq-panel-title">Display and Data Preferences</div>',unsafe_allow_html=True)
    p1,p2=st.columns(2); history=p1.selectbox("History period",["6mo","1y","2y","5y"],index=["6mo","1y","2y","5y"].index(prefs["history_period"]),key="profile_history_period"); interval=p2.selectbox("Default candle interval",["1m","5m","15m","30m","1h","1d","1wk"],index=["1m","5m","15m","30m","1h","1d","1wk"].index(prefs["candle_interval"]),key="profile_default_candle_interval"); developer=p2.toggle("Enable Developer mode",value=bool(prefs["developer_mode"]),key="profile_developer_mode")
    if st.button("Save Display Preferences",key="profile_save_display_preferences"): WORKSPACE.save(history_period=history,candle_interval=interval,developer_mode=developer); st.rerun()


def _extract_funds(payload):
    """Map common broker response names without inventing absent values."""
    if not isinstance(payload,dict): return {}
    data=payload.get("data",payload); data=data.get("equity",data) if isinstance(data,dict) else {}
    def first(*names):
        for name in names:
            if isinstance(data,dict) and data.get(name) is not None:return data[name]
        return None
    return {"Available Cash":first("available_margin","cash","available_cash"),"Used Margin":first("used_margin","utilised_margin"),"Available Margin":first("available_margin"),"Collateral":first("collateral","collateral_amount"),"Realized P&L":first("realized_pnl","realised_pnl"),"Unrealized P&L":first("unrealized_pnl","unrealised_pnl")}


# One process-wide truth for broker, routing and quote health.  It is separate
# from PaperBroker: execution routing and market-data transport are orthogonal.
BROKER_HEALTH_STATES = {"NOT_CONFIGURED", "DISCONNECTED", "AUTHENTICATING", "CONNECTED",
    "LIVE_DATA_CONNECTED", "READY_FOR_PAPER", "READY_FOR_LIVE", "DEGRADED", "ERROR"}
ONBOARDING_STATES = {"NOT_CONFIGURED", "BROKER_OPTIONAL", "BROKER_CONNECTING", "BROKER_CONNECTED",
    "DATA_READY", "CONFIG_READY", "READY_TO_RUN", "RUNNING", "DEGRADED", "ERROR"}
MARKET_DATA_SOURCES = {"BROKER_LIVE", "BROKER_SNAPSHOT", "YFINANCE_INTRADAY_FALLBACK",
    "HISTORICAL_CACHE", "UNAVAILABLE"}
UPSTOX_INDEX_INSTRUMENTS = {
    "NIFTY 50":"NSE_INDEX|Nifty 50", "BANK NIFTY":"NSE_INDEX|Nifty Bank",
    "FINNIFTY":"NSE_INDEX|Nifty Fin Service", "NIFTY MIDCAP":"NSE_INDEX|NIFTY MIDCAP 100",
    "NIFTY SMALLCAP":"NSE_INDEX|NIFTY SMLCAP 100", "INDIA VIX":"NSE_INDEX|India VIX",
}


class AuthoritativeBrokerState:
    """Thread-safe canonical state used by every UI surface and quote worker."""
    def __init__(self):
        self.lock = threading.RLock()
        self.values = {"broker_name":"Not selected", "profile_name":"", "configured":False,
            "authenticated":False, "connected":False, "market_data_connected":False,
            "execution_connected":False, "execution_mode":"PAPER", "data_source":"UNAVAILABLE",
            "last_quote_time":None, "last_sync_time":None, "latency_ms":None,
            "connection_error":None, "token_expiry":None, "health_status":"NOT_CONFIGURED",
            "substatuses":[], "onboarding_state":"NOT_CONFIGURED"}
        self.quotes = {}

    def update(self, **changes):
        with self.lock:
            if changes.get("health_status", self.values["health_status"]) not in BROKER_HEALTH_STATES:
                raise ValueError("Invalid broker health state")
            if changes.get("data_source", self.values["data_source"]) not in MARKET_DATA_SOURCES:
                raise ValueError("Invalid market-data source")
            self.values.update(changes)
            return dict(self.values)

    def snapshot(self):
        with self.lock:
            return {**self.values, "quotes": {k:dict(v) for k,v in self.quotes.items()}}

    def publish_quotes(self, quotes, source):
        now = datetime.now(timezone.utc)
        with self.lock:
            for symbol, quote in quotes.items():
                received = quote.get("received_at") or now
                previous = quote.get("previous_close")
                ltp = quote.get("ltp")
                valid_previous = previous is not None and float(previous) > 0
                change = float(ltp) - float(previous) if ltp is not None and valid_previous else None
                age = max(0.0, (now - _as_utc_datetime(received)).total_seconds())
                stale = age > 30
                self.quotes[symbol] = {
                    "instrument_key": quote.get("instrument_key", ""), "symbol": symbol,
                    "exchange": quote.get("exchange", "NSE"), "ltp": float(ltp) if ltp is not None else None,
                    "previous_close": float(previous) if valid_previous else None,
                    "previous_close_source": quote.get("previous_close_source", "BROKER_OHLC" if valid_previous else "UNAVAILABLE"),
                    "open": quote.get("open"), "high": quote.get("high"), "low": quote.get("low"),
                    "volume": quote.get("volume"), "change": change,
                    "change_percent": change / float(previous) * 100 if change is not None else None,
                    "broker_timestamp": quote.get("broker_timestamp", quote.get("timestamp")),
                    "received_at": received, "source": source,
                    "freshness_status": "STALE" if stale else "FRESH", "is_stale": stale,
                }
            if quotes:
                self.values.update(data_source=source, last_quote_time=now, last_sync_time=now,
                    market_data_connected=source in {"BROKER_LIVE", "BROKER_SNAPSHOT"})
                if source == "BROKER_LIVE": self.values["health_status"] = "LIVE_DATA_CONNECTED"
                elif self.values.get("authenticated"): self.values["health_status"] = "READY_FOR_PAPER" if self.values["execution_mode"] == "PAPER" else "CONNECTED"
        try:
            get_market_state().publish_quotes(quotes, source)
            get_market_state().update(
                broker_name=self.values.get("broker_name"),
                health_status=self.values.get("health_status"),
            )
        except Exception:
            logging.exception("MarketState sync failed")


def _as_utc_datetime(value):
    """Coerce broker timestamps safely; malformed timestamps are stale, not fatal."""
    try:
        parsed = pd.to_datetime(value, utc=True)
        return parsed.to_pydatetime()
    except (TypeError, ValueError, OverflowError):
        return datetime.fromtimestamp(0, timezone.utc)


def market_breadth(active_symbols=None, tolerance=1e-8):
    """Calculate breadth solely from the authoritative normalized snapshot."""
    quotes = get_broker_state().snapshot().get("quotes", {})
    symbols = list(active_symbols or quotes)
    counts = {"advancing": 0, "declining": 0, "unchanged": 0, "unavailable": 0}
    for symbol in symbols:
        quote = quotes.get(symbol, {})
        ltp, previous = quote.get("ltp"), quote.get("previous_close")
        if quote.get("is_stale") or ltp is None or previous is None:
            counts["unavailable"] += 1
        elif abs(float(ltp) - float(previous)) <= tolerance * max(1.0, abs(float(previous))):
            counts["unchanged"] += 1
        elif float(ltp) > float(previous):
            counts["advancing"] += 1
        else:
            counts["declining"] += 1
    valid = counts["advancing"] + counts["declining"] + counts["unchanged"]
    ratio = counts["advancing"] / max(1, counts["declining"])
    counts.update(total=len(symbols), valid=valid, advance_decline_ratio=ratio,
        classification="INSUFFICIENT DATA" if not valid else
        ("STRONG BREADTH" if ratio >= 1.5 else "WEAK BREADTH" if ratio <= 0.67 else "BALANCED"))
    return counts


def market_regime_snapshot(active_symbols=None):
    breadth = market_breadth(active_symbols)
    quotes = get_broker_state().snapshot().get("quotes", {})
    missing = []
    if not breadth["valid"]: missing.append("valid previous-close quotes")
    index_changes = [quotes.get(name, {}).get("change_percent") for name in ("NIFTY 50", "BANK NIFTY")]
    index_changes = [float(value) for value in index_changes if value is not None]
    if not index_changes: missing.append("NIFTY/BANK NIFTY movement")
    if missing: return {"state": "INSUFFICIENT DATA", "missing_inputs": missing, "breadth": breadth}
    mean_index = sum(index_changes) / len(index_changes)
    if abs(mean_index) >= 1.5: state = "VOLATILE"
    elif mean_index > .15 and breadth["advance_decline_ratio"] >= 1: state = "BULLISH"
    elif mean_index < -.15 and breadth["advance_decline_ratio"] <= 1: state = "BEARISH"
    elif breadth["classification"] in {"STRONG BREADTH", "WEAK BREADTH"}: state = breadth["classification"]
    else: state = "RANGE-BOUND"
    return {"state": state, "missing_inputs": [], "breadth": breadth, "index_change": mean_index}


def sector_snapshot(active_symbols=None):
    quotes = get_broker_state().snapshot().get("quotes", {})
    rows = []
    for symbol in list(active_symbols or quotes):
        quote = quotes.get(symbol, {})
        change = quote.get("change_percent")
        if change is None or quote.get("is_stale"): continue
        rows.append({"Symbol": symbol, "Sector": STOCK_SECTOR_MAP.get(symbol, "Other"),
            "Change %": float(change), "Volume": quote.get("volume")})
    if not rows: return pd.DataFrame(columns=["Sector", "Advancing", "Declining", "Average Change %", "Median Change %", "Valid Symbols", "Strongest", "Weakest", "State"])
    frame = pd.DataFrame(rows)
    output = []
    for sector, group in frame.groupby("Sector", sort=True):
        strongest = group.loc[group["Change %"].idxmax(), "Symbol"]
        weakest = group.loc[group["Change %"].idxmin(), "Symbol"]
        average = group["Change %"].mean()
        output.append({"Sector": sector, "Advancing": int((group["Change %"] > 0).sum()),
            "Declining": int((group["Change %"] < 0).sum()), "Average Change %": average,
            "Median Change %": group["Change %"].median(), "Valid Symbols": len(group),
            "Strongest": strongest, "Weakest": weakest,
            "State": "STRONG" if average > .25 else "WEAK" if average < -.25 else "MIXED"})
    return pd.DataFrame(output)


@st.cache_resource(show_spinner=False)
def get_broker_connection_manager():
    return BrokerConnectionManager()


@st.cache_resource(show_spinner=False)
def get_broker_state():
    return AuthoritativeBrokerState()


class BrokerQuoteWorker:
    """Sole short-TTL snapshot poller; never calls Streamlit from its thread."""
    def __init__(self, state):
        self.state, self.lock = state, threading.RLock()
        self.stop_event, self.thread, self.profile = threading.Event(), None, None

    def start(self, profile):
        with self.lock:
            self.profile = {k:v for k,v in profile.items() if k not in {"api_secret","refresh_token","totp"}}
            if self.thread and self.thread.is_alive(): return False
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._run, name="alphaquant-broker-quotes", daemon=True)
            self.thread.start(); return True

    def stop(self):
        self.stop_event.set()

    def _run(self):
        while not self.stop_event.wait(3.0):
            profile = dict(self.profile or {})
            token = profile.get("access_token")
            if not token: continue
            try:
                response = requests.get("https://api.upstox.com/v2/market-quote/quotes",
                    headers={"Authorization":f"Bearer {token}", "Accept":"application/json"},
                    params={"instrument_key":",".join(UPSTOX_INDEX_INSTRUMENTS.values())}, timeout=10)
                response.raise_for_status()
                data = response.json().get("data", {})
                normalized = {}
                for symbol, key in UPSTOX_INDEX_INSTRUMENTS.items():
                    raw = data.get(key) or data.get(key.replace("|", ":")) or {}
                    ohlc = raw.get("ohlc", {}) or {}
                    ltp = raw.get("last_price") or raw.get("ltp")
                    previous = ohlc.get("close") or raw.get("previous_close")
                    if ltp is None: continue
                    normalized[symbol] = {"instrument_key":key, "ltp":float(ltp),
                        "open":ohlc.get("open"), "high":ohlc.get("high"), "low":ohlc.get("low"),
                        "previous_close":previous, "previous_close_source":"BROKER_OHLC" if previous else "UNAVAILABLE",
                        "volume":raw.get("volume"), "timestamp":raw.get("last_trade_time") or datetime.now(timezone.utc),
                        "received_at":datetime.now(timezone.utc)}
                self.state.publish_quotes(normalized, "BROKER_SNAPSHOT")
                get_core_runtime().heartbeat("market-data")
            except Exception as exc:
                # Never leak response bodies or credential-bearing SDK objects.
                self.state.update(market_data_connected=False, data_source="YFINANCE_INTRADAY_FALLBACK",
                    health_status="DEGRADED", connection_error="Broker quote polling failed; delayed fallback remains available.")
                get_core_runtime().heartbeat("market-data", error=exc)


@st.cache_resource(show_spinner=False)
def get_broker_quote_worker():
    return BrokerQuoteWorker(get_broker_state())


def _friendly_connection_failure(exc=None, status_code=None):
    """Translate transport failures into a corrective, credential-safe message."""
    if status_code in {401, 403}:
        return "Access token is invalid or expired.", "Enter a current access token and reconnect."
    if status_code == 429:
        return "Broker service is temporarily unavailable.", "Wait a few minutes, then reconnect."
    if status_code and status_code >= 500:
        return "Broker service is temporarily unavailable.", "Try again later."
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return "Network connection failed.", "Check internet access and reconnect."
    return "Authentication or quote permission could not be verified.", "Check the API key, secret, token, and quote API permission."


def _safe_connection_test(profile):
    """SAVE & CONNECT: auth, profile, quote, funds, holdings, positions, orders."""
    from broker.upstox_adapter import UpstoxAdapter
    from broker.credential_store import CredentialStore

    started = time.perf_counter()
    result = {
        "success": False, "authenticated": False, "profile_api": False, "quote_api": False,
        "funds_api": False, "holdings_api": False, "positions_api": False, "orders_api": False,
        "latency_ms": None, "message": "Connection was not tested.", "reason": "", "action": "", "details": {},
    }
    missing = [label for key, label in (
        ("api_key", "API key"), ("api_secret", "API secret"), ("access_token", "Access token"),
    ) if not str(profile.get(key) or "").strip()]
    if missing:
        result.update(reason=f"INVALID CREDENTIALS — missing {', '.join(missing)}", action="Complete every required field.")
        return result

    merged = dict(profile)
    store = CredentialStore()
    if profile.get("name"):
        merged = store.merge_secrets(profile["name"], profile)

    status = UpstoxAdapter().authenticate(merged)
    caps = status.capabilities or {}
    result.update(
        authenticated=bool(caps.get("authentication")),
        profile_api=bool(caps.get("profile")),
        quote_api=bool(caps.get("quote_api")),
        funds_api=bool(caps.get("funds")),
        holdings_api=bool(caps.get("holdings")),
        positions_api=bool(caps.get("positions")),
        orders_api=bool(caps.get("orders")),
        success=status.connected,
        details={"capabilities": caps, "api_versions": __import__("market.upstox_v3_feed", fromlist=["UPSTOX_API_VERSIONS"]).UPSTOX_API_VERSIONS},
    )
    if status.connected:
        result["message"] = "CONNECTED — Authentication and market data available."
    else:
        msg = status.message or "CONNECTION FAILED"
        if "TOKEN EXPIRED" in msg.upper():
            result.update(reason="TOKEN EXPIRED", action="Renew access token and reconnect.")
        elif "NETWORK" in msg.upper():
            result.update(reason="NETWORK ERROR", action="Check internet and retry.")
        elif "QUOTE" in msg.upper():
            result.update(reason="QUOTE ACCESS FAILED", action="Enable market-quote permission.")
        elif "PROFILE" in msg.upper():
            result.update(reason="PROFILE ACCESS FAILED", action="Verify API credentials.")
        else:
            result.update(reason=msg.split(".")[0] if msg else "CONNECTION FAILED", action=status.message or "")
    result["latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
    return result


def _token_reminder(profile):
    raw=profile.get("token_expiry_date") or profile.get("token_expiry")
    if not raw: return "", "NONE"
    try: expiry=pd.Timestamp(raw).date(); days=(expiry-datetime.now(timezone.utc).date()).days
    except (TypeError,ValueError): return "Token expiry date needs correction.", "ERROR"
    broker=str(profile.get("broker_name","Broker")).title()
    if days < 0: return f"{broker} token has expired. Reconnect now to restore service.","EXPIRED"
    if days == 0: return f"{broker} token expires today. Reconnect now to avoid interruption.","EXPIRING"
    if days == 1: return f"{broker} token expires tomorrow. Reconnect now to avoid interruption.","EXPIRING"
    if days in {2,3}: return f"{broker} token expires in {days} days. Plan to reconnect.","EXPIRING"
    if days <= 7: return f"{broker} token expires in {days} days. Plan to reconnect.","EXPIRING"
    return "","NONE"


def _apply_connection_result(profile,result):
    mode=str(profile.get("mode","Paper")).upper(); now=datetime.now(timezone.utc).isoformat()
    profile["last_successful_validation"]=now if result["success"] else profile.get("last_successful_validation")
    profile["last_failed_validation"]=now if not result["success"] else profile.get("last_failed_validation")
    reminder,reminder_state=_token_reminder(profile); profile["reminder_state"]=reminder_state
    get_broker_connection_manager().save_profile(profile)
    BrokerConfigManager().save(profile)
    get_broker_quote_worker().stop()
    feed_mgr = UpstoxV3FeedManager.instance()
    data_source = "UNAVAILABLE"
    if result["success"]:
        replay = Path(_APP_DIR) / "fixtures" / "upstox_v3_replay.json"
        use_replay = os.environ.get("UPSTOX_USE_REPLAY", "").lower() in {"1", "true", "yes"}
        if use_replay and replay.exists():
            feed_mgr.start(profile, replay_path=replay)
            data_source = "BROKER_LIVE"
        else:
            feed_mgr.start(profile)
            data_source = "BROKER_LIVE" if feed_mgr.health().get("connected") else "BROKER_SNAPSHOT"
    state=get_broker_state(); state.update(broker_name=profile.get("broker_name","Upstox"),profile_name=profile.get("name","default"),
        configured=True,authenticated=result["authenticated"],connected=result["success"],market_data_connected=result["quote_api"],
        execution_connected=False,execution_mode=mode,
        data_source=data_source if result["success"] else "YFINANCE_INTRADAY_FALLBACK",
        last_sync_time=datetime.now(timezone.utc),latency_ms=result["latency_ms"],connection_error=None if result["success"] else result.get("reason"),
        token_expiry=profile.get("token_expiry_date"),health_status=("READY_FOR_LIVE" if mode=="LIVE" else "READY_FOR_PAPER") if result["success"] else ("DEGRADED" if mode=="PAPER" else "ERROR"),
        substatuses=[],onboarding_state="BROKER_CONNECTED" if result["success"] else "DEGRADED")
    try:
        active = list(st.session_state.get("market_data", {}).keys()) or list(STOCK_SECTOR_MAP.keys())[:50]
        refresh_market_analytics(active[:200], STOCK_SECTOR_MAP)
    except Exception:
        logging.exception("Market analytics refresh failed")
    return state.snapshot()


def _broker_product_state():
    state=get_broker_state().snapshot(); profiles=st.session_state.get("broker_profiles",{}); profile=profiles.get(state.get("profile_name"),{})
    reminder,kind=_token_reminder(profile)
    if kind=="EXPIRED": return "TOKEN EXPIRED",reminder
    if kind=="EXPIRING": return "TOKEN EXPIRING",reminder
    if state.get("health_status")=="AUTHENTICATING": return "CONNECTING",reminder
    if state.get("connection_error") and state.get("execution_mode")=="LIVE": return "ERROR",reminder
    if state.get("authenticated") and state.get("data_source")=="BROKER_LIVE": return "CONNECTED — LIVE DATA",reminder
    if state.get("authenticated") and state.get("data_source")=="BROKER_SNAPSHOT": return "CONNECTED — SNAPSHOT DATA",reminder
    if state.get("authenticated"): return "CONNECTED",reminder
    if state.get("data_source")=="YFINANCE_INTRADAY_FALLBACK": return "DELAYED FALLBACK",reminder
    return "NOT CONNECTED",reminder


def _render_connection_result(result,profile):
    expiry=profile.get("token_expiry_date") or "not specified (user supplied)"
    if result["success"]:
        st.success(f"UPSTOX CONNECTED\n\nAuthentication successful\n\nMarket data available\n\nToken valid until {expiry}")
    else:
        st.error(f"UPSTOX CONNECTION FAILED\n\n{result['reason']}\n\n{result['action']}")


def render_broker_connection():
    prefs=WORKSPACE.preferences; bcm=BrokerConfigManager(); profiles=st.session_state.setdefault("broker_profiles",bcm.load())
    profile=dict(profiles.get("default",{"name":"default","broker_name":"Upstox","mode":prefs.get("execution_mode","PAPER").title()}))
    state_label,reminder=_broker_product_state(); st.markdown(f"**{state_label}**")
    if reminder: st.warning(reminder)
    saved=bool(profile.get("access_token")); editing=st.session_state.get("broker_editing",not saved)
    if saved and not editing:
        st.info("Credentials saved securely: API key •••••••• · API secret •••••••• · Access token ••••••••")
        st.caption(f"Expected token expiry: {profile.get('token_expiry_date','Not provided')} · Last successful validation: {profile.get('last_successful_validation','Never')} · Last failed validation: {profile.get('last_failed_validation','Never')}")
        a,b=st.columns(2)
        if a.button("EDIT CONNECTION",width="stretch",key="broker_edit"):
            st.session_state["broker_editing"]=True; st.rerun()
        if b.button("DISCONNECT",width="stretch",key="broker_disconnect"):
            get_broker_quote_worker().stop(); get_broker_state().update(authenticated=False,connected=False,market_data_connected=False,execution_connected=False,data_source="YFINANCE_INTRADAY_FALLBACK",health_status="DISCONNECTED",connection_error=None)
            st.session_state.pop("broker_connection_result",None); st.rerun()
    else:
        st.caption("Enter replacement values. Saved secrets are never displayed or inserted into these fields.")
        with st.form("broker_connection_form",clear_on_submit=True):
            broker=st.selectbox("Broker",["Upstox"],key="broker_name")
            api_key=st.text_input("API Key",type="password",value="",key="broker_api_key")
            api_secret=st.text_input("API Secret",type="password",value="",key="broker_api_secret")
            access_token=st.text_input("Access Token",type="password",value="",key="broker_access_token")
            expiry=st.date_input("Token Expiry Date",value=None,key="broker_expiry",help="Expected expiry supplied by you; it is not described as broker-verified.")
            confirm=st.checkbox("Confirm replacement of the saved connection",value=not saved,key="broker_confirm")
            submitted=st.form_submit_button("SAVE AND CONNECT",type="primary",width="stretch")
        if submitted:
            if saved and not confirm: st.error("Confirm credential replacement to continue.")
            else:
                candidate={**profile,"name":"default","broker_name":broker,"mode":prefs.get("execution_mode","PAPER").title(),"api_key":api_key,"api_secret":api_secret,"access_token":access_token,"token_creation_date":datetime.now(timezone.utc).date().isoformat(),"token_expiry_date":expiry.isoformat() if expiry else None}
                get_broker_state().update(health_status="AUTHENTICATING",onboarding_state="BROKER_CONNECTING")
                with st.spinner("Validating configuration, profile access, and market quotes…"):
                    result=_safe_connection_test(candidate)
                _apply_connection_result(candidate,result); st.session_state["broker_connection_result"]=(result,candidate); st.session_state["broker_editing"]=not result["success"]
    if st.session_state.get("broker_connection_result"):
        result,candidate=st.session_state["broker_connection_result"]; _render_connection_result(result,candidate)


def render_profile_contents():
    prefs=WORKSPACE.preferences
    account,broker,notifications,display,advanced=st.tabs(["ACCOUNT","BROKER CONNECTION","NOTIFICATIONS","DISPLAY","ADVANCED"])
    with account:
        with st.form("account_form"):
            name=st.text_input("Display name",prefs.get("display_name","Trader")); zone=st.selectbox("Time zone",["Asia/Kolkata","UTC"],index=0 if prefs.get("time_zone","Asia/Kolkata")=="Asia/Kolkata" else 1)
            if st.form_submit_button("SAVE ACCOUNT"): WORKSPACE.save(display_name=name,time_zone=zone); st.success("Account saved.")
    with broker: render_broker_connection()
    with notifications:
        _,reminder=_broker_product_state()
        if reminder: st.warning(reminder)
        else: st.info("No broker token reminders.")
        st.markdown("#### News intelligence")
        news=dict(prefs.get("news_settings",{}))
        with st.form("news_settings_form"):
            enabled=st.toggle("Enable market news",value=bool(news.get("enabled",False)))
            provider=st.selectbox("Provider",["RSS","NewsAPI"],index=1 if news.get("provider")=="NewsAPI" else 0)
            api_key=st.text_input("News API key",type="password",value="",help="Stored only in process memory; NEWSAPI_KEY is preferred for restarts.")
            interval=st.number_input("Fetch interval (minutes)",5,240,int(news.get("fetch_interval_minutes",15)))
            a,b,c=st.columns(3)
            market_wide=a.checkbox("Market-wide news",value=news.get("market_wide",True)); portfolio=b.checkbox("Portfolio news",value=news.get("portfolio",True)); watchlist=c.checkbox("Watchlist news",value=news.get("watchlist",True))
            candidates=a.checkbox("Candidate news",value=news.get("candidates",True)); earnings=b.checkbox("Earnings announcements",value=news.get("earnings",True)); regulatory=c.checkbox("Regulatory announcements",value=news.get("regulatory",True)); corporate=a.checkbox("Corporate actions",value=news.get("corporate_actions",True))
            spoken=b.checkbox("Spoken briefing enabled",value=news.get("spoken_briefing",False)); auto_speak=c.checkbox("Auto-speak critical alerts",value=news.get("auto_speak_critical",False),help="Off by default; browser permission may still require interaction.")
            rate=st.slider("Speech rate",0.5,2.0,float(news.get("speech_rate",1.0)),0.1); pitch=st.slider("Speech pitch",0.5,2.0,float(news.get("speech_pitch",1.0)),0.1); volume=st.slider("Speech volume",0.0,1.0,float(news.get("speech_volume",1.0)),0.1)
            quiet_start=st.text_input("Quiet hours start",news.get("quiet_hours_start","22:00")); quiet_end=st.text_input("Quiet hours end",news.get("quiet_hours_end","07:00"))
            if st.form_submit_button("SAVE NEWS SETTINGS"):
                saved={**news,"enabled":enabled,"provider":provider,"fetch_interval_minutes":interval,
                    "market_wide":market_wide,"portfolio":portfolio,"watchlist":watchlist,"candidates":candidates,
                    "earnings":earnings,"regulatory":regulatory,"corporate_actions":corporate,
                    "spoken_briefing":spoken,"auto_speak_critical":auto_speak,"speech_rate":rate,
                    "speech_pitch":pitch,"speech_volume":volume,"quiet_hours_start":quiet_start,"quiet_hours_end":quiet_end}
                if api_key: st.session_state["news_api_key"]=api_key
                WORKSPACE.save(news_settings=saved); get_news_manager().configure(**saved,api_key=st.session_state.get("news_api_key") or os.environ.get("NEWSAPI_KEY","")); st.success("News settings saved. Credentials were not persisted.")
    with display:
        with st.form("display_form"):
            compact=st.toggle("Compact mode",value=bool(prefs.get("display_preferences",{}).get("compact",True))); density=st.selectbox("Table density",["Compact","Comfortable"])
            if st.form_submit_button("SAVE DISPLAY"): WORKSPACE.save(display_preferences={"compact":compact,"density":density}); st.success("Display preferences saved.")
    with advanced:
        developer=st.toggle("Developer Mode",value=bool(prefs.get("developer_mode",False)),help="Disabled by default.")
        if developer != bool(prefs.get("developer_mode",False)): WORKSPACE.save(developer_mode=developer)
        if developer:
            devtabs=st.tabs(["BROKER","RUNTIME","STATE","LOGS","SELF-TESTS"])
            with devtabs[0]: st.json({**get_broker_state().snapshot(),"quotes":"Hidden from summary"}); st.json((st.session_state.get("broker_connection_result") or ({},))[0].get("details",{}))
            with devtabs[1]: st.json(get_core_runtime().snapshot())
            with devtabs[2]: st.json({"threads":threading.active_count(),"pipeline":st.session_state.get("pipeline_state")})
            with devtabs[3]: st.json(st.session_state.get("errors",[]))
            with devtabs[4]: show_startup_health_check()
            with devtabs[4]:
                st.markdown("#### Paper Execution Self-Test")
                st.caption("Validation-only records use the paper adapter and are excluded from normal reports.")
                a,b=st.columns(2)
                if a.button("RUN PAPER EXECUTION SELF-TEST",type="primary",key="run_paper_execution_self_test"):
                    _paper_validation_lifecycle(int(WORKSPACE.preferences.get("paper_validation_quantity",1)))
                if b.button("CLEAN UP VALIDATION RECORDS",key="cleanup_paper_execution_self_test"):
                    _cleanup_paper_validation(); st.success("Validation-only records removed.")


if hasattr(st,"dialog"):
    @st.dialog("USER PROFILE",width="large")
    def render_profile_dialog():
        render_profile_contents()
        if st.button("CLOSE",width="stretch",key="profile_close"):
            st.session_state["_profile_open"]=False; st.rerun()
else:
    def render_profile_dialog(): render_profile_contents()


def _empty_state(message):
    st.markdown(f'<div class="aq-empty">{message}</div>',unsafe_allow_html=True)


def render_speech_controls(text, key, autoplay=False):
    """Render isolated browser speech controls; only plain metadata is serialized."""
    settings=WORKSPACE.preferences.get("news_settings",{})
    payload=json.dumps(str(text)[:4000]).replace("</", "<\\/")
    rate=float(settings.get("speech_rate",1.0)); pitch=float(settings.get("speech_pitch",1.0)); volume=float(settings.get("speech_volume",1.0))
    component_html(f"""<div style='display:flex;gap:6px;align-items:center'>
      <button onclick='speak()'>Speak</button><button onclick='speechSynthesis.pause()'>Pause</button>
      <button onclick='speechSynthesis.resume()'>Resume</button><button onclick='speechSynthesis.cancel()'>Stop</button>
      <select id='voices'></select></div><script>
      const text={payload}, voices=document.getElementById('voices');
      function loadVoices(){{voices.innerHTML='';speechSynthesis.getVoices().forEach((v,i)=>{{let o=document.createElement('option');o.value=i;o.textContent=v.name+' ('+v.lang+')';voices.appendChild(o);}})}}
      loadVoices();speechSynthesis.onvoiceschanged=loadVoices;
      function speak(){{speechSynthesis.cancel();const u=new SpeechSynthesisUtterance(text),vs=speechSynthesis.getVoices();u.voice=vs[Number(voices.value)]||null;u.rate={rate};u.pitch={pitch};u.volume={volume};u.lang='en-IN';speechSynthesis.speak(u);}}
      {"setTimeout(speak, 250);" if autoplay else ""}
      </script>""",height=45)


def render_news_page():
    manager=get_news_manager(); state=manager.snapshot(); settings=WORKSPACE.preferences.get("news_settings",{})
    top,refresh=st.columns([4,1]); top.markdown("### Market Brief")
    if refresh.button("REFRESH NEWS",disabled=not settings.get("enabled",False),width="stretch"): manager.request_refresh(); st.info("Refresh queued; trading workers continue independently.")
    if not settings.get("enabled",False): st.info("Market news is disabled. Enable it under USER → NOTIFICATIONS.")
    elif state.get("provider_status")=="DEGRADED": st.warning(f"News is temporarily unavailable. Last successful update: {state.get('last_successful_fetch') or 'never'}.")
    brief=manager.briefing("INTRADAY"); st.write(brief)
    if settings.get("spoken_briefing",False): render_speech_controls(brief,"market_brief")
    if settings.get("auto_speak_critical",False):
        try:
            now=datetime.now().time(); start=datetime.strptime(settings.get("quiet_hours_start","22:00"),"%H:%M").time(); end=datetime.strptime(settings.get("quiet_hours_end","07:00"),"%H:%M").time()
            quiet=(now >= start or now < end) if start > end else start <= now < end
        except ValueError: quiet=True
        alert=None if quiet else manager.claim_critical_alert()
        if alert: render_speech_controls(alert,"critical_alert",autoplay=True)
    articles=state.get("articles",[])
    sections=[("Breaking / Critical",lambda a:a.get("urgency")=="CRITICAL"),
        ("Portfolio News",lambda a:bool(set(a.get("related_symbols",[])) & set(st.session_state.get("paper_positions",{})))),
        ("Candidate News",lambda a:bool(set(a.get("related_symbols",[])) & {getattr(x,"symbol","") for x in st.session_state.get("final_trade_list",[])})),
        ("Watchlist News",lambda a:bool(set(a.get("related_symbols",[])) & set(st.session_state.get("watchlist",[])))),
        ("Sector News",lambda a:bool(a.get("related_sectors"))), ("All News",lambda a:True)]
    for title,predicate in sections:
        matches=[a for a in articles if predicate(a)]
        st.markdown(f"#### {title}")
        if not matches: _empty_state(f"No {title.lower()} in the current cache."); continue
        for index,item in enumerate(matches[:20]):
            with st.container(border=True):
                st.markdown(f"**{item.get('headline','')}**")
                st.caption(f"{item.get('published_at','')} · {item.get('source','Unknown')} · {item.get('category','OTHER')} · {item.get('urgency','LOW')} · Sentiment: {item.get('sentiment','UNKNOWN')} (not a prediction)")
                st.write(item.get("description") or "No provider snippet supplied.")
                st.caption(f"Symbols: {', '.join(item.get('related_symbols',[])) or 'Market-wide / unassigned'} · Relevance {item.get('news_relevance_score',0)} · Risk {item.get('news_risk_score',0)}")
                if item.get("score_reasons"): st.caption("Why: " + " · ".join(item["score_reasons"]))
                st.link_button("OPEN SOURCE",item.get("url","#")); render_speech_controls(f"{item.get('headline','')}. {item.get('description','')}",f"{title}_{index}")
        if title != "All News": st.divider()
    if WORKSPACE.preferences.get("developer_mode",False):
        with st.expander("Developer · news diagnostics"):
            safe={k:v for k,v in state.items() if k not in {"articles","clusters","briefing_history","alerted_ids"}}; st.json(safe)


def render_market_page():
    state_label,reminder=_broker_product_state(); running=bool(st.session_state.get("autonomous_active")); candidates=filtered_opportunities(WORKSPACE.preferences.get("filters",{})); normal=_normal_opportunity_frame(candidates)
    st.header("MARKET")
    a,b,c,d,e=st.columns(5); a.metric("Market",market_status()); b.metric("Data / Broker",state_label); c.metric("AlphaQuant","RUNNING" if running else "STOPPED"); d.metric("Opportunities",int((normal.get("Status",pd.Series(dtype=str))=="READY").sum())); e.metric("Watching",int(normal.get("Status",pd.Series(dtype=str)).isin(["WATCHING","WAITING FOR ENTRY"]).sum()))
    if reminder: st.warning(reminder)
    if WORKSPACE.preferences.get("execution_mode","PAPER")=="PAPER" and state_label in {"NOT CONNECTED","DELAYED FALLBACK"}: st.info("PAPER TRADING READY WITH DELAYED DATA")
    run,stop=st.columns(2)
    if run.button("RUN ALPHAQUANT",type="primary",disabled=running,width="stretch",key="market_run"): st.session_state["alphaquant_run_pending"]=True; st.rerun()
    if stop.button("STOP ALPHAQUANT",disabled=not running,width="stretch",key="market_stop"): st.session_state.update(autonomous_active=False,stop_requested=True,pipeline_state="STOPPED"); get_core_runtime().stop(); st.rerun()
    overview,watch,news,search,chart,opportunities=st.tabs(["OVERVIEW","WATCHLISTS","NEWS","SYMBOL SEARCH","CHARTS","OPPORTUNITIES"])
    with overview:
        active=list(st.session_state.get("scan_universe",[])) or None
        regime=market_regime_snapshot(active); breadth=regime["breadth"]
        x,y,z=st.columns(3); x.metric("Market regime",regime["state"].replace("_"," ").title()); y.metric("Breadth",f"{breadth['advancing']} advancing / {breadth['declining']} declining"); z.metric("Valid quotes",f"{breadth['valid']} / {breadth['total']}")
        st.caption(f"Unchanged {breadth['unchanged']} · Unavailable {breadth['unavailable']} · A/D {breadth['advance_decline_ratio']:.2f} · {breadth['classification']}")
        if regime["missing_inputs"]: st.info("Regime unavailable — missing " + ", ".join(regime["missing_inputs"]) + ".")
        sectors=sector_snapshot(active); st.markdown("#### Sector overview")
        st.dataframe(sectors,width="stretch",hide_index=True) if not sectors.empty else _empty_state(f"Sector overview unavailable — only {breadth['valid']} of {breadth['total']} symbols have valid previous-close data.")
    with watch: render_watchlist(True)
    with news: render_news_page()
    with search:
        symbol=st.text_input("Symbol search",placeholder="RELIANCE",key="market_symbol_search").strip().upper().replace(".NS","")
        if symbol and st.button("VIEW CHART",type="primary",key="market_symbol_open"): st.session_state.selected_symbol=symbol; st.rerun()
    with chart:
        symbol=st.session_state.get("selected_symbol") or next(iter(st.session_state.get("watchlist",[])),"")
        render_symbol_details(symbol) if symbol else _empty_state("Search for a symbol or select a watchlist symbol.")
    with opportunities: render_opportunities()


def render_configuration_page():
    prefs=WORKSPACE.preferences; saved=dict(prefs.get("filters",{})); risk=dict(prefs.get("risk_preferences",{})); filters_tab,risk_tab,scoring_tab,live_tab=st.tabs(["FILTERS","RISK","SCORING","LIVE CASH"])
    with filters_tab:
        with st.form("filters_configuration"):
            a,b=st.columns(2)
            universe=a.selectbox("Universe",UNIVERSE_SOURCE_OPTIONS,index=UNIVERSE_SOURCE_OPTIONS.index(prefs.get("universe_source")) if prefs.get("universe_source") in UNIVERSE_SOURCE_OPTIONS else 0)
            indices=b.multiselect("Index selection",["NIFTY 50","NIFTY 100","NIFTY 200","NIFTY 500","BANK NIFTY","FINNIFTY"],default=saved.get("indices",[])); sectors=a.multiselect("Sector selection",sorted(set(STOCK_SECTOR_MAP.values())),default=saved.get("sectors",[])); watchlist=b.selectbox("Watchlist",list((prefs.get("watchlists") or {"Default":[]}).keys()))
            maximum=a.number_input("Maximum stocks to scan",1,5000,int(saved.get("maximum_stocks",500))); price=b.slider("Price range",0,100000,tuple(saved.get("price_range",[20,10000]))); volume=a.number_input("Minimum volume",0,1000000000,int(saved.get("minimum_volume",100000)),step=10000); cap=b.multiselect("Market-cap category",["Large Cap","Mid Cap","Small Cap"],default=saved.get("market_cap",[])); fno=a.toggle("F&O only",value=bool(saved.get("fno_only",False)))
            strategies=b.multiselect("Strategy selection",sorted(SCAN_STYLE_STRATEGY_MAP),default=saved.get("strategies",[])); confidence=a.slider("Minimum confidence",0,100,int(saved.get("minimum_confidence",70))); breakout=b.toggle("Breakout",value=saved.get("breakout",True)); pullback=a.toggle("Pullback",value=saved.get("pullback",True)); momentum=b.toggle("Momentum",value=saved.get("momentum",True)); high=a.toggle("High-confidence only",value=bool(saved.get("high_confidence_only",False)))
            if st.form_submit_button("SAVE FILTERS",type="primary"):
                WORKSPACE.save(universe_source=universe,filters={**saved,"indices":indices,"sectors":sectors,"watchlist":watchlist,"maximum_stocks":maximum,"price_range":list(price),"minimum_volume":volume,"market_cap":cap,"fno_only":fno,"strategies":strategies,"minimum_confidence":confidence,"breakout":breakout,"pullback":pullback,"momentum":momentum,"high_confidence_only":high}); st.success("Filters saved. The pipeline was not started or restarted.")
    with risk_tab:
        with st.form("risk_configuration"):
            a,b=st.columns(2); capital=a.number_input("Paper capital",1.0,value=float(prefs.get("paper_trading_capital",1000000)),step=10000.0); per=b.number_input("Risk per trade (%)",0.01,100.0,float(risk.get("risk_per_trade",1.0))); positions=a.number_input("Maximum open positions",1,100,int(prefs.get("maximum_positions",10))); daily=b.number_input("Daily loss limit (%)",0.01,100.0,float(risk.get("maximum_daily_loss",3.0))); sector=a.number_input("Maximum sector exposure (%)",0.01,100.0,float(risk.get("maximum_sector_exposure",25.0))); rr=b.number_input("Minimum reward/risk",0.1,20.0,float(risk.get("minimum_reward_risk",2.0))); stop=a.selectbox("Stop-loss method",["ATR","Technical level","Percentage"]); sizing=b.selectbox("Position-sizing method",["Risk based","Fixed amount","Equal weight"]); holding=a.selectbox("Trade duration",["Intraday","Carry-forward"]); expiry=b.number_input("Signal expiry (minutes)",1,1440,int(prefs.get("signal_expiry_minutes",30)))
            if st.form_submit_button("SAVE RISK",type="primary"):
                WORKSPACE.save(paper_trading_capital=capital,maximum_positions=positions,signal_expiry_minutes=expiry,risk_preferences={**risk,"risk_per_trade":per,"maximum_daily_loss":daily,"maximum_sector_exposure":sector,"minimum_reward_risk":rr,"stop_loss_method":stop,"position_sizing_method":sizing,"trade_duration":holding}); st.success("Risk settings saved. The pipeline was not started or restarted.")
    with scoring_tab:
        st.subheader("SCORING V2 · Trade Confidence")
        st.caption("Risk Manager, reward/risk, entry gates, and the critical-news veto are separate mandatory gates and cannot be disabled here.")
        store=ScoringProfileStore(Path(_APP_DIR)/"data"/"scoring_profiles.json"); active=store.active()
        st.info(f"Active profile: {active.name} v{active.version} · modified {active.modified_at}")
        mode=st.radio("Configuration detail",["BASIC","ADVANCED"],horizontal=True,key="scoring_detail_mode")
        labels={"structure":"Structure","participation":"Participation","momentum":"Momentum","market_sector":"Market / Sector","historical":"Historical","news":"News"}
        proposed={}; cols=st.columns(2)
        for i,key in enumerate(COMPONENTS):
            lo,hi=SAFE_BOUNDS[key]
            proposed[key]=cols[i%2].number_input(labels[key],min_value=float(lo),max_value=float(hi),value=float(active.weights[key]),step=1.0,key=f"score_weight_{key}")
        total=sum(proposed.values()); st.metric("TOTAL",f"{total:.0f} / 100")
        if total != 100: st.error("Weights must total exactly 100 before this profile can be saved.")
        if mode=="ADVANCED":
            st.caption("Advanced subweights and strategy-specific distributions are stored in the versioned profile. They must remain non-negative and total their parent component.")
            st.json({"subweights":active.subweights,"strategy_profiles":active.strategy_profiles},expanded=False)
        quality={key:0.0 for key in COMPONENTS}
        latest=(st.session_state.get("final_trade_list") or [None])[0]
        score=getattr(latest,"trade_score_v2",None) if latest else None
        if score:
            quality={c.component:(c.weighted_contribution/c.weight if c.weight else 0) for c in score.all_components()}
        current=simulate(quality,active.weights); comparison=simulate(quality,proposed) if total==100 else None
        st.markdown("#### Score simulation")
        x,y=st.columns(2); x.metric("Current Profile",current["trade_confidence"]); y.metric("Proposed Profile",comparison["trade_confidence"] if comparison else "INVALID")
        if comparison: st.dataframe(pd.DataFrame({"Component":[labels[k] for k in COMPONENTS],"Current":[current['components'][k] for k in COMPONENTS],"Proposed":[comparison['components'][k] for k in COMPONENTS]}),hide_index=True,width="stretch")
        profile_name=st.text_input("New profile name",value=f"{active.name} Copy",key="new_scoring_profile_name")
        a,b,c,d=st.columns(4)
        if a.button("SAVE AS NEW PROFILE",disabled=total!=100,key="save_new_scoring"):
            try: store.save_new(profile_name,proposed); st.success("Profile saved with immutable version metadata.")
            except ValueError as exc: st.error(str(exc))
        if b.button("DUPLICATE PROFILE",key="duplicate_scoring"):
            try: store.save_new(profile_name,dict(active.weights),duplicate_from=active.name); st.success("Profile duplicated.")
            except ValueError as exc: st.error(str(exc))
        if c.button("ACTIVATE PROFILE",disabled=total!=100,key="activate_scoring"):
            try:
                updated=store.save_version(active.name,proposed); store.activate(updated.name); st.success(f"Activated {updated.name} v{updated.version}")
            except ValueError as exc: st.error(str(exc))
        if d.button("RESET TO ALPHAQUANT DEFAULTS",key="reset_scoring"):
            store.reset(); st.success("AlphaQuant Default v1 restored.")
    with live_tab:
        st.warning("LIVE TRADING USES REAL MONEY. All limits below are required before LIVE CASH GUARDED can be enabled.")
        live_scoring_mode = "DEFAULT" if active.weights == DEFAULT_WEIGHTS else "CUSTOM"
        st.info(f"LIVE scoring: {live_scoring_mode} · {active.name} v{active.version} · V2 (LIVE CASH GUARDED remains locked)")
        with st.form("live_cash_configuration"):
            a,b=st.columns(2)
            max_notional=a.number_input("Maximum order notional (INR)",1000.0,value=float(prefs.get("live_max_order_notional") or 0),step=1000.0)
            max_deployed=b.number_input("Maximum total deployed capital (INR)",1000.0,value=float(prefs.get("live_max_deployed_capital") or 0),step=10000.0)
            max_trades=a.number_input("Maximum number of live trades (session)",1,100,int(prefs.get("live_max_trades") or 0))
            daily_loss=b.number_input("Daily loss limit (INR)",100.0,value=float(prefs.get("live_daily_loss_limit") or 0),step=1000.0)
            risk_per=a.number_input("Risk per trade (%)",0.01,100.0,float(prefs.get("live_risk_per_trade") or 0))
            sector_limit=b.number_input("Maximum sector exposure (%)",0.01,100.0,float(prefs.get("live_sector_exposure_limit") or 0))
            slippage=a.number_input("Slippage protection (bps)",0,500,int(prefs.get("live_slippage_bps") or 0))
            first_day=b.number_input("First-day max live orders (optional)",0,50,int(prefs.get("live_first_day_max_orders") or 1))
            enable=st.checkbox("Enable LIVE CASH GUARDED",value=bool(prefs.get("live_mode_enabled")))
            if enable:
                st.error("LIVE — REAL MONEY. Confirm you understand external orders use broker funds.")
                confirm=st.checkbox("I confirm LIVE CASH GUARDED uses real money and limits above are correct",value=False)
            else:
                confirm=False
            if st.form_submit_button("SAVE LIVE CASH SETTINGS",type="primary"):
                if enable and not confirm:
                    st.error("Explicit confirmation required to enable LIVE CASH GUARDED.")
                elif enable and not all([max_notional, max_deployed, max_trades, daily_loss, risk_per]):
                    st.error("Configure all required live limits before enabling LIVE CASH GUARDED.")
                else:
                    WORKSPACE.save(
                        live_max_order_notional=max_notional,
                        live_max_deployed_capital=max_deployed,
                        live_max_trades=max_trades,
                        live_daily_loss_limit=daily_loss,
                        live_risk_per_trade=risk_per,
                        live_max_open_positions=int(prefs.get("live_max_open_positions") or prefs.get("maximum_positions") or 10),
                        live_sector_exposure_limit=sector_limit,
                        live_slippage_bps=slippage,
                        live_first_day_max_orders=first_day if first_day > 0 else None,
                        live_mode_enabled=bool(enable and confirm),
                        execution_mode="LIVE" if enable and confirm else prefs.get("execution_mode", "PAPER"),
                    )
                    st.success("Live cash settings saved. Live execution remains LOCKED until LIVE READINESS PASS.")


def _business_reason(value):
    raw=str(value or "").upper(); return {"AI_REJECTED":"Confidence too low","WAITING_VOLUME":"Waiting for volume confirmation","WAITING_VWAP":"Price is below VWAP","NOT_SUBMITTED":"No order submitted","WAITING_PRICE":"Waiting for entry price","STALE_DATA":"Market data is stale"}.get(raw,raw.replace("_"," ").capitalize() if raw else "No reason supplied")


ORDER_COLUMNS = ["Order ID", "Symbol", "Side", "Quantity", "Order Type", "Price",
    "Execution Price", "Status", "Strategy", "Source", "Created At", "Completed At"]
CLOSED_TRADE_COLUMNS = ["Date", "Symbol", "Side", "Strategy", "Quantity", "Entry",
    "Exit", "P&L", "P&L %", "Exit Reason", "Holding Time", "Source"]


def _developer_internal_error(component, exc):
    """Keep implementation failures out of the product UI unless requested."""
    if WORKSPACE.preferences.get("developer_mode", False):
        st.session_state.setdefault("developer_internal_errors", []).append({
            "Time": datetime.now(timezone.utc).isoformat(), "Component": component,
            "Exception": type(exc).__name__, "Message": str(exc),
        })


def _orders_frame():
    """Return only the active execution namespace in a stable UI schema."""
    rows = []
    try:
        mode = WORKSPACE.preferences.get("execution_mode", "PAPER")
        orders = (st.session_state.get("paper_broker", {}).get("orders", {}) if mode == "PAPER"
            else st.session_state.get("live_orders", {})) or {}
        records = orders.values() if isinstance(orders, dict) else orders
        for raw in records:
            if not isinstance(raw, dict):
                continue
            rows.append({"Order ID": raw.get("order_id", ""), "Symbol": raw.get("symbol", ""),
                "Side": raw.get("side", ""), "Quantity": raw.get("qty", raw.get("quantity", 0)),
                "Order Type": raw.get("order_type", "MARKET"), "Price": raw.get("price", raw.get("ltp")),
                "Execution Price": raw.get("execution_price"), "Status": raw.get("status", "UNKNOWN"),
                "Strategy": raw.get("strategy", raw.get("tag", "")), "Source": raw.get("source", "PAPER"),
                "Created At": raw.get("timestamp", raw.get("created_at")), "Completed At": raw.get("completed_at")})
    except Exception as exc:
        _developer_internal_error("_orders_frame", exc)
    return pd.DataFrame(rows, columns=ORDER_COLUMNS)


def _closed_trades_frame(include_validation=False):
    """Return restored PaperPosition history; validation is opt-in for reports."""
    rows = []
    try:
        mode = WORKSPACE.preferences.get("execution_mode", "PAPER")
        history = st.session_state.get("paper_history", []) if mode == "PAPER" else st.session_state.get("live_closed_trades", [])
        for trade in history or []:
            get = lambda name, default=None: trade.get(name, default) if isinstance(trade, dict) else getattr(trade, name, default)
            source = str(get("source", "PAPER") or "PAPER")
            if source in {"PAPER_VALIDATION", "VALIDATION_TEST"} and not include_validation:
                continue
            entry, exit_price = float(get("entry", 0) or 0), float(get("current_price", get("exit_price", 0)) or 0)
            qty, pnl = int(get("qty", get("quantity", 0)) or 0), float(get("realized_pnl", get("pnl", 0)) or 0)
            entered, exited = get("entry_time"), get("exit_time")
            duration = ""
            try:
                if entered and exited: duration = str(pd.to_datetime(exited) - pd.to_datetime(entered)).split(".")[0]
            except (TypeError, ValueError):
                pass
            rows.append({"Date": exited, "Symbol": get("symbol", ""), "Side": "BUY",
                "Strategy": get("strategy", ""), "Quantity": qty, "Entry": entry, "Exit": exit_price,
                "P&L": pnl, "P&L %": pnl / (entry * qty) * 100 if entry and qty else 0,
                "Exit Reason": get("exit_reason", ""), "Holding Time": duration, "Source": source})
    except Exception as exc:
        _developer_internal_error("_closed_trades_frame", exc)
    return pd.DataFrame(rows, columns=CLOSED_TRADE_COLUMNS)


def _paper_validation_lifecycle(quantity=1):
    """Exercise paper bookkeeping end-to-end without touching broker routing."""
    stages = []
    def stage(name, passed, detail=""):
        stages.append({"Stage": name, "Result": "PASS" if passed else "FAIL", "Detail": detail})
        if not passed: raise RuntimeError(f"{name}: {detail}")
    run_id = "paper-validation-" + uuid.uuid4().hex[:10]
    symbol, strategy, side = "AQVALID", "PAPER EXECUTION SELF-TEST", "BUY"
    signal_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    key = hashlib.sha256(f"{run_id}|{symbol}|{strategy}|{signal_time}|{side}".encode()).hexdigest()
    try:
        stage("Candidate", True, "deterministic synthetic candidate")
        stage("Fast AI approval", True, "local deterministic validation gate")
        stage("Risk approval", quantity > 0 and quantity <= 10, f"tiny quantity={quantity}")
        stage("Entry ready", True, "synthetic fresh quote")
        ledger = st.session_state.setdefault("paper_broker", {"connected":False,"cash":500000.0,
            "starting_capital":500000.0,"positions":{},"orders":{},"trade_history":[],"realized_pnl":0.0,"risk":{}})
        ledger.setdefault("orders", {}); ledger.setdefault("trade_history", [])
        if any(o.get("idempotency_key") == key for o in ledger["orders"].values() if isinstance(o, dict)):
            stage("Duplicate protection", False, "idempotency key already exists")
        entry, stop, target = 100.0, 99.0, 102.0
        order_id = "PV-" + key[:12]
        order = {"order_id":order_id,"idempotency_key":key,"symbol":symbol,"side":side,"qty":int(quantity),
            "price":entry,"execution_price":entry,"order_type":"MARKET","status":"COMPLETE",
            "strategy":strategy,"source":"VALIDATION_TEST","timestamp":datetime.now(timezone.utc),
            "completed_at":datetime.now(timezone.utc)}
        ledger["orders"][order_id] = order; ledger["trade_history"].append(dict(order))
        stage("Simulated order", order_id in ledger["orders"]); stage("Simulated fill", order["status"] == "COMPLETE")
        position = PaperPosition(symbol=symbol, strategy=strategy, qty=int(quantity), entry=entry,
            stop=stop, target1=target, confidence=100, ai_score=100, source="VALIDATION_TEST", idempotency_key=key)
        position.initialise(); st.session_state.setdefault("paper_positions", {})[symbol] = position
        ledger["cash"] = float(ledger.get("cash", 0)) - entry * quantity
        stage("Open position", st.session_state.paper_positions.get(symbol) is position)
        stage("Capital reservation", entry * quantity > 0, f"reserved ₹{entry * quantity:.2f}")
        position.update_price(target); stage("Price update", position.current_price == target)
        position.close_trade("VALIDATION TARGET", target); stage("Stop/target exit", position.status == "CLOSED")
        st.session_state.paper_positions.pop(symbol, None); st.session_state.setdefault("paper_history", []).append(position)
        ledger["cash"] += target * quantity; ledger["realized_pnl"] = float(ledger.get("realized_pnl", 0)) + position.realized_pnl
        stage("Closed trade", position in st.session_state.paper_history)
        stage("Realized P&L", position.realized_pnl == (target-entry)*quantity, f"₹{position.realized_pnl:.2f}")
        persist_trading_state(); stage("Persistence", PAPER_STATE_PATH.exists(), str(PAPER_STATE_PATH))
        saved = json.loads(PAPER_STATE_PATH.read_text(encoding="utf-8"))
        restored = any(x.get("idempotency_key") == key for x in saved.get("paper_history", []) if isinstance(x, dict))
        stage("Restart restoration", restored, "serialized state re-read from disk")
    except Exception as exc:
        if not stages or stages[-1]["Result"] != "FAIL": stages.append({"Stage":"Lifecycle", "Result":"FAIL", "Detail":f"{type(exc).__name__}: {exc}"})
        _developer_internal_error("paper_validation", exc)
    result = {"run_id":run_id, "idempotency_key":key, "passed":all(x["Result"] == "PASS" for x in stages), "stages":stages}
    st.session_state["paper_validation_result"] = result
    return result


def _cleanup_paper_validation():
    ledger = st.session_state.get("paper_broker", {})
    validation_sources = {"PAPER_VALIDATION", "VALIDATION_TEST"}
    ledger["orders"] = {k:v for k,v in (ledger.get("orders", {}) or {}).items() if v.get("source") not in validation_sources}
    ledger["trade_history"] = [v for v in ledger.get("trade_history", []) if v.get("source") not in validation_sources]
    st.session_state["paper_history"] = [v for v in st.session_state.get("paper_history", []) if getattr(v,"source",None) not in validation_sources]
    st.session_state["paper_positions"] = {k:v for k,v in st.session_state.get("paper_positions",{}).items() if getattr(v,"source",None) not in validation_sources}
    st.session_state.pop("paper_validation_result", None); persist_trading_state()


def _paper_idempotency_key(trade):
    run_id = get_core_runtime().snapshot().get("run_id") or st.session_state.get("pipeline_run_id", "manual")
    signal_time = getattr(trade, "signal_time", getattr(trade, "created_at", "unknown"))
    raw = f"{run_id}|{getattr(trade,'symbol','')}|{getattr(trade,'strategy','')}|{signal_time}|BUY"
    return hashlib.sha256(raw.encode()).hexdigest()


def _execution_quote_is_stale(symbol: str) -> tuple[bool, str]:
    """Block fills when a live broker quote exists but is stale."""
    quotes = get_broker_state().snapshot().get("quotes", {})
    base = str(symbol or "").replace(".NS", "").replace(".BO", "")
    quote = quotes.get(symbol) or quotes.get(f"{base}.NS") or quotes.get(base)
    if not quote:
        return False, ""
    if quote.get("is_stale") or quote.get("freshness_status") == "STALE":
        return True, "Execution blocked: stale live quote"
    return False, ""


def create_atomic_paper_trade(trade):
    """Create order, fill, position, reservation and audit exactly once."""
    if WORKSPACE.preferences.get("execution_mode", "PAPER") != "PAPER":
        return None, "Paper execution is disabled"
    key = _paper_idempotency_key(trade)
    ledger = st.session_state.setdefault("paper_broker", {})
    orders = ledger.setdefault("orders", {})
    if any(isinstance(o, dict) and o.get("idempotency_key") == key for o in orders.values()):
        return None, "Duplicate evaluation blocked"
    symbol = getattr(trade, "symbol", "")
    if symbol in st.session_state.setdefault("paper_positions", {}):
        return None, "Position already exists"
    discovery = WORKSPACE.preferences.get("paper_preset","PAPER NORMAL") == "PAPER DISCOVERY"
    if discovery and len(st.session_state.get("paper_positions",{})) >= 2: return None, "Discovery position limit reached"
    qty = int(getattr(trade, "position_size", 0) or 0); entry = float(getattr(trade, "entry", 0) or 0)
    if discovery: qty=max(1,qty//2)
    if qty <= 0 or entry <= 0: return None, "Invalid paper quantity or entry"
    stale, stale_reason = _execution_quote_is_stale(symbol)
    if stale:
        trade.entry_status = "BLOCKED_STALE_DATA"
        trade.execution_status = "BLOCKED_STALE_DATA"
        return None, stale_reason
    order_id = "PAPER-" + key[:16]; now = datetime.now(timezone.utc)
    order = {"order_id":order_id,"idempotency_key":key,"symbol":symbol,"side":"BUY","qty":qty,
        "price":entry,"execution_price":entry,"order_type":"MARKET","status":"COMPLETE",
        "strategy":getattr(trade,"strategy",""),"source":"PAPER","timestamp":now,"completed_at":now,
        "score_version": getattr(trade, "score_version", get_scoring_engine_version()),
        "trade_confidence": getattr(trade, "ai_score", None)}
    position = PaperPosition(symbol=symbol,strategy=getattr(trade,"strategy",""),qty=qty,entry=entry,
        stop=float(getattr(trade,"stop",0) or 0),target1=float(getattr(trade,"target1",0) or 0),
        target2=float(getattr(trade,"target2",0) or 0),target3=float(getattr(trade,"target3",0) or 0),
        confidence=int(getattr(trade,"confidence",0) or 0),ai_score=int(getattr(trade,"ai_score",0) or 0),
        score_version=getattr(trade, "score_version", get_scoring_engine_version()),
        trade_confidence=float(getattr(trade, "ai_score", 0) or 0),
        decision_id=getattr(trade,"decision_id",None),source="PAPER",idempotency_key=key)
    position.initialise()
    # Mutate the existing persistent ledger only after every object validates.
    orders[order_id] = order; ledger.setdefault("trade_history", []).append(dict(order))
    ledger["cash"] = float(ledger.get("cash", WORKSPACE.preferences["paper_trading_capital"])) - qty*entry
    ledger.setdefault("positions", {})[symbol] = {"symbol":symbol,"qty":qty,"avg_price":entry,"source":"PAPER"}
    st.session_state.paper_positions[symbol] = position
    st.session_state.setdefault("paper_audit", []).append({"time":now,"event":"ATOMIC_PAPER_FILL","order_id":order_id,"idempotency_key":key})
    persist_trading_state()
    return position, "Paper order filled"


def render_trading_page():
    st.header("TRADING")
    mode_label = "LIVE CASH GUARDED" if WORKSPACE.preferences.get("live_mode_enabled") else "LIVE DATA + PAPER"
    st.caption(f"Execution workspace: {mode_label}")
    tabs=st.tabs(["POSITIONS","HOLDINGS","TRADE SETUPS","REJECTED","ORDERS","CLOSED TRADES"])
    pos=position_frame(); hold=holdings_frame(); orders=_orders_frame(); closed=_closed_trades_frame(); source=_normal_opportunity_frame(filtered_opportunities(WORKSPACE.preferences.get("filters",{})))
    frames=[pos,hold,None,None,orders,closed]; messages=["No open positions.","No holdings.","","","No orders.","No closed trades."]
    for index in [0,1,4,5]:
        with tabs[index]: st.dataframe(frames[index],width="stretch",hide_index=True) if not frames[index].empty else _empty_state(messages[index])
    with tabs[2]:
        setup_tabs=st.tabs(["ACTIONABLE","WAITING FOR ENTRY","WATCHING","EXPIRED"])
        statuses=["READY","WAITING FOR ENTRY","WATCHING","EXPIRED"]
        for tab,status in zip(setup_tabs,statuses):
            with tab:
                frame=source[source["Status"]==status].copy(); frame["Last Updated"]=get_broker_state().snapshot().get("last_quote_time")
                cols=["Symbol","Side","Strategy","Current Price","Entry","Stop","Target","Confidence","Status","Reason","Last Updated"]
                st.dataframe(frame.reindex(columns=cols),width="stretch",hide_index=True) if not frame.empty else _empty_state(f"No {status.lower()} trade setups.")
    with tabs[3]:
        rejected=source[source["Status"]=="REJECTED"].copy()
        if not rejected.empty: rejected["Reason"]=rejected["Reason"].map(_business_reason); st.dataframe(rejected,width="stretch",hide_index=True)
        else: _empty_state("No rejected candidates.")
    counts=st.session_state.get("pipeline_stage_counts",{})
    if counts and not len(st.session_state.get("final_trade_list",[])):
        reviewed=counts.get("Universe selected",0); available=counts.get("Data available",0); screened=counts.get("Passed fast screen",0); signalled=counts.get("Strategy signalled",0)
        state="MARKET CLOSED" if not is_market_open() else ("DATA UNAVAILABLE" if not available else "NO CANDIDATES")
        st.info(f"**{state}** — No valid setups were found.\n\n{reviewed} symbols reviewed · {available} had sufficient history · {screened} passed initial screening · {signalled} met strategy conditions")
    production_diagnostics = st.session_state.get("production_diagnostics", {}).get("stages", {})
    if production_diagnostics:
        labels = {"history_ready":"History Ready", "eligible":"Eligible", "focus":"Focus",
            "strategy_signals":"Strategy Signals", "v2_qualified":"V2 Qualified",
            "risk_approved":"Risk Approved", "waiting_entry":"Waiting Entry", "ready":"Ready",
            "persisted":"Persisted", "displayed":"Displayed", "executed":"Executed"}
        st.subheader("Pipeline Readiness")
        st.dataframe(pd.DataFrame([{"Stage": labels.get(stage, stage), "Count": count}
            for stage, count in production_diagnostics.items()]), hide_index=True, width="stretch")
    if WORKSPACE.preferences.get("developer_mode",False):
        with st.expander("Developer · candidate funnel and paper validation",expanded=False):
            preset=st.radio("Paper test preset",["Normal","Discovery"],horizontal=True,
                index=1 if WORKSPACE.preferences.get("paper_preset","PAPER NORMAL")=="PAPER DISCOVERY" else 0,
                key="developer_paper_preset")
            stored_preset = "PAPER DISCOVERY" if preset == "Discovery" else "PAPER NORMAL"
            if stored_preset != WORKSPACE.preferences.get("paper_preset","PAPER NORMAL"):
                WORKSPACE.save(paper_preset=stored_preset)
            if counts: st.dataframe(pd.DataFrame([{"Stage":k,"Count":v} for k,v in counts.items()]),hide_index=True,width="stretch")
            rejected=st.session_state.get("candidate_rejections",{})
            if rejected: st.dataframe(pd.DataFrame(rejected.values()),hide_index=True,width="stretch")
            timings=st.session_state.get("pipeline_timings",{})
            if timings: st.dataframe(pd.DataFrame([{"Stage":k,"Seconds":v} for k,v in timings.items()]),hide_index=True,width="stretch")
            result=st.session_state.get("paper_validation_result")
            if result:
                (st.success if result["passed"] else st.error)("Paper lifecycle PASS" if result["passed"] else "Paper lifecycle FAIL")
                st.dataframe(pd.DataFrame(result["stages"]),hide_index=True,width="stretch")
        if get_scoring_engine_version() == "V2":
            with st.expander("Developer · V2 trade confidence breakdown", expanded=False):
                v2_rows = []
                for trade in st.session_state.get("final_trade_list", []):
                    v2 = getattr(trade, "trade_score_v2", None)
                    if v2 is None:
                        continue
                    row = {
                        "Symbol": getattr(trade, "symbol", ""),
                        "Trade Confidence": v2.trade_confidence,
                        "Structure": f"{v2.structure.weighted_contribution:.0f}/{v2.structure.weight:.0f}",
                        "Participation": f"{v2.participation.weighted_contribution:.0f}/{v2.participation.weight:.0f}",
                        "Momentum": f"{v2.momentum.weighted_contribution:.0f}/{v2.momentum.weight:.0f}",
                        "Market/Sector": f"{v2.market_sector.weighted_contribution:.0f}/{v2.market_sector.weight:.0f}",
                        "Historical": f"{v2.historical.weighted_contribution:.0f}/{v2.historical.weight:.0f}",
                        "News": f"{v2.news.weighted_contribution:.0f}/{v2.news.weight:.0f}",
                        "Confluence": v2.confluence_bonus,
                        "Entry Blocker": v2.entry_blocker or getattr(trade, "entry_status", ""),
                    }
                    v2_rows.append(row)
                if v2_rows:
                    st.dataframe(pd.DataFrame(v2_rows), hide_index=True, width="stretch")
                    selected = st.selectbox("Inspect V2 detail", [r["Symbol"] for r in v2_rows], key="dev_v2_detail_symbol")
                    detail = next(t for t in st.session_state.get("final_trade_list", []) if getattr(t, "symbol", "") == selected)
                    payload = getattr(detail, "trade_score_v2", None)
                    if payload is not None:
                        st.json(payload.to_dict())
                else:
                    st.caption("No V2-scored candidates in the current run.")


def render_reports_page():
    st.header("REPORTS"); tabs=st.tabs(["DAILY","WEEKLY","MONTHLY","P&L","STRATEGY PERFORMANCE","AI PERFORMANCE","RISK","CAPITAL ALLOCATION","TRADE JOURNAL","DOWNLOADS"])
    trades=_closed_trades_frame(); setups=get_final_trade_dataframe(); positions=position_frame()
    periods=[("D",tabs[0]),("W",tabs[1]),("ME",tabs[2])]
    for period,tab in periods:
        with tab:
            frame=trades.copy()
            if not frame.empty and "Date" in frame: frame["Date"]=pd.to_datetime(frame["Date"],errors="coerce"); frame=frame.set_index("Date").resample(period).agg({"P&L":"sum","Symbol":"count"}).rename(columns={"Symbol":"Trades"}).reset_index()
            st.dataframe(frame,width="stretch",hide_index=True) if not frame.empty else _empty_state("No completed trades for this period.")
    with tabs[3]: st.dataframe(trades,width="stretch",hide_index=True) if not trades.empty else _empty_state("No realized P&L yet.")
    with tabs[4]: st.dataframe(setups.groupby("Strategy").size().rename("Setups").reset_index(),width="stretch",hide_index=True) if not setups.empty and "Strategy" in setups else _empty_state("No strategy performance data.")
    with tabs[5]: _empty_state("AI performance appears after reviewed trade outcomes.")
    with tabs[6]: st.dataframe(positions,width="stretch",hide_index=True) if not positions.empty else _empty_state("No active portfolio risk.")
    with tabs[7]: st.dataframe(holdings_frame(),width="stretch",hide_index=True) if not holdings_frame().empty else _empty_state("No capital is currently allocated.")
    with tabs[8]: st.dataframe(trades,width="stretch",hide_index=True) if not trades.empty else _empty_state("The trade journal is empty.")
    with tabs[9]:
        for name,frame in {"Trade Journal":trades,"Trade Setups":setups,"Positions":positions}.items(): st.download_button(f"DOWNLOAD {name.upper()} CSV",frame.to_csv(index=False).encode("utf-8-sig"),f"{name.lower().replace(' ','_')}.csv","text/csv",key=f"download_{name}")


def dispatch_application():
    """Route four product areas; the common ticker always precedes page content."""
    render_ticker_strip()
    render_live_status_panel()
    if st.session_state.get("_profile_open"): render_profile_dialog()
    page=st.session_state.get("_page","Market")
    renderer={"Market":render_market_page,"Configuration":render_configuration_page,"Trading":render_trading_page,"Reports":render_reports_page}.get(page, render_market_page)
    try:
        renderer()
        get_core_runtime().heartbeat("streamlit-ui")
    except Exception as exc:
        run_id=get_core_runtime().snapshot().get("run_id") or st.session_state.get("pipeline_run_id") or "UI"
        incident={"exception_type":type(exc).__name__,"traceback":traceback.format_exc(),
            "component":page,"timestamp":datetime.now(timezone.utc).isoformat(),"run_id":run_id}
        st.session_state.setdefault("component_errors",[]).append(incident)
        get_core_runtime().heartbeat("streamlit-ui", error=exc)
        logging.exception("Product page failed: %s", page)
        st.error("This section could not load. AlphaQuant is still running.")
        if WORKSPACE.preferences.get("developer_mode",False): st.json(incident)

def render_live_status_panel():
    """Temporary live-control panel — broker, feed, readiness, mode."""
    from execution.live_adapter import LiveExecutionAdapter, LiveReadinessGate
    from execution.base import OrderIntent

    broker = get_broker_state().snapshot()
    ms = get_market_state().snapshot()
    prefs = WORKSPACE.preferences
    mode = prefs.get("execution_mode", "PAPER")
    market_mode = prefs.get("market_data_mode", "YFINANCE")
    feed = UpstoxV3FeedManager.instance().health()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Broker", "CONNECTED" if broker.get("connected") else "FAILED")
    src = ms.get("data_source", "OFFLINE")
    c2.metric("Market Feed", "LIVE" if src == "BROKER_LIVE" else ("STALE" if ms.get("quotes") and any(q.get("stale") for q in ms.get("quotes", {}).values()) else src))
    c3.metric("Scoring", get_scoring_engine_version())
    live_enabled = bool(prefs.get("live_mode_enabled"))
    c4.metric("Mode", "LIVE — REAL MONEY" if live_enabled else ("LIVE DATA + PAPER" if market_mode == "UPSTOX_LIVE" else mode))
    funds = broker.get("available_cash", broker.get("cash", "N/A"))
    st.caption(f"Broker available cash: {funds if funds is not None else 'N/A'}")
    intent = OrderIntent(trade_id="PREVIEW", decision_id="preview", client_order_id="preview-readiness",
        symbol="RELIANCE.NS", side="BUY", quantity=1, price=100.0, trade_confidence=0, score_version="V2")
    trade = type("T", (), {"symbol": "RELIANCE.NS", "strategy": "PREVIEW", "risk_verdict": {"verdict": "APPROVED"},
        "entry_status": "READY", "entry": 100, "stop": 95, "target": 110, "ai_score": 0, "decision_id": "preview"})()
    import sys as _sys
    _mod = _sys.modules[__name__]
    ok, blockers = LiveReadinessGate.evaluate(_mod, intent)
    st.caption(f"Live Readiness: {'PASS' if ok else 'FAIL — ' + ', '.join(blockers[:5])}")
    if live_enabled:
        st.error("LIVE — REAL MONEY. External orders require LIVE CASH GUARDED enablement and readiness PASS.")
    if prefs.get("developer_mode", False):
        with st.expander("Developer · guarded order preview (NOT SENT)", expanded=False):
            preview = LiveExecutionAdapter.build_order_preview(intent, trade, _mod)
            st.dataframe(pd.DataFrame([preview]), hide_index=True, width="stretch")


def main():
    restore_trading_state_once()
    configure_news_manager()
    core = get_core_runtime()
    core_snapshot = core.snapshot()
    if core_snapshot.get("status") in {"RUNNING", "MONITORING", "RESTORED"}:
        st.session_state["autonomous_active"] = True
        st.session_state["pipeline_state"] = core_snapshot["status"]
    if st.session_state.pop("alphaquant_run_pending",False):
        with st.status("Running AlphaQuant end-to-end…",expanded=True) as status:
            ok,message=AlphaQuantOrchestrator().run(trigger="MANUAL"); status.update(label=message,state="complete" if ok else "error",expanded=not ok)
        (st.success if ok else st.error)(message)
        if ok:
            persist_trading_state()
            core.publish(pipeline={"state":"MONITORING","last_cycle":datetime.now(timezone.utc).isoformat()},
                positions=_portable_value(st.session_state.get("paper_positions", {})),
                orders=_portable_value(st.session_state.get("paper_broker", {}).get("orders", {})),
                candidates=_portable_value(st.session_state.get("candidate_archive", []))[-1000:])
    dispatch_application()
    if st.session_state.get("autonomous_active"): autonomous_loop_fragment()


if __name__ == "__main__":
    main()
