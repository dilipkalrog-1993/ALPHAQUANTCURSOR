from __future__ import annotations

import importlib, json, subprocess, sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.headless_pipeline import (Candidate, StageAudit, compute_candidate,
    normalize_history, persist_candidates, prepare_indicators, reconcile_candidates)
from execution.base import OrderIntent
from execution.live_adapter import LiveExecutionAdapter
from execution.paper_adapter import PaperExecutionAdapter
from news_intelligence import NewsManager


def history(rows=240, trend=True):
    close = np.linspace(100, 150, rows) if trend else np.full(rows, 100.0)
    return pd.DataFrame({"Open":close-.2,"High":close+1,"Low":close-1,"Close":close,
                         "Volume":np.full(rows, 500_000)}, index=pd.date_range("2025-01-01", periods=rows))


def test_headless_validator_imports_no_streamlit_modules():
    code = "import sys,tools.run_real_nse_validation; assert not any(x=='streamlit' or x.startswith('streamlit.') for x in sys.modules)"
    subprocess.run([sys.executable,"-c",code],check=True)


def test_history_normalization():
    raw=history(); raw.columns=pd.MultiIndex.from_tuples([(c,"X.NS") for c in raw.columns])
    out=normalize_history(raw)
    assert list(out.columns)==["Open","High","Low","Close","Volume"] and len(out)==240


def test_insufficient_history_reason():
    _, failure=prepare_indicators("NEW.NS",history(87))
    assert failure.reason=="INSUFFICIENT_ROWS" and failure.required_rows==200 and failure.normalized_rows==87


def test_bad_ohlcv_reason():
    raw=history(); raw=raw.drop(columns="Volume")
    _, failure=prepare_indicators("BAD.NS",raw)
    assert failure.reason=="BAD_OHLCV" and failure.missing_columns==["Volume"]


def test_indicator_nan_reason(monkeypatch):
    original=pd.core.window.ewm.ExponentialMovingWindow.mean
    calls={"n":0}
    def bad(self,*a,**k):
        calls["n"]+=1; result=original(self,*a,**k)
        if calls["n"]==1: result.iloc[-1]=np.nan
        return result
    monkeypatch.setattr(pd.core.window.ewm.ExponentialMovingWindow,"mean",bad)
    _, failure=prepare_indicators("NAN.NS",history())
    assert failure.reason=="INDICATOR_NAN" and "EMA20" in failure.nan_columns


def test_calculation_failure_reason(monkeypatch):
    monkeypatch.setattr(pd.Series,"ewm",lambda *a,**k: (_ for _ in ()).throw(RuntimeError("boom")))
    _, failure=prepare_indicators("ERR.NS",history())
    assert failure.reason=="CALCULATION_FAILURE" and failure.exception_type=="RuntimeError"


def test_genuine_computed_candidate_creation():
    frame, failure=prepare_indicators("REAL.NS",history())
    candidate=compute_candidate("REAL.NS",frame)
    assert failure is None and candidate and candidate.computed and candidate.entry==frame.Close.iloc[-1]


def test_candidate_transition_diagnostics():
    audit=StageAudit(28,20,{"NO_STRATEGY_SIGNAL":8}).to_dict()
    assert audit=={"input":28,"output":20,"rejected":8,"rejections":{"NO_STRATEGY_SIGNAL":8},"invariant_ok":True}


def test_trade_setups_backend_ui_reconciliation():
    cs=[Candidate("A.NS","S",80,1,1,2),Candidate("B.NS","S",70,1,1,2)]
    result=reconcile_candidates(cs,cs,{})
    assert result["backend_candidates"]==result["persisted_candidates"]==result["displayed_candidates"]==2


def test_saved_filters_cannot_silently_hide_candidates():
    cs=[Candidate("A.NS","S",80,1,1,2),Candidate("B.NS","S",70,1,1,2)]
    result=reconcile_candidates(cs,cs,{"search":"A"})
    assert result["hidden_by_filters"]==1 and result["notice"]=="1 valid setups are hidden by your saved Opportunity Filters." and result["reset_filters_available"]


def test_v2_candidate_persistence(tmp_path):
    path=tmp_path/"candidates.json"; candidate=Candidate("A.NS","S",80,1,1,2)
    assert persist_candidates([candidate],path)==1 and json.loads(path.read_text())[0]["computed"] is True


def test_news_subsystem_failure_isolation(tmp_path,monkeypatch):
    manager=NewsManager(tmp_path/"news.json"); manager.configure(enabled=True)
    monkeypatch.setattr(manager,"_fetch_rss_articles",lambda: (_ for _ in ()).throw(TimeoutError("offline")))
    manager._refresh(); state=manager.snapshot()
    assert state["provider_status"]=="DEGRADED" and state["reason"]=="PROVIDER_FAILURE" and state["stale"]


def test_paper_execution_safety():
    app=SimpleNamespace(entry_trigger_status=lambda t:(False,"not ready"))
    trade=SimpleNamespace(risk_verdict={"verdict":"REJECTED"},entry_status="PENDING")
    intent=OrderIntent(trade_id="t",decision_id=None,symbol="A.NS",side="BUY",quantity=1,price=100,product="D",client_order_id="x")
    result=PaperExecutionAdapter().execute(intent,trade,app)
    assert not result.success and "Risk" in result.message


def test_static_live_execution_lock():
    assert LiveExecutionAdapter.LOCKED is True


def test_no_synthetic_sample_candidate_injection():
    production=[Path("core/headless_pipeline.py"),Path("tools/run_real_nse_validation.py")]
    text="\n".join(p.read_text().lower() for p in production)
    assert "synthetic candidate" not in text and "sample candidate" not in text
