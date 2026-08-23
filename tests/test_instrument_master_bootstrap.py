import gzip
import json
from datetime import datetime, timezone

from market.instrument_master import InstrumentMaster


def response(rows):
    payload = gzip.compress(json.dumps(rows).encode())
    class R:
        def read(self): return payload
        def __enter__(self): return self
        def __exit__(self, *args): pass
    return R()


def equity(symbol="RELIANCE", kind="EQUITY", **extra):
    return {"segment": "NSE_EQ", "instrument_type": kind,
            "trading_symbol": symbol, "instrument_key": "NSE_EQ|INE000000001", **extra}


def test_missing_master_refreshes_and_accepts_current_schema(tmp_path, monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: response([equity()]))
    master = InstrumentMaster(tmp_path / "master.json")
    state = master.bootstrap(retries=0)
    assert state["status"] == "FRESH" and state["refreshed"]
    assert master.symbols() == ["RELIANCE.NS"]
    assert master._cache["metadata"]["cash_equity_count"] == 1


def test_fresh_disk_master_does_not_refresh(tmp_path, monkeypatch):
    master = InstrumentMaster(tmp_path / "master.json")
    master._cache.update(symbols={"TCS.NS": {"instrument_key": "NSE_EQ|INE1"}},
                         refreshed_at=datetime.now(timezone.utc).isoformat())
    master._persist()
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: (_ for _ in ()).throw(AssertionError()))
    assert InstrumentMaster(tmp_path / "master.json").bootstrap()["refreshed"] is False


def test_empty_refresh_cannot_replace_lkg(tmp_path, monkeypatch):
    master = InstrumentMaster(tmp_path / "master.json")
    master._cache.update(symbols={"TCS.NS": {"instrument_key": "NSE_EQ|INE1"}})
    master._persist()
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: response([]))
    state = master.bootstrap(force_refresh=True, retries=0)
    assert state["status"] == "STALE_FALLBACK"
    assert master.symbols() == ["TCS.NS"]
    assert "TCS.NS" in json.loads((tmp_path / "master.json").read_text())["symbols"]


def test_no_master_failure_is_explicit(tmp_path, monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: (_ for _ in ()).throw(OSError("offline")))
    state = InstrumentMaster(tmp_path / "master.json").bootstrap(retries=0)
    assert state["status"] == "MASTER_UNAVAILABLE" and state["count"] == 0


def test_non_equities_etfs_and_suspended_are_excluded(tmp_path, monkeypatch):
    rows = [equity("GOOD"), equity("FUND", asset_type="ETF"),
            equity("HALT", status="SUSPENDED"), equity("FUT", kind="FUT")]
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: response(rows))
    master = InstrumentMaster(tmp_path / "master.json")
    master.refresh_upstox()
    assert master.symbols() == ["GOOD.NS"]
    assert master._cache["metadata"]["excluded_etfs"] == 1
    assert master._cache["metadata"]["excluded_invalid_suspended"] == 1
