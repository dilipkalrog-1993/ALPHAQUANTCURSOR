import json
import logging
import os
import pickle

import pandas as pd

from core.history_providers import HistoryStatus, ProviderResult, YahooHistoryProvider, classify_provider_error
from core.history_service import HistoryService, atomic_json, cache_readiness, read_cache
from core.readiness import build_pre_market_report
from market.instrument_master import InstrumentMaster


def candles(rows=220, start="2025-01-01"):
    index = pd.date_range(start, periods=rows)
    close = pd.Series(range(100, 100 + rows), index=index, dtype=float)
    return pd.DataFrame({"Open": close, "High": close + 1, "Low": close - 1,
                         "Close": close, "Volume": 1000}, index=index)


def master(tmp_path, status=None):
    path = tmp_path / "master.json"
    row = {"display_symbol": "ABC", "broker_symbol": "ABC",
           "historical_provider_symbol": "ABC.NS", "instrument_key": "NSE_EQ|INE123"}
    if status:
        row["status"] = status
    path.write_text(json.dumps({"symbols": {"ABC.NS": row}, "aliases": {},
                                "refreshed_at": "2026-01-01T00:00:00+00:00"}))
    return InstrumentMaster(path)


class FakeProvider:
    def __init__(self, name, *results):
        self.name, self.results, self.calls = name, list(results), []
    def fetch(self, **kwargs):
        self.calls.append(kwargs)
        result = self.results.pop(0)
        result.symbol, result.provider = kwargs["symbol"], self.name
        return result


def test_yahoo_crumb_is_auth_failure_not_delisted():
    assert classify_provider_error(Exception("HTTP 401 Unauthorized: Invalid Crumb")) == HistoryStatus.AUTH_FAILURE
    provider = YahooHistoryProvider(downloader=lambda *a, **k: (_ for _ in ()).throw(Exception("User is unable to access this feature")), requests_per_second=10000)
    result = provider.fetch(symbol="ABC.NS", instrument_key=None, start="2025-01-01", end="2025-02-01", timeout=1)
    assert result.status == HistoryStatus.AUTH_FAILURE
    assert result.status != HistoryStatus.DELISTED_CONFIRMED


def test_fallback_success_and_attempt_audit(tmp_path):
    bad = ProviderResult("", "", HistoryStatus.AUTH_FAILURE, error_detail="expired")
    good = ProviderResult("", "", HistoryStatus.SUCCESS, candles())
    service = HistoryService([FakeProvider("UPSTOX", bad), FakeProvider("YAHOO", good)],
                             cache_dir=tmp_path / "cache", master=master(tmp_path), retries=0)
    result = service.repair("ABC.NS", now=pd.Timestamp("2025-08-09", tz="UTC").to_pydatetime())
    assert result.provider_selected == "YAHOO"
    assert [x["status"] for x in result.provider_attempts] == ["AUTH_FAILURE", "SUCCESS"]


def test_both_providers_fail_without_deleting_cache(tmp_path):
    cache = tmp_path / "cache"; cache.mkdir()
    original = candles(20)
    (cache / "ABC.pkl").write_bytes(pickle.dumps(original))
    providers = [FakeProvider("UPSTOX", ProviderResult("", "", HistoryStatus.AUTH_FAILURE)),
                 FakeProvider("YAHOO", ProviderResult("", "", HistoryStatus.NO_DATA))]
    result = HistoryService(providers, cache_dir=cache, master=master(tmp_path), retries=0).repair("ABC.NS")
    assert result.status == "HISTORY_UNAVAILABLE" and len(read_cache("ABC.NS", cache)) == 20


def test_incremental_merge_is_deduplicated_and_atomic(tmp_path):
    cache = tmp_path / "cache"; cache.mkdir()
    old = candles(210)
    (cache / "ABC.pkl").write_bytes(pickle.dumps(old))
    tail = candles(20, str(old.index[-2].date()))
    provider = FakeProvider("UPSTOX", ProviderResult("", "", HistoryStatus.SUCCESS, tail))
    result = HistoryService([provider], cache_dir=cache, master=master(tmp_path), retries=0).repair(
        "ABC.NS", now=pd.Timestamp("2026-01-01", tz="UTC").to_pydatetime())
    stored = read_cache("ABC.NS", cache)
    assert result.incremental_update and stored.index.is_unique and not list(cache.glob("*.tmp"))
    assert provider.calls[0]["start"] == (old.index[-1] + pd.Timedelta(days=1)).date().isoformat()


def test_invalid_ohlcv_and_minimum_rows():
    bad = candles().drop(columns="Volume")
    assert cache_readiness(bad) == "BAD_OHLCV"
    assert cache_readiness(candles(50)) == "INSUFFICIENT_ROWS"


def test_only_authoritative_status_confirms_delisting(tmp_path):
    result = HistoryService([], cache_dir=tmp_path / "cache", master=master(tmp_path, "DELISTED"), retries=0).repair("ABC.NS")
    assert result.failure_category == "DELISTED_CONFIRMED"


def test_checkpoint_atomic_persistence_supports_resume(tmp_path):
    path = tmp_path / "checkpoint.json"
    atomic_json(path, {"completed": {"ABC.NS": {"status": "HISTORY_READY"}}})
    saved = json.loads(path.read_text())
    assert saved["completed"]["ABC.NS"]["status"] == "HISTORY_READY"
    assert not list(tmp_path.glob("*.tmp"))


def test_checkpoint_windows_permission_error_retries_then_succeeds(tmp_path, monkeypatch):
    path = tmp_path / "checkpoint.json"
    real_replace = os.replace
    calls = []
    def locked_twice(source, destination):
        calls.append((source, destination))
        if len(calls) < 3:
            raise PermissionError(5, "Access is denied")
        return real_replace(source, destination)
    monkeypatch.setattr("core.history_service.os.replace", locked_twice)
    assert atomic_json(path, {"completed": {}}, retry_delay=0)
    assert len(calls) == 3 and json.loads(path.read_text()) == {"completed": {}}


def test_checkpoint_repeated_lock_is_deferred_and_old_file_survives(tmp_path, monkeypatch):
    path = tmp_path / "checkpoint.json"
    path.write_text('{"old": true}')
    monkeypatch.setattr("core.history_service.os.replace",
                        lambda *args: (_ for _ in ()).throw(PermissionError(5, "locked")))
    assert atomic_json(path, {"new": True}, replace_retries=2, retry_delay=0) is False
    assert json.loads(path.read_text()) == {"old": True}
    assert not list(tmp_path.glob("*.tmp"))


def test_checkpoint_cleans_only_stale_owned_temp_files(tmp_path):
    stale = tmp_path / "checkpoint.json.1.2.deadbeef.tmp"
    unrelated = tmp_path / "unrelated.tmp"
    stale.write_text("abandoned")
    unrelated.write_text("keep")
    os.utime(stale, (0, 0))
    assert atomic_json(tmp_path / "checkpoint.json", {"ok": True})
    assert not stale.exists() and unrelated.exists()


def test_deferred_checkpoint_does_not_lose_durable_symbol_cache(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    provider = FakeProvider("YAHOO", ProviderResult("", "", HistoryStatus.SUCCESS, candles()))
    result = HistoryService([provider], cache_dir=cache, master=master(tmp_path), retries=0).repair(
        "ABC.NS", now=pd.Timestamp("2025-08-09", tz="UTC").to_pydatetime())
    monkeypatch.setattr("core.history_service.os.replace",
                        lambda *args: (_ for _ in ()).throw(PermissionError(5, "locked")))
    assert not atomic_json(tmp_path / "checkpoint.json", {"completed": {}}, replace_retries=1)
    assert result.rows_written and not read_cache("ABC.NS", cache).empty


def test_yahoo_suppresses_speculative_delisted_output(capsys):
    def noisy(*args, **kwargs):
        import sys
        print("$ABC.NS: possibly delisted", file=sys.stderr)
        logging.getLogger("yfinance").error("possibly delisted")
        return pd.DataFrame()
    result = YahooHistoryProvider(downloader=noisy, requests_per_second=10000).fetch(
        symbol="ABC.NS", instrument_key=None, start="2025-01-01", end="2025-02-01", timeout=1)
    assert result.status == HistoryStatus.NO_DATA
    assert "delisted" not in capsys.readouterr().err


def test_checkpoint_is_written_only_by_coordinator():
    source = open("tools/bootstrap_nse_history.py", encoding="utf-8").read()
    assert "pool.submit(service.repair, symbol)" in source
    assert source.index("future.result().report()") < source.index("saved = atomic_json")


def test_benchmark_remains_cache_only():
    source = open("tools/run_discovery_benchmark.py", encoding="utf-8").read()
    assert "load_incremental_history" not in source
    assert "HistoryService" not in source
    assert '"provider_fetches": 0' in source


def test_pre_market_history_count_is_truthful(tmp_path):
    data = tmp_path / "data"; data.mkdir()
    row = {"display_symbol": "ABC", "broker_symbol": "ABC",
           "historical_provider_symbol": "ABC.NS", "instrument_key": "NSE_EQ|INE123"}
    (data / "instrument_master.json").write_text(json.dumps({"symbols": {"ABC.NS": row}}))
    report = build_pre_market_report(tmp_path)
    assert report["checks"]["Instrument Master"] == "READY (1)"
    assert report["checks"]["History Ready"] == "0 / 1"
    assert report["checks"]["History Missing"] == 1
    assert report["checks"]["History Bootstrap Required"] == "YES"
