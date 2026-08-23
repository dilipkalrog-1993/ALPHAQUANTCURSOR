import json

from market.instrument_master import InstrumentMaster


def test_stale_nse_symbols_resolve_to_canonical_provider_symbols(tmp_path):
    master = InstrumentMaster(tmp_path / "master.json")
    assert master.canonical_mapping("LTIM.NS")["historical_provider_symbol"] == "LTM.NS"
    assert master.canonical_mapping("TATAMOTORS.NS")["historical_provider_symbol"] == "TMPV.NS"
    assert master.canonical_mapping("LTIM.NS")["broker_symbol"] == "LTM"


def test_refreshed_mapping_persists_all_canonical_fields(tmp_path, monkeypatch):
    rows = [{"segment": "NSE_EQ", "instrument_type": "EQ", "trading_symbol": "LTM",
             "instrument_key": "NSE_EQ|INE214T01019"}]
    import gzip
    payload = gzip.compress(json.dumps(rows).encode())
    class Response:
        def read(self): return payload
        def __enter__(self): return self
        def __exit__(self, *args): pass
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: Response())
    master = InstrumentMaster(tmp_path / "master.json")
    assert master.refresh_upstox() == 1
    assert master.canonical_mapping("LTIM") == {
        "display_symbol": "LTM", "broker_symbol": "LTM",
        "historical_provider_symbol": "LTM.NS",
        "instrument_key": "NSE_EQ|INE214T01019", "exchange": "NSE"}


def test_history_provider_is_called_with_canonical_symbol(tmp_path):
    import pandas as pd
    from core.history import load_incremental_history
    called = []
    def download(symbol, **kwargs):
        called.append(symbol)
        return pd.DataFrame()
    result = load_incremental_history("TATAMOTORS.NS", cache_dir=tmp_path,
                                      downloader=download, retries=0)
    assert called == ["TMPV.NS"]
    assert result.symbol == "TMPV.NS"
