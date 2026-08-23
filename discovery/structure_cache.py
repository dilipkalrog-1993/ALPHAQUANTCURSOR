"""Disk cache for structural analysis keyed by symbol + candle signature."""

from __future__ import annotations

import pickle
import threading
from pathlib import Path
from typing import Any

_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "structure_cache"
_LOCK = threading.Lock()


def _signature(df: Any, timeframe: str = "1d") -> tuple:
    try:
        return (timeframe, str(df.index[-1]))
    except Exception:
        return (0, "", 0.0)


def get_cached_structure(symbol: str, df: Any, timeframe: str = "1d") -> dict[str, Any] | None:
    sig = _signature(df, timeframe)
    path = _CACHE_DIR / f"{symbol.replace('.NS', '')}_{timeframe}.pkl"
    if not path.exists():
        return None
    try:
        payload = pickle.loads(path.read_bytes())
        if payload.get("signature") == sig:
            return payload.get("structure")
    except Exception:
        return None
    return None


def store_structure_cache(symbol: str, df: Any, structure: dict[str, Any], timeframe: str = "1d") -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _CACHE_DIR / f"{symbol.replace('.NS', '')}_{timeframe}.pkl"
    payload = {"signature": _signature(df, timeframe), "structure": structure}
    with _LOCK:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))
        tmp.replace(path)
