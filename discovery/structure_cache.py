"""Disk cache for structural analysis keyed by symbol + candle signature."""

from __future__ import annotations

import pickle
import threading
from pathlib import Path
from typing import Any

_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "structure_cache"
_LOCK = threading.Lock()


def _signature(df: Any) -> tuple:
    try:
        return (len(df), str(df.index[-1]), float(df.iloc[-1]["Close"]))
    except Exception:
        return (0, "", 0.0)


def get_cached_structure(symbol: str, df: Any) -> dict[str, Any] | None:
    sig = _signature(df)
    path = _CACHE_DIR / f"{symbol.replace('.NS', '')}.pkl"
    if not path.exists():
        return None
    try:
        payload = pickle.loads(path.read_bytes())
        if payload.get("signature") == sig:
            return payload.get("structure")
    except Exception:
        return None
    return None


def store_structure_cache(symbol: str, df: Any, structure: dict[str, Any]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _CACHE_DIR / f"{symbol.replace('.NS', '')}.pkl"
    payload = {"signature": _signature(df), "structure": structure}
    with _LOCK:
        path.write_bytes(pickle.dumps(payload))
