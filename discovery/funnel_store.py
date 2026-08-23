"""Atomic, queryable per-symbol discovery explanations for the whole universe."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

DEFAULT_FUNNEL_PATH = Path(__file__).resolve().parent.parent / "data" / "funnel_state.json"


def persist_funnel(rows: dict[str, dict[str, Any]], path: Path = DEFAULT_FUNNEL_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": datetime.now(timezone.utc).isoformat(), "symbols": rows}
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def load_funnel(path: Path = DEFAULT_FUNNEL_PATH) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload.get("symbols", {})
    except (OSError, ValueError, TypeError):
        return {}


def symbol_funnel(symbol: str, path: Path = DEFAULT_FUNNEL_PATH) -> dict[str, Any] | None:
    """UI/API adapter: explain why any master symbol did or did not advance."""
    states = load_funnel(path)
    key = str(symbol).upper().strip()
    return states.get(key) or states.get(f"{key}.NS")
