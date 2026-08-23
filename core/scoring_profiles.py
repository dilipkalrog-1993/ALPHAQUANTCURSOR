"""Persistent, validated configuration for AlphaQuant Scoring V2."""

from __future__ import annotations

import copy
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

COMPONENTS = ("structure", "participation", "momentum", "market_sector", "historical", "news")
DEFAULT_WEIGHTS = dict(zip(COMPONENTS, (30.0, 20.0, 20.0, 10.0, 10.0, 10.0)))
SAFE_BOUNDS = {
    "structure": (20, 40), "participation": (10, 30), "momentum": (10, 30),
    "market_sector": (5, 20), "historical": (0, 20), "news": (0, 20),
}
DEFAULT_SUBWEIGHTS = {
    "structure": {"breakout_confirmation": 8, "support_resistance": 6, "demand_supply": 6,
                  "retest_quality": 5, "room_extension": 3, "higher_timeframe": 2},
    "participation": {"rvol": 8, "vwap": 5, "volume_expansion": 3,
                      "institutional_evidence": 2, "obv_adl": 2},
    "momentum": {"relative_strength": 5, "adx": 4, "ema_alignment": 4,
                 "macd_acceleration": 3, "multi_timeframe": 2, "rsi": 1, "other_oscillator": 1},
}
STRATEGIES = ("BREAKOUT", "VCP", "DEMAND_SUPPLY", "ORDER_BLOCK", "FVG", "PRICE SQUEEZE")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rebalance_subweights(subweights: dict[str, dict[str, float]], weights: dict[str, float]) -> dict[str, dict[str, float]]:
    """Preserve each subweight's relative emphasis when its parent changes."""
    result = copy.deepcopy(subweights)
    for component, values in result.items():
        total = sum(map(float, values.values()))
        if total and component in weights:
            keys = list(values)
            allocated = 0.0
            for key in keys[:-1]:
                values[key] = round(float(values[key]) / total * float(weights[component]), 6)
                allocated += values[key]
            values[keys[-1]] = round(float(weights[component]) - allocated, 6)
    return result


def validate_weights(weights: dict[str, Any], bounds: dict[str, tuple[float, float]] | None = None) -> None:
    """Reject incomplete, unsafe, negative, or non-100 scoring allocations."""
    bounds = bounds or SAFE_BOUNDS
    if set(weights) != set(COMPONENTS):
        raise ValueError("Scoring weights must contain exactly the six V2 components")
    numeric = {key: float(value) for key, value in weights.items()}
    if any(value < 0 for value in numeric.values()):
        raise ValueError("Scoring weights cannot be negative")
    if abs(sum(numeric.values()) - 100.0) > 1e-6:
        raise ValueError("Scoring weights must total 100")
    for key, value in numeric.items():
        lo, hi = bounds[key]
        if not lo <= value <= hi:
            raise ValueError(f"{key} weight must be between {lo} and {hi}")


@dataclass
class ScoringProfile:
    name: str = "AlphaQuant Default"
    version: int = 1
    weights: dict[str, float] = field(default_factory=lambda: copy.deepcopy(DEFAULT_WEIGHTS))
    subweights: dict[str, dict[str, float]] = field(default_factory=lambda: copy.deepcopy(DEFAULT_SUBWEIGHTS))
    strategy_profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    created_at: str = field(default_factory=_now)
    modified_at: str = field(default_factory=_now)

    def validate(self) -> None:
        validate_weights(self.weights)
        for strategy in self.strategy_profiles:
            if strategy not in STRATEGIES:
                raise ValueError(f"Unsupported strategy profile: {strategy}")
        for component, values in self.subweights.items():
            if component not in self.weights or any(float(v) < 0 for v in values.values()):
                raise ValueError("Subweights must be non-negative V2 components")
            if values and abs(sum(map(float, values.values())) - self.weights[component]) > 1e-6:
                raise ValueError(f"{component} subweights must total its component weight")

    def snapshot(self, strategy: str | None = None) -> dict[str, Any]:
        data = asdict(self)
        override = self.strategy_profiles.get(strategy or "", {})
        if override.get("weights"):
            validate_weights(override["weights"])
            data["weights"] = copy.deepcopy(override["weights"])
        data["strategy"] = strategy
        return data


class ScoringProfileStore:
    """Atomic JSON profile persistence; activating never mutates old snapshots."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            default = ScoringProfile()
            return {"active": default.name, "profiles": {default.name: asdict(default)}}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _save(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp.replace(self.path)

    def active(self) -> ScoringProfile:
        data = self._load()
        return ScoringProfile(**data["profiles"][data["active"]])

    def save_new(self, name: str, weights: dict[str, float], *, duplicate_from: str | None = None) -> ScoringProfile:
        data = self._load()
        if name in data["profiles"]:
            raise ValueError("Profile name already exists; save a new version instead")
        base = copy.deepcopy(data["profiles"].get(duplicate_from or data["active"], {}))
        base.update(name=name, version=1, weights=weights, created_at=_now(), modified_at=_now())
        base["subweights"] = _rebalance_subweights(base.get("subweights", {}), weights)
        profile = ScoringProfile(**base); profile.validate()
        data["profiles"][name] = asdict(profile); self._save(data)
        return profile

    def save_version(self, name: str, weights: dict[str, float], **changes: Any) -> ScoringProfile:
        data = self._load(); old = data["profiles"][name]
        payload = {**old, **changes, "weights": weights, "version": int(old["version"]) + 1, "modified_at": _now()}
        if "subweights" not in changes:
            payload["subweights"] = _rebalance_subweights(old.get("subweights", {}), weights)
        profile = ScoringProfile(**payload); profile.validate()
        data["profiles"][name] = asdict(profile); self._save(data)
        return profile

    def activate(self, name: str) -> ScoringProfile:
        data = self._load()
        if name not in data["profiles"]: raise ValueError("Unknown scoring profile")
        data["active"] = name; self._save(data)
        return ScoringProfile(**data["profiles"][name])

    def reset(self) -> ScoringProfile:
        data = self._load(); default = ScoringProfile()
        data["profiles"][default.name] = asdict(default); data["active"] = default.name; self._save(data)
        return default


def simulate(component_quality: dict[str, float], weights: dict[str, float]) -> dict[str, Any]:
    validate_weights(weights)
    breakdown = {key: round(max(0, min(1, float(component_quality.get(key, 0)))) * weights[key], 2)
                 for key in COMPONENTS}
    return {"trade_confidence": round(sum(breakdown.values()), 2), "components": breakdown}
