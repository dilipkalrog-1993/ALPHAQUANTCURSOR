"""Structured, invariant-checked production pipeline diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

STAGES = ("history_ready", "eligible", "focus", "strategy_signals", "v2_qualified",
          "risk_approved", "waiting_entry", "ready", "persisted", "displayed", "executed")


@dataclass
class PipelineDiagnostics:
    counts: dict[str, int] = field(default_factory=lambda: {stage: 0 for stage in STAGES})
    rejections: dict[str, dict[str, int]] = field(default_factory=dict)

    def record(self, stage: str, count: int, rejections: dict[str, int] | None = None) -> None:
        if stage not in self.counts:
            raise ValueError(f"Unknown pipeline stage: {stage}")
        self.counts[stage] = int(count)
        if rejections:
            self.rejections[stage] = dict(rejections)

    def to_dict(self) -> dict[str, Any]:
        return {"stages": dict(self.counts), "rejections": dict(self.rejections)}
