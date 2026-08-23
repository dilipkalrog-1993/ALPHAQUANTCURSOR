"""Structured, invariant-checked production pipeline diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Canonical vocabulary for every production-pipeline diagnostic.  The discovery
# funnel is deliberately a separate definition: signals and candidates are
# event/output counts, not progressively filtered symbol universes.
STAGES = ("master", "history_ready", "eligible", "active", "focus",
          "strategy_signals", "candidates", "v2_qualified", "risk_approved",
          "waiting_entry", "ready", "persisted", "displayed", "executed")
DISCOVERY_FUNNEL = ("master", "eligible", "active", "focus")


@dataclass
class PipelineDiagnostics:
    counts: dict[str, int] = field(default_factory=lambda: {stage: 0 for stage in STAGES})
    rejections: dict[str, dict[str, int]] = field(default_factory=dict)
    _recorded: set[str] = field(default_factory=set, repr=False)

    def record(self, stage: str, count: int, rejections: dict[str, int] | None = None) -> None:
        if stage not in self.counts:
            raise ValueError(f"Unknown pipeline stage: {stage}")
        if count < 0:
            raise ValueError(f"Pipeline stage count cannot be negative: {stage}={count}")
        previous = self.counts[stage]
        was_recorded = stage in self._recorded
        self._recorded.add(stage)
        self.counts[stage] = int(count)
        try:
            self._validate_discovery_funnel()
        except ValueError:
            self.counts[stage] = previous
            if not was_recorded:
                self._recorded.remove(stage)
            raise
        if rejections:
            self.rejections[stage] = dict(rejections)

    def _validate_discovery_funnel(self) -> None:
        """Enforce MASTER >= ELIGIBLE >= ACTIVE >= FOCUS when adjacent values exist."""
        for upstream, downstream in zip(DISCOVERY_FUNNEL, DISCOVERY_FUNNEL[1:]):
            if upstream in self._recorded and downstream in self._recorded:
                if self.counts[upstream] < self.counts[downstream]:
                    raise ValueError(
                        f"Invalid pipeline funnel: {upstream}={self.counts[upstream]} "
                        f"< {downstream}={self.counts[downstream]}"
                    )

    def to_dict(self) -> dict[str, Any]:
        return {"stages": dict(self.counts), "rejections": dict(self.rejections),
                "discovery_funnel": list(DISCOVERY_FUNNEL)}
