"""AlphaQuant OS Pipeline Manager.

This module orchestrates existing application/Brain functions only. It does
not contain trading rules, scoring logic, veto logic, sizing logic or learning
logic; those remain in their current independent Brains and app engines.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional


@dataclass
class PipelineStep:
    name: str
    action: Callable[[], object]
    detail: str = ""
    required: bool = True


@dataclass
class PipelineEvent:
    step: str
    status: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)


class PipelineManager:
    """Runs AlphaQuant stages sequentially and records professional status."""

    def __init__(self, on_event: Optional[Callable[[PipelineEvent], None]] = None):
        self.on_event = on_event
        self.events: List[PipelineEvent] = []

    def emit(self, step: str, status: str, message: str) -> None:
        event = PipelineEvent(step=step, status=status, message=message)
        self.events.append(event)
        if self.on_event:
            self.on_event(event)

    def run(self, steps: List[PipelineStep]) -> bool:
        for step in steps:
            self.emit(step.name, "RUNNING", step.detail or "Started")
            try:
                result = step.action()
            except Exception as exc:  # orchestration boundary; preserve error visibility
                self.emit(step.name, "FAILED", str(exc))
                if step.required:
                    return False
                continue

            if result is False:
                self.emit(step.name, "FAILED", "Stage returned False")
                if step.required:
                    return False
            else:
                msg = result if isinstance(result, str) else "Completed"
                self.emit(step.name, "COMPLETED", msg)
        return True
