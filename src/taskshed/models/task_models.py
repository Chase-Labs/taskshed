from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal, TypeVar
from uuid import uuid4

T = TypeVar("T")


@dataclass(kw_only=True)
class TaskExecutionTime:
    task_id: str
    run_at: datetime

    def __post_init__(self):
        self.run_at = self.run_at.astimezone(timezone.utc)


@dataclass(kw_only=True)
class Task:
    callback: str
    run_at: datetime
    run_type: Literal["once", "recurring"] = "once"
    task_id: str = field(default_factory=lambda: uuid4().hex)
    kwargs: dict[str, T] = field(default_factory=dict)
    interval: timedelta | None = None
    group_id: str | None = None
    paused: bool = False

    def __post_init__(self):
        if self.run_type == "recurring" and self.interval is None:
            raise ValueError("An 'interval' must be provided for recurring tasks.")
        self.run_at = self.run_at.astimezone(timezone.utc)

    def interval_seconds(self) -> float | None:
        if self.interval:
            return self.interval.total_seconds()
        return None
