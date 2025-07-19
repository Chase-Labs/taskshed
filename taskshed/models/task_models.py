from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal, TypeVar
from uuid import uuid4

T = TypeVar("T")


@dataclass(kw_only=True)
class TaskId:
    # The unique ID of the task
    task_id: str = field(default_factory=lambda: uuid4().hex)


@dataclass(kw_only=True)
class TaskExecutionTime(TaskId):
    # The datetime that the Task is supposed to be executed at
    run_at: datetime

    def __post_init__(self):
        self.run_at = self.run_at.astimezone(timezone.utc)


# @dataclass(kw_only=True)
# class Task(TaskExecutionTime):
#     schedule_type: Literal["date", "interval"]
#     callback: Callable[..., Awaitable[T]]
#     kwargs: dict[str, T] = field(default_factory=dict)
#     interval: timedelta | None = None
#     group_id: str | None = None
#     paused: bool = False

#     def __post_init__(self):
#         if self.schedule_type == "interval" and self.interval is None:
#             raise ValueError(
#                 "A 'schedule_interval' must be provided for interval tasks."
#             )
#         self.run_at = self.run_at.astimezone(timezone.utc)

#     def interval_seconds(self) -> float | None:
#         if self.interval:
#             return self.interval.total_seconds()
#         return None


@dataclass(kw_only=True)
class Task(TaskExecutionTime):
    schedule_type: Literal["date", "interval"]
    callback: str
    kwargs: dict[str, T] = field(default_factory=dict)
    interval: timedelta | None = None
    group_id: str | None = None
    paused: bool = False

    def __post_init__(self):
        if self.schedule_type == "interval" and self.interval is None:
            raise ValueError(
                "A 'schedule_interval' must be provided for interval tasks."
            )
        self.run_at = self.run_at.astimezone(timezone.utc)

    def interval_seconds(self) -> float | None:
        if self.interval:
            return self.interval.total_seconds()
        return None
