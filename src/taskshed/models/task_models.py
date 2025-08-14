from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal, TypeVar
from uuid import uuid4

T = TypeVar("T")


@dataclass(kw_only=True)
class TaskExecutionTime:
    """
    Represents the scheduled execution time for a specific task.

    This class is primarily used to update the `run_at` time of an existing task
    in the datastore.

    Attributes:
        task_id: The unique identifier of the task to be updated.
        run_at: The new scheduled execution time for the task. The timezone
            is automatically converted to UTC upon initialization.
    """

    task_id: str
    run_at: datetime

    def __post_init__(self):
        self.run_at = self.run_at.astimezone(timezone.utc)


@dataclass(kw_only=True)
class Task:
    """
    Represents a schedulable task.

    Attributes:
        callback: The string name of the function to be executed. This name
            maps to a callable in the worker's `callback_map`.
        run_at: The scheduled time for the task's first execution. The
            timezone is automatically converted to UTC upon initialization.
        run_type: Specifies if the task runs 'once' or is 'recurring'.
        task_id: A unique identifier for the task. If not provided, a UUID
            will be generated.
        kwargs: A dictionary of keyword arguments to be passed to the
            callback function during execution.
        interval: The time delta between executions for recurring tasks.
            Required if `run_type` is 'recurring'.
        group_id: An optional identifier used to group multiple tasks together,
            allowing for bulk operations.
        paused: A flag to indicate if the task's execution is currently
            suspended.
    """

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
