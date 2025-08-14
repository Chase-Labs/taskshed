from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Literal, TypeVar
from uuid import uuid4

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task, TaskExecutionTime
from taskshed.workers.event_driven_worker import EventDrivenWorker

T = TypeVar("T")


class AsyncScheduler:
    """
    An asynchronous scheduler for managing and scheduling tasks.

    This class provides a high-level API for adding, removing, fetching,
    and updating tasks. It interacts with a specified datastore to persist
    task information and can notify an `EventDrivenWorker` of schedule
    changes.
    """

    def __init__(
        self, datastore: DataStore, *, worker: EventDrivenWorker | None = None
    ):
        """
        Initializes the AsyncScheduler.

        Args:
            datastore: The datastore instance for storing tasks.
            worker: An optional `EventDrivenWorker` to notify of
                schedule changes. This is required for event-driven
                execution but not for polling-based workers.
        """
        self._datastore = datastore
        self._worker = worker

    # ------------------------------------------------------------------------------ public methods

    async def add_task(
        self,
        callback: str,
        run_at: datetime | None = None,
        kwargs: dict[str, T] | None = None,
        run_type: Literal["once", "recurring"] = "once",
        interval: timedelta | None = None,
        task_id: str | None = None,
        group_id: str | None = None,
        paused: bool = False,
        *,
        replace_existing: bool = True,
    ):
        """
        Schedules a single task to be executed.

        Args:
            callback: The name of the callback function to execute. This name
                must exist in the worker's `callback_map`.
            run_at: The datetime for the task's first execution. Defaults to
                the current time in UTC if not provided.
            kwargs: A dictionary of keyword arguments to pass to the callback.
            run_type: Specifies if the task is a one-time ('once') or
                'recurring' task.
            interval: The `timedelta` between executions for recurring tasks.
                Required if `run_type` is "recurring".
            task_id: A unique identifier for the task. A UUID is generated
                if not provided.
            group_id: An optional identifier to group related tasks.
            paused: If True, the task is scheduled but will not be executed
                until resumed.
            replace_existing: If True, an existing task with the same
                `task_id` will be overwritten.
        """
        task = Task(
            callback=callback,
            run_at=run_at or datetime.now(timezone.utc),
            kwargs=kwargs or dict(),
            run_type=run_type,
            interval=interval,
            task_id=task_id or uuid4().hex,
            group_id=group_id,
            paused=paused,
        )
        await self._datastore.add_tasks((task,), replace_existing=replace_existing)
        await self._notify_worker(task.run_at)

    async def add_tasks(self, tasks: Iterable[Task], *, replace_existing: bool = True):
        """
        Schedules multiple tasks in a single transaction.

        Args:
            tasks: An iterable of `Task` objects to schedule.
            replace_existing: If True, existing tasks with the same IDs
                will be overwritten.
        """
        await self._datastore.add_tasks(tasks, replace_existing=replace_existing)
        await self._notify_worker()

    async def fetch_tasks(
        self,
        *,
        task_ids: Iterable[str] | None = None,
        group_id: str | None = None,
    ) -> list[Task]:
        """
        Fetches tasks by their IDs or group ID.

        Args:
            task_ids: An iterable of task IDs to fetch.
            group_id: The ID of the task group to fetch.

        Returns:
            A list of `Task` objects matching the specified criteria.

        Raises:
            ValueError: If neither `task_ids` nor `group_id` is provided.
        """
        if not task_ids and not group_id:
            raise ValueError("Must specify either a list of Task IDs or a group ID.")
        if task_ids:
            tasks = await self._datastore.fetch_tasks(task_ids)
            return tasks
        return await self._datastore.fetch_group_tasks(group_id)

    async def pause_tasks(
        self,
        *,
        task_ids: Iterable[str] | None = None,
        group_id: str | None = None,
    ):
        """
        Pauses the execution of specified tasks.

        Paused tasks will not be executed by workers until they are resumed.

        Args:
            task_ids: An iterable of task IDs to pause.
            group_id: The ID of the task group to pause.

        Raises:
            ValueError: If neither `task_ids` nor `group_id` is provided.
        """
        if not task_ids and not group_id:
            raise ValueError("Must specify either a list of Task IDs or a group ID.")

        if task_ids:
            await self._datastore.update_tasks_paused_status(task_ids, paused=True)
        elif group_id:
            await self._datastore.update_group_paused_status(group_id, paused=True)

        await self._notify_worker()

    async def remove_tasks(
        self,
        *,
        task_ids: Iterable[str] | None = None,
        group_id: str | None = None,
    ):
        """
        Removes tasks from the datastore permanently.

        Args:
            task_ids: An iterable of task IDs to remove.
            group_id: The ID of the task group to remove.

        Raises:
            ValueError: If neither `task_ids` nor `group_id` is provided.
        """
        if not task_ids and not group_id:
            raise ValueError("Must specify either a list of Task IDs or a group ID.")

        if task_ids:
            await self._datastore.remove_tasks(task_ids=task_ids)
        elif group_id:
            await self._datastore.remove_group_tasks(group_id=group_id)

        await self._notify_worker()

    async def resume_tasks(
        self,
        *,
        task_ids: Iterable[str] | None = None,
        group_id: str | None = None,
    ):
        """
        Resumes the execution of paused tasks.

        Resumed tasks will be eligible for execution by workers according to
        their schedule.

        Args:
            task_ids: An iterable of task IDs to resume.
            group_id: The ID of the task group to resume.

        Raises:
            ValueError: If neither `task_ids` nor `group_id` is provided.
        """
        if not task_ids and not group_id:
            raise ValueError("Must specify either a list of Task IDs or a group ID.")

        if task_ids:
            await self._datastore.update_tasks_paused_status(task_ids, paused=False)
        elif group_id:
            await self._datastore.update_group_paused_status(group_id, paused=False)

        await self._notify_worker()

    async def update_execution_times(self, *, tasks: Iterable[TaskExecutionTime]):
        """
        Updates the execution time for one or more tasks.

        Args:
            tasks: An iterable of `TaskExecutionTime` objects, each
                specifying a `task_id` and a new `run_at` datetime.
        """
        await self._datastore.update_execution_times(tasks)
        await self._notify_worker()

    async def shutdown(self):
        """
        Shuts down the scheduler and closes datastore connections.
        """
        await self._datastore.shutdown()

    async def start(self):
        """
        Starts the scheduler and initializes datastore connections.
        """
        await self._datastore.start()

    # ------------------------------------------------------------------------------ private methods

    async def _notify_worker(self, run_at: datetime | None = None):
        if isinstance(self._worker, EventDrivenWorker):
            await self._worker.update_schedule(run_at)
