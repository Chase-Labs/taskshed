from collections.abc import Iterable
from datetime import datetime

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task, TaskExecutionTime
from taskshed.workers.event_driven_worker import EventDrivenWorker


class AsyncScheduler:
    """
    An asynchronous task scheduler using an asyncio event loop.

    The scheduler stores tasks via a task store and uses an executor to run them.
    It manages periodic or one-time tasks, waking up to execute due tasks and
    rescheduling itself based on the next task's schedule_type time.
    """

    def __init__(
        self, datastore: DataStore, *, worker: EventDrivenWorker | None = None
    ):
        self._datastore = datastore
        self._worker = worker

    # ------------------------------------------------------------------------------ public methods

    async def add_task(self, task: Task, *, replace_existing: bool = True):
        """
        Schedules a single task.

        Args:
            task (`Task`): The task to schedule.
            replace_existing (`bool`): If True, replaces an existing task with the same ID.
        """
        await self._datastore.add_tasks((task,), replace_existing=replace_existing)
        await self._notify_worker(task.run_at)

    async def add_tasks(self, tasks: Iterable[Task], *, replace_existing: bool = True):
        """
        Schedules multiple tasks.

        Args:
            tasks (`Iterable[Task]`): An iterable of tasks to schedule.
            replace_existing (`bool`): If True, replaces existing tasks with the same IDs.
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
        Fetches tasks by their IDs or all tasks in a specific group.

        Args:
            task_ids (`Iterable[str]` | None): A list of task IDs to fetch.
            group_id (`str` | None): The ID of the task group to fetch all tasks from.

        Returns:
            list[Task]: A list of tasks matching the criteria.
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
        Pauses tasks. Paused tasks will not be executed until resumed.

        Args:
            task_ids (`Iterable[str]` | None): A list of task IDs to pause.
            group_id (`str` | None): The ID of the task group to pause all tasks in.
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
        Removes tasks by their IDs or all tasks in a specific group.

        Args:
            task_ids (`Iterable[str]` | None): A list of task IDs to remove.
            group_id (`str | None`): The ID of the task group to remove all tasks from.
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
        Resumes paused tasks. Resumed tasks will be executed according to their schedule.

        Args:
            task_ids (`Iterable[str]` | None): A list of task IDs to resume.
            group_id (`str` | None): The ID of the task group to resume all tasks in.
        """
        if not task_ids and not group_id:
            raise ValueError("Must specify either a list of Task IDs or a group ID.")

        if task_ids:
            await self._datastore.update_tasks_paused_status(task_ids, paused=False)
        elif group_id:
            await self._datastore.update_group_paused_status(group_id, paused=False)

        await self._notify_worker()

    async def update_execution_times(self, *, tasks: Iterable[TaskExecutionTime]):
        await self._datastore.update_execution_times(tasks)
        await self._notify_worker()

    async def shutdown(self):
        await self._datastore.shutdown()

    async def start(self):
        await self._datastore.start()

    # ------------------------------------------------------------------------------ private methods

    async def _notify_worker(self, run_at: datetime | None = None):
        if isinstance(self._worker, EventDrivenWorker):
            await self._worker.update_schedule(run_at)
