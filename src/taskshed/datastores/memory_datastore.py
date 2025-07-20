import asyncio
from collections.abc import Iterable
from datetime import datetime

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task, TaskExecutionTime
from taskshed.utils.errors import TaskNotFoundError


class InMemoryDataStore(DataStore):
    """
    An in-memory task store with no persistence. Useful for unit tests or quick prototyping.
    """

    def __init__(self):
        self._db: dict[str, Task] = {}
        self._lock: asyncio.Lock | None = None

    async def start(self) -> None:
        if self._lock is not None:
            return

        self._db.clear()
        self._lock = asyncio.Lock()

    async def shutdown(self) -> None:
        self._db.clear()
        self._lock = None

    async def add_tasks(
        self, tasks: Iterable[Task], *, replace_existing: bool = True
    ) -> None:
        async with self._lock:
            for task in tasks:
                if replace_existing or task.task_id not in self._db:
                    self._db[task.task_id] = task

    async def fetch_due_tasks(self, dt: datetime) -> list[Task]:
        async with self._lock:
            return [
                task
                for task in self._db.values()
                if task.run_at <= dt and not task.paused
            ]

    async def fetch_next_wakeup(self) -> datetime | None:
        if not self._db:
            return

        async with self._lock:
            wakeups = [task.run_at for task in self._db.values() if not task.paused]
            return min(wakeups) if wakeups else None

    async def fetch_tasks(self, task_ids: Iterable[str]) -> list[Task]:
        async with self._lock:
            return [self._db[task_id] for task_id in task_ids if task_id in self._db]

    async def fetch_group_tasks(self, group_id: str) -> list[Task]:
        async with self._lock:
            return [task for task in self._db.values() if task.group_id == group_id]

    async def update_execution_times(self, tasks: Iterable[TaskExecutionTime]) -> None:
        async with self._lock:
            for definition in tasks:
                try:
                    task = self._db[definition.task_id]
                except KeyError:
                    raise TaskNotFoundError(
                        f"task with ID {definition.task_id} not found."
                    )
                task.run_at = definition.run_at

    async def update_tasks_paused_status(
        self, task_ids: Iterable[str], paused: bool
    ) -> None:
        async with self._lock:
            for task_id in task_ids:
                try:
                    task = self._db[task_id]
                    task.paused = paused
                except KeyError:
                    raise TaskNotFoundError(f"task with ID {task_id} not found.")

    async def update_group_paused_status(self, group_id: str, paused: bool) -> None:
        async with self._lock:
            for task in self._db.values():
                if task.group_id == group_id:
                    task.paused = paused

    async def remove_tasks(self, task_ids: Iterable[str]) -> None:
        async with self._lock:
            for task_id in task_ids:
                self._db.pop(task_id, None)

    async def remove_all_tasks(self) -> None:
        async with self._lock:
            self._db.clear()

    async def remove_group_tasks(self, group_id: str) -> None:
        async with self._lock:
            to_delete = [
                id for id, task in self._db.items() if task.group_id == group_id
            ]
            for task_id in to_delete:
                del self._db[task_id]
