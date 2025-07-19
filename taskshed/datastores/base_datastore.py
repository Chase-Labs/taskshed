from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable

from taskshed.models.task_models import Task, TaskExecutionTime


class DataStore(ABC):
    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def shutdown(self) -> None: ...

    @abstractmethod
    async def add_tasks(
        self, tasks: Iterable[Task], *, replace_existing: bool = True
    ) -> None: ...

    @abstractmethod
    async def fetch_due_tasks(self, dt: datetime) -> list[Task]: ...

    @abstractmethod
    async def fetch_next_wakeup(self) -> datetime | None: ...

    @abstractmethod
    async def fetch_tasks(self, task_ids: Iterable[str]) -> list[Task]: ...

    @abstractmethod
    async def fetch_group_tasks(self, group_id: str) -> list[Task]: ...

    @abstractmethod
    async def update_execution_times(self, tasks: Iterable[TaskExecutionTime]): ...

    @abstractmethod
    async def update_tasks_paused_status(
        self, task_ids: Iterable[str], paused: bool
    ) -> None: ...

    @abstractmethod
    async def update_group_paused_status(self, group_id: str, paused: bool) -> None: ...

    @abstractmethod
    async def remove_tasks(self, task_ids: Iterable[str]) -> None: ...

    @abstractmethod
    async def remove_all_tasks(self) -> None: ...

    @abstractmethod
    async def remove_group_tasks(self, group_id: str) -> None: ...
