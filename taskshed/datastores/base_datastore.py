from abc import ABC, abstractmethod
from datetime import datetime
from typing import Awaitable, Callable, Iterable, TypeVar

from taskshed.models.task_models import Task, TaskExecutionTime

T = TypeVar("T")


class DataStore(ABC):
    def __init__(self, callback_map: dict[str, Callable[..., Awaitable[T]]]):
        super().__init__()
        self._callback_map = callback_map
        self._reversed_map = {value: key for key, value in callback_map.items()}

    def _get_callback(self, name: str) -> Callable[..., Awaitable[T]]:
        return self._callback_map[name]

    def _get_callback_name(self, callback: Callable[..., Awaitable[T]]) -> str:
        try:
            return self._reversed_map[callback]
        except KeyError:
            raise ValueError(
                f"Callback {callback} not found in callback map. Ensure it is registered. "
                f"Reversed callback map: {self._reversed_map}"
            )

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
