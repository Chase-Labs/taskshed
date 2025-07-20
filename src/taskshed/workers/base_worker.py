from abc import ABC, abstractmethod
from datetime import datetime
from typing import Awaitable, Callable, TypeVar

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task

T = TypeVar("T")


class BaseWorker(ABC):
    def __init__(
        self,
        callback_map: dict[str, Callable[..., Awaitable[T]]],
        datastore: DataStore,
    ):
        """
        Args:
            callback_map: A dictionary mapping task callback names to their
                corresponding awaitable functions.
            datastore: An instance of a DataStore used to fetch and update tasks.
        """
        self._callback_map = callback_map
        self._datastore = datastore

    @abstractmethod
    async def _process_due_tasks(self):
        """
        The main operational loop that fetches and executes due tasks.

        This method should query the data store for all tasks that are scheduled
        to run at or before the current time, execute them, and handle any
        rescheduling or cleanup required.
        """
        pass

    @abstractmethod
    def _run_task(self, task: Task):
        """
        Schedules a single task for immediate execution in the event loop.

        Args:
            task: The task object to run.
        """
        pass

    @abstractmethod
    async def start(self):
        """
        Starts the worker's main processing loop.
        """
        pass

    @abstractmethod
    async def shutdown(self):
        """
        Gracefully shuts down the worker.

        This should stop any future task processing and allow any currently
        running tasks to complete.
        """
        pass

    @abstractmethod
    async def update_schedule(self, run_at: datetime | None = None):
        """
        Recalculates and sets the timer for the next task processing cycle.

        This method is responsible for determining when `_process_due_tasks`
        should next be called and scheduling it with the event loop's timer.

        Args:
            run_at: An optional specific time for the next wakeup. If provided,
                the scheduler may use it to set a more precise timer. If None,
                the scheduler will determine the next time based on its strategy.
        """
        pass
