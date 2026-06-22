import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Coroutine, TypeAlias

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task

Callback: TypeAlias = Callable[..., Coroutine[Any, Any, Any]]

_logger = logging.getLogger(__name__)

class BaseWorker(ABC):
    """
    An abstract base class that defines the interface for all workers.

    Workers are responsible for fetching due tasks from a datastore, executing
    their associated callbacks, and managing their lifecycle (e.g., rescheduling
    or removing them).
    """

    def __init__(
        self,
        callback_map: dict[str, Callback],
        datastore: DataStore,
    ):
        """
        Initializes the BaseWorker.

        Args:
            callback_map: A dictionary mapping task callback names to their
                corresponding awaitable functions.
            datastore: An instance of a `DataStore` used to fetch and
                update tasks.
        """
        self._callback_map = callback_map
        self._datastore = datastore

        self._current_tasks: set[asyncio.Task] = set()
        self._event_loop: asyncio.AbstractEventLoop | None = None

    @staticmethod
    def _next_run_at(task: Task, now: datetime) -> datetime:
        """
        Computes the next ``run_at`` for a recurring task.

        When ``task.coalesce`` is True and the task has fallen behind, all
        missed intervals are collapsed into a single step: the task fires once
        and is fast-forwarded to its first occurrence strictly after ``now``,
        so only the latest missed run fires. Otherwise the task advances by
        exactly one interval, letting the worker catch up one interval per loop.

        Args:
            task: The recurring `Task` being rescheduled.
            now: The reference time the worker used to fetch due tasks.

        Returns:
            The next scheduled execution time.
        """
        interval = task.interval  # type: ignore - recurring tasks always have an interval

        if task.coalesce and task.run_at < now:
            # Number of intervals to skip so that run_at lands strictly after
            # `now`. This matches the run_at the non-coalescing path would reach
            # after firing once per missed interval, but in a single step.
            missed = (now - task.run_at) // interval
            return task.run_at + (missed + 1) * interval

        return task.run_at + interval

    def _on_task_done(self, task: asyncio.Task) -> None:
        """
        Done-callback for a fired task's coroutine.
        """
        self._current_tasks.discard(task)

        # Cancellation (e.g. during shutdown) is expected, not an error.
        if task.cancelled():
            return

        exc = task.exception()
        if exc is not None:
            _logger.error("Task callback raised an exception", exc_info=exc)

    def add_callback(self, callback_name: str, callback: Callback) -> None:
        """
        Adds a new callback function to the worker's callback map.
        
        Args:
            callback_name: The name to associate with the callback function.
            callback: An awaitable function to be called when a task with the corresponding callback name is executed.
        
        Raises:
            ValueError: If the callback name already exists in the callback map.
        """
        if callback_name in self._callback_map:
            raise ValueError("Callback name already exists in the callback map.")
        self._callback_map[callback_name] = callback

    @abstractmethod
    async def _process_due_tasks(self):
        """
        Fetches and executes due tasks from the datastore.

        Implementations of this method should form the core operational loop of
        the worker. It should query the datastore for all tasks scheduled to
        run at or before the current time, execute them, and handle any
        rescheduling (for recurring tasks) or cleanup (for one-time tasks).
        """

    def _run_task(self, task: Task):
        """
        Schedules a single task's callback to run on the event loop.

        A malformed task (e.g., unregistered callback or mismatched kwargs) is logged
        and skipped, but does not stop the worker from processing other tasks.

        Args:
            task: The `Task` object to execute.

        Raises:
            RuntimeError: If the worker's event loop has not been started.
        """
        if not self._event_loop:
            raise RuntimeError("Event loop is not running. Call start() first.")

        try:
            callback = self._callback_map[task.callback]
        except KeyError:
            _logger.error(
                "Run Task - Skipping task '%s': callback '%s' is not registered. "
                "Available callbacks: %s",
                task.task_id,
                task.callback,
                list(self._callback_map),
            )
            return

        try:
            _task = self._event_loop.create_task(callback(**task.kwargs))
        except TypeError:
            _logger.exception(
                "Run Task - Skipping task '%s': kwargs do not match callback '%s'.",
                task.task_id,
                task.callback,
            )
            return

        # Add future to set of tasks currently running.
        self._current_tasks.add(_task)

        # On completion, drop it from the pending set and surface any error.
        _task.add_done_callback(self._on_task_done)

    @abstractmethod
    async def start(self):
        """
        Starts the worker's operation.

        This method should handle any necessary setup, such as initializing
        datastore connections, and then start the main processing loop that
        finds and executes tasks.
        """

    @abstractmethod
    async def shutdown(self):
        """
        Gracefully shuts down the worker.

        This method should stop any future task processing, allow any currently
        running tasks to complete within a reasonable timeout, and clean up
        any resources, such as closing datastore connections.
        """

    @abstractmethod
    async def update_schedule(self, run_at: datetime | None = None):
        """
        Updates the worker's schedule for the next processing cycle.

        Args:
            run_at: An optional `datetime` hint for the next wakeup time.
                This can be used by event-driven workers to avoid unnecessary
                datastore queries when a new, earlier task is added.
        """
