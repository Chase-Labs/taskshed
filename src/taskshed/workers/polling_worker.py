import asyncio
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Awaitable, Callable, TypeVar

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task
from taskshed.utils.errors import IncorrectCallbackNameError
from taskshed.workers.base_worker import BaseWorker

T = TypeVar("T")


class PollingWorker(BaseWorker):
    """
    A worker that polls a datastore at a fixed interval to run tasks.
    """

    def __init__(
        self,
        callback_map: dict[str, Callable[..., Awaitable[T]]],
        datastore: DataStore,
        polling_interval: timedelta = timedelta(seconds=3),
    ):
        """
        Initializes the PollingWorker.

        Args:
            callback_map: A mapping of string names to awaitable callback
                functions.
            datastore: The datastore instance for fetching tasks.
            polling_interval: The interval at which the worker should poll
                the datastore for due tasks.
        """
        super().__init__(callback_map, datastore)
        self._polling_interval = polling_interval

        self._current_tasks: set[asyncio.Task] = set()
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._timer_handle: asyncio.TimerHandle | None = None

    # ------------------------------------------------------------------------------ private methods

    async def _process_due_tasks(self):
        async with self._lock:
            while True:
                # Retrieve tasks that are scheduled to run now or earlier
                tasks = await self._datastore.fetch_due_tasks(
                    datetime.now(tz=timezone.utc)
                )
                if not tasks:
                    break  # No further tasks to execute.

                interval_tasks = []
                date_tasks = []

                for task in tasks:
                    self._run_task(task)

                    if task.run_type == "recurring":
                        # Reschedule recurring task for its next run based on interval
                        task.run_at += task.interval
                        interval_tasks.append(task)

                    elif task.run_type == "once":
                        date_tasks.append(task.task_id)

                if interval_tasks:
                    # Persist updated schedule for recurring interval tasks
                    await self._datastore.update_execution_times(interval_tasks)
                if date_tasks:
                    # Remove completed one-time tasks from the store
                    await self._datastore.remove_tasks(date_tasks)

        await self.update_schedule()

    def _run_task(self, task: Task):
        # Takes the coroutine and schedules it for execution on the event loop.
        if not self._event_loop:
            raise RuntimeError("Event loop is not running. Call start() first.")

        try:
            callback = self._callback_map[task.callback]
        except KeyError:
            raise IncorrectCallbackNameError(
                f"Callback '{task.callback}' not found in callback map. Available callbacks: {list(self._callback_map.keys())}"
            )

        _task = self._event_loop.create_task(callback(**task.kwargs))

        # Add future to set of tasks currently running.
        self._current_tasks.add(_task)

        # Add a callback to be run when the future becomes done.
        # Remove task from pending set when it completes.
        _task.add_done_callback(lambda t: self._current_tasks.discard(t))

    # ------------------------------------------------------------------------------ public methods

    async def start(self):
        """
        Initializes the worker and starts the polling loop.

        This method starts the datastore connection, captures the running event
        loop, creates a lock, and kicks off the first task processing cycle,
        which will then schedule subsequent polls.
        """
        await self._datastore.start()

        if not self._event_loop:
            self._event_loop = asyncio.get_running_loop()

        if not self._lock:
            # A lock is bound to the event loop that is current at the moment it is created.
            # If the scheduler is started inside any other running loop, the executor will
            # hit a RuntimeError.
            self._lock = asyncio.Lock()

        await self._process_due_tasks()

    async def shutdown(self):
        """
        Gracefully shuts down the worker.

        This method cancels the scheduled polling timer, waits for any
        currently running tasks to complete, and closes the datastore
        connection.
        """
        if self._timer_handle:
            self._timer_handle.cancel()
        if self._current_tasks:
            await asyncio.wait(
                self._current_tasks, return_when=asyncio.ALL_COMPLETED, timeout=30
            )
        await self._datastore.shutdown()

    async def update_schedule(self):
        """
        Schedules the next poll.

        This method sets a timer to call `_process_due_tasks` after the
        configured `_polling_interval` has elapsed.
        """
        if self._timer_handle:
            self._timer_handle.cancel()
        # Event loop provides mechanisms to schedule callback functions to
        # be called at some point in the future. Event loop uses monotonic clocks to track time.
        # An instance of asyncio.TimerHandle is returned which can be used to cancel the callback.
        self._timer_handle = self._event_loop.call_later(
            delay=self._polling_interval.total_seconds(),
            callback=partial(self._event_loop.create_task, self._process_due_tasks()),
        )
