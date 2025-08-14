import asyncio
from datetime import datetime, timezone
from typing import Awaitable, Callable, TypeVar

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task
from taskshed.utils.errors import IncorrectCallbackNameError
from taskshed.workers.base_worker import BaseWorker

T = TypeVar("T")


class EventDrivenWorker(BaseWorker):
    """
    A worker that executes tasks by setting a timer to wake up exactly
    when the next task is due. It relies on notifications from a scheduler
    to be aware of newly added or modified tasks.
    """

    def __init__(
        self,
        callback_map: dict[str, Callable[..., Awaitable[T]]],
        datastore: DataStore,
    ):
        """
        Initializes the EventDrivenWorker.

        Args:
            callback_map: A mapping of string names to awaitable callback
                functions.
            datastore: The datastore instance for fetching tasks.
        """
        super().__init__(callback_map, datastore)

        self._current_tasks: set[asyncio.Task] = set()
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._next_wakeup: datetime | None = None
        self._timer_handle: asyncio.TimerHandle | None = None

    # ------------------------------------------------------------------------------ private methods

    def _cancel_timer(self):
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None
            self._next_wakeup = None

    async def _process_due_tasks(self):
        async with self._lock:
            while True:
                # Retrieve tasks that are scheduled to run now or earlier
                tasks = await self._datastore.fetch_due_tasks(
                    datetime.now(tz=timezone.utc)
                )

                if not tasks:
                    # No further tasks to execute.
                    break

                interval_tasks = []
                date_tasks = []
                for task in tasks:
                    self._run_task(task)

                    if task.run_type == "recurring":
                        # Reschedule recurring task for its next run based on interval
                        task.run_at = task.run_at + task.interval
                        interval_tasks.append(task)

                    elif task.run_type == "once":
                        date_tasks.append(task.task_id)

                if interval_tasks:
                    # Persist updated schedule for recurring interval tasks
                    await self._datastore.update_execution_times(interval_tasks)

                if date_tasks:
                    # Remove completed one-time tasks from the store
                    await self._datastore.remove_tasks(date_tasks)

        self._cancel_timer()
        await self.update_schedule()

    def _process_due_tasks_helper(self):
        self._event_loop.create_task(self._process_due_tasks())

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
        Initializes the worker and starts its operation.

        This method starts the datastore connection, captures the running event
        loop, creates a lock, and performs an initial check for any tasks that
        may have become due while the worker was offline.
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

        This method cancels any scheduled wakeup timer, waits for any
        currently running tasks to complete, and closes the datastore
        connection.
        """
        self._cancel_timer()
        if self._current_tasks:
            await asyncio.wait(
                self._current_tasks, return_when=asyncio.ALL_COMPLETED, timeout=30
            )
        await self._datastore.shutdown()

    async def update_schedule(self, run_at: datetime | None = None):
        """
        Sets or updates the timer for the next task wakeup.

        Args:
            run_at: An optional `datetime` hint for a potential next wakeup
                time. If not provided, the method will query the datastore
                for the next scheduled task.
        """
        if run_at:
            if self._next_wakeup and self._next_wakeup < run_at:
                return
            wakeup = run_at

        else:
            wakeup = await self._datastore.fetch_next_wakeup()
            # When fetching the next wakeup from the store we always update the
            # current timer since the earliest task might have changed (e.g. when
            # tasks are removed).
            self._cancel_timer()
            if not wakeup:
                return

        wakeup = wakeup.astimezone(timezone.utc)
        if self._next_wakeup is None or wakeup < self._next_wakeup:
            self._cancel_timer()
            self._next_wakeup = wakeup

            # Event loop provides mechanisms to schedule callback functions to
            # be called at some point in the future. Event loop uses monotonic clocks to track time.
            # An instance of asyncio.TimerHandle is returned which can be used to cancel the callback.
            delay = max((wakeup - datetime.now(tz=timezone.utc)).total_seconds(), 0)
            self._timer_handle = self._event_loop.call_later(
                delay=delay,
                callback=lambda: self._event_loop.create_task(
                    self._process_due_tasks()
                ),
            )
