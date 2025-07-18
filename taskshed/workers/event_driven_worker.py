import asyncio
import logging
from datetime import datetime, timezone
from functools import partial

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task


class EventDrivenWorker:
    """
    Worker that schedules tasks to run in the asyncio event loop.
    """

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

        self._current_tasks: set[asyncio.Task] = set()
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._next_wakeup: datetime | None = None
        self._timer_handle: asyncio.TimerHandle | None = None

        self._logger = logging.getLogger("EventDrivenWorker")
        self._logger.setLevel(logging.INFO)

    # ------------------------------------------------------------------------------ public methods

    async def start(self):
        if not self._event_loop:
            self._event_loop = asyncio.get_running_loop()

        if not self._lock:
            # A lock is bound to the event loop that is current at the moment it is created.
            # If the scheduler is started inside any other running loop, the executor will
            # hit a RuntimeError.
            self._lock = asyncio.Lock()

        await self._wakeup()

    async def shutdown(self):
        if self._current_tasks:
            await asyncio.wait(
                self._current_tasks, return_when=asyncio.ALL_COMPLETED, timeout=30
            )
        self._cancel_timer()

    def run_task(self, task: Task):
        """
        Submits a coroutine task to the event loop for execution.

        Args:
            task (task): The task to schedule.
        """
        # Takes the coroutine and schedules it for execution on the event loop.
        if not self._event_loop:
            raise RuntimeError("Event loop is not running. Call start() first.")

        # task = self._event_loop.create_task(self._run_coroutine_task(task))
        task = self._event_loop.create_task(task.callback(**task.kwargs))

        # Add future to set of tasks currently running.
        self._current_tasks.add(task)

        # Add a callback to be run when the future becomes done.
        # Remove task from pending set when it completes.
        task.add_done_callback(lambda t: self._current_tasks.discard(t))

    async def update_wakeup(self, run_at: datetime | None = None) -> None | float:
        """
        Resets the internal timer to the next task's scheduled execution.

        Args:
            timestamp (float | None): Optional timestamp to set the next wakeup time.
                This is used when a task is added or updated with a specific execution time.
                If provided, the next wakeup time is set to this timestamp
                If not provided, the next wakeup time is fetched from the task store.
        """
        if run_at:
            if self._next_wakeup and self._next_wakeup < run_at:
                return
            wakeup = run_at

        else:
            wakeup = await self._data_store.fetch_next_wakeup()
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
                callback=partial(self._event_loop.create_task, self._wakeup()),
            )

    # ------------------------------------------------------------------------------ private methods

    # async def _run_coroutine_task(self, task: Task):
    #     """
    #     Execute the coroutine task's callback with provided keyword arguments.

    #     Args:
    #         task (Task): The task to execute.
    #     """
    #     try:
    #         await task.callback(**task.kwargs)
    #     except Exception as e:
    #         # Possibly handle this error depending on context.
    #         raise e

    async def _wakeup(self):
        """
        Wakes up the scheduler to process and execute due tasks.
        """
        async with self._lock:
            while True:
                # Retrieve tasks that are scheduled to run now or earlier
                tasks = await self._data_store.fetch_due_tasks(
                    datetime.now(tz=timezone.utc)
                )

                if not tasks:
                    # No further tasks to execute.
                    break

                interval_tasks = []
                date_tasks = []
                for task in tasks:
                    self.run_task(task)

                    if task.schedule_type == "interval":
                        # Reschedule interval task for its next run based on interval
                        task.run_at = task.run_at + task.interval
                        interval_tasks.append(task)

                    elif task.schedule_type == "date":
                        date_tasks.append(task.task_id)

                if interval_tasks:
                    # Persist updated schedule for recurring interval tasks
                    await self._data_store.update_execution_times(interval_tasks)

                if date_tasks:
                    # Remove completed one-time tasks from the store
                    await self._data_store.remove_tasks(date_tasks)

        self._cancel_timer()
        await self.update_wakeup()

    def _cancel_timer(self):
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None
            self._next_wakeup = None
