import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from taskshed.datastores.memory_datastore import InMemoryDataStore
from taskshed.models.task_models import Task
from taskshed.workers.polling_worker import PollingWorker

# -------------------------------------------------------------------------------- fixtures

POLLING_DELAY = 0.1
mock_callback = AsyncMock()


@pytest_asyncio.fixture()
async def worker() -> AsyncGenerator[PollingWorker, None]:
    store = InMemoryDataStore()
    await store.start()
    worker = PollingWorker(
        callback_map={"mock_callback": mock_callback},
        data_store=store,
        polling_interval=timedelta(seconds=POLLING_DELAY),
    )
    await worker.start()
    try:
        yield worker
    finally:
        await worker.shutdown()


# -------------------------------------------------------------------------------- tests


@pytest.mark.asyncio
async def test_run_date_task(worker: PollingWorker):
    task = Task(
        callback="mock_callback",
        kwargs={"some_arg": 123},
        schedule_type="date",
        run_at=datetime.now(timezone.utc),
        group_id=1,
    )

    worker._run_task(task)

    # Ensure the task was added to the executor's pending tasks
    assert len(worker._current_tasks) == 1

    await worker.shutdown()

    # Verify that the callback was called with the expected arguments
    mock_callback.assert_called_once_with(some_arg=123)

    # After shutdown, no tasks should remain pending
    assert len(worker._current_tasks) == 0


@pytest.mark.asyncio
async def test_polling_schedule(worker: PollingWorker):
    num_iterations = 3
    mock_fetch_due_tasks = AsyncMock(return_value=[])
    # The fetch_due_tasks method is called periodically by the worker
    # in the _process_due_tasks method. It is a good way to track the number of times
    # the _process_due_tasks method is called.
    worker._data_store.fetch_due_tasks = mock_fetch_due_tasks

    await asyncio.sleep(POLLING_DELAY * num_iterations + (POLLING_DELAY / 2))
    assert mock_fetch_due_tasks.call_count == num_iterations
