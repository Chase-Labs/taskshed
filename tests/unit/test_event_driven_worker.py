from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from taskshed.datastores.memory_datastore import InMemoryDataStore
from taskshed.models.task_models import Task
from taskshed.workers.event_driven_worker import EventDrivenWorker

# -------------------------------------------------------------------------------- fixtures

mock_callback = AsyncMock()


@pytest_asyncio.fixture
async def worker() -> EventDrivenWorker:
    store = InMemoryDataStore()
    await store.start()
    worker = EventDrivenWorker(
        callback_map={"mock_callback": mock_callback}, datastore=store
    )
    await worker.start()
    return worker


# -------------------------------------------------------------------------------- tests


@pytest.mark.asyncio
async def test_run_date_task(worker: EventDrivenWorker):
    task = Task(
        callback="mock_callback",
        kwargs={"some_arg": 123},
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
