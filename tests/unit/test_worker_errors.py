import asyncio
import logging
from datetime import datetime, timezone

import pytest

from taskshed.datastores.memory_datastore import InMemoryDataStore
from taskshed.models.task_models import Task
from taskshed.workers.event_driven_worker import EventDrivenWorker
from taskshed.workers.polling_worker import PollingWorker

WORKERS = [EventDrivenWorker, PollingWorker]


@pytest.mark.parametrize("worker_cls", WORKERS)
@pytest.mark.asyncio
async def test_callback_exception_is_logged_and_isolated(worker_cls, caplog):
    store = InMemoryDataStore()
    await store.start()

    async def boom():
        raise ValueError("boom")

    worker = worker_cls(callback_map={"boom": boom}, datastore=store)
    await worker.start()

    with caplog.at_level(logging.ERROR):
        worker._run_task(Task(callback="boom", run_at=datetime.now(timezone.utc)))
        await asyncio.sleep(0.05)  # let the failing coroutine resolve

    # The failing task is dropped from the in-flight set rather than leaking.
    assert len(worker._current_tasks) == 0

    # Its exception is surfaced via logging (with traceback), not swallowed.
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None
    assert isinstance(errors[0].exc_info[1], ValueError)

    await worker.shutdown()


@pytest.mark.parametrize("worker_cls", WORKERS)
@pytest.mark.asyncio
async def test_successful_callback_logs_nothing(worker_cls, caplog):
    store = InMemoryDataStore()
    await store.start()

    ran = False

    async def ok():
        nonlocal ran
        ran = True

    worker = worker_cls(callback_map={"ok": ok}, datastore=store)
    await worker.start()

    with caplog.at_level(logging.ERROR):
        worker._run_task(Task(callback="ok", run_at=datetime.now(timezone.utc)))
        await asyncio.sleep(0.05)

    assert ran is True
    assert len(worker._current_tasks) == 0
    assert [r for r in caplog.records if r.levelno == logging.ERROR] == []

    await worker.shutdown()
