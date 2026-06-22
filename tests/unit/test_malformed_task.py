"""
`_run_task` can raise before any coroutine is scheduled in two ways:

1. The task's callback is not registered in the worker's callback map.
2. The task's callback is registered, but the task's kwargs do not match the callback's signature.

These tests assert that one bad task must not halt the rest. They are written
against the desired behaviour, so they fail until the bug is fixed.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from taskshed.datastores.memory_datastore import InMemoryDataStore
from taskshed.models.task_models import Task
from taskshed.workers.event_driven_worker import EventDrivenWorker
from taskshed.workers.polling_worker import PollingWorker

# -------------------------------------------------------------------------------- helpers

WORKERS = [EventDrivenWorker, PollingWorker]


def _build_worker(worker_cls, callback_map, datastore):
    # A long polling interval keeps the PollingWorker's background timer from
    # firing during the test; we drive _process_due_tasks() explicitly.
    if worker_cls is PollingWorker:
        return PollingWorker(
            callback_map=callback_map,
            datastore=datastore,
            polling_interval=timedelta(seconds=30),
        )
    return EventDrivenWorker(callback_map=callback_map, datastore=datastore)


async def _needs_arg(x):  # registered, but fed mismatched kwargs
    pass


# -------------------------------------------------------------------------- a bad task must not block healthy ones


@pytest.mark.parametrize("worker_cls", WORKERS)
@pytest.mark.asyncio
async def test_unregistered_callback_does_not_block_healthy_tasks(worker_cls):
    store = InMemoryDataStore()
    await store.start()

    good = AsyncMock()
    worker = _build_worker(worker_cls, {"good": good}, store)
    await worker.start()

    now = datetime.now(timezone.utc)
    # The malformed task is added first, so the buggy loop hits it before "good".
    await store.add_tasks(
        (
            Task(task_id="malformed", callback="not_registered", run_at=now),
            Task(task_id="good-1", callback="good", run_at=now),
            Task(task_id="good-2", callback="good", run_at=now),
        )
    )

    await worker._process_due_tasks()
    await asyncio.sleep(0.05)

    # Both healthy tasks must still run despite the malformed one.
    assert good.call_count == 2

    await worker.shutdown()


@pytest.mark.parametrize("worker_cls", WORKERS)
@pytest.mark.asyncio
async def test_mismatched_kwargs_do_not_block_healthy_tasks(worker_cls):
    store = InMemoryDataStore()
    await store.start()

    good = AsyncMock()
    worker = _build_worker(worker_cls, {"good": good, "needs_arg": _needs_arg}, store)
    await worker.start()

    now = datetime.now(timezone.utc)
    # "needs_arg" is registered but given no kwargs -> TypeError inside _run_task.
    await store.add_tasks(
        (
            Task(task_id="malformed", callback="needs_arg", kwargs={}, run_at=now),
            Task(task_id="good-1", callback="good", run_at=now),
            Task(task_id="good-2", callback="good", run_at=now),
        )
    )

    await worker._process_due_tasks()
    await asyncio.sleep(0.05)

    # Both healthy tasks must still run despite the malformed one.
    assert good.call_count == 2

    await worker.shutdown()


# -------------------------------------------------------------------------- a bad task must not stop future scheduling


@pytest.mark.asyncio
async def test_unregistered_callback_does_not_stop_future_scheduling():
    # EventDrivenWorker-specific: a malformed due task must not stop the worker
    # from scheduling a healthy task that becomes due later.
    store = InMemoryDataStore()
    await store.start()

    worker = EventDrivenWorker(callback_map={"good": AsyncMock()}, datastore=store)
    await worker.start()

    now = datetime.now(timezone.utc)
    await store.add_tasks(
        (
            Task(task_id="malformed", callback="not_registered", run_at=now),
            Task(task_id="future", callback="good", run_at=now + timedelta(hours=1)),
        )
    )

    await worker._process_due_tasks()

    # The future task should still be scheduled; the worker must not be dead.
    assert worker._next_wakeup is not None

    await worker.shutdown()


@pytest.mark.asyncio
async def test_mismatched_kwargs_do_not_stop_future_scheduling():
    # EventDrivenWorker-specific: a malformed due task must not stop the worker
    # from scheduling a healthy task that becomes due later.
    store = InMemoryDataStore()
    await store.start()

    worker = EventDrivenWorker(
        callback_map={"good": AsyncMock(), "needs_arg": _needs_arg}, datastore=store
    )
    await worker.start()

    now = datetime.now(timezone.utc)
    await store.add_tasks(
        (
            Task(task_id="malformed", callback="needs_arg", kwargs={}, run_at=now),
            Task(task_id="future", callback="good", run_at=now + timedelta(hours=1)),
        )
    )

    await worker._process_due_tasks()

    # The future task should still be scheduled; the worker must not be dead.
    assert worker._next_wakeup is not None

    await worker.shutdown()
