import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from taskshed.datastores.memory_datastore import InMemoryDataStore
from taskshed.models.task_models import Task
from taskshed.workers.base_worker import BaseWorker
from taskshed.workers.event_driven_worker import EventDrivenWorker
from taskshed.workers.polling_worker import PollingWorker

# -------------------------------------------------------------------------------- _next_run_at


def _recurring_task(run_at: datetime, *, coalesce: bool) -> Task:
    return Task(
        callback="cb",
        run_at=run_at,
        run_type="recurring",
        interval=timedelta(seconds=10),
        coalesce=coalesce,
    )


def test_next_run_at_coalesce_fast_forwards_past_now():
    now = datetime.now(timezone.utc)
    # Behind by 5 whole intervals (plus a fraction).
    task = _recurring_task(now - timedelta(seconds=53), coalesce=True)

    next_run = BaseWorker._next_run_at(task, now)

    # Collapsed to a single step landing strictly after `now`.
    assert next_run > now
    assert next_run - now <= task.interval


def test_next_run_at_no_coalesce_advances_single_interval():
    now = datetime.now(timezone.utc)
    task = _recurring_task(now - timedelta(seconds=53), coalesce=False)

    next_run = BaseWorker._next_run_at(task, now)

    # One interval at a time, regardless of how far behind.
    assert next_run == task.run_at + task.interval


def test_next_run_at_coalesce_when_not_behind_advances_single_interval():
    now = datetime.now(timezone.utc)
    # run_at == now: due exactly, not behind. Should advance by one interval
    # so it does not immediately re-fire.
    task = _recurring_task(now, coalesce=True)

    next_run = BaseWorker._next_run_at(task, now)

    assert next_run == task.run_at + task.interval


def test_next_run_at_coalesce_aligns_to_schedule_grid():
    now = datetime.now(timezone.utc)
    origin = now - timedelta(seconds=53)
    task = _recurring_task(origin, coalesce=True)

    next_run = BaseWorker._next_run_at(task, now)

    # Stays on the original cadence: an exact multiple of interval from origin.
    offset = (next_run - origin).total_seconds()
    assert offset % task.interval.total_seconds() == 0


# -------------------------------------------------------------------------------- worker behaviour


@pytest.mark.parametrize("worker_cls", [EventDrivenWorker, PollingWorker])
@pytest.mark.asyncio
async def test_coalescing_recurring_task_fires_once(worker_cls):
    store = InMemoryDataStore()
    await store.start()

    calls = 0

    async def cb():
        nonlocal calls
        calls += 1

    worker = worker_cls(callback_map={"cb": cb}, datastore=store)
    await worker.start()

    interval = timedelta(seconds=10)
    now = datetime.now(timezone.utc)
    task = Task(
        task_id="behind",
        callback="cb",
        run_at=now - 5 * interval,
        run_type="recurring",
        interval=interval,
        coalesce=True,
    )
    await store.add_tasks((task,))

    await worker._process_due_tasks()
    await asyncio.sleep(0.05)  # let the scheduled callback run

    assert calls == 1

    stored = (await store.fetch_tasks(["behind"]))[0]
    assert stored.run_at > now

    await worker.shutdown()


@pytest.mark.parametrize("worker_cls", [EventDrivenWorker, PollingWorker])
@pytest.mark.asyncio
async def test_non_coalescing_recurring_task_fires_per_missed_interval(worker_cls):
    store = InMemoryDataStore()
    await store.start()

    calls = 0

    async def cb():
        nonlocal calls
        calls += 1

    worker = worker_cls(callback_map={"cb": cb}, datastore=store)
    await worker.start()

    interval = timedelta(seconds=10)
    now = datetime.now(timezone.utc)
    task = Task(
        task_id="behind",
        callback="cb",
        run_at=now - 5 * interval,
        run_type="recurring",
        interval=interval,
        coalesce=False,
    )
    await store.add_tasks((task,))

    await worker._process_due_tasks()
    await asyncio.sleep(0.05)  # let the scheduled callbacks run

    # One fire per missed interval as the worker catches up (5 behind + the
    # one that lands on/just before `now`).
    assert calls == 6

    stored = (await store.fetch_tasks(["behind"]))[0]
    assert stored.run_at > now

    await worker.shutdown()
