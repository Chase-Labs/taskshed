import asyncio
import os
from datetime import datetime, timedelta, timezone
from random import shuffle
from typing import AsyncGenerator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from taskshed.datastores.memory_datastore import InMemoryDataStore
from taskshed.datastores.mysql_datastore import MySQLConfig, MySQLDataStore
from taskshed.datastores.redis_datastore import RedisConfig, RedisDataStore
from taskshed.models.task_models import Task, TaskExecutionTime
from taskshed.schedulers.async_scheduler import AsyncScheduler
from taskshed.workers.event_driven_worker import EventDrivenWorker

# -------------------------------------------------------------------------------- helpers


load_dotenv()

mock_callback = AsyncMock()
second_callback = AsyncMock()
third_callback = AsyncMock()


callback_map = {
    "mock_callback": mock_callback,
    "second_callback": second_callback,
    "third_callback": third_callback,
}


@pytest_asyncio.fixture(params=["in-memory", "mysql", "redis"])
async def scheduler(request) -> AsyncGenerator[AsyncScheduler, None]:
    """
    A parametrized fixture that yields a scheduler configured for different data stores.
    """
    if request.param == "in-memory":
        data_store = InMemoryDataStore()

    elif request.param == "mysql":
        data_store = MySQLDataStore(
            config=MySQLConfig(
                host=os.environ.get("MYSQL_HOST"),
                user=os.environ.get("MYSQL_USER"),
                password=os.environ.get("MYSQL_PASSWORD"),
                db=os.environ.get("MYSQL_DB"),
            ),
        )

    elif request.param == "redis":
        data_store = RedisDataStore(
            config=RedisConfig(
                host="localhost",
                port=6379,
                username=None,
                password=None,
            ),
        )

    await data_store.start()
    await data_store.remove_all_tasks()
    worker = EventDrivenWorker(callback_map=callback_map, data_store=data_store)
    await worker.start()
    scheduler_instance = AsyncScheduler(data_store=data_store, worker=worker)

    try:
        yield scheduler_instance
    finally:
        await scheduler_instance.shutdown()
        for callback in callback_map.values():
            callback.reset_mock()


# -------------------------------------------------------------------------------- tests


@pytest.mark.asyncio
async def test_add_task(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc) + timedelta(minutes=1)
    task_id = uuid4().hex
    scheduled_task = Task(
        run_at=start_time,
        callback="mock_callback",
        schedule_type="date",
        task_id=task_id,
    )

    await scheduler.add_task(scheduled_task)

    # Check that the task was added to the taskstore
    fetched_task = await scheduler.fetch_tasks(task_ids=[task_id])
    assert fetched_task == [scheduled_task]

    # Check that the worker has updated its wakeup time to the task's scheduled time
    assert scheduler._worker._next_wakeup == start_time


@pytest.mark.asyncio
async def test_add_date_task_and_run(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc)
    task_id = uuid4().hex
    scheduled_task = Task(
        run_at=start_time,
        callback="mock_callback",
        schedule_type="date",
        task_id=task_id,
        kwargs={"some_kwarg": 123},
    )

    await scheduler.add_task(scheduled_task)

    # Check that the task was added to the taskstore
    fetched_task = await scheduler.fetch_tasks(task_ids=[task_id])
    assert fetched_task == [scheduled_task]
    assert scheduler._worker._next_wakeup == start_time

    await asyncio.sleep(0.05)  # Allow time for the task to run

    # Verify that the callback was called with the expected kwargs
    mock_callback.assert_called_once_with(some_kwarg=123)

    # No more tasks to execute
    assert scheduler._worker._next_wakeup is None


@pytest.mark.asyncio
async def test_add_interval_task_and_run(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc)
    execution_count = 3
    delay = 0.1

    task_id = uuid4().hex
    scheduled_task = Task(
        run_at=start_time,
        callback="mock_callback",
        schedule_type="interval",
        interval=timedelta(seconds=delay),
        task_id=task_id,
        kwargs={"some_kwarg": 123},
    )

    await scheduler.add_task(scheduled_task)
    await asyncio.sleep((delay * execution_count) + delay / 2)
    assert (
        mock_callback.call_count == execution_count + 1
    )  # Initial call + execution_count


@pytest.mark.asyncio
async def test_add_many_tasks(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc) + timedelta(minutes=1)
    num_tasks = 10

    scheduled_tasks = []
    for i in range(num_tasks):
        scheduled_tasks.append(
            Task(
                run_at=start_time + timedelta(seconds=i),
                callback="mock_callback",
                schedule_type="date",
            )
        )

    await scheduler.add_tasks(scheduled_tasks)

    fetched_tasks = await scheduler._data_store.fetch_due_tasks(
        start_time + timedelta(seconds=num_tasks)
    )
    for task in fetched_tasks:
        assert task in scheduled_tasks

    # Check that the worker has updated its wakeup time to the task's scheduled time
    assert scheduler._worker._next_wakeup == start_time


@pytest.mark.asyncio
async def test_pause_and_resume_tasks(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc) + timedelta(minutes=1)
    group_id = "testing-group-1"
    num_tasks = 10

    scheduled_tasks = []
    for i in range(num_tasks):
        scheduled_tasks.append(
            Task(
                run_at=start_time + timedelta(seconds=i),
                callback="mock_callback",
                schedule_type="date",
                group_id=group_id,
            )
        )

    shuffle(scheduled_tasks)
    await scheduler.add_tasks(scheduled_tasks)

    # Check that the worker has updated its wakeup time to the earliest scheduled time
    assert scheduler._worker._next_wakeup == start_time

    # pause the tasks and check that the next wakeup time is None
    await scheduler.pause_tasks(group_id=group_id)
    assert scheduler._worker._next_wakeup is None

    # Check that there are no due tasks
    due_tasks = await scheduler._data_store.fetch_due_tasks(
        start_time + timedelta(minutes=2)
    )
    assert len(due_tasks) == 0

    # Finally, fetch the tasks and check that their paused flag is False
    group_tasks = await scheduler.fetch_tasks(group_id=group_id)
    for task in group_tasks:
        assert task.paused


@pytest.mark.asyncio
async def test_update_task_execution_time(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc) + timedelta(minutes=1)
    group_id = "testing-group-1"
    num_tasks = 10

    scheduled_tasks = []
    for i in range(num_tasks):
        scheduled_tasks.append(
            Task(
                run_at=start_time + timedelta(seconds=i),
                callback="mock_callback",
                schedule_type="date",
                group_id=group_id,
            )
        )

    task: Task = scheduled_tasks[num_tasks // 2]

    shuffle(scheduled_tasks)

    await scheduler.add_tasks(scheduled_tasks)
    assert scheduler._worker._next_wakeup == start_time

    new_execution_time = datetime.now(timezone.utc) + timedelta(seconds=30)
    await scheduler.update_execution_times(
        tasks=(TaskExecutionTime(task_id=task.task_id, run_at=new_execution_time),)
    )
    assert scheduler._worker._next_wakeup == new_execution_time


@pytest.mark.asyncio
async def test_add_tasks_in_different_timezones(scheduler: AsyncScheduler):
    delay = 0.1
    interval = 0.1
    naive_start = datetime.now() + timedelta(seconds=delay)
    iana_timezones = ("America/New_York", "Europe/Berlin", "Asia/Tokyo")

    # Add a task without a specified timezone
    naive_task = Task(
        run_at=naive_start,
        callback="mock_callback",
        schedule_type="date",
    )

    aware_tasks = []
    for i, iana_timezone in enumerate(iana_timezones, start=1):
        aware_start = naive_start.astimezone(ZoneInfo(iana_timezone))
        aware_tasks.append(
            Task(
                run_at=aware_start + timedelta(seconds=interval * i),
                callback="mock_callback",
                schedule_type="date",
                task_id=iana_timezone,
            )
        )

    await scheduler.add_task(naive_task)
    await scheduler.add_tasks(aware_tasks)

    # Check that the next wakeup time is equal to earliest task's run_at
    assert scheduler._worker._next_wakeup == naive_start.astimezone(timezone.utc)

    await asyncio.sleep(delay + (len(iana_timezones) * interval) + delay / 2)

    # Verify that the callback was called for each task
    assert mock_callback.call_count == len(iana_timezones) + 1


@pytest.mark.asyncio
async def test_remove_tasks(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc) + timedelta(minutes=1)
    num_isolated_tasks = 3
    num_grouped_tasks = 4

    # Add isolated tasks - tasks without a group
    isolated_tasks = [
        Task(run_at=start_time, callback="mock_callback", schedule_type="date")
        for _ in range(num_isolated_tasks)
    ]
    await scheduler.add_tasks(isolated_tasks)

    # Add grouped tasks
    group_id = "test-group-remove-tasks"
    group_run_time = start_time + timedelta(minutes=1)
    group_tasks = [
        Task(
            run_at=group_run_time,
            callback="mock_callback",
            schedule_type="date",
            group_id=group_id,
        )
        for _ in range(num_grouped_tasks)
    ]
    await scheduler.add_tasks(group_tasks)

    task_ids = [task.task_id for task in isolated_tasks + group_tasks]

    # Check that the next wakeup time is equal to the start time
    assert scheduler._worker._next_wakeup == start_time
    stored_tasks = await scheduler.fetch_tasks(task_ids=task_ids)
    assert len(stored_tasks) == num_isolated_tasks + num_grouped_tasks

    # Remove isolated tasks
    await scheduler.remove_tasks(task_ids=[task.task_id for task in isolated_tasks])
    stored_tasks = await scheduler.fetch_tasks(task_ids=task_ids)
    assert len(stored_tasks) == num_grouped_tasks
    assert all(task.group_id == group_id for task in stored_tasks)
    assert scheduler._worker._next_wakeup == group_run_time

    # Remove grouped tasks
    await scheduler.remove_tasks(group_id=group_id)
    stored_tasks = await scheduler.fetch_tasks(task_ids=task_ids)
    assert len(stored_tasks) == 0
    assert scheduler._worker._next_wakeup is None


@pytest.mark.asyncio
async def test_fetch_tasks_by_group_id(scheduler: AsyncScheduler):
    group_id = "group-fetch"
    start_time = datetime.now(timezone.utc) + timedelta(minutes=1)

    group_tasks = [
        Task(
            run_at=start_time + timedelta(seconds=i),
            callback="mock_callback",
            schedule_type="date",
            group_id=group_id,
        )
        for i in range(3)
    ]

    other_task = Task(
        run_at=start_time,
        callback="mock_callback",
        schedule_type="date",
    )

    await scheduler.add_tasks(group_tasks + [other_task])
    fetched = await scheduler.fetch_tasks(group_id=group_id)

    assert len(fetched) == len(group_tasks)
    assert all(task.group_id == group_id for task in fetched)


@pytest.mark.asyncio
async def test_add_task_replace_existing_behaviour(scheduler: AsyncScheduler):
    start_time = datetime.now(timezone.utc) + timedelta(seconds=10)
    task_id = uuid4().hex

    first_task = Task(
        run_at=start_time,
        callback="mock_callback",
        schedule_type="date",
        task_id=task_id,
    )

    await scheduler.add_task(first_task)

    later_task = Task(
        run_at=start_time + timedelta(seconds=10),
        callback="second_callback",
        schedule_type="date",
        task_id=task_id,
    )

    # Replace existing task
    await scheduler.add_task(later_task)
    stored_tasks = await scheduler.fetch_tasks(task_ids=[task_id])
    stored_task = stored_tasks[0]
    assert stored_task.callback == "second_callback"

    # Do not replace existing task
    even_later_task = Task(
        run_at=start_time + timedelta(seconds=20),
        callback="third_callback",
        schedule_type="date",
        task_id=task_id,
    )
    await scheduler.add_task(even_later_task, replace_existing=False)
    stored_task = (await scheduler.fetch_tasks(task_ids=[task_id]))[0]
    assert stored_task.callback == "second_callback"
    assert stored_task.run_at == start_time + timedelta(seconds=10)
