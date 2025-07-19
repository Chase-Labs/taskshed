import asyncio
import random
from datetime import datetime, timedelta, timezone
from time import perf_counter
from uuid import uuid4

from tests.benchmarks.observers.schedule_observer import ScheduleObserver

random.seed(42)  # For reproducibility


# -------------------------------------------------------------------------------- aioscheduler + mysql


async def benchmark_aioscheduler_mysql_schedule_batch(num_tasks: int, runs: int):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_mysql_taskshed

    observer = ScheduleObserver(num_tasks)
    scheduler = await build_mysql_taskshed({"observer_callback": observer.callback})

    schedule_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
    tasks = [
        Task(
            run_at=schedule_datetime + timedelta(seconds=i),
            callback="observer_callback",
            schedule_type="date",
        )
        for i in range(num_tasks)
    ]

    random.shuffle(tasks)

    for _ in range(runs):
        start = perf_counter()
        for task in tasks:
            await scheduler.add_task(task)
        observer.record(perf_counter() - start)
        await scheduler._task_store.remove_all_tasks()

    observer.print_results()


# -------------------------------------------------------------------------------- aioscheduler + redis


async def benchmark_aioscheduler_redis_schedule_batch(num_tasks: int, runs: int):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_redis_taskshed

    observer = ScheduleObserver(num_tasks)
    scheduler = await build_redis_taskshed({"observer_callback": observer.callback})

    schedule_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
    tasks = [
        Task(
            run_at=schedule_datetime + timedelta(seconds=i),
            callback="observer_callback",
            schedule_type="date",
        )
        for i in range(num_tasks)
    ]

    random.shuffle(tasks)

    for _ in range(runs):
        start = perf_counter()
        for task in tasks:
            await scheduler.add_task(task)
        observer.record(perf_counter() - start)
        await scheduler._task_store.remove_all_tasks()

    observer.print_results()


# -------------------------------------------------------------------------------- apscheduler


async def benchmark_apscheduler_schedule_batch(num_tasks: int, runs: int):
    from tests.benchmarks.utils import build_apscheduler

    observer = ScheduleObserver(num_tasks)
    scheduler = build_apscheduler()

    run_date = datetime.now(timezone.utc) + timedelta(hours=1)
    tasks = [
        {
            "id": uuid4().hex,
            "func": observer.callback,
            "trigger": "date",
            "run_date": run_date + timedelta(seconds=i),
        }
        for i in range(num_tasks)
    ]

    random.shuffle(tasks)

    for _ in range(runs):
        start = perf_counter()
        for task in tasks:
            scheduler.add_task(**task)
        observer.record(perf_counter() - start)
        scheduler.remove_all_tasks()

    observer.print_results()


if __name__ == "__main__":
    NUM_TASKS = 1000
    NUM_RUNS = 1000

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(
        benchmark_aioscheduler_mysql_schedule_batch(num_tasks=NUM_TASKS, runs=NUM_RUNS)
    )
    loop.run_forever()
