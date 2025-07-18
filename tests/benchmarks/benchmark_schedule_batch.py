import asyncio
import random
from datetime import datetime, timedelta, timezone
from time import perf_counter
from uuid import uuid4

from observers.schedule_observer import ScheduleObserver

random.seed(42)  # For reproducibility


# -------------------------------------------------------------------------------- aioscheduler + mysql


async def benchmark_aioscheduler_mysql_schedule_batch(num_tasks: int, runs: int):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_mysql_aioscheduler

    observer = ScheduleObserver(num_tasks)
    scheduler = await build_mysql_aioscheduler({"callback": observer.callback})

    schedule_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
    tasks = [
        Task(
            run_at=schedule_datetime + timedelta(seconds=i),
            callback=observer.callback,
            schedule_type="date",
        )
        for i in range(num_tasks)
    ]

    random.shuffle(tasks)

    for _ in range(runs):
        start = perf_counter()
        await scheduler.add_tasks(tasks)
        observer.record(perf_counter() - start)
        await scheduler._task_store.remove_all_tasks()

    observer.print_results()


# -------------------------------------------------------------------------------- aioscheduler + redis


async def benchmark_aioscheduler_redis_schedule_batch(num_tasks: int, runs: int):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_redis_aioscheduler

    observer = ScheduleObserver(num_tasks)
    scheduler = await build_redis_aioscheduler({"callback": observer.callback})

    schedule_datetime = datetime.now(timezone.utc) + timedelta(hours=1)
    tasks = [
        Task(
            run_at=schedule_datetime + timedelta(seconds=i),
            callback=observer.callback,
            schedule_type="date",
        )
        for i in range(num_tasks)
    ]

    random.shuffle(tasks)

    for _ in range(runs):
        start = perf_counter()
        await scheduler.add_tasks(tasks)
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


# -------------------------------------------------------------------------------- arq


async def _arq_task(ctx):
    observer: ScheduleObserver = ctx["observer"]
    await observer.callback()


async def benchmark_arq_schedule_batch(num_tasks: int, runs: int):
    from arq import create_pool
    from arq.connections import RedisSettings
    from arq.worker import Worker

    # ---------------------------------------- setup

    observer = ScheduleObserver(num_tasks=num_tasks)
    redis_settings = RedisSettings()

    async def startup(ctx):
        ctx["observer"] = observer

    worker = Worker(
        functions=[_arq_task],
        on_startup=startup,
        redis_settings=redis_settings,
        keep_result=0,
        max_tasks=num_tasks + 1,
    )

    asyncio.create_task(worker.async_run())
    redis = await create_pool(redis_settings)
    await redis.flushall()

    # ---------------------------------------- benchmark

    run_date = datetime.now(timezone.utc) + timedelta(hours=1)
    tasks = [
        {
            "function": "_arq_task",
            "_task_id": uuid4().hex,
            "_defer_until": run_date + timedelta(seconds=i),
        }
        for i in range(num_tasks)
    ]
    random.shuffle(tasks)

    for _ in range(runs):
        start = perf_counter()
        for task in tasks:
            await redis.enqueue_task(**task)
        observer.record(perf_counter() - start)
        await redis.flushall()

    observer.print_results()


if __name__ == "__main__":
    NUM_TASKS = 1000
    NUM_RUNS = 1000

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(
        benchmark_aioscheduler_redis_schedule_batch(num_tasks=NUM_TASKS, runs=NUM_RUNS)
    )
    loop.run_forever()
