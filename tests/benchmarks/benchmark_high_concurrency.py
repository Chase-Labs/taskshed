import asyncio
import resource
from datetime import datetime, timedelta, timezone
from random import uniform
from time import perf_counter
from uuid import uuid4

from observers.high_concurrency_observer import HighConcurrencyObserver

START_DT = datetime.now(tz=timezone.utc) + timedelta(seconds=20)


def _build_observer(num_tasks: int) -> HighConcurrencyObserver:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return HighConcurrencyObserver(
        num_tasks=num_tasks,
        cpu_start=usage.ru_utime + usage.ru_stime,
        mem_start=usage.ru_maxrss,
    )


# -------------------------------------------------------------------------------- aioscheduler + mysql


async def benchmark_aioscheduler_mysql_high_concurrency(num_tasks: int):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_mysql_aioscheduler

    observer = _build_observer(num_tasks)
    scheduler = await build_mysql_aioscheduler({"callback": observer.callback})

    schedule_start = perf_counter()
    tasks = [
        Task(
            task_id=uuid4().hex,
            run_at=START_DT + timedelta(seconds=uniform(0, 1)),
            callback=observer.callback,
            schedule_type="date",
        )
        for _ in range(num_tasks)
    ]
    await scheduler.add_tasks(tasks)
    print(f"Scheduled {num_tasks} in {(perf_counter() - schedule_start):.5f}s")


# -------------------------------------------------------------------------------- aioscheduler + redis


async def benchmark_aioscheduler_redis_high_concurrency(num_tasks: int):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_redis_aioscheduler

    observer = _build_observer(num_tasks)
    scheduler = await build_redis_aioscheduler({"callback": observer.callback})

    schedule_start = perf_counter()
    tasks = [
        Task(
            task_id=uuid4().hex,
            run_at=START_DT + timedelta(seconds=uniform(0, 1)),
            callback=observer.callback,
            schedule_type="date",
        )
        for _ in range(num_tasks)
    ]

    await scheduler.add_tasks(tasks)
    print(f"Scheduled {num_tasks} in {(perf_counter() - schedule_start):.5f}s")


# -------------------------------------------------------------------------------- apscheduler


async def benchmark_apscheduler_high_concurrency(num_tasks: int):
    from tests.benchmarks.utils import build_apscheduler

    observer = _build_observer(num_tasks)
    scheduler = build_apscheduler()

    schedule_start = perf_counter()
    for _ in range(num_tasks):
        scheduler.add_task(
            id=uuid4().hex,
            func=observer.callback,
            trigger="date",
            run_date=START_DT + timedelta(seconds=uniform(0, 1)),
        )

    print(f"Scheduled {num_tasks} in {(perf_counter() - schedule_start):.5f}s")


# -------------------------------------------------------------------------------- arq


async def _arq_task(ctx):
    observer: HighConcurrencyObserver = ctx["observer"]
    await observer.callback()


async def benchmark_arq_high_concurrency(num_tasks: int):
    from arq import create_pool
    from arq.connections import RedisSettings
    from arq.worker import Worker

    # ---------------------------------------- setup

    observer = _build_observer(num_tasks)
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

    schedule_start = perf_counter()
    for _ in range(num_tasks):
        scheduled_datetime = START_DT + timedelta(seconds=uniform(0, 1))
        await redis.enqueue_task(
            "_arq_task",
            _task_id=uuid4().hex,
            _defer_until=scheduled_datetime,
        )
    print(f"Scheduled {num_tasks} in {(perf_counter() - schedule_start):.5f}s")


if __name__ == "__main__":
    NUM_TASKS = 50_000

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(benchmark_aioscheduler_redis_high_concurrency(NUM_TASKS))
    loop.run_forever()
