import asyncio
from datetime import datetime, timedelta, timezone
from random import randint
from uuid import uuid4

from observers.execution_lag_observer import ExecutionLagObserver

# -------------------------------------------------------------------------------- taskshed + mysql


async def benchmark_taskshed_mysql_execution_lag(num_tasks: int = 1000):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_mysql_taskshed

    observer = ExecutionLagObserver(num_tasks=num_tasks)
    scheduler = await build_mysql_taskshed({"observer_callback": observer.callback})

    initial_schedule = datetime.now(tz=timezone.utc) + timedelta(seconds=10)

    tasks = []
    for _ in range(num_tasks):
        run_at = initial_schedule + timedelta(
            seconds=randint(0, 59),
            milliseconds=randint(0, 999),
            microseconds=randint(0, 999999),
        )

        task_id = uuid4().hex
        tasks.append(
            Task(
                task_id=task_id,
                run_at=run_at,
                callback="observer_callback",
                kwargs={
                    "scheduled_run_time": run_at,
                    "scheduled_task_id": task_id,
                },
                run_type="once",
                interval=None,
                group_id=None,
            )
        )

    await scheduler.add_tasks(tasks)


# -------------------------------------------------------------------------------- taskshed + redis


async def benchmark_taskshed_redis_execution_lag(num_tasks: int = 1000):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_redis_taskshed

    observer = ExecutionLagObserver(num_tasks=num_tasks)
    scheduler = await build_redis_taskshed({"observer_callback": observer.callback})

    initial_schedule = datetime.now(tz=timezone.utc) + timedelta(seconds=10)

    tasks = []
    for _ in range(num_tasks):
        run_at = initial_schedule + timedelta(
            seconds=randint(0, 59),
            milliseconds=randint(0, 999),
            microseconds=randint(0, 999999),
        )

        task_id = uuid4().hex

        tasks.append(
            Task(
                task_id=task_id,
                run_at=run_at,
                callback="observer_callback",
                kwargs={
                    "scheduled_run_time": run_at,
                    "scheduled_task_id": task_id,
                },
                run_type="once",
            )
        )

    await scheduler.add_tasks(tasks)


# -------------------------------------------------------------------------------- apscheduler


async def benchmark_apscheduler_execution_lag(num_tasks: int = 1000):
    from tests.benchmarks.utils import build_apscheduler

    observer = ExecutionLagObserver(num_tasks=num_tasks)
    scheduler = build_apscheduler()

    initial_schedule = datetime.now(tz=timezone.utc) + timedelta(seconds=10)

    for _ in range(num_tasks):
        run_at = initial_schedule + timedelta(
            seconds=randint(0, 59),
            milliseconds=randint(0, 999),
            microseconds=randint(0, 999999),
        )
        task_id = uuid4().hex

        scheduler.add_task(
            id=task_id,
            func=observer.callback,
            trigger="once",
            run_date=run_at,
            kwargs={
                "scheduled_run_time": run_at,
                "scheduled_task_id": task_id,
            },
        )


# -------------------------------------------------------------------------------- arq


async def _arq_task(ctx, *, scheduled_run_time: datetime, scheduled_task_id: str):
    observer: ExecutionLagObserver = ctx["observer"]
    await observer.callback(
        scheduled_run_time=scheduled_run_time,
        scheduled_task_id=scheduled_task_id,
    )


async def benchmark_arq_execution_lag(num_tasks: int = 1000):
    from arq import create_pool
    from arq.connections import RedisSettings
    from arq.worker import Worker

    observer = ExecutionLagObserver(num_tasks=num_tasks)
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

    initial_schedule = datetime.now(tz=timezone.utc) + timedelta(seconds=10)
    for _ in range(num_tasks):
        run_at = initial_schedule + timedelta(
            seconds=randint(0, 59),
            milliseconds=randint(0, 999),
            microseconds=randint(0, 999_999),
        )
        task_id = uuid4().hex
        await redis.enqueue_task(
            "_arq_task",
            _task_id=task_id,
            _defer_until=run_at,
            scheduled_run_time=run_at,
            scheduled_task_id=task_id,
        )


if __name__ == "__main__":
    NUM_TASKS = 1000

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(benchmark_taskshed_redis_execution_lag(num_tasks=NUM_TASKS))
    loop.run_forever()
