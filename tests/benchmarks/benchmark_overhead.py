import asyncio
from datetime import datetime, timedelta, timezone
from random import uniform

from observers.overhead_observer import OverheadObserver

# -------------------------------------------------------------------------------- aioscheduler + mysql


async def benchmark_aioscheduler_mysql_overhead(
    test_length_seconds: int, num_tasks: int
):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_mysql_taskshed

    observer = OverheadObserver(num_tasks)
    scheduler = await build_mysql_taskshed({"observer_callback": observer.callback})
    sleep_time = test_length_seconds / num_tasks

    await scheduler.add_tasks(
        (
            Task(
                task_id="Task 0",
                run_at=datetime.now(timezone.utc) + timedelta(seconds=0.1),
                callback_name="observer_callback",
                kwargs={"delay": test_length_seconds, "scheduled_task_id": "Task 0"},
                schedule_type="date",
                interval=None,
                group_id=1,
            ),
        )
    )

    for i in range(1, num_tasks):
        delay = uniform(0.5, 5)
        func_wait = uniform(0.5, 5)
        await scheduler.add_tasks(
            (
                Task(
                    task_id=f"Task {i}",
                    run_at=datetime.now(timezone.utc) + timedelta(seconds=delay),
                    callback_name="observer_callback",
                    kwargs={"delay": func_wait, "scheduled_task_id": f"Task {i}"},
                    schedule_type="date",
                    interval=None,
                    group_id=None,
                ),
            )
        )
        await asyncio.sleep(sleep_time)


# -------------------------------------------------------------------------------- aioscheduler + redis


async def benchmark_aioscheduler_redis_overhead(
    test_length_seconds: int, num_tasks: int
):
    from taskshed.models.task_models import Task
    from tests.benchmarks.utils import build_redis_taskshed

    observer = OverheadObserver(num_tasks)
    scheduler = await build_redis_taskshed({"observer_callback": observer.callback})
    sleep_time = test_length_seconds / num_tasks
    group = "group-1"

    await scheduler.add_tasks(
        (
            Task(
                task_id="Task 0",
                run_at=datetime.now(timezone.utc) + timedelta(seconds=0.1),
                callback_name="observer_callback",
                kwargs={"delay": test_length_seconds, "scheduled_task_id": "Task 0"},
                schedule_type="date",
                interval=None,
                group_id=group,
            ),
        )
    )

    for i in range(1, num_tasks):
        delay = uniform(0.5, 5)
        func_wait = uniform(0.5, 5)
        await scheduler.add_tasks(
            (
                Task(
                    task_id=f"Task {i}",
                    run_at=datetime.now(timezone.utc) + timedelta(seconds=delay),
                    callback_name="observer_callback",
                    kwargs={"delay": func_wait, "scheduled_task_id": f"Task {i}"},
                    schedule_type="date",
                    group_id=group,
                ),
            )
        )
        await asyncio.sleep(sleep_time)


# -------------------------------------------------------------------------------- apscheduler


async def benchmark_apscheduler_overhead(test_length_seconds: int, num_tasks: int):
    from tests.benchmarks.utils import build_apscheduler

    scheduler = build_apscheduler()
    observer = OverheadObserver(num_tasks)
    sleep_time = test_length_seconds / num_tasks

    scheduler.add_task(
        func=observer.callback,
        trigger="date",
        run_date=datetime.now(timezone.utc) + timedelta(seconds=0.1),
        kwargs={"delay": test_length_seconds, "scheduled_task_id": "Task 0"},
    )

    for i in range(1, num_tasks):
        delay = uniform(0.5, 5)
        func_wait = uniform(0.5, 5)
        scheduler.add_task(
            id=f"Task {i}",
            func=observer.callback,
            trigger="date",
            run_date=datetime.now(timezone.utc) + timedelta(seconds=delay),
            kwargs={"delay": func_wait, "scheduled_task_id": f"Task {i}"},
        )
        await asyncio.sleep(sleep_time)


# -------------------------------------------------------------------------------- arq


async def _arq_task(ctx, *, delay: float, scheduled_task_id: str):
    observer: OverheadObserver = ctx["observer"]
    await observer.callback(
        scheduled_task_id=scheduled_task_id,
        delay=delay,
    )


async def benchmark_arq_execution_lag(test_length_seconds: int, num_tasks: int):
    from arq import create_pool
    from arq.connections import RedisSettings
    from arq.worker import Worker

    # ---------------------------------------- setup

    observer = OverheadObserver(num_tasks=num_tasks)
    redis_settings = RedisSettings()
    sleep_time = test_length_seconds / num_tasks

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

    await redis.enqueue_task(
        "_arq_task",
        _task_id="Task 0",
        _defer_until=datetime.now(tz=timezone.utc) + timedelta(seconds=0.1),
        delay=test_length_seconds,
        scheduled_task_id="Task 0",
    )

    for i in range(1, num_tasks):
        delay = uniform(0.5, 5)
        func_wait = uniform(0.5, 5)
        await redis.enqueue_task(
            "_arq_task",
            _task_id=f"Task {i}",
            _defer_until=datetime.now(tz=timezone.utc) + timedelta(seconds=delay),
            delay=func_wait,
            scheduled_task_id=f"Task {i}",
        )
        await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    NUM_TASKS = 500
    TEST_LENGTH_SECONDS = 30

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(
        benchmark_aioscheduler_redis_overhead(
            test_length_seconds=TEST_LENGTH_SECONDS, num_tasks=NUM_TASKS
        )
    )
    loop.run_forever()
