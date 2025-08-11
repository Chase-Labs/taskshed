# Workers

Workers are responsible for pulling due tasks from a datastore and executing their associated callbacks. TaskShed ships with two workers:

* **EventDrivenWorker**: Dynamically adjusts wakeups based on scheduled tasks. Lowest latency and best for single-process or tightly-coordinated systems where a scheduler can notify the worker of schedule changes.
* **PollingWorker**: Polls the datastore at a fixed interval for due tasks. Easier to run in fully distributed setups where the worker and scheduler are decoupled.

??? info "Task Latency"

    Task latency = (when the task actually ran) - (the task's scheduled run time).

    **EventDrivenWorker** generally achieves the lowest latency because it sets timers to wake the event loop exactly when tasks should run. **PollingWorker** has higher latency determined by the polling interval.

Use **EventDrivenWorker** when you can run a Scheduler and a Worker in the same process. Use **PollingWorker** when you need to decouple the Scheduler and Worker, or run them on different machines.

## Callback Map

Every worker accepts a `callback_map` argument. This is a mapping of callback names (strings) to async callables (coroutines).

The worker looks up `task.callback` in `callback_map`. If the callback name is not present the worker raises `IncorrectCallbackNameError`.

Callback functions must be coroutines. If you need to use a synchronous function, wrap it in an `async` wrapper that offloads work to a thread or process.

## Event Driven Worker

The **EventDrivenWorker** achieves the lowest _possible task execution latency_ by waking up exactly when the next task is due, rather than polling on a fixed interval. This makes it ideal when precise timing is important and you want to minimise the delay between a task’s scheduled time and when it actually runs.

However, it must be _notified whenever tasks are added, removed, or rescheduled in the datastore_. In TaskShed, the **Scheduler** takes care of these notifications, so an **EventDrivenWorker** is typically passed directly to the scheduler when it’s created.

``` py title="Passing EventDrivenWorker to the AsyncScheduler" hl_lines="11"
from taskshed.datastores import InMemoryDataStore
from taskshed.schedulers import AsyncScheduler
from taskshed.workers import EventDrivenWorker

datastore = InMemoryDataStore()
worker = EventDrivenWorker(
    callback_map={"callback": callback},
    datastore=datastore,
)

scheduler = AsyncScheduler(datastore=datastore, worker=worker)
```

## Polling Worker

The **PollingWorker** checks the datastore at regular intervals to see if any tasks are due. You can control the frequency using the optional `polling_interval` parameter (default: 3 seconds). A shorter interval reduces latency but increases datastore load.

``` py title="PollingWorker With a Polling Interval of 1 Second"
from taskshed.workers import PollingWorker

PollingWorker(
    callback_map={"callback": callback},
    datastore=get_scheduler_datastore(),
    polling_interval=timedelta(seconds=1)
)
```

Because it doesn’t rely on notifications, the **PollingWorker** can operate completely independently from the scheduler. This makes it well-suited for distributed systems, where workers and the scheduler run on separate machines but share the same datastore.

You can also run multiple **PollingWorkers** against the same datastore, even on different machines. This allows you to scale out processing capacity as each worker will claim and execute different tasks without interfering with the others.

### Distributed Example

Below, the scheduler runs on one machine, a shared Redis datastore on another and the worker on a third.

``` py title="schedule_tasks.py"
from datetime import datetime, timedelta

from taskshed.datastores import RedisConfig, RedisDataStore
from taskshed.schedulers import AsyncScheduler

datastore = RedisDataStore(
    RedisConfig(
        host="redis-datastore.example.com",
        port=16379,
        username="user",
        password="password",
        ssl=True,
    )
)

scheduler = AsyncScheduler(datastore)


async def main():
    await scheduler.start()
    while True:
        await scheduler.add_task(
            callback="say_hello",
            run_at=datetime.now() + timedelta(seconds=3),
        )


if __name__ == "__main__":
    import asyncio

    loop = asyncio.new_event_loop()
    loop.create_task(main())
    loop.run_forever()

```

``` py title="execute_tasks.py"
from taskshed.datastores import RedisConfig, RedisDataStore
from taskshed.workers import PollingWorker


async def say_hello():
    print("Hello!")


datastore = RedisDataStore(
    RedisConfig(
        host="redis-datastore.example.com",
        port=16379,
        username="user",
        password="password",
        ssl=True,
    )
)
worker = PollingWorker(
    callback_map={"say_hello": say_hello},
    datastore=datastore,
)


if __name__ == "__main__":
    import asyncio

    loop = asyncio.new_event_loop()
    loop.create_task(worker.start())
    loop.run_forever()
```