# Introduction

A high-performance, asynchronous, ready for production job scheduling framework.

TaskShed provides a simple API to schedule your Python coroutines for later execution. You can run tasks just once or on a recurring interval. The scheduler is dynamic, allowing you to add, update or remove tasks on the fly. Furthermore, by connecting to a persistent datastore, TaskShed ensures your tasks survive restarts and automatically catches up on any executions that were missed while the system was offline.

The key features are:

* **Fast**: TaskShed has an extremely [low latency, overhead and can execute several thousands tasks a second](benchmarks.md).
* **Distributed**: TaskShed has the capacity to spawn several workers and schedules across many machines, while also providing support for monolinth architectures.
* **Persistant**: Tasks are stored in database, meaning that tasks won't get dropped on shutdown. TaskShed currently supports Redis and MySQL.
* **Easy**: TaskShed is straightforward to run. 

## Installation

```title="Basic Installation"
pip install taskshed
```

The base version of TaskShed has no additional dependencies, though needs a driver for task persistance. This can be installed by running:

=== "Redis"

    ```
    pip install "taskshed[redis]"
    ```

=== "MySQL"

    ```
    pip install "taskshed[mysql]"
    ```

## Quickstart

Create a file `main.py` with:

``` py
from datetime import datetime, timedelta
from taskshed.datastores import InMemoryDataStore
from taskshed.schedulers import AsyncScheduler
from taskshed.workers import EventDrivenWorker


async def say_hello(name: str):
    print(f"Hello, {name}!")


datastore = InMemoryDataStore()
worker = EventDrivenWorker(callback_map={"say_hello": say_hello}, datastore=datastore)
scheduler = AsyncScheduler(datastore=datastore, worker=worker)


async def main():
    await scheduler.start()
    await worker.start()
    await scheduler.add_task(
        callback="say_hello",
        run_at=datetime.now() + timedelta(seconds=3),
        kwargs={"name": "World"},
    )


if __name__ == "__main__":
    import asyncio

    loop = asyncio.new_event_loop()
    loop.create_task(main())
    loop.run_forever()
```

Then run it with.

```
python main.py
```