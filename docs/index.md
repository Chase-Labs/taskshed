# Introduction

TaskShed is a high-performance, asynchronous, ready for production task scheduling framework.

The key features are:

* **Fast**: TaskShed has an extremely [low latency, overhead and can execute several thousands tasks a second](benchmarks.md).
* **Distributed**: TaskShed has the capacity to spawn several workers and schedules across many machines, while also providing support for monolinth architectures.
* **Persistant**: Jobs are stored in database, meaning that jobs won't get dropped on shutdown. TaskShed currently supports Redis and MySQL.
* **Easy**: TaskShed is straightforward to run. 

## Installation

```title="Basic Installation"
pip install taskshed
```

The base version of TaskShed has no additional dependencies, though needs a driver for job persistance. This can be installed by running:

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


async def foo():
    print("\nHello from the future! 👋")


datastore = InMemoryDataStore()
worker = EventDrivenWorker(callback_map={"foo": foo}, datastore=datastore)
scheduler = AsyncScheduler(datastore=datastore, worker=worker)


async def main():
    await scheduler.start()
    await worker.start()
    await scheduler.add_task(
        callback="foo",
        run_at=datetime.now() + timedelta(seconds=3),
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