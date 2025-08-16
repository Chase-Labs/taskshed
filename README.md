# TaskShed

A high-performance, asynchronous, ready for production job scheduling framework.

TaskShed provides a simple API to schedule your Python coroutines for later execution. You can run tasks just once or on a recurring interval. The scheduler is dynamic, allowing you to add, update or remove tasks on the fly. Furthermore, by connecting to a persistent datastore, TaskShed ensures your tasks survive restarts and automatically catches up on any executions that were missed while the system was offline.

The key features are:

* **Fast**: TaskShed has a [very low latency, overhead and can execute several thousands tasks a second](https://chase-labs.github.io/taskshed/benchmarks/).
* **Distributed**: TaskShed has the capacity to spawn several workers and schedules across many machines, while also providing optimisation for monolinth architectures.
* **Persistant**: Tasks are stored in database, meaning that they won't get dropped on shutdown. TaskShed currently supports Redis and MySQL.
* **Easy**: TaskShed's modular architecture is straightforward and easy to set-up, and works in any asynchronous environement.


# Installation 🔧

Install the core package using pip:

```sh
pip install taskshed
```

TaskShed has no extra dependencies beyond its core framework. However, if you want **persistent task storage**, you’ll need to install one of the optional backends. TaskShed currently supports [Redis](https://redis.io/) and [MySQL](https://www.mysql.com/). You can install the appropriate driver using:

```shs
pip install "taskshed[redis]"
```

or

```sh
pip install "taskshed[mysql]"
```

# Quick Start 🏁

Here's a simple example of scheduling a task to run in 5 seconds.

```py
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

# Documentation 📚

https://chase-labs.github.io/taskshed/

# Contributing 🤝

Contributions are welcome! Please feel free to submit a pull request or open an issue.

# License 📜

This project is licensed under the MIT License.
