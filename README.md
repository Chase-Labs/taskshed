# TaskShed 🛖

TaskShed is a high-performance, asynchronous, ready-for-production job scheduling framework.

The key features are:

* **Fast**: TaskShed has an extremely low latency, overhead and can execute several thousands jobs a second.
* **Distributed**: TaskShed has the capacity to spawn several workers and schedules across many machines, while also providing support for monolinth architectures.
* **Persistant**: Jobs are stored in database, meaning that jobs won't get dropped on shutdown. TaskShed currently supports Redis and MySQL.
* **Easy**: TaskShed is straightforward to run. 


# Installation 🔧

Install the core package using pip:

```sh
pip install taskshed
```

To use persistent datastores like Redis or MySQL, you need to install the optional dependencies:

```sh
pip install "taskshed[redis]"
```


```sh
pip install "taskshed[mysql]"
```

# Quick Start 🏁

Here's a simple example of scheduling a job to run in 5 seconds.

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

Coming soon! 🚧

# Contributing 🤝

Contributions are welcome! Please feel free to submit a pull request or open an issue.

# License 📜

This project is licensed under the MIT License.