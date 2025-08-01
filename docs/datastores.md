# Datastores

At present, there are three supported datastore techniques that TaskShed supports:

* **In Memory**: Keeps the Task data in Python dictionaries and lists. This is useful for prototyping and testing, but does not have persistence, and shouldn't be used in production.
* **MySQL**: Uses the awesome [aiomysql](https://github.com/aio-libs/aiomysql) library to create a connection pool to a MySQL server, and asynchronously executes commands.
* **Redis**: Uses the equally awesome [redis-py](https://github.com/redis/redis-py) interface to the Redis key-value store.

In order to levearage a persistant datastore you'll need to install the additional dependencies, which can be done with:

=== "Redis"

    ```
    pip install "taskshed[redis]"
    ```

=== "MySQL"

    ```
    pip install "taskshed[mysql]"
    ```

## Serialization

Serialization is the process of converting data into a format that can be easily stored and reconstructed later. TaskShed serializes/deserializes data in **JSON**, which has many benefits: it is human readable, quick to parse and is supported by all databases and languages.

That being said, there are a few downsides that you should be aware of. JSON only supports a limited set of primitive data types, such as strings, numbers, booleans, arrays, null, objects and nested combinations of these types. 

There may be occasions when you might want to store things like `datetime` objects or `Pydantic` models. As such your code will have to do additional work to convert these objects into JSON seriablizable formats, i.e. calling `datetime.isoformat().` or `BaseModel.model_dump()`.

```py title="Example When Passing Datetime Objects as Callback Kwargs"
from datetime import datetime, timedelta

from taskshed.datastores import InMemoryDataStore
from taskshed.schedulers import AsyncScheduler
from taskshed.workers import EventDrivenWorker


async def calculate_job_latency(run_at: str):
    current_time = datetime.now()
    scheduled_time = datetime.fromisoformat(run_at)
    latency = current_time - scheduled_time
    print(
        f"\nExecuted at:\t{current_time}\nScheduled for:\t{scheduled_time}\nLatency:\t{latency.total_seconds()} s"
    )


datastore = InMemoryDataStore()
worker = EventDrivenWorker(callback_map={"calculate_job_latency": calculate_job_latency}, datastore=datastore)
scheduler = AsyncScheduler(datastore=datastore, worker=worker)


async def main():
    await scheduler.start()
    await worker.start()

    run_at = datetime.now() + timedelta(seconds=1)

    await scheduler.add_task(
        callback="calculate_job_latency",
        run_at=run_at,
        kwargs={"run_at": run_at.isoformat()},
    )


if __name__ == "__main__":
    import asyncio

    loop = asyncio.new_event_loop()
    loop.create_task(main())
    loop.run_forever()
```