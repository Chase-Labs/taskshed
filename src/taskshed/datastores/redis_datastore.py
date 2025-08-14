import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isinf
from typing import Iterable

from redis.asyncio import Redis

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task, TaskExecutionTime


@dataclass(frozen=True, kw_only=True)
class RedisConfig:
    """
    Configuration for connecting to a Redis server.

    Attributes:
        host: The Redis server host address.
        port: The connection port number.
        username: The username for authentication, if required.
        password: The password for authentication, if required.
        unix_socket_path: The path to a Unix socket file for the connection,
            used for local connections instead of host and port.
        ssl: A boolean flag to enable or disable SSL/TLS for a secure
            connection.
    """

    host: str = "localhost"
    port: int = 6379
    username: str | None = None
    password: str | None = None
    unix_socket_path: str | None = None
    ssl: bool = False


class RedisDataStore(DataStore):
    """
    There are three data structures used to store the data:

    1. A Soreted Set that stores Task IDs sorted by the task's execution timestamp.
    2. A Hash that stores the task information: Task ID -> info
    3. A Set that stores the Task IDs relating to each Group: Group ID -> Task IDs

    Notes for each can be found:

    1. https://redis.io/docs/latest/develop/data-types/sorted-sets/
    2. https://redis.io/docs/latest/develop/data-types/hashes/
    3. https://redis.io/docs/latest/develop/data-types/sets/
    """

    KEY_PREFIX = "scheduler"

    # -------------------------------------------------------------------------------- scripts

    LUA_HSETNX_SCRIPT = """
    if redis.call("EXISTS", KEYS[1]) == 0 then
        return redis.call("HSET", KEYS[1], unpack(ARGV))
    else
        return 0
    end
    """

    # -------------------------------------------------------------------------------- private methods

    def __init__(self, config: RedisConfig | None = None):
        self._config = config or RedisConfig()
        self._client: Redis | None = None
        self._queue_index = f"{self.KEY_PREFIX}:task_queue"

    def _get_group_index(self, group_id: str) -> str:
        return f"{self.KEY_PREFIX}:group:{group_id}:tasks"

    def _get_task_index(self, task_id: int | str) -> str:
        return f"{self.KEY_PREFIX}:task:{task_id}"

    def _serialize_task(self, task: Task) -> dict:
        return {
            "task_id": task.task_id,
            "run_at": task.run_at.timestamp(),
            "paused": int(task.paused),
            "callback": task.callback,
            "kwargs": json.dumps(task.kwargs),
            "run_type": task.run_type,
            "interval": task.interval_seconds() if task.interval else "",
            "group_id": task.group_id if task.group_id is not None else "",
        }

    def _deserialize_task(self, data: dict) -> Task:
        if data["interval"]:
            interval = timedelta(seconds=float(data["interval"]))
        else:
            interval = None

        return Task(
            task_id=data["task_id"],
            run_at=datetime.fromtimestamp(float(data["run_at"])),
            paused=bool(int(data["paused"])),
            callback=data["callback"],
            kwargs=json.loads(data["kwargs"]),
            run_type=data.get("run_type"),
            interval=interval,
            group_id=data["group_id"] if data.get("group_id") else None,
        )

    # -------------------------------------------------------------------------------- public methods

    async def start(self) -> None:
        if self._client is not None:
            return

        self._client = Redis(
            host=self._config.host,
            port=self._config.port,
            username=self._config.username,
            password=self._config.password,
            unix_socket_path=self._config.unix_socket_path,
            ssl=self._config.ssl,
            decode_responses=True,
        )
        self._hsetallnx = self._client.register_script(self.LUA_HSETNX_SCRIPT)

    async def shutdown(self) -> None:
        if self._client is None:
            return

        await self._client.close()
        self._client = None

    async def add_tasks(
        self, tasks: Iterable[Task], *, replace_existing: bool = True
    ) -> None:
        pipeline = self._client.pipeline()
        for task in tasks:
            pipeline.zadd(
                self._queue_index,
                {task.task_id: task.run_at.timestamp()},
                nx=not replace_existing,
            )

            serialized_task = self._serialize_task(task)
            if replace_existing:
                pipeline.hset(
                    self._get_task_index(task.task_id),
                    mapping=serialized_task,
                )
            else:
                args = [item for pair in serialized_task.items() for item in pair]
                self._hsetallnx(
                    keys=[self._get_task_index(task.task_id)],
                    args=args,
                    client=pipeline,  # Execute the script within the pipeline
                )

            if task.group_id is not None:
                pipeline.sadd(self._get_group_index(task.group_id), task.task_id)
        await pipeline.execute()

    async def fetch_due_tasks(self, dt: datetime) -> list[Task]:
        task_ids = await self._client.zrangebyscore(
            self._queue_index, "-inf", dt.timestamp()
        )
        if not task_ids:
            return []

        pipe = self._client.pipeline()
        for task_id in task_ids:
            pipe.hgetall(self._get_task_index(task_id))
        data = await pipe.execute()

        return [
            self._deserialize_task(task)
            for task in data
            if task and not int(task["paused"])
        ]

    async def fetch_next_wakeup(self) -> datetime | None:
        res = await self._client.zrange(self._queue_index, 0, 0, withscores=True)
        if res:
            next_wakeup = res[0][1]
            if not isinf(next_wakeup):
                return datetime.fromtimestamp(next_wakeup)

    async def fetch_tasks(self, task_ids: Iterable[str]) -> list[Task]:
        if not task_ids:
            return []

        # Get all the data from the task Hash
        pipeline = self._client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(self._get_task_index(task_id))
        data = await pipeline.execute()

        return [self._deserialize_task(task) for task in data if task]

    async def fetch_group_tasks(self, group_id: str) -> list[Task]:
        group_task_ids = await self._client.smembers(self._get_group_index(group_id))
        pipeline = self._client.pipeline()
        for task_id in group_task_ids:
            pipeline.hgetall(self._get_task_index(task_id))
        res = await pipeline.execute()
        return [self._deserialize_task(data) for data in res if data]

    async def update_execution_times(self, tasks: Iterable[TaskExecutionTime]):
        pipeline = self._client.pipeline()
        for task in tasks:
            timestamp = task.run_at.timestamp()
            pipeline.hset(
                self._get_task_index(task.task_id),
                mapping={"run_at": timestamp},
            )
            pipeline.zadd(self._queue_index, {task.task_id: timestamp}, xx=True)
        await pipeline.execute()

    async def update_tasks_paused_status(
        self, task_ids: Iterable[str], paused: bool
    ) -> None:
        pipeline = self._client.pipeline()
        for task_id in task_ids:
            task_idx = self._get_task_index(task_id)
            pipeline.hset(task_idx, "paused", int(paused))
            if paused:
                pipeline.zadd(self._queue_index, {task_id: "+inf"}, xx=True)
            else:
                timestamp = await self._client.hget(task_idx, "run_at")
                pipeline.zadd(self._queue_index, {task_id: timestamp}, xx=True)
        await pipeline.execute()

    async def update_group_paused_status(self, group_id: str, paused: bool) -> None:
        group_task_ids = await self._client.smembers(self._get_group_index(group_id))
        pipeline = self._client.pipeline()
        for task_id in group_task_ids:
            task_idx = self._get_task_index(task_id)
            pipeline.hset(task_idx, mapping={"paused": int(paused)})
            if paused:
                pipeline.zadd(self._queue_index, mapping={task_id: "+inf"}, xx=True)
            else:
                timestamp = await self._client.hget(task_idx, "run_at")
                pipeline.zadd(self._queue_index, {task_id: timestamp}, xx=True)
        await pipeline.execute()

    async def remove_tasks(self, task_ids: Iterable[str]) -> None:
        # Batch get all group_ids first
        pipeline = self._client.pipeline()
        for task_id in task_ids:
            pipeline.hget(self._get_task_index(task_id), "group_id")
        group_ids = await pipeline.execute()

        # Then batch removal
        pipeline = self._client.pipeline()
        for task_id, group_id in zip(task_ids, group_ids):
            # Remove the task ID from the group Set
            if group_id:
                pipeline.srem(self._get_group_index(group_id), task_id)

            # Remove the task ID from the task Queue (Sorted Set)
            pipeline.zrem(self._queue_index, task_id)

            # Delete the task Hash with corresponding task ID
            pipeline.delete(self._get_task_index(task_id))

        await pipeline.execute()

    async def remove_all_tasks(self) -> None:
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(
                cursor=cursor, match=f"{self.KEY_PREFIX}*"
            )
            if keys:
                await self._client.delete(*keys)
            if cursor == 0:
                break

    async def remove_group_tasks(self, group_id: str) -> None:
        group_task_ids = await self._client.smembers(self._get_group_index(group_id))
        if not group_task_ids:
            return

        pipeline = self._client.pipeline()
        for task_id in group_task_ids:
            task_index = self._get_task_index(task_id)
            # Remove the task ID from the task Queue (Sorted Set)
            pipeline.zrem(self._queue_index, task_id)
            # Delete the task Hash with corresponding task ID
            pipeline.delete(task_index)

        # Remove the group Set
        pipeline.delete(self._get_group_index(group_id))
        await pipeline.execute()
