import asyncio
import json
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncGenerator

import aiomysql

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task, TaskExecutionTime


@dataclass(frozen=True, kw_only=True)
class MySQLConfig:
    host: str
    user: str
    password: str
    db: str
    port: int = 3306


class MySQLDataStore(DataStore):
    # -------------------------------------------------------------------------------- queries

    _CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS _taskshed_data (
        `task_id` VARCHAR(63) NOT NULL,
        `run_at` TIMESTAMP(6) NOT NULL,
        `paused` TINYINT NOT NULL DEFAULT 0,
        `callback_name` VARCHAR(63) NOT NULL,
        `kwargs` JSON NOT NULL,
        `schedule_type` ENUM('date', 'interval') NOT NULL,
        `interval` FLOAT DEFAULT NULL,
        `group_id` VARCHAR(63) DEFAULT NULL,
    PRIMARY KEY (task_id),
    UNIQUE INDEX task_id_UNIQUE (task_id ASC),
    INDEX idx_group_id (group_id ASC),
    INDEX idx_paused_run_at (paused ASC, run_at ASC)
    """

    _DELETE_TASKS_QUERY = """
    DELETE FROM _taskshed_data
    WHERE
        task_id IN %s
    """

    _DELETE_ALL_TASKS_QUERY = """
    DELETE FROM _taskshed_data
    WHERE
        paused IN (0 , 1)
    """

    _DELETE_GROUP_TASKS_QUERY = """
    DELETE FROM _taskshed_data
    WHERE
        group_id = %s
    """

    _INSERT_TASKS_WITHOUT_REPLACEMENT_QUERY = """
    INSERT INTO _taskshed_data (task_id, run_at, paused, callback_name, kwargs, schedule_type, interval, group_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    _INSERT_TASKS_WITH_REPLACEMENT_QUERY = """
    INSERT INTO _taskshed_data (task_id, run_at, paused, callback_name, kwargs, schedule_type, interval, group_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            run_at = new.run_at,
            callback_name = new.callback_name,
            kwargs = new.kwargs,
            paused = new.paused,
            group_id = new.group_id
    """

    _SELECT_TASKS_QUERY = """
    SELECT
        *
    FROM
        _taskshed_data
    WHERE
        task_id IN %s
    """

    _SELECT_DUE_TASKS_QUERY = """
    SELECT 
        *
    FROM
        _taskshed_data
    WHERE
        paused = 0 AND run_at <= %s 
    """

    _SELECT_NEXT_WAKEUP_QUERY = """
    SELECT
        MIN(run_at) AS next_wakeup
    FROM
        _taskshed_data
    WHERE
        paused = 0
    LIMIT 1
    """

    _SELECT_GROUP_TASKS_QUERY = """
    SELECT
        *
    FROM
        _taskshed_data
    WHERE
        group_id = %s
    """

    _UPDATE_TASKS_RUN_AT_QUERY = """
    UPDATE _taskshed_data
    SET
        run_at = %s
    WHERE
        task_id = %s
    """

    _UPDATE_TASKS_PAUSED_STATUS_QUERY = """
    UPDATE _taskshed_data
    SET
        paused = %s
    WHERE
        task_id IN %s
    """

    _UPDATE_GROUP_PAUSE_QUERY = """
    UPDATE _taskshed_data
    SET
        paused = %s
    WHERE
        group_id = %s
    """

    # -------------------------------------------------------------------------------- private methods

    def __init__(self, config: MySQLConfig):
        self._config = config
        self._lock: asyncio.Lock | None = None
        self._pool: aiomysql.Pool | None = None

    @asynccontextmanager
    async def _get_cursor(self) -> AsyncGenerator[aiomysql.Cursor, None]:
        connection: aiomysql.Connection = await self._pool.acquire()
        cursor: aiomysql.Cursor = await connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()
            self._pool.release(connection)

    def _create_task(self, row: dict) -> Task:
        if row["interval"] is not None:
            interval = timedelta(seconds=row["interval"])
        else:
            interval = None

        return Task(
            task_id=row["task_id"],
            run_at=row["run_at"],
            paused=bool(row["paused"]),
            callback=row["callback_name"],
            kwargs=json.loads(row["kwargs"]),
            schedule_type=row["schedule_type"],
            interval=interval,
            group_id=row["group_id"],
        )

    # -------------------------------------------------------------------------------- public methods

    async def start(self):
        self._lock = asyncio.Lock()
        async with self._lock:
            if self._pool is None:
                self._pool = await aiomysql.create_pool(
                    **{
                        **self._config.__dict__,
                        "charset": "utf8mb4",
                        "use_unicode": True,
                        "autocommit": True,
                        "cursorclass": aiomysql.DictCursor,
                    }
                )

        # Create the table if it doesn't already exist
        async with self._get_cursor() as cursor:
            await cursor.execute(self._CREATE_TABLE_QUERY)

    async def shutdown(self):
        self._pool.close()
        await self._pool.wait_closed()

    async def add_tasks(
        self, tasks: Iterable[Task], *, replace_existing: bool = True
    ) -> None:
        query = (
            self._INSERT_TASKS_WITH_REPLACEMENT_QUERY
            if replace_existing
            else self._INSERT_TASKS_WITHOUT_REPLACEMENT_QUERY
        )

        async with self._get_cursor() as cursor:
            await cursor.executemany(
                query,
                (
                    (
                        task.task_id,
                        task.run_at,
                        task.paused,
                        task.callback,
                        json.dumps(task.kwargs),
                        task.schedule_type,
                        task.interval_seconds(),
                        task.group_id,
                    )
                    for task in tasks
                ),
            )

    async def fetch_due_tasks(self, dt: datetime) -> list[Task]:
        async with self._lock:
            async with self._get_cursor() as cursor:
                await cursor.execute(self._SELECT_DUE_TASKS_QUERY, (dt,))
                rows = await cursor.fetchall()

            return [self._create_task(row) for row in rows]

    async def fetch_next_wakeup(self) -> float | None:
        async with self._lock:
            async with self._get_cursor() as cursor:
                await cursor.execute(self._SELECT_NEXT_WAKEUP_QUERY)
                row = await cursor.fetchone()

            if row and row["next_wakeup"]:
                return row["next_wakeup"]

    async def fetch_tasks(self, task_ids: Iterable[str]) -> list[Task]:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._SELECT_TASKS_QUERY, (task_ids,))
            rows = await cursor.fetchall()
        return [self._create_task(row) for row in rows]

    async def fetch_group_tasks(self, group_id: str) -> list[Task]:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._SELECT_GROUP_TASKS_QUERY, (group_id,))
            rows = await cursor.fetchall()
        return [self._create_task(row) for row in rows]

    async def update_execution_times(self, tasks: Iterable[TaskExecutionTime]) -> None:
        async with self._get_cursor() as cursor:
            await cursor.executemany(
                self._UPDATE_TASKS_RUN_AT_QUERY,
                ((task.run_at, task.task_id) for task in tasks),
            )

    async def update_tasks_paused_status(
        self, task_ids: Iterable[str], paused: bool
    ) -> None:
        async with self._get_cursor() as cursor:
            await cursor.execute(
                self._UPDATE_TASKS_PAUSED_STATUS_QUERY, (paused, task_ids)
            )

    async def update_group_paused_status(self, group_id: str, paused: bool) -> None:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._UPDATE_GROUP_PAUSE_QUERY, (paused, group_id))

    async def remove_tasks(self, task_ids: Iterable[str]) -> None:
        async with self._lock:
            async with self._get_cursor() as cursor:
                await cursor.execute(self._DELETE_TASKS_QUERY, (task_ids,))

    async def remove_all_tasks(self) -> None:
        async with self._lock:
            async with self._get_cursor() as cursor:
                await cursor.execute(self._DELETE_ALL_TASKS_QUERY)

    async def remove_group_tasks(self, group_id: str) -> None:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._DELETE_GROUP_TASKS_QUERY, (group_id,))
