import json
from collections.abc import Collection, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from ssl import SSLContext
from typing import AsyncGenerator

import aiomysql

from taskshed.datastores.base_datastore import DataStore
from taskshed.models.task_models import Task, TaskExecutionTime, COMPACT_JSON_SEPARATORS


@dataclass(frozen=True, kw_only=True)
class MySQLConfig:
    """
    Configuration for connecting to a MySQL database.

    Attributes:
        host: The database server host address.
        port: The connection port number.
        user: The username for authentication.
        password: The password for authentication. Can be None.
        db: The name of the database to connect to.
        connect_timeout: The timeout in seconds for establishing a connection.
        ssl: SSL configuration for a secure connection. Can be a boolean to
            enable/disable or an `SSLContext` object for advanced settings.
        unix_socket: The path to a Unix socket file for the connection, if applicable.
        auth_plugin: The authentication plugin to use (e.g., 'mysql_native_password').
        server_public_key: The server's public key, used for certain
            authentication methods like 'caching_sha2_password'.
    """

    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str | None = None
    db: str
    connect_timeout: int | None = None
    ssl: bool | SSLContext | None = None
    unix_socket: str | None = None
    auth_plugin: str | None = None
    server_public_key: str | bytes | None = None


class MySQLDataStore(DataStore):
    # Columns that may be absent on tables created by older versions of
    # TaskShed. Maps each column name to the DDL fragment used to add it via
    # `ALTER TABLE ... ADD COLUMN`.
    _EXPECTED_COLUMNS = {
        "coalesce": "`coalesce` tinyint NOT NULL DEFAULT '1'",
        "paused_at": "`paused_at` bigint unsigned DEFAULT NULL",
    }

    # -------------------------------------------------------------------------------- queries

    _SELECT_EXISTING_COLUMNS_QUERY = """
    SELECT 
        `COLUMN_NAME` AS `column_name`
    FROM
        information_schema.COLUMNS
    WHERE
        `TABLE_SCHEMA` = DATABASE()
            AND `TABLE_NAME` = '_taskshed_data';
    """

    _CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS `_taskshed_data` (
        `task_id` varchar(63) NOT NULL,
        `run_at` bigint unsigned NOT NULL,
        `paused` tinyint NOT NULL DEFAULT '0',
        `callback` varchar(63) NOT NULL,
        `kwargs` MEDIUMTEXT NOT NULL,
        `run_type` enum('once','recurring') NOT NULL,
        `interval` float DEFAULT NULL,
        `group_id` varchar(63) DEFAULT NULL,
        `coalesce` tinyint NOT NULL DEFAULT '1',
        `paused_at` bigint unsigned DEFAULT NULL,
        PRIMARY KEY (`task_id`),
        UNIQUE KEY `task_id_UNIQUE` (`task_id`),
        KEY `idx_group_id` (`group_id`),
        KEY `idx_paused_run_at` (`paused`,`run_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """

    _DELETE_TASKS_QUERY = """
    DELETE FROM _taskshed_data
    WHERE
        `task_id` IN %s;
    """

    _DELETE_ALL_TASKS_QUERY = """
    DELETE FROM _taskshed_data
    WHERE
        `paused` IN (0 , 1);
    """

    _DELETE_GROUP_TASKS_QUERY = """
    DELETE FROM _taskshed_data
    WHERE
        `group_id` = %s;
    """

    _INSERT_TASKS_WITHOUT_REPLACEMENT_QUERY = """
    INSERT IGNORE INTO _taskshed_data (`task_id`, `run_at`, `paused`, `callback`, `kwargs`, `run_type`, `interval`, `group_id`, `coalesce`, `paused_at`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    _INSERT_TASKS_WITH_REPLACEMENT_QUERY = """
    INSERT INTO _taskshed_data (`task_id`, `run_at`, `paused`, `callback`, `kwargs`, `run_type`, `interval`, `group_id`, `coalesce`, `paused_at`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            `run_at` = new.run_at,
            `callback` = new.callback,
            `kwargs` = new.kwargs,
            `paused` = new.paused,
            `group_id` = new.group_id,
            `coalesce` = new.coalesce,
            `paused_at` = new.paused_at;
    """

    _SELECT_TASKS_QUERY = """
    SELECT
        `task_id`, `run_at`, `paused`, `callback`, `kwargs`, `run_type`, `interval`, `group_id`, `coalesce`, `paused_at`
    FROM
        _taskshed_data
    WHERE
        `task_id` IN %s;
    """

    _SELECT_DUE_TASKS_QUERY = """
    SELECT
        `task_id`, `run_at`, `paused`, `callback`, `kwargs`, `run_type`, `interval`, `group_id`, `coalesce`, `paused_at`
    FROM
        _taskshed_data
    WHERE
        `paused` = 0 AND `run_at` <= %s;
    """

    _SELECT_NEXT_WAKEUP_QUERY = """
    SELECT
        MIN(`run_at`) AS next_wakeup
    FROM
        _taskshed_data
    WHERE
        `paused` = 0
    LIMIT 1;
    """

    _SELECT_GROUP_TASKS_QUERY = """
    SELECT
        `task_id`, `run_at`, `paused`, `callback`, `kwargs`, `run_type`, `interval`, `group_id`, `coalesce`, `paused_at`
    FROM
        _taskshed_data
    WHERE
        `group_id` = %s;
    """

    _UPDATE_TASKS_RUN_AT_QUERY = """
    UPDATE _taskshed_data
    SET
        `run_at` = %s
    WHERE
        `task_id` = %s;
    """
    
    _PAUSE_TASKS_QUERY = """
    UPDATE _taskshed_data
    SET
        `paused` = 1,
        `paused_at` = COALESCE(`paused_at`, %s)
    WHERE
        `task_id` IN %s;
    """

    _RESUME_TASKS_QUERY = """
    UPDATE _taskshed_data
    SET
        `paused` = 0,
        `paused_at` = NULL
    WHERE
        `task_id` IN %s;
    """

    _PAUSE_GROUP_QUERY = """
    UPDATE _taskshed_data
    SET
        `paused` = 1,
        `paused_at` = COALESCE(`paused_at`, %s)
    WHERE
        `group_id` = %s;
    """

    _RESUME_GROUP_QUERY = """
    UPDATE _taskshed_data
    SET
        `paused` = 0,
        `paused_at` = NULL
    WHERE
        `group_id` = %s;
    """

    # -------------------------------------------------------------------------------- private methods

    def __init__(self, *, config: MySQLConfig | None = None, pool: aiomysql.Pool | None = None):
        if config is None and pool is None:
            raise ValueError("Must provide either a MySQLConfig or an aiomysql.Pool.")
        self._config = config
        self._pool = pool
        self._injected_pool = pool is not None
        self._started = False

    @asynccontextmanager
    async def _get_cursor(self) -> AsyncGenerator[aiomysql.Cursor, None]:
        connection: aiomysql.Connection = await self._pool.acquire()
        autocommit = connection.get_autocommit()

        # force a tuple cursor regardless of the pool's default cursor type
        cursor: aiomysql.Cursor = await connection.cursor(aiomysql.Cursor)
        try:
            yield cursor
        except BaseException:
            if not autocommit:
                await connection.rollback()
            raise
        else:
            if not autocommit:
                await connection.commit()
        finally:
            await cursor.close()
            self._pool.release(connection)

    def _create_task(self, row: tuple) -> Task:
        # Column order must match the SELECT statements above.
        (
            task_id,
            run_at,
            paused,
            callback,
            kwargs,
            run_type,
            interval,
            group_id,
            coalesce,
            paused_at,
        ) = row

        return Task(
            task_id=task_id,
            run_at=self._convert_timestamp(run_at),
            paused=bool(paused),
            callback=callback,
            kwargs=json.loads(kwargs),
            run_type=run_type,
            interval=timedelta(seconds=interval) if interval is not None else None,
            group_id=group_id,
            coalesce=bool(coalesce),
            paused_at=(
                self._convert_timestamp(paused_at) if paused_at is not None else None
            ),
        )

    def _convert_datetime(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1_000_000)

    def _convert_timestamp(self, timestamp: int) -> datetime:
        return datetime.fromtimestamp(timestamp / 1_000_000, tz=timezone.utc)

    async def _reconcile_columns(self, cursor: aiomysql.Cursor) -> None:
        """
        Adds any columns missing from a pre-existing table.
        """
        await cursor.execute(self._SELECT_EXISTING_COLUMNS_QUERY)
        rows = await cursor.fetchall()
        existing = {row[0] for row in rows}

        additions = [
            f"ADD COLUMN {ddl}"
            for column, ddl in self._EXPECTED_COLUMNS.items()
            if column not in existing
        ]
        if additions:
            await cursor.execute(
                f"ALTER TABLE `_taskshed_data` {', '.join(additions)};"
            )

    # -------------------------------------------------------------------------------- public methods

    async def start(self):
        if self._started:
            return

        if not self._injected_pool:
            self._pool = await aiomysql.create_pool(
                **{
                    **self._config.__dict__,
                    "charset": "utf8mb4",
                    "use_unicode": True,
                    "autocommit": True,
                }
            )

        # Create the table if it doesn't already exist, then bring an existing
        # table up to date with any newly introduced columns.
        async with self._get_cursor() as cursor:
            await cursor.execute(self._CREATE_TABLE_QUERY)
            await self._reconcile_columns(cursor)

        self._started = True

    async def shutdown(self):
        self._started = False

        if self._pool is None or self._injected_pool:
            # an injected pool should not be closed by the datastore
            return

        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None

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
                        self._convert_datetime(task.run_at),
                        task.paused,
                        task.callback,
                        json.dumps(task.kwargs, separators=COMPACT_JSON_SEPARATORS),
                        task.run_type,
                        task.interval_seconds(),
                        task.group_id,
                        int(task.coalesce),
                        self._convert_datetime(task.paused_at)
                        if task.paused_at is not None
                        else None,
                    )
                    for task in tasks
                ),
            )

    async def fetch_due_tasks(self, dt: datetime) -> list[Task]:
        async with self._get_cursor() as cursor:
            await cursor.execute(
                self._SELECT_DUE_TASKS_QUERY, (self._convert_datetime(dt),)
            )
            rows = await cursor.fetchall()

        return [self._create_task(row) for row in rows]

    async def fetch_next_wakeup(self) -> datetime | None:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._SELECT_NEXT_WAKEUP_QUERY)
            row = await cursor.fetchone()

        if row and row[0] is not None:
            return self._convert_timestamp(row[0])

    async def fetch_tasks(self, task_ids: Collection[str]) -> list[Task]:
        if not task_ids:
            return []

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
                (
                    (
                        self._convert_datetime(task.run_at),
                        task.task_id,
                    )
                    for task in tasks
                ),
            )

    async def update_tasks_paused_status(
        self, task_ids: Collection[str], paused: bool
    ) -> None:
        async with self._get_cursor() as cursor:
            if paused:
                now = self._convert_datetime(datetime.now(timezone.utc))
                await cursor.execute(self._PAUSE_TASKS_QUERY, (now, task_ids))
            else:
                await cursor.execute(self._RESUME_TASKS_QUERY, (task_ids,))

    async def update_group_paused_status(self, group_id: str, paused: bool) -> None:
        async with self._get_cursor() as cursor:
            if paused:
                now = self._convert_datetime(datetime.now(timezone.utc))
                await cursor.execute(self._PAUSE_GROUP_QUERY, (now, group_id))
            else:
                await cursor.execute(self._RESUME_GROUP_QUERY, (group_id,))

    async def remove_tasks(self, task_ids: Collection[str]) -> None:
        if not task_ids:
            return

        async with self._get_cursor() as cursor:
            await cursor.execute(self._DELETE_TASKS_QUERY, (task_ids,))

    async def remove_all_tasks(self) -> None:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._DELETE_ALL_TASKS_QUERY)

    async def remove_group_tasks(self, group_id: str) -> None:
        async with self._get_cursor() as cursor:
            await cursor.execute(self._DELETE_GROUP_TASKS_QUERY, (group_id,))
