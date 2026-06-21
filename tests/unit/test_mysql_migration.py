import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from dotenv import load_dotenv

from taskshed.datastores.mysql_datastore import MySQLConfig, MySQLDataStore
from taskshed.models.task_models import Task

load_dotenv()

# A pre-`coalesce` schema, mirroring an older TaskShed release.
_OLD_CREATE_TABLE_QUERY = """
CREATE TABLE `_taskshed_data` (
    `task_id` varchar(63) NOT NULL,
    `run_at` bigint unsigned NOT NULL,
    `paused` tinyint NOT NULL DEFAULT '0',
    `callback` varchar(63) NOT NULL,
    `kwargs` MEDIUMTEXT NOT NULL,
    `run_type` enum('once','recurring') NOT NULL,
    `interval` float DEFAULT NULL,
    `group_id` varchar(63) DEFAULT NULL,
    PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

_OLD_INSERT_QUERY = """
INSERT INTO _taskshed_data (`task_id`, `run_at`, `paused`, `callback`, `kwargs`, `run_type`, `interval`, `group_id`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""


@pytest_asyncio.fixture
async def datastore():
    if not os.environ.get("MYSQL_DB"):
        pytest.skip("MySQL is not configured for this environment")

    store = MySQLDataStore(
        config=MySQLConfig(
            host=os.environ.get("MYSQL_HOST"),
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            db=os.environ.get("MYSQL_DB"),
        ),
    )
    await store.start()
    try:
        yield store
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_startup_adds_missing_coalesce_column(datastore: MySQLDataStore):
    legacy_run_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    # Replace the current table with the legacy schema and seed a legacy row.
    async with datastore._get_cursor() as cursor:
        await cursor.execute("DROP TABLE `_taskshed_data`;")
        await cursor.execute(_OLD_CREATE_TABLE_QUERY)
        await cursor.execute(
            _OLD_INSERT_QUERY,
            (
                "legacy",
                datastore._convert_datetime(legacy_run_at),
                0,
                "cb",
                "{}",
                "recurring",
                10.0,
                None,
            ),
        )

    # Re-run the startup schema step; it must add the missing column.
    async with datastore._get_cursor() as cursor:
        await cursor.execute(datastore._CREATE_TABLE_QUERY)
        await datastore._reconcile_columns(cursor)

        # Reconciliation is idempotent — a second pass is a no-op.
        await datastore._reconcile_columns(cursor)

    # The legacy row backfills to the column default (coalesce enabled).
    legacy = (await datastore.fetch_tasks(["legacy"]))[0]
    assert legacy.coalesce is True

    # New rows persist an explicit coalesce value across the round trip.
    await datastore.add_tasks(
        (
            Task(
                task_id="fresh",
                callback="cb",
                run_at=legacy_run_at,
                run_type="recurring",
                interval=timedelta(seconds=10),
                coalesce=False,
            ),
        )
    )
    fresh = (await datastore.fetch_tasks(["fresh"]))[0]
    assert fresh.coalesce is False

    await datastore.remove_all_tasks()
