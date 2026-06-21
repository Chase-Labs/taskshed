import os
from datetime import datetime, timedelta, timezone

import aiomysql
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from taskshed.datastores.mysql_datastore import MySQLDataStore
from taskshed.models.task_models import Task

load_dotenv()


def _conn_kwargs() -> dict:
    return dict(
        host=os.environ.get("MYSQL_HOST"),
        user=os.environ.get("MYSQL_USER"),
        password=os.environ.get("MYSQL_PASSWORD"),
        db=os.environ.get("MYSQL_DB"),
    )


@pytest_asyncio.fixture
async def external_pool():
    try:
        # autocommit=False + the default tuple cursor is the awkward case the
        # datastore must cope with when reusing a caller-owned pool.
        pool = await aiomysql.create_pool(autocommit=False, **_conn_kwargs())
    except Exception:
        pytest.skip("MySQL is not available for this environment")

    try:
        yield pool
    finally:
        pool.close()
        await pool.wait_closed()


# -------------------------------------------------------------------------------- tests


def test_requires_config_or_pool():
    with pytest.raises(ValueError):
        MySQLDataStore()


@pytest.mark.asyncio
async def test_injected_pool_lifecycle(external_pool):
    ds = MySQLDataStore(pool=external_pool)

    # start() must not create a pool (config is None) and must still create the
    # table and initialise the lock.
    await ds.start()
    await ds.remove_all_tasks()  # would raise if self._lock were never created

    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await ds.add_tasks(
        (
            Task(
                task_id="injected",
                callback="cb",
                run_at=run_at,
                run_type="recurring",
                interval=timedelta(seconds=10),
                coalesce=False,
            ),
        )
    )

    # The value round-trips through the tuple-cursor deserialisation.
    fetched = await ds.fetch_tasks(["injected"])
    assert len(fetched) == 1
    assert fetched[0].coalesce is False
    assert (await ds.fetch_next_wakeup()) is not None

    # Independently confirm the row is visible to a separate session: this only
    # holds if the datastore committed the write itself (the pool does not
    # autocommit), rather than relying on read-your-own-writes within one txn.
    verify = await aiomysql.connect(autocommit=True, **_conn_kwargs())
    try:
        async with verify.cursor() as cur:
            await cur.execute(
                "SELECT `coalesce` FROM _taskshed_data WHERE `task_id` = %s",
                ("injected",),
            )
            row = await cur.fetchone()
    finally:
        verify.close()
    assert row is not None and row[0] == 0

    await ds.remove_all_tasks()
    await ds.shutdown()

    # An injected pool is owned by the caller: shutdown must leave it open...
    assert not external_pool.closed
    # ...and fully usable.
    async with external_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            assert await cur.fetchone() == (1,)
