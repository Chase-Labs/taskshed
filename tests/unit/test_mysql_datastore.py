import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from dotenv import load_dotenv

from taskshed.datastores.mysql_datastore import MySQLConfig, MySQLDataStore
from taskshed.models.task_models import Task

load_dotenv()


@pytest_asyncio.fixture
async def datastore():
    ds = MySQLDataStore(
        config=MySQLConfig(
            host=os.environ.get("MYSQL_HOST"),
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            db=os.environ.get("MYSQL_DB"),
        ),
    )
    try:
        await ds.start()
    except Exception:
        pytest.skip("MySQL is not available for this environment")

    await ds.remove_all_tasks()
    try:
        yield ds
    finally:
        await ds.remove_all_tasks()
        await ds.shutdown()


# -------------------------------------------------------------------------------- tests


@pytest.mark.asyncio
async def test_add_without_replacing_inserts_new_and_keeps_existing(
    datastore: MySQLDataStore
):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await datastore.add_tasks(
        (Task(task_id="existing", callback="original", run_at=run_at),)
    )

    # The batch leads with a duplicate followed by new tasks. A
    # single conflict must not abort the rest of the batch.
    batch = [
        Task(task_id="existing", callback="changed", run_at=run_at),
        Task(task_id="new-1", callback="cb", run_at=run_at),
        Task(task_id="new-2", callback="cb", run_at=run_at),
    ]
    await datastore.add_tasks(batch, replace_existing=False)
    task_ids = {"existing", "new-1", "new-2"}

    fetched = {
        task.task_id: task
        for task in await datastore.fetch_tasks(task_ids)
    }

    # The new tasks were inserted despite the leading duplicate...
    assert set(fetched) == task_ids

    # ...and the pre-existing task was left untouched (NX semantics).
    assert fetched["existing"].callback == "original"


@pytest.mark.asyncio
async def test_fetch_tasks_with_empty_collection(datastore: MySQLDataStore):
    # An empty collection must not render `IN ()` (a MySQL syntax error).
    assert await datastore.fetch_tasks([]) == []


@pytest.mark.asyncio
async def test_remove_tasks_with_empty_collection(datastore: MySQLDataStore):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await datastore.add_tasks((Task(task_id="keep", callback="cb", run_at=run_at),))

    # A no-op removal must not raise and must leave existing tasks intact.
    await datastore.remove_tasks([])

    fetched = await datastore.fetch_tasks(["keep"])
    assert len(fetched) == 1
