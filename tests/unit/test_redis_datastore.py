from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from taskshed.datastores.redis_datastore import RedisConfig, RedisDataStore
from taskshed.models.task_models import Task


@pytest_asyncio.fixture
async def store():
    ds = RedisDataStore(config=RedisConfig())
    await ds.start()
    try:
        await ds._client.ping()
    except Exception:
        await ds.shutdown()
        pytest.skip("Redis is not available for this environment")

    await ds.remove_all_tasks()
    try:
        yield ds
    finally:
        await ds.remove_all_tasks()
        await ds.shutdown()


# -------------------------------------------------------------------------------- tests


@pytest.mark.asyncio
async def test_fetch_next_wakeup_is_utc_aware(store: RedisDataStore):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks((Task(task_id="t", callback="cb", run_at=run_at),))

    wakeup = await store.fetch_next_wakeup()

    # Returned datetime must be timezone-aware UTC, not naive local time, so it
    # does not depend on the host's timezone.
    assert wakeup is not None
    assert wakeup.tzinfo is not None
    assert wakeup.utcoffset() == timedelta(0)
    assert wakeup == run_at


@pytest.mark.asyncio
async def test_run_at_round_trips_as_utc(store: RedisDataStore):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks((Task(task_id="t", callback="cb", run_at=run_at),))

    fetched = await store.fetch_tasks(["t"])
    assert fetched[0].run_at.tzinfo is not None
    assert fetched[0].run_at == run_at


@pytest.mark.asyncio
async def test_resume_skips_missing_tasks(store: RedisDataStore):
    # Resuming a batch that includes an id with no stored hash (e.g. removed
    # concurrently) must not raise, and must still resume the real task.
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks((Task(task_id="real", callback="cb", run_at=run_at),))
    await store.update_tasks_paused_status(["real"], paused=True)

    await store.update_tasks_paused_status(["real", "ghost"], paused=False)

    # The real task is resumed (back on the queue at its run_at, unpaused)...
    fetched = await store.fetch_tasks(["real"])
    assert len(fetched) == 1
    assert fetched[0].paused is False
    assert await store.fetch_next_wakeup() == run_at
    # ...and the non-existent id created no phantom queue entry.
    assert await store.fetch_tasks(["ghost"]) == []


@pytest.mark.asyncio
async def test_coalesce_round_trip(store: RedisDataStore):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks(
        (
            Task(
                task_id="t",
                callback="cb",
                run_at=run_at,
                run_type="recurring",
                interval=timedelta(seconds=10),
                coalesce=False,
            ),
        )
    )

    fetched = await store.fetch_tasks(["t"])
    assert len(fetched) == 1
    assert fetched[0].coalesce is False


@pytest.mark.asyncio
async def test_add_without_replacing_persists_new_task(store: RedisDataStore):
    # Regression: the no-replace path runs a Lua script into the pipeline; if
    # that command is not awaited it is silently dropped, leaving a queue entry
    # with no task hash.
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks(
        (Task(task_id="new", callback="cb", run_at=run_at),),
        replace_existing=False,
    )

    fetched = await store.fetch_tasks(["new"])
    assert len(fetched) == 1
    assert fetched[0].callback == "cb"


@pytest.mark.asyncio
async def test_add_without_replacing_does_not_overwrite(store: RedisDataStore):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks((Task(task_id="dup", callback="original", run_at=run_at),))

    await store.add_tasks(
        (Task(task_id="dup", callback="changed", run_at=run_at),),
        replace_existing=False,
    )

    fetched = await store.fetch_tasks(["dup"])
    assert len(fetched) == 1
    assert fetched[0].callback == "original"


@pytest.mark.asyncio
async def test_legacy_hash_defaults_to_true(store: RedisDataStore):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await store.add_tasks((Task(task_id="t", callback="cb", run_at=run_at),))

    # Mimic a hash written before the `coalesce` field existed.
    await store._client.hdel(store._get_task_index("t"), "coalesce")

    fetched = await store.fetch_tasks(["t"])
    assert len(fetched) == 1
    assert fetched[0].coalesce is True
