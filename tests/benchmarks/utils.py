import os
from os.path import abspath, dirname, join

from apscheduler.executors.asyncio import AsyncIOExecutor as APAsyncIOExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler as APAsyncIOScheduler
from redis import Redis
from rq import Queue as RQQueue
from rq_scheduler import Scheduler as RQScheduler

from taskshed.datastores.mysql_datastore import MySQLConfig, MySQLDataStore
from taskshed.datastores.redis_datastore import RedisConfig, RedisDataStore
from taskshed.schedulers.async_scheduler import AsyncScheduler
from taskshed.workers.event_driven_worker import EventDrivenWorker


def _load_env():
    from dotenv import load_dotenv

    dotenv_path = join(dirname(dirname(abspath(__file__))), ".env")
    load_dotenv(dotenv_path)


async def build_mysql_taskshed(callback_map: dict) -> AsyncScheduler:
    _load_env()
    datastore = MySQLDataStore(
        config=MySQLConfig(
            host=os.environ.get("MYSQL_HOST"),
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            db=os.environ.get("MYSQL_DB"),
        ),
    )

    worker = EventDrivenWorker(callback_map=callback_map, datastore=datastore)
    scheduler = AsyncScheduler(datastore=datastore, worker=worker)

    await scheduler._datastore.start()
    await scheduler._datastore.remove_all_tasks()
    await scheduler._worker.start()
    return scheduler


async def build_redis_taskshed(callback_map: dict) -> AsyncScheduler:
    datastore = RedisDataStore(callback_map=callback_map, config=RedisConfig())
    worker = EventDrivenWorker(callback_map=callback_map, datastore=datastore)
    scheduler = AsyncScheduler(datastore=datastore, worker=worker)

    await scheduler._datastore.start()
    await scheduler._datastore.remove_all_tasks()
    await scheduler._worker.start()
    return scheduler


def build_apscheduler() -> APAsyncIOScheduler:
    jobstores = {
        "default": SQLAlchemyJobStore(
            url="mysql+pymysql://root:-Ft7dnc168117tR2SI9ar00aV5SYZ-TWi_qMtvZLjzk@localhost/scheduling"
        )
    }
    executors = {"default": APAsyncIOExecutor()}
    job_defaults = {
        "misfire_grace_time": None,
        "replace_existing": True,
        "max_instances": 50_000,
    }
    scheduler = APAsyncIOScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )

    scheduler.remove_all_jobs()
    scheduler.start()

    return scheduler


def build_rqscheduler() -> RQScheduler:
    redis_conn = Redis()
    queue = RQQueue("default_queue", connection=redis_conn)
    return RQScheduler(queue=queue, connection=redis_conn, interval=1)
