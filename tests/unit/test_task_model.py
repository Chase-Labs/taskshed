from datetime import datetime, timedelta, timezone

import pytest

from taskshed.models.task_models import Task


def _create_task(**overrides) -> Task:
    kwargs = {
        "callback": "cb",
        "run_at": datetime.now(timezone.utc),
        "run_type": "recurring",
        "interval": timedelta(seconds=10),
        **overrides,
    }
    return Task(**kwargs)


def test_recurring_requires_interval():
    with pytest.raises(ValueError):
        _create_task(interval=None)


def test_recurring_rejects_zero_interval():
    with pytest.raises(ValueError):
        _create_task(interval=timedelta(0))


def test_recurring_rejects_negative_interval():
    with pytest.raises(ValueError):
        _create_task(interval=timedelta(seconds=-5))


def test_recurring_accepts_positive_interval():
    task = _create_task(interval=timedelta(seconds=1))
    assert task.interval == timedelta(seconds=1)


def test_once_task_ignores_interval():
    task = _create_task(run_type="once", interval=None)
    assert task.run_type == "once"
