from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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


def test_paused_at_defaults_to_none_when_not_paused():
    task = _create_task()
    assert task.paused is False
    assert task.paused_at is None


def test_paused_task_auto_sets_paused_at():
    task = _create_task(paused=True)
    assert task.paused_at is not None
    assert task.paused_at.tzinfo is not None
    assert task.paused_at.utcoffset() == timedelta(0)


def test_explicit_paused_at_is_normalized_to_utc():
    aware = datetime(2025, 1, 1, 12, 0, tzinfo=ZoneInfo("America/New_York"))
    task = _create_task(paused=True, paused_at=aware)
    assert task.paused_at.utcoffset() == timedelta(0)
    assert task.paused_at == aware  # same instant, UTC-normalized
