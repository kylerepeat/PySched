"""Pure unit tests for the cron parser."""

from datetime import datetime, timezone

import pytest

from pysched.cron import CronError, CronSchedule


def dt(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def test_simple_every_five_minutes():
    schedule = CronSchedule("*/5 * * * *")
    assert schedule.matches(dt("2026-11-24T10:00:00+00:00"))
    assert schedule.matches(dt("2026-11-24T10:05:00+00:00"))
    assert not schedule.matches(dt("2026-11-24T10:03:00+00:00"))


def test_list_range_and_step_fields():
    schedule = CronSchedule("0,30 9-17/2 1-10 1,6 *")
    assert schedule.matches(dt("2026-01-01T09:00:00+00:00"))
    assert schedule.matches(dt("2026-06-10T17:30:00+00:00"))
    assert not schedule.matches(dt("2026-06-10T18:30:00+00:00"))  # hour out
    assert not schedule.matches(dt("2026-06-11T09:00:00+00:00"))  # dom out


def test_day_of_week_sunday_is_zero():
    schedule = CronSchedule("0 0 * * 0")  # every Sunday midnight
    # 2026-11-22 is a Sunday
    assert schedule.matches(dt("2026-11-22T00:00:00+00:00"))
    assert not schedule.matches(dt("2026-11-23T00:00:00+00:00"))


def test_dom_and_dow_or_semantics():
    # dom restricted to 13th OR Fridays
    schedule = CronSchedule("0 0 13 * 5")
    friday = dt("2026-11-27T00:00:00+00:00")  # Friday the 27th
    thirteenth = dt("2026-11-13T00:00:00+00:00")  # Friday the 13th anyway
    monday = dt("2026-11-16T00:00:00+00:00")
    assert schedule.matches(friday)
    assert schedule.matches(thirteenth)
    assert not schedule.matches(monday)


def test_next_after_advances_minute_by_minute():
    schedule = CronSchedule("*/15 * * * *")
    nxt = schedule.next_after(dt("2026-11-24T10:07:00+00:00"))
    assert nxt == dt("2026-11-24T10:15:00+00:00")


def test_next_after_rolls_over_hour_and_day():
    schedule = CronSchedule("0 0 * * *")  # daily at midnight
    nxt = schedule.next_after(dt("2026-11-24T10:07:00+00:00"))
    assert nxt == dt("2026-11-25T00:00:00+00:00")


@pytest.mark.parametrize("bad", ["* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *"])
def test_invalid_expressions_raise(bad):
    with pytest.raises(CronError):
        CronSchedule(bad)
