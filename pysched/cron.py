"""Minimal 5-field cron parser: minute hour day-of-month month day-of-week.

Supported per field:
  *        every value
  5        exact value
  1,3,5    value list
  1-5      range
  */5      step over the whole field
  0-30/5   range with step

Ranges: minute 0-59, hour 0-23, dom 1-31, month 1-12, dow 0-6 (0 = Sunday).
Calculations are done in UTC.
"""

from datetime import datetime, timedelta

_FIELD_RANGES = (
    (0, 59),   # minute
    (0, 23),   # hour
    (1, 31),   # day of month
    (1, 12),   # month
    (0, 6),    # day of week (0=Sunday)
)


class CronError(ValueError):
    pass


def _parse_field(expr: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for raw_part in expr.split(","):
        part = raw_part.strip()
        if not part:
            raise CronError(f"empty cron field part in {expr!r}")
        step = 1
        if "/" in part:
            range_part, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) <= 0:
                raise CronError(f"invalid step {part!r}")
            step = int(step_text)
        else:
            range_part = part

        if range_part == "*":
            start, end = lo, hi
        elif "-" in range_part:
            start_text, end_text = range_part.split("-", 1)
            if not (start_text.isdigit() and end_text.isdigit()):
                raise CronError(f"invalid range {part!r}")
            start, end = int(start_text), int(end_text)
        else:
            if not range_part.isdigit():
                raise CronError(f"invalid cron value {part!r}")
            start = end = int(range_part)
            if "/" in part:  # e.g. "5/10" -> 5,15,25,... up to hi
                end = hi

        if not (lo <= start <= end <= hi):
            raise CronError(f"cron value out of range [{lo},{hi}]: {part!r}")
        values.update(range(start, end + 1, step))

    if not values:
        raise CronError(f"cron field {expr!r} matches nothing")
    return values


class CronSchedule:
    def __init__(self, expression: str):
        fields = expression.split()
        if len(fields) != 5:
            raise CronError("cron expression must have exactly 5 fields")
        parsed = [
            _parse_field(field, lo, hi)
            for field, (lo, hi) in zip(fields, _FIELD_RANGES)
        ]
        self.minutes, self.hours, self.doms, self.months, self.dows = parsed
        self.expression = expression

    def matches(self, moment: datetime) -> bool:
        # Standard cron semantics: if both dom and dow are restricted, a match
        # on either one is enough.
        dom_restricted = not self._is_full(self.doms, 1, 31)
        dow_restricted = not self._is_full(self.dows, 0, 6)
        dom_match = moment.day in self.doms
        dow_match = (moment.isoweekday() % 7) in self.dows  # Mon=1..Sun=0
        if dom_restricted and dow_restricted:
            day_match = dom_match or dow_match
        else:
            day_match = dom_match and dow_match
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.month in self.months
            and day_match
        )

    @staticmethod
    def _is_full(values: set[int], lo: int, hi: int) -> bool:
        return values == set(range(lo, hi + 1))

    def next_after(self, after: datetime) -> datetime:
        """Return the first whole-second (minute boundary) match strictly after `after`."""
        candidate = (after.replace(second=0, microsecond=0) + timedelta(minutes=1))
        for _ in range(366 * 24 * 60):  # at most one year of minutes
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise CronError(f"no cron fire time within one year for {self.expression!r}")
