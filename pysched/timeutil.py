"""UTC time helpers. All timestamps stored in SQLite are ISO-8601 UTC strings."""

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_now(seconds_only: bool = True) -> str:
    """Current UTC time as ISO-8601. Seconds precision for scheduled points,
    microsecond precision for created_at / manual triggers."""
    value = utcnow()
    if seconds_only:
        value = value.replace(microsecond=0)
    return value.isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
