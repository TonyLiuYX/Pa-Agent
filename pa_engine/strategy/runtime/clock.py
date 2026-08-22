"""Small injectable UTC clocks used by the G023 scheduler.

The runtime never sleeps.  Callers advance a :class:`FakeClock` and then ask
the scheduler to process due timers.  This makes deadline and stale-token
behaviour reproducible without threads or wall-clock sleeps.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


UTC = timezone.utc


def utc_instant(value: datetime | str | None, *, default: datetime | None = None) -> datetime:
    """Normalize one explicit instant to an aware UTC ``datetime``.

    Naive values are deliberately rejected: scheduler deadlines must state
    their timezone rather than inheriting a machine-local timezone.
    """

    if value is None:
        if default is None:
            raise ValueError("UTC instant is required")
        value = default
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("invalid UTC instant") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("UTC instant must be timezone-aware")
    return value.astimezone(UTC)


class Clock(Protocol):
    def now(self) -> datetime:
        ...


class SystemClock:
    """UTC wall clock adapter for integrations that do not need a fake clock."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class FakeClock:
    """Deterministic UTC clock.

    ``advance`` only changes the current instant.  Timer processing remains an
    explicit scheduler operation so duplicate and stale wakeups can be tested
    directly.
    """

    def __init__(self, start: datetime | str | None = None) -> None:
        self._now = utc_instant(start, default=datetime(2000, 1, 1, tzinfo=UTC))

    def now(self) -> datetime:
        return self._now

    @property
    def current(self) -> datetime:
        return self._now

    def set(self, value: datetime | str) -> datetime:
        self._now = utc_instant(value)
        return self._now

    def advance(self, seconds: int | float | timedelta) -> datetime:
        if isinstance(seconds, timedelta):
            delta = seconds
        else:
            delta = timedelta(seconds=seconds)
        if delta.total_seconds() < 0:
            raise ValueError("fake clock cannot move backwards")
        self._now += delta
        return self._now

    def copy(self) -> "FakeClock":
        return FakeClock(self._now)


FakeUTCClock = FakeClock
UTCClock = FakeClock


__all__ = ["UTC", "Clock", "FakeClock", "FakeUTCClock", "SystemClock", "UTCClock", "utc_instant"]
