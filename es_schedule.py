"""Conservative watchdog eligibility, NOT an authoritative exchange calendar.

Sources and annual review/override instructions: docs/WATCHDOG.md.
"""

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REVIEWED_YEAR = 2026


def calendar_expired(now: datetime) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        return False
    try:
        return now.astimezone(ZoneInfo("America/Chicago")).year > REVIEWED_YEAR
    except ZoneInfoNotFoundError:
        return False


# CME's published 2026 holiday windows, extended by one civil day each side
# to suppress uncertain early closes and evening sessions as well.
HOLIDAY_WINDOWS_2026 = (
    ("2025-12-31", "2026-01-02"),
    ("2026-01-18", "2026-01-20"),
    ("2026-02-15", "2026-02-17"),
    ("2026-04-02", "2026-04-04"),
    ("2026-05-24", "2026-05-26"),
    ("2026-06-18", "2026-06-19"),
    ("2026-07-03", "2026-07-05"),
    ("2026-09-06", "2026-09-08"),
    ("2026-11-26", "2026-11-28"),
    ("2026-12-24", "2026-12-26"),
    ("2026-12-31", "2027-01-01"),
)
UNCERTAIN_DATES = frozenset(
    date.fromisoformat(start) + timedelta(days=offset)
    for start, end in HOLIDAY_WINDOWS_2026
    for offset in range(-1, (date.fromisoformat(end) - date.fromisoformat(start)).days + 2)
)


def es_session_start(now: datetime, excluded: frozenset[date] = frozenset()) -> datetime | None:
    """Return current regular session start, or None when not safe to recover."""
    if now.tzinfo is None or now.utcoffset() is None:
        return None
    try:
        local = now.astimezone(ZoneInfo("America/Chicago"))
    except ZoneInfoNotFoundError:
        return None
    day = local.date()
    if day.year != REVIEWED_YEAR:  # Fail closed until the next year's calendar is reviewed.
        return None
    if local.hour == 16:
        return None
    start_day = day if local.hour >= 17 else day - timedelta(days=1)
    trade_day = start_day + timedelta(days=1)
    if trade_day.weekday() >= 5:
        return None
    if any(d in UNCERTAIN_DATES or d in excluded for d in (start_day, trade_day)):
        return None
    return datetime.combine(start_day, time(17), tzinfo=local.tzinfo)
