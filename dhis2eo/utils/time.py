import re
from datetime import datetime, date, timedelta
from typing import Union, Iterable

YEAR = "YEAR"
MONTH = "MONTH"
WEEK = "WEEK"
DAY = "DAY"


def ensure_date(d: Union[str, date, datetime]) -> date:
    """
    Normalize input into a `date` object.
    Accepts:
    - YYYY-MM-DD strings
    - datetime.datetime
    - datetime.date
    This keeps the public API flexible while ensuring internal consistency.
    """
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def detect_period_type(s):
    # TODO: more robust parsing of period types and maybe even using external library
    s = str(s).strip()
    if re.fullmatch(r"\d{4}$", s):
        return YEAR
    elif re.fullmatch(r"\d{6}$", s) or re.fullmatch(r"\d{4}-\d{2}$", s):
        return MONTH
    elif re.fullmatch(r"\d{4}-W\d{2}$", s):
        return WEEK
    elif re.fullmatch(r"\d{8}$", s) or re.fullmatch(r"\d{4}-\d{2}-\d{2}$", s):
        return DAY
    else:
        return None


def dhis2_period(year=None, month=None, day=None, week=None):
    # DAILY
    if year and month and day:
        return f"{year:04d}{month:02d}{day:02d}"

    # WEEKLY
    if year and week:
        return f"{year:04d}W{week:02d}"

    # MONTHLY
    if year and month:
        return f"{year:04d}{month:02d}"

    # YEARLY
    if year:
        return f"{year:04d}"

    raise ValueError("Not enough information to form a DHIS2 period code.")


def iter_days(start: date, end: date) -> Iterable[date]:
    """
    Yield all dates from `start` to `end`, inclusive.
    """
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def iter_months(start_year, start_month, end_year, end_month):
    """
    Yield all year-month tuples from `start` to `end`, inclusive.
    """
    # TODO: switch to start and end params instead
    # ... 
    for year in range(start_year, end_year + 1):
        for month in range(1, 12 + 1):
            # skip months before or after our defined time range
            if (year, month) < (start_year, start_month):
                continue
            if (year, month) > (end_year, end_month):
                continue

            # yield iter
            yield year, month


def months_ago(d: date, n: int = 1) -> date:
    """
    Return a date representing the first day of the month `n` months before the given date.

    Handles year boundaries automatically.

    Args:
        d: Reference date.
        n: Number of months to go back (default 1).

    Returns:
        A `date` object corresponding to the first day of the target month.
    """
    year, month = d.year, d.month - n
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)

