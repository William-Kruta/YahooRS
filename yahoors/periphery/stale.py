import datetime as dt

# US market holidays (fixed + observed rules)
def _get_us_market_holidays(year: int) -> set[dt.date]:
    """Generate NYSE holidays for a given year."""
    holidays = set()

    # New Year's Day
    nyd = dt.date(year, 1, 1)
    holidays.add(_observe(nyd))

    # MLK Day - 3rd Monday in January
    holidays.add(_nth_weekday(year, 1, 0, 3))

    # Presidents' Day - 3rd Monday in February
    holidays.add(_nth_weekday(year, 2, 0, 3))

    # Good Friday
    holidays.add(_good_friday(year))

    # Memorial Day - last Monday in May
    holidays.add(_last_weekday(year, 5, 0))

    # Juneteenth
    holidays.add(_observe(dt.date(year, 6, 19)))

    # Independence Day
    holidays.add(_observe(dt.date(year, 7, 4)))

    # Labor Day - 1st Monday in September
    holidays.add(_nth_weekday(year, 9, 0, 1))

    # Thanksgiving - 4th Thursday in November
    holidays.add(_nth_weekday(year, 11, 3, 4))

    # Christmas
    holidays.add(_observe(dt.date(year, 12, 25)))

    return holidays


def _observe(d: dt.date) -> dt.date:
    """Shift Saturday holidays to Friday, Sunday to Monday."""
    if d.weekday() == 5:
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:
        return d + dt.timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    """Get the nth occurrence of a weekday in a month. weekday: 0=Mon, 3=Thu, etc."""
    first = dt.date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + dt.timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    """Get the last occurrence of a weekday in a month."""
    if month == 12:
        last_day = dt.date(year + 1, 1, 1) - dt.timedelta(days=1)
    else:
        last_day = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - dt.timedelta(days=offset)


def _good_friday(year: int) -> dt.date:
    """Compute Good Friday via anonymous Gregorian Easter algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    easter = dt.date(year, month, day + 1)
    return easter - dt.timedelta(days=2)


def is_market_open(now: dt.datetime | None = None) -> bool:
    """Check if US equity market is currently open."""
    now = now or dt.datetime.now(dt.timezone(dt.timedelta(hours=-4)))  # ET
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    et = now.astimezone(dt.timezone(dt.timedelta(hours=-4)))
    today = et.date()

    # Weekend
    if today.weekday() >= 5:
        return False

    # Holiday
    if today in _get_us_market_holidays(today.year):
        return False

    # Regular hours: 9:30 AM - 4:00 PM ET
    market_open = et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= et < market_close


def next_market_open(now: dt.datetime | None = None) -> dt.datetime:
    """Get the next market open timestamp."""
    et_tz = dt.timezone(dt.timedelta(hours=-4))
    now = now or dt.datetime.now(et_tz)
    et = now.astimezone(et_tz)

    # If market is currently open or before open today, check today
    candidate = et.replace(hour=9, minute=30, second=0, microsecond=0)

    if et < candidate:
        # Before open today — check if today is a trading day
        if candidate.date().weekday() < 5 and candidate.date() not in _get_us_market_holidays(candidate.date().year):
            return candidate

    # Otherwise scan forward day by day
    day = et.date() + dt.timedelta(days=1)
    for _ in range(10):  # max 10 days covers any holiday cluster
        if day.weekday() < 5 and day not in _get_us_market_holidays(day.year):
            return dt.datetime.combine(day, dt.time(9, 30), tzinfo=et_tz)
        day += dt.timedelta(days=1)

    # Fallback (should never hit)
    return dt.datetime.combine(day, dt.time(9, 30), tzinfo=et_tz)


def get_stale_threshold(interval: str) -> dt.timedelta:
    """Dynamic stale threshold based on interval and market state."""
    open_thresholds = {
        "1m":  dt.timedelta(minutes=10),
        "2m":  dt.timedelta(minutes=15),
        "5m":  dt.timedelta(minutes=30),
        "15m": dt.timedelta(minutes=45),
        "30m": dt.timedelta(hours=1),
        "60m": dt.timedelta(hours=2),
        "90m": dt.timedelta(hours=3),
        "1h":  dt.timedelta(hours=2),
        "1d":  dt.timedelta(hours=36),
        "5d":  dt.timedelta(days=7),
        "1wk": dt.timedelta(days=10),
        "1mo": dt.timedelta(days=35),
        "3mo": dt.timedelta(days=100),
    }

    base = open_thresholds.get(interval, dt.timedelta(hours=36))

    if is_market_open():
        return base

    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=-4)))
    time_until_open = next_market_open(now) - now

    is_intraday = interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h")

    if is_intraday:
        return time_until_open + base
    else:
        return max(base, time_until_open + dt.timedelta(hours=4))