import datetime as dt
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

# One-off full-session closures that are not described by the recurring holiday
# rules. Keep this explicit so an exceptional closure can be added without
# changing the calendar algorithm.
_SPECIAL_MARKET_CLOSURES = {
    dt.date(2001, 9, 11),
    dt.date(2001, 9, 12),
    dt.date(2001, 9, 13),
    dt.date(2001, 9, 14),
    dt.date(2004, 6, 11),
    dt.date(2007, 1, 2),
    dt.date(2012, 10, 29),
    dt.date(2012, 10, 30),
    dt.date(2018, 12, 5),
    dt.date(2025, 1, 9),
}


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
    weekday_adjustment = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_adjustment) // 451
    month, day = divmod(h + weekday_adjustment - 7 * m + 114, 31)
    easter = dt.date(year, month, day + 1)
    return easter - dt.timedelta(days=2)


def _is_trading_day(day: dt.date) -> bool:
    holidays = _get_us_market_holidays(day.year) | _get_us_market_holidays(day.year + 1)
    return day.weekday() < 5 and day not in holidays and day not in _SPECIAL_MARKET_CLOSURES


def _previous_trading_day(day: dt.date) -> dt.date:
    candidate = day - dt.timedelta(days=1)
    for _ in range(10):
        if _is_trading_day(candidate):
            return candidate
        candidate -= dt.timedelta(days=1)
    return candidate


def _get_early_closes(year: int) -> set[dt.date]:
    """Return the recurring 1:00 PM ET NYSE sessions for a year."""
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    early_closes = {thanksgiving + dt.timedelta(days=1)}

    independence_observed = _observe(dt.date(year, 7, 4))
    early_closes.add(_previous_trading_day(independence_observed))

    christmas_eve = dt.date(year, 12, 24)
    if _is_trading_day(christmas_eve):
        early_closes.add(christmas_eve)

    return {day for day in early_closes if _is_trading_day(day)}


def _market_close_time(day: dt.date) -> dt.time:
    if day in _get_early_closes(day.year):
        return dt.time(13, 0)
    return dt.time(16, 0)


def is_market_open(now: dt.datetime | None = None) -> bool:
    """Check if US equity market is currently open."""
    now = now or dt.datetime.now(_ET)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    et = now.astimezone(_ET)
    today = et.date()

    if not _is_trading_day(today):
        return False

    # Regular hours are 9:30 AM - 4:00 PM ET, with 1:00 PM closes on
    # recurring shortened sessions.
    market_open = et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = _market_close_time(today)
    market_close = et.replace(
        hour=close_time.hour,
        minute=close_time.minute,
        second=0,
        microsecond=0,
    )
    return market_open <= et < market_close


def next_market_open(now: dt.datetime | None = None) -> dt.datetime:
    """Get the next market open timestamp."""
    now = now or dt.datetime.now(_ET)
    et = now.astimezone(_ET)

    # If market is currently open or before open today, check today
    candidate = et.replace(hour=9, minute=30, second=0, microsecond=0)

    if et < candidate:
        # Before open today — check if today is a trading day
        if _is_trading_day(candidate.date()):
            return candidate

    # Otherwise scan forward day by day
    day = et.date() + dt.timedelta(days=1)
    for _ in range(10):  # max 10 days covers any holiday cluster
        if _is_trading_day(day):
            return dt.datetime.combine(day, dt.time(9, 30), tzinfo=_ET)
        day += dt.timedelta(days=1)

    # Fallback (should never hit)
    return dt.datetime.combine(day, dt.time(9, 30), tzinfo=_ET)


def _previous_market_close(now: dt.datetime) -> dt.datetime:
    et = now.astimezone(_ET)
    day = et.date()
    if _is_trading_day(day):
        close = dt.datetime.combine(day, _market_close_time(day), tzinfo=_ET)
        if et >= close:
            return close
    previous_day = _previous_trading_day(day)
    return dt.datetime.combine(previous_day, _market_close_time(previous_day), tzinfo=_ET)


def get_stale_threshold(interval: str, now: dt.datetime | None = None) -> dt.timedelta:
    """Dynamic stale threshold based on interval and market state."""
    open_thresholds = {
        "1m": dt.timedelta(minutes=10),
        "2m": dt.timedelta(minutes=15),
        "5m": dt.timedelta(minutes=30),
        "15m": dt.timedelta(minutes=45),
        "30m": dt.timedelta(hours=1),
        "60m": dt.timedelta(hours=2),
        "90m": dt.timedelta(hours=3),
        "1h": dt.timedelta(hours=2),
        "1d": dt.timedelta(hours=36),
        "5d": dt.timedelta(days=7),
        "1wk": dt.timedelta(days=10),
        "1mo": dt.timedelta(days=35),
        "3mo": dt.timedelta(days=100),
    }

    base = open_thresholds.get(interval, dt.timedelta(hours=36))

    now = now or dt.datetime.now(_ET)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(_ET)

    if is_market_open(now):
        return base

    time_until_open = next_market_open(now) - now
    time_since_close = now - _previous_market_close(now)

    is_intraday = interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h")

    if is_intraday:
        return time_since_close + time_until_open + base
    else:
        return max(
            base,
            time_since_close + time_until_open + dt.timedelta(hours=4),
        )
