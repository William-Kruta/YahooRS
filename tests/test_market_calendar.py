import datetime as dt
import unittest
from zoneinfo import ZoneInfo

from yahoors.periphery.stale import (
    get_stale_threshold,
    is_market_open,
    next_market_open,
)


_ET = ZoneInfo("America/New_York")


class MarketCalendarTests(unittest.TestCase):
    def test_weekend_threshold_keeps_friday_session_fresh(self):
        sunday = dt.datetime(2026, 8, 16, 12, 0, tzinfo=_ET)
        friday_close_cache = dt.datetime(2026, 8, 14, 15, 55, tzinfo=_ET)
        thursday_cache = dt.datetime(2026, 8, 13, 15, 55, tzinfo=_ET)
        threshold = get_stale_threshold("1m", now=sunday)

        self.assertLess(sunday - friday_close_cache, threshold)
        self.assertGreater(sunday - thursday_cache, threshold)

    def test_day_after_thanksgiving_uses_one_pm_close(self):
        before_close = dt.datetime(2026, 11, 27, 12, 59, tzinfo=_ET)
        after_close = dt.datetime(2026, 11, 27, 13, 1, tzinfo=_ET)

        self.assertTrue(is_market_open(before_close))
        self.assertFalse(is_market_open(after_close))

    def test_exceptional_full_day_closure_is_honored(self):
        closure = dt.datetime(2025, 1, 9, 12, 0, tzinfo=_ET)

        self.assertFalse(is_market_open(closure))
        self.assertEqual(
            next_market_open(closure),
            dt.datetime(2025, 1, 10, 9, 30, tzinfo=_ET),
        )

    def test_next_year_observed_new_year_holiday_is_honored(self):
        observed_holiday = dt.datetime(2021, 12, 31, 12, 0, tzinfo=_ET)

        self.assertFalse(is_market_open(observed_holiday))


if __name__ == "__main__":
    unittest.main()
