import datetime as dt
import os
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

from yahoors.modules.dividends import Dividends
from yahoors.modules.earnings import Earnings


class CacheRefreshTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_stale_earnings_dates_are_refreshed_and_upserted(self):
        earnings = Earnings(self.db_path)
        earnings_date = dt.datetime(2026, 8, 15, tzinfo=dt.timezone.utc)
        old_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
        new_time = dt.datetime.now(dt.timezone.utc)
        old = pl.DataFrame(
            {
                "earnings_date": [earnings_date],
                "ticker": ["AAPL"],
                "eps_estimate": [1.0],
                "reported_eps": [None],
                "surprise_pct": [None],
                "collected_at": [old_time],
            }
        )
        fresh = old.with_columns(
            pl.lit(1.2).alias("reported_eps"),
            pl.lit(20.0).alias("surprise_pct"),
            pl.lit(new_time).alias("collected_at"),
        )
        earnings._insert(old, "dates")

        with patch.object(
            earnings,
            "_download_earnings",
            return_value={"dates": fresh},
        ) as download:
            result = earnings.get_earnings_dates(["AAPL"])

        download.assert_called_once_with(["AAPL"])
        self.assertEqual(result[0, "reported_eps"], 1.2)
        self.assertEqual(result[0, "surprise_pct"], 20.0)
        earnings.conn.close()

    def test_non_dividend_ticker_uses_refresh_metadata_as_negative_cache(self):
        dividends = Dividends(self.db_path, debug=False)
        old_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=60)
        dividends.conn.execute(
            "INSERT INTO dividend_info VALUES (?, ?, ?, ?)",
            ["SPYTEST", 0, False, old_time],
        )

        with patch.object(
            dividends,
            "_download_dividends",
            return_value=pl.DataFrame(),
        ) as download:
            first = dividends.get_dividends(["SPYTEST"])
            second = dividends.get_dividends(["SPYTEST"])

        self.assertTrue(first.is_empty())
        self.assertTrue(second.is_empty())
        download.assert_called_once_with(["SPYTEST"])
        dividends.candles.conn.close()
        dividends.conn.close()


if __name__ == "__main__":
    unittest.main()
