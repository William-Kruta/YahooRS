import datetime as dt
import os
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

from yahoors.modules.candles import Candles


def _candle_frame(close: float, collected_at: dt.datetime) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [dt.datetime(2026, 8, 10)],
            "ticker": ["AAPL"],
            "interval": ["1d"],
            "open": [100.0],
            "high": [110.0],
            "low": [90.0],
            "close": [close],
            "volume": [1_000.0],
            "collected_at": [collected_at],
        }
    )


class CandlesRefreshTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.candles = Candles(self.db_path, debug=False)

    def tearDown(self):
        self.candles.conn.close()
        self.tmp.cleanup()

    def test_force_update_downloads_and_replaces_cached_ohlcv(self):
        old_time = dt.datetime(2026, 8, 10, tzinfo=dt.timezone.utc)
        new_time = old_time + dt.timedelta(hours=1)
        self.candles._insert_candles(_candle_frame(100.0, old_time))

        with patch.object(
            self.candles,
            "_download_candles",
            return_value=_candle_frame(105.0, new_time),
        ) as download:
            result = self.candles.get_candles(
                ["AAPL"],
                interval="1d",
                period="1y",
                force_update=True,
            )

        download.assert_called_once_with(["AAPL"], "1d", "1y", start=None, end=None)
        self.assertEqual(result["close"].to_list(), [105.0])
        self.assertEqual(result["collected_at"].to_list(), [new_time])

    def test_max_period_backfills_an_existing_short_cache_only_once(self):
        now = dt.datetime.now(dt.timezone.utc)
        self.candles._insert_candles(_candle_frame(100.0, now))

        with patch.object(
            self.candles,
            "_download_candles",
            return_value=_candle_frame(105.0, now),
        ) as download:
            first = self.candles.get_candles(["AAPL"], period="max")
            second = self.candles.get_candles(["AAPL"], period="max")

        download.assert_called_once_with(["AAPL"], "1d", period="max")
        self.assertEqual(first[0, "close"], 105.0)
        self.assertEqual(second[0, "close"], 105.0)

        state = self.candles.conn.execute(
            "SELECT full_history FROM candle_cache_state WHERE ticker = 'AAPL'"
        ).fetchone()
        self.assertEqual(state, (True,))

    def test_longer_finite_period_backfills_an_existing_short_cache_once(self):
        now = dt.datetime.now(dt.timezone.utc)
        self.candles._insert_candles(_candle_frame(100.0, now))

        with patch.object(
            self.candles,
            "_download_candles",
            return_value=_candle_frame(100.0, now),
        ) as download:
            self.candles.get_candles(
                ["AAPL"],
                period="1y",
                end="2026-08-12",
            )
            self.candles.get_candles(
                ["AAPL"],
                period="1y",
                end="2026-08-12",
            )

        download.assert_called_once_with(
            ["AAPL"],
            "1d",
            start="2025-08-11",
            end="2026-08-10",
        )

    def test_value_column_and_alias_reject_sql_expressions(self):
        with self.assertRaises(ValueError):
            self.candles.get_last_price(["AAPL"], select_col="read_text('/etc/passwd')")

        with self.assertRaises(ValueError):
            self.candles.get_last_price(["AAPL"], alias="value FROM candles")


if __name__ == "__main__":
    unittest.main()
