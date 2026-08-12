import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import polars as pl

from yahoors.modules.options import Options


class OptionsDateDeltaTests(unittest.TestCase):
    def test_calc_date_deltas_returns_dict_limited_to_max_dte(self):
        options = Options.__new__(Options)

        result = options._calc_date_deltas(
            dates=["2026-01-03", "2026-01-07", "2026-01-12"],
            max_dte=7,
            ref_date="2026-01-01",
        )

        self.assertEqual(
            result,
            {
                "2026-01-03": 2,
                "2026-01-07": 6,
            },
        )

    def test_dte_range_fetches_every_matching_expiration_through_cache(self):
        options = Options.__new__(Options)
        expiration = (dt.date.today() + dt.timedelta(days=30)).isoformat()
        outside_range = (dt.date.today() + dt.timedelta(days=90)).isoformat()
        options.get_options = Mock(
            return_value=pl.DataFrame(
                {
                    "dte": [30],
                    "bid": [1.0],
                    "ask": [1.1],
                    "option_type": ["put"],
                    "prob_profit": [0.7],
                    "hist_prob_profit": [0.6],
                }
            )
        )

        with patch(
            "yahoors.modules.options.yf.Ticker",
            return_value=SimpleNamespace(options=(expiration, outside_range)),
        ):
            result = options.get_options_by_dte_range(
                ["AAPL"],
                min_dte=20,
                max_dte=45,
                force_update=True,
            )

        options.get_options.assert_called_once_with(
            ["AAPL"],
            get_latest=True,
            expirations=[expiration],
            force_update=True,
            rfr_ticker="^TNX",
        )
        self.assertEqual(result.height, 1)

    def test_explicit_expiration_missing_from_fresh_cache_is_downloaded(self):
        options = Options.__new__(Options)
        cached_expiration = dt.date.today() + dt.timedelta(days=7)
        requested_expiration = dt.date.today() + dt.timedelta(days=30)
        cached = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "expiration": [cached_expiration],
                "collected_at": [dt.datetime.now(dt.timezone.utc)],
            }
        )
        options._read_options = Mock(side_effect=[cached, cached])
        options._download_options = Mock(return_value=pl.DataFrame())
        options._insert_options = Mock()

        result = options.get_options(
            ["AAPL"],
            expirations=[requested_expiration.isoformat()],
        )

        options._download_options.assert_called_once_with(
            ["AAPL"],
            expirations=[requested_expiration.isoformat()],
            get_latest=False,
            rfr_ticker="^TNX",
        )
        self.assertTrue(result.is_empty())


if __name__ == "__main__":
    unittest.main()
