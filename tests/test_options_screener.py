import datetime as dt
import unittest

import polars as pl

from yahoors.modules.options import Options
from yahoors.modules.screener import options_screener
from yahoors.periphery.greeks import add_greeks_to_df


def _screen_frame(
    *,
    dtes: list[int],
    option_type: str = "put",
    bs_price: float = 1.5,
) -> pl.DataFrame:
    count = len(dtes)
    return pl.DataFrame(
        {
            "strike": [100.0] * count,
            "bid": [2.0] * count,
            "ask": [2.2] * count,
            "last_price": [2.0] * count,
            "dte": dtes,
            "in_the_money": [False] * count,
            "prob_profit": [0.8] * count,
            "hist_prob_profit": [0.7] * count,
            "stock_price": [105.0] * count,
            "option_type": [option_type] * count,
            "bs_price": [bs_price] * count,
            "last_trade_date": [dt.datetime.now(dt.timezone.utc)] * count,
        }
    )


class OptionsScreenerTests(unittest.TestCase):
    def test_short_side_flips_both_probabilities_and_zero_dte_is_not_annualized(self):
        result = options_screener(
            _screen_frame(dtes=[0, 10]),
            min_dte=0,
            min_premium=0,
            min_roc=0,
            max_trade_age=None,
        ).sort("dte")

        self.assertAlmostEqual(result[0, "prob_profit"], 0.2)
        self.assertAlmostEqual(result[0, "hist_prob_profit"], 0.3)
        self.assertIsNone(result[0, "annualized_roc"])
        self.assertIsNotNone(result[1, "annualized_roc"])

    def test_long_model_edge_uses_buyer_direction_and_premium_as_collateral(self):
        result = options_screener(
            _screen_frame(dtes=[10], bs_price=3.0),
            long=True,
            min_premium=0,
            max_trade_age=None,
        )

        self.assertAlmostEqual(result[0, "expected_return"], 0.009)
        self.assertAlmostEqual(result[0, "collateral"], 210.0)
        self.assertIsNone(result[0, "roc"])

    def test_short_calls_are_modeled_as_covered_calls(self):
        result = options_screener(
            _screen_frame(dtes=[10], option_type="call"),
            min_premium=0,
            min_roc=0,
            max_trade_age=None,
        )

        self.assertAlmostEqual(result[0, "collateral"], 10_500.0)
        self.assertAlmostEqual(result[0, "max_loss_per_share"], 102.9)

    def test_historical_windows_use_calendar_days(self):
        options = Options.__new__(Options)
        options_df = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "dte": [3],
                "strike": [100.0],
                "option_type": ["call"],
                "bid": [5.0],
                "ask": [5.0],
                "stock_price": [100.0],
            }
        )
        candles_df = pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL"],
                "date": [dt.date(2026, 1, 2), dt.date(2026, 1, 5)],
                "close": [100.0, 110.0],
            }
        )

        result = options.calculate_historical_probs(options_df, candles_df)

        self.assertEqual(result[0, "hist_prob_profit"], 1.0)

    def test_invalid_ranges_raise_value_error(self):
        with self.assertRaises(ValueError):
            options_screener(_screen_frame(dtes=[10]), min_dte=20, max_dte=10)

    def test_ask_only_quote_can_produce_greeks(self):
        quote = pl.DataFrame(
            {
                "strike": [100.0],
                "impliedVolatility": [0.3],
                "lastPrice": [0.0],
                "bid": [0.0],
                "ask": [2.0],
                "dte": [30],
                "option_type": ["call"],
                "stock_price": [100.0],
            }
        )

        result = add_greeks_to_df(quote)

        self.assertIsNotNone(result[0, "delta"])
        self.assertGreater(result[0, "bs_price"], 0)


if __name__ == "__main__":
    unittest.main()
