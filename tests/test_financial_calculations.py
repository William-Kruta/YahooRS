import datetime as dt
import unittest

import polars as pl

from yahoors.modules.dividends import calculate_dividend_yield
from yahoors.modules.statements import Statements


class FinancialCalculationTests(unittest.TestCase):
    def test_ttm_dividend_uses_a_trailing_year_not_four_payments(self):
        dates = [dt.datetime(2025, month, 1) for month in range(1, 13)] + [dt.datetime(2026, 1, 1)]
        dividends = pl.DataFrame(
            {
                "date": dates,
                "ticker": ["MONTHLY"] * len(dates),
                "dividend": [1.0] * len(dates),
            }
        )
        candles = pl.DataFrame(
            {
                "date": dates,
                "ticker": ["MONTHLY"] * len(dates),
                "close": [100.0] * len(dates),
            }
        )

        result = calculate_dividend_yield(dividends, candles).sort("date")

        self.assertEqual(result[-1, "ttm_dividend"], 12.0)
        self.assertEqual(result[-1, "dividend_yield_pct"], 12.0)

    def test_ev_to_ebitda_subtracts_cash(self):
        date_column = "2025-12-31"
        income = pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL"],
                "label": ["EBITDA", "Diluted Average Shares"],
                date_column: [100.0, 10.0],
            }
        )
        balance_sheet = pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL"],
                "label": ["Total Debt", "Cash And Cash Equivalents"],
                date_column: [50.0, 20.0],
            }
        )
        candles = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "date": [dt.datetime(2025, 12, 31)],
                "close": [10.0],
            }
        )

        result = Statements.__new__(Statements).get_ratios(
            ["AAPL"],
            income_df=income,
            balance_sheet_df=balance_sheet,
            candles_df=candles,
        )
        ev_to_ebitda = result.filter(pl.col("ratio_name") == "EV/EBITDA")

        self.assertAlmostEqual(ev_to_ebitda[0, "value"], 1.3)


if __name__ == "__main__":
    unittest.main()
