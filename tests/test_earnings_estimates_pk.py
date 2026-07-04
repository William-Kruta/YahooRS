import datetime as dt
import os
import tempfile
import unittest

import duckdb
import polars as pl

from yahoors.modules.earnings import Earnings
from yahoors.periphery.db import _init_tables


def _estimates_frame(collected_at: dt.datetime) -> pl.DataFrame:
    """Quarterly (Q4 2026) and annual (FY 2026) estimates both resolve to
    period 2026-12-31 — they must coexist, distinguished by period_label."""
    return pl.DataFrame(
        {
            "period": [dt.date(2026, 12, 31), dt.date(2026, 12, 31)],
            "ticker": ["AAPL", "AAPL"],
            "avg": [1.0, 2.0],
            "low": [0.5, 1.5],
            "high": [1.5, 2.5],
            "year_ago_eps": [0.9, 1.8],
            "number_of_analysts": [10, 12],
            "growth": [0.1, 0.2],
            "label": ["future", "current"],
            "period_label": ["Q", "A"],
            "collected_at": [collected_at, collected_at],
        }
    )


class EarningsEstimatesPKTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_quarterly_and_annual_estimates_for_same_period_date_both_insert(self):
        earnings = Earnings(db_path=self.db_path)
        now = dt.datetime.now(dt.timezone.utc)

        earnings._insert(_estimates_frame(now), "estimates")

        rows = earnings.conn.execute(
            "SELECT period_label FROM earnings_estimates ORDER BY period_label"
        ).fetchall()
        self.assertEqual([r[0] for r in rows], ["A", "Q"])

    def test_reinsert_same_snapshot_does_not_raise_or_duplicate(self):
        earnings = Earnings(db_path=self.db_path)
        now = dt.datetime.now(dt.timezone.utc)

        earnings._insert(_estimates_frame(now), "estimates")
        earnings._insert(_estimates_frame(now), "estimates")

        count = earnings.conn.execute(
            "SELECT count(*) FROM earnings_estimates"
        ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_old_pk_schema_is_migrated_with_data_preserved(self):
        conn = duckdb.connect(self.db_path)
        conn.execute(
            """
            CREATE TABLE earnings_estimates (
                period              VARCHAR NOT NULL,
                ticker              VARCHAR NOT NULL,
                avg                 DOUBLE,
                low                 DOUBLE,
                high                DOUBLE,
                year_ago_eps        DOUBLE,
                number_of_analysts  INTEGER,
                growth              DOUBLE,
                label               VARCHAR,
                period_label        VARCHAR,
                collected_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (period, ticker, collected_at)
            )
            """
        )
        conn.execute(
            "INSERT INTO earnings_estimates VALUES "
            "('2026-12-31', 'AAPL', 1.0, 0.5, 1.5, 0.9, 10, 0.1, 'current', NULL, CURRENT_TIMESTAMP)"
        )
        conn.close()

        conn = _init_tables(self.db_path)
        pk_cols = conn.execute(
            """
            SELECT constraint_column_names FROM duckdb_constraints()
            WHERE table_name = 'earnings_estimates' AND constraint_type = 'PRIMARY KEY'
            """
        ).fetchone()[0]
        self.assertEqual(pk_cols, ["period", "period_label", "ticker", "collected_at"])

        count = conn.execute("SELECT count(*) FROM earnings_estimates").fetchone()[0]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
