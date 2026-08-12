import datetime as dt
import os
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import polars as pl

from yahoors.modules.statements import Statements
from yahoors.periphery.db import _init_tables


def _statement_frame(value: float | None, date: dt.datetime | None = None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [date or dt.datetime(2025, 9, 27)],
            "ticker": ["AAPL"],
            "label": ["Total Revenue"],
            "value": pl.Series([value], dtype=pl.Float64),
            "statement_type": ["income_statement"],
            "period": ["A"],
        }
    )


class StatementCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.statements = Statements(self.db_path)

    def tearDown(self):
        self.statements.close()
        self.tmp.cleanup()

    def test_stale_refresh_upserts_restated_values_and_runs_once(self):
        self.statements._cache_statement_download(
            _statement_frame(100.0),
            "AAPL",
            "income_statement",
            "A",
        )
        old_fetch = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=366)
        self.statements.conn.execute(
            """
            UPDATE statement_cache_state
            SET updated_at = ?
            WHERE ticker = 'AAPL'
              AND statement_type = 'income_statement'
              AND period = 'A'
            """,
            [old_fetch],
        )

        with patch.object(
            self.statements,
            "_download_statements",
            return_value=_statement_frame(125.0),
        ) as download:
            first = self.statements.get_income_statement("AAPL", period="A")
            second = self.statements.get_income_statement("AAPL", period="A")

        download.assert_called_once_with("AAPL", "income_statement", "A")
        self.assertEqual(first[0, "2025-09-27"], 125.0)
        self.assertEqual(second[0, "2025-09-27"], 125.0)

        rows = self.statements.conn.execute(
            "SELECT value FROM statements WHERE ticker = 'AAPL'"
        ).fetchall()
        self.assertEqual(rows, [(125.0,)])

    def test_recent_fetch_is_fresh_even_when_fiscal_date_is_old(self):
        self.statements._cache_statement_download(
            _statement_frame(80.0, dt.datetime(2020, 9, 26)),
            "AAPL",
            "income_statement",
            "A",
        )

        with patch.object(self.statements, "_download_statements") as download:
            result = self.statements.get_income_statement("AAPL", period="A")

        download.assert_not_called()
        self.assertEqual(result[0, "2020-09-26"], 80.0)

    def test_empty_download_uses_short_negative_cache(self):
        empty = pl.DataFrame()
        with patch.object(self.statements, "_download_statements", return_value=empty) as download:
            first = self.statements.get_income_statement("EMPTY", period="A")
            second = self.statements.get_income_statement("EMPTY", period="A")

            old_fetch = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
            self.statements.conn.execute(
                """
                UPDATE statement_cache_state
                SET updated_at = ?
                WHERE ticker = 'EMPTY'
                """,
                [old_fetch],
            )
            third = self.statements.get_income_statement("EMPTY", period="A")

        self.assertEqual(download.call_count, 2)
        self.assertTrue(first.is_empty())
        self.assertTrue(second.is_empty())
        self.assertTrue(third.is_empty())

    def test_force_update_bypasses_recent_statement_cache(self):
        self.statements._cache_statement_download(
            _statement_frame(100.0),
            "AAPL",
            "income_statement",
            "A",
        )

        with patch.object(
            self.statements,
            "_download_statements",
            return_value=_statement_frame(140.0),
        ) as download:
            result = self.statements.get_income_statement("AAPL", period="A", force_update=True)

        download.assert_called_once_with("AAPL", "income_statement", "A")
        self.assertEqual(result[0, "2025-09-27"], 140.0)

    def test_missing_statement_values_remain_null(self):
        self.statements._cache_statement_download(
            _statement_frame(None),
            "AAPL",
            "income_statement",
            "A",
        )

        result = self.statements.get_income_statement("AAPL", period="A")

        self.assertIsNone(result[0, "2025-09-27"])
        self.assertEqual(
            self.statements.conn.execute(
                "SELECT value FROM statements WHERE ticker = 'AAPL'"
            ).fetchone(),
            (None,),
        )

    def test_old_statement_schema_is_migrated_without_losing_cache_state(self):
        self.statements.close()
        conn = duckdb.connect(self.db_path)
        conn.execute("DROP TABLE statement_cache_state")
        conn.execute("DROP TABLE statements")
        conn.execute(
            """
            CREATE TABLE statements (
                date TIMESTAMP NOT NULL,
                ticker VARCHAR NOT NULL,
                label VARCHAR NOT NULL,
                value DOUBLE NOT NULL,
                statement_type VARCHAR NOT NULL,
                period VARCHAR NOT NULL,
                PRIMARY KEY (date, ticker, label, statement_type, period)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE statement_cache_state (
                ticker VARCHAR NOT NULL,
                statement_type VARCHAR NOT NULL,
                period VARCHAR NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (ticker, statement_type, period)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO statements VALUES
            ('2025-09-27', 'AAPL', 'Total Revenue', 100, 'income_statement', 'A')
            """
        )
        conn.execute(
            """
            INSERT INTO statement_cache_state VALUES
            ('AAPL', 'income_statement', 'A', CURRENT_TIMESTAMP)
            """
        )
        conn.close()

        migrated = _init_tables(self.db_path)
        self.statements = Statements.__new__(Statements)
        self.statements.conn = migrated
        self.statements.candles = None
        self.statements._owns_candles = False
        self.statements.table_name = "statements"

        self.assertEqual(
            migrated.execute("SELECT row_count FROM statement_cache_state").fetchone(),
            (1,),
        )
        migrated.execute(
            """
            INSERT INTO statements VALUES
            ('2024-09-28', 'AAPL', 'Missing Value', NULL, 'income_statement', 'A')
            """
        )


if __name__ == "__main__":
    unittest.main()
