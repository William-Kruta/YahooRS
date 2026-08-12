import asyncio
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import duckdb
import polars as pl

from yahoors.modules.candles import Candles
from yahoors.modules.tickers import Ticker
from yahoors.periphery.utils import clean_tickers
from yahoors.server import create_app


class ResourceLifecycleTests(unittest.TestCase):
    def test_symbol_normalization_strips_uppercases_and_deduplicates(self):
        self.assertEqual(
            clean_tickers([" aapl ", "AAPL", " brk.b ", "", "msft"]),
            ["AAPL", "BRK-B", "MSFT"],
        )

    def test_candles_context_manager_closes_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            candles = Candles(os.path.join(tmp, "test.db"), debug=False)
            with candles:
                candles.conn.execute("SELECT 1")

            with self.assertRaises(duckdb.ConnectionException):
                candles.conn.execute("SELECT 1")

    def test_ticker_download_uses_custom_database_candles(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "custom.db")
            with Ticker(" aapl ", db_path=db_path) as ticker:
                with patch(
                    "yahoors.modules.tickers._download_ticker_info",
                    return_value=pl.DataFrame(),
                ) as download:
                    result = ticker.info

                self.assertEqual(ticker.ticker, "AAPL")
                self.assertTrue(result.is_empty())
                self.assertIs(download.call_args.kwargs["candle_obj"], ticker._candles_obj)

    def test_fastapi_lifespan_closes_cached_api(self):
        app = create_app(db_path=":memory:")
        api = Mock()
        app.state.api = api

        async def run_lifespan():
            async with app.router.lifespan_context(app):
                pass

        asyncio.run(run_lifespan())
        api.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
