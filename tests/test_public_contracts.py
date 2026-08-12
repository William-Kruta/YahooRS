import unittest
import time
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version
from threading import Lock
from unittest.mock import Mock, patch

import polars as pl

from yahoors import __version__
from yahoors.client import YahooRSClient
from yahoors.server import YahooRSAPI, create_app


class _Response:
    def raise_for_status(self):
        return None

    def json(self):
        return {"rows": []}


class _Client:
    def __init__(self):
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        return _Response()


class PublicContractTests(unittest.TestCase):
    def test_runtime_version_matches_package_metadata(self):
        self.assertEqual(__version__, version("yahoors"))

    def test_openapi_exposes_force_update_and_restricts_value_columns(self):
        schema = create_app(db_path=":memory:").openapi()
        self.assertEqual(schema["info"]["version"], __version__)

        candle_params = {
            item["name"]: item for item in schema["paths"]["/candles"]["get"]["parameters"]
        }
        self.assertIn("force_update", candle_params)

        price_params = {
            item["name"]: item
            for item in schema["paths"]["/candles/last-price"]["get"]["parameters"]
        }
        self.assertIn("force_update", price_params)
        self.assertEqual(
            set(price_params["select_col"]["schema"]["enum"]),
            {"date", "open", "high", "low", "close", "volume", "collected_at"},
        )

        for path in (
            "/statements/{statement_type}",
            "/statements/margins",
            "/statements/ratios",
        ):
            statement_params = {
                item["name"]: item for item in schema["paths"][path]["get"]["parameters"]
            }
            self.assertIn("force_update", statement_params)

    def test_http_client_forwards_candle_force_update(self):
        transport = _Client()
        client = YahooRSClient("http://example.test", client=transport)

        client.get_candles("AAPL", force_update=True)

        self.assertEqual(transport.calls[0][0], "/candles")
        self.assertIs(transport.calls[0][1]["force_update"], True)

    def test_http_client_forwards_statement_force_update(self):
        transport = _Client()
        client = YahooRSClient("http://example.test", client=transport)

        client.get_statement("AAPL", "income_statement", force_update=True)

        self.assertEqual(transport.calls[0][0], "/statements/income_statement")
        self.assertIs(transport.calls[0][1]["force_update"], True)

    def test_api_forwards_statement_force_update(self):
        api = YahooRSAPI.__new__(YahooRSAPI)
        api.statements = Mock()
        api.statements.get_statement.return_value = pl.DataFrame()

        api.get_statement("AAPL", "income_statement", force_update=True)

        api.statements.get_statement.assert_called_once_with(
            tickers=["AAPL"],
            statement="income_statement",
            period="A",
            force_update=True,
        )

    def test_api_screener_fetches_the_requested_dte_range(self):
        api = YahooRSAPI.__new__(YahooRSAPI)
        api.options = Mock()
        api.options.get_options_by_dte_range.return_value = pl.DataFrame()

        with patch("yahoors.server.options_screener", return_value=pl.DataFrame()):
            api.screen_options(
                ["AAPL"],
                min_dte=30,
                max_dte=60,
                force_update=True,
            )

        api.options.get_options_by_dte_range.assert_called_once_with(
            tickers=["AAPL"],
            min_dte=30,
            max_dte=60,
            force_update=True,
        )

    def test_api_serializes_shared_module_access(self):
        api = YahooRSAPI.__new__(YahooRSAPI)
        api.options = Mock()
        state_lock = Lock()
        active = 0
        max_active = 0

        def fetch_options(**kwargs):
            nonlocal active, max_active
            with state_lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with state_lock:
                active -= 1
            return pl.DataFrame()

        api.options.get_options_by_dte_range.side_effect = fetch_options
        with patch("yahoors.server.options_screener", return_value=pl.DataFrame()):
            with ThreadPoolExecutor(max_workers=2) as pool:
                calls = [pool.submit(api.screen_options, ["AAPL"]) for _ in range(2)]
                for call in calls:
                    call.result()

        self.assertEqual(max_active, 1)


if __name__ == "__main__":
    unittest.main()
