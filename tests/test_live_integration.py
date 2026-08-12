import os
import tempfile

import pytest

from yahoors.modules.candles import Candles
from yahoors.modules.statements import Statements


pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("YAHOORS_RUN_LIVE_TESTS") != "1",
        reason="set YAHOORS_RUN_LIVE_TESTS=1 to call Yahoo Finance",
    ),
]


def test_live_candle_schema_and_cache_round_trip():
    with tempfile.TemporaryDirectory() as tmp:
        with Candles(os.path.join(tmp, "live.db"), debug=False) as candles:
            frame = candles.get_candles("AAPL", interval="1d", period="5d")
            cached = candles.get_candles("AAPL", interval="1d", period="5d")

    assert not frame.is_empty()
    assert frame.columns == cached.columns
    assert {"date", "ticker", "close", "collected_at"} <= set(frame.columns)


def test_live_statement_schema_and_cache_round_trip():
    with tempfile.TemporaryDirectory() as tmp:
        with Statements(os.path.join(tmp, "live.db")) as statements:
            frame = statements.get_income_statement("AAPL", period="A")
            cached = statements.get_income_statement("AAPL", period="A")

    assert not frame.is_empty()
    assert frame.equals(cached)
    assert {"ticker", "label"} <= set(frame.columns)
