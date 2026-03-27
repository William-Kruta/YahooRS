import yfinance as yf
from typing import Literal
import pandas as pd
import polars as pl
import datetime as dt
from .candles import Candles
from ..periphery.utils import list_difference, clean_tickers
from ..periphery.db import _init_tables, insert_data


class Tickers:
    def __init__(self, db_path: str = None, candle_obj: Candles = None):
        self.conn = _init_tables(db_path)
        self.table_name = "company_info"
        if candle_obj is None:
            self.candles = Candles(db_path)
        else:
            self.candles = candle_obj

    def get_info(self, tickers: list[str]):
        if isinstance(tickers, str):
            tickers = [tickers]

        df = self._read_ticker_info(tickers)

        if df.is_empty():
            df = self._download_ticker_info(tickers)
            self._insert_ticker_info(df)
        else:
            local_tickers = df["symbol"].to_list()
            missing_tickers = list_difference(local_tickers, tickers)
            if missing_tickers:
                df = self._download_ticker_info(tickers=missing_tickers)
                self._insert_ticker_info(df)

        return self._read_ticker_info(tickers)

    def _download_ticker_info(self, tickers: list[str]):
        if isinstance(tickers, str):
            tickers = [tickers]

        # Batch fetch first prices and candles upfront
        first_prices = self.candles.get_first_price(
            tickers, select_col="date", alias="value"
        )
        latest_prices = self.candles.get_last_price(tickers)
        print(f"LATEST: {latest_prices}")
        data = []
        for t in tickers:
            try:
                obj = yf.Ticker(t)
                info = obj.info
                try:
                    _ = first_prices[t]
                    status = True
                except KeyError:
                    status = False
                data.append(
                    {
                        "symbol": t,
                        "name": info.get("longName", ""),
                        "sector": info.get("sector", ""),
                        "industry": info.get("industry"),
                        "country": info.get("country", ""),
                        "website": info.get("website", ""),
                        "business_summary": info.get("longBusinessSummary", ""),
                        "first_trading_day": first_prices.get(t),
                        "trading_status": status,
                        "asset_type": info.get("quoteType"),
                    }
                )
            except Exception:
                break

        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.lit(dt.datetime.now(dt.timezone.utc)).alias("updated_at")
        )
        return df

    def _read_ticker_info(self, tickers: list[str]) -> pl.DataFrame:
        return self.conn.execute(
            "SELECT * FROM company_info WHERE symbol = ANY($1)", [tickers]
        ).pl()

    def _insert_ticker_info(self, df: pl.DataFrame):
        if df.is_empty():
            return

        db_cols = [
            "symbol",
            "name",
            "sector",
            "industry",
            "country",
            "website",
            "business_summary",
            "first_trading_day",
            "asset_type",
            "trading_status",
            "updated_at",
        ]
        insert_data(
            df,
            db_cols=db_cols,
            table_name=self.table_name,
            conn=self.conn,
            pk_cols=["symbol"],
        )

    def read_from_csv(
        self,
        csv_path: str,
        symbol_col: str,
        exclude: list = [],
        filter_status: bool = False,
    ):
        df = pl.read_csv(csv_path)

        tickers = df[symbol_col].to_list()
        bad_values = ["--"]
        tickers = [t for t in tickers if t not in bad_values]
        tickers = clean_tickers(tickers)
        tickers = [t for t in tickers if t not in exclude]
        if filter_status:
            return (
                self.conn.execute(
                    f"SELECT * FROM {self.table_name} WHERE symbol = ANY($1) AND trading_status = True",
                    [tickers],
                )
                .pl()["symbol"]
                .to_list()
            )
        return tickers

    def _read_trading_status(self, ticker: str):
        query = f"SELECT trading_status FROM {self.table_name} WHERE symbol = ?"
        return self.conn.execute(query=query, parameters=[ticker])

    def read_from_text_file(self, file_path: str) -> list:
        with open(file_path, "r") as file:
            lines = file.read().split("\n")
        filtered_data = list(filter(None, lines))
        return filtered_data

    def is_ticker_valid(self, symbol: str, yf_obj: yf.Ticker = None):
        obj = yf_obj or yf.Ticker(symbol)
        try:
            hist = obj.history(
                period="1d"
            )  # was using 'ticker' which is the yf_obj parameter
            if hist.empty:
                return False
            return "symbol" in obj.history_metadata
        except Exception:
            return False
