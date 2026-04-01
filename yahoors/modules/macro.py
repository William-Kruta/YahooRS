

from .candles import Candles
from ..periphery.db import _init_tables
import polars as pl

class Macro:
    def __init__(self, db_path: str = None, candles_obj: Candles = None):
        self.conn = _init_tables(db_path)
        if candles_obj is None:
            self.candles = Candles(db_path=db_path)
        else:
            self.candles = candles_obj
        self.table_name = "macro"
        


    def get_risk_free_rate(self, ticker: str = "^TNX", interval: str = "1d", period: str = "max"):
        df = self.candles.get_candles(tickers=ticker, interval=interval, period=period)
        return df


    def get_yield_curve(self, short_term_ticker: str = "2YY=F", long_term_ticker: str = "^TNX", interval: str = "1d", period: str = "max"):
        short_term_df = self.get_risk_free_rate(ticker=short_term_ticker, interval=interval, period=period)
        long_term_df = self.get_risk_free_rate(ticker=long_term_ticker, interval=interval, period=period)
        df = (
            short_term_df
            .select(["date", "close"])
            .rename({"close": "short_term_yield"})
            .join(
                long_term_df.select(["date", "close"]).rename({"close": "long_term_yield"}),
                on="date",
                how="inner",
            )
            .with_columns(
                (pl.col("long_term_yield") - pl.col("short_term_yield")).alias("yield_spread")
            )
        ) 
        return df

        
    def get_currency_exchange_rate(self, currency_a: str, currency_b: str, interval: str = "1d", period: str = "max"):
        df = self.candles.get_candles(tickers=f"{currency_a}{currency_b}=X", interval=interval, period=period)
        return df
    # def get_macro(self, tickers: list, period: str) -> pl.DataFrame:
    #     """
    #     tickers: list, List of tickers to search. Will be converted to a list if single string is passed.
    #     period: str, Period of financial statements. 'A' for annual and 'Q' for quarterly.

    #     returns: pl.DataFrame, Dataframe containing the financial statement.
    #     """
    #     if isinstance(tickers, str):
    #         tickers = [tickers]
    #     df = self.get_statement(
    #         tickers, statement="macro", period=period.upper()
    #     )
    #     return df