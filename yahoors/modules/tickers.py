import yfinance as yf
from typing import Literal
import pandas as pd
import polars as pl
import datetime as dt
import duckdb
from .candles import Candles
from .statements import Statements
from ..periphery.utils import list_difference, clean_tickers
from ..periphery.db import _init_tables, insert_data



class Ticker:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.conn = _init_tables()
        self.table_name = "company_info"
        self._statements_obj = Statements()
        self._cache: dict[str, pl.DataFrame] = {}

    def _get_cached(self, key: str, fetcher) -> pl.DataFrame:
        if key not in self._cache:
            self._cache[key] = fetcher()
        return self._cache[key]

    @property
    def income_statement(self) -> pl.DataFrame:
        return self._get_cached("income", lambda: self._statements_obj.get_income_statement(self.ticker, period="A"))

    @property
    def quarterly_income_statement(self) -> pl.DataFrame:
        return self._get_cached("income_q", lambda: self._statements_obj.get_income_statement(self.ticker, period="Q"))

    @property
    def balance_sheet(self) -> pl.DataFrame:
        return self._get_cached("balance", lambda: self._statements_obj.get_balance_sheet(self.ticker, period="A"))

    @property
    def quarterly_balance_sheet(self) -> pl.DataFrame:
        return self._get_cached("balance_q", lambda: self._statements_obj.get_balance_sheet(self.ticker, period="Q"))

    @property
    def cash_flow(self) -> pl.DataFrame:
        return self._get_cached("cash_flow", lambda: self._statements_obj.get_cash_flow(self.ticker, period="A"))

    @property
    def quarterly_cash_flow(self) -> pl.DataFrame:
        return self._get_cached("cash_flow_q", lambda: self._statements_obj.get_cash_flow(self.ticker, period="Q"))

    @property
    def ratios(self) -> pl.DataFrame:
        return self._get_cached("ratios", lambda: self._statements_obj.get_ratios(self.ticker, period="A"))

    @property
    def quarterly_ratios(self) -> pl.DataFrame:
        return self._get_cached("ratios_q", lambda: self._statements_obj.get_ratios(self.ticker, period="Q"))

    @property
    def margins(self) -> pl.DataFrame:
        return self._get_cached("margins", lambda: self._statements_obj.get_margins(self.ticker, period="A"))

    @property
    def quarterly_margins(self) -> pl.DataFrame:
        return self._get_cached("margins_q", lambda: self._statements_obj.get_margins(self.ticker, period="Q"))

    @property
    def info(self) -> pl.DataFrame:
        return self._get_cached("info", lambda: _get_info(self.ticker, self.table_name, self.conn))

    @property
    def trading_status(self) -> pl.DataFrame:
        return self._get_cached("trading_status", lambda: _get_trading_status(self.ticker, self.table_name, self.conn))

    def update_trading_status(self, is_active: bool):
        _update_trading_status(self.ticker, is_active, self.table_name, self.conn)
        self._cache.pop("trading_status", None)

    def delete_info(self):
        _delete_info(self.ticker, self.table_name, self.conn)
        self._cache.clear()

    def clear_cache(self):
        """Let users force a refresh if needed."""
        self._cache.clear()





# class Ticker: 
#     def __init__(self, ticker: str, db_path: str = None):
#         self.ticker = ticker
#         self.candles_obj = Candles(db_path=db_path)
#         self.statements_obj = Statements(db_path=db_path)
#         self.table_name = "company_info"
#         self.conn = _init_tables(db_path)

#         # Holding values for properties 
#         self._candles = None
#         self._statements = {
#             "annual_income_statement": None,
#             "quarterly_income_statement": None,
#             "annual_balance_sheet": None,
#             "quarterly_balance_sheet": None,
#             "annual_cash_flow": None, 
#             "quarterly_cash_flow": None,
#             "annual_ratios": None,
#             "quarterly_ratios": None,
#             "annual_margins": None,
#             "quarterly_margins": None,
#         }
#         self._info = {
#             "general": None,
#             "trading_status": None
#         }

#     def candles(self, interval: str = "1d", period: str = "max"):
#         if self._candles is None:
#             self._candles = self.candles_obj.get_candles(self.ticker, interval=interval, period=period)
#         return self._candles

#     @property
#     def income_statement(self):
#         if self._statements["annual_income_statement"] is None:
#             self._statements["annual_income_statement"] = self.statements_obj.get_income_statement(self.ticker, period="A")
#         return self._statements["annual_income_statement"]

#     @property
#     def quarterly_income_statement(self):
#         if self._statements["quarterly_income_statement"] is None:
#             self._statements["quarterly_income_statement"] = self.statements_obj.get_income_statement(self.ticker, period="Q")
#         return self._statements["quarterly_income_statement"]

#     @property
#     def annual_balance_sheet(self):
#         if self._statements["annual_balance_sheet"] is None:
#             self._statements["annual_balance_sheet"] = self.statements_obj.get_balance_sheet(self.ticker, period="A")
#         return self._statements["annual_balance_sheet"]

#     @property
#     def quarterly_balance_sheet(self):
#         if self._statements["quarterly_balance_sheet"] is None:
#             self._statements["quarterly_balance_sheet"] = self.statements_obj.get_balance_sheet(self.ticker, period="Q")
#         return self._statements["quarterly_balance_sheet"]

#     @property
#     def annual_cash_flow(self):
#         if self._statements["annual_cash_flow"] is None:
#             self._statements["annual_cash_flow"] = self.statements_obj.get_cash_flow(self.ticker, period="A")
#         return self._statements["annual_cash_flow"]

#     @property
#     def quarterly_cash_flow(self):
#         if self._statements["quarterly_cash_flow"] is None:
#             self._statements["quarterly_cash_flow"] = self.statements_obj.get_cash_flow(self.ticker, period="Q")
#         return self._statements["quarterly_cash_flow"]

#     @property
#     def annual_ratios(self): 
#         if self._statements["annual_ratios"] is None:
#             self._statements["annual_ratios"] = self.statements_obj.get_ratios(self.ticker, period="A")
#         return self._statements["annual_ratios"]
    
#     @property
#     def quarterly_ratios(self): 
#         if self._statements["quarterly_ratios"] is None:
#             self._statements["quarterly_ratios"] = self.statements_obj.get_ratios(self.ticker, period="Q")
#         return self._statements["quarterly_ratios"]
    
#     @property
#     def annual_margins(self):
#         if self._statements["annual_margins"] is None:
#             self._statements["annual_margins"] = self.statements_obj.get_margins(self.ticker, period="A")
#         return self._statements["annual_margins"]
    
#     @property
#     def quarterly_margins(self):
#         if self._statements["quarterly_margins"] is None:
#             self._statements["quarterly_margins"] = self.statements_obj.get_margins(self.ticker, period="Q")
#         return self._statements["quarterly_margins"]
    
#     @property
#     def info(self):
#         if self._info["general"] is None:
#             self._info["general"] = self._get_info()
#         return self._info["general"]

#     def _get_info(self): 

#         df = _read_ticker_info(self.ticker, self.table_name, self.conn)
#         if df.is_empty():
#             df = _download_ticker_info(self.ticker)
#             print(f"DF: {df}")
#             exit()
#             _insert_ticker_info(df, self.table_name, self.conn)
#         return df         
    
#     @property
#     def trading_status(self):
#         if self._info["trading_status"] is None:
#             self._info["trading_status"] = self.info["trading_status"][0]
#         return self._info["trading_status"]
    





class BatchTickers:
    def __init__(self, db_path: str = None, candle_obj: Candles = None):
        self.conn = _init_tables(db_path)
        self.table_name = "company_info"
        if candle_obj is None:
            self.candles = Candles(db_path)
        else:
            self.candles = candle_obj



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



def _get_info(ticker: str, table_name: str, conn: duckdb.DuckDBPyConnection): 
    if isinstance(ticker, str):
        ticker = [ticker]
    df = _read_ticker_info(ticker, table_name, conn)
    if df.is_empty():
        df = _download_ticker_info(ticker)
        _insert_ticker_info(df, table_name, conn)
    return df         



def _download_ticker_info(tickers: list[str]):
    if isinstance(tickers, str):
        tickers = [tickers]

    candles = Candles()
    # Batch fetch first prices and candles upfront
    first_date = candles.get_first_price(
        tickers, select_col="date", alias="value"
    )
    last_date = candles.get_last_price(tickers, select_col="date", alias="value")
    data = []
    now = dt.datetime.now()
    for t in tickers:
        try:
            obj = yf.Ticker(t)
            info = obj.info

            try:
                first_date = first_date[t]
                last_date = last_date[t]
                delta = now - last_date
                if delta.days < 30:
                    status = True
                else:
                    status = False
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
                    "first_trading_day": first_date,
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


def _read_ticker_info(ticker: str, table_name: str, conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    if isinstance(ticker, str):
        ticker = [ticker]
    return conn.execute(
        f"SELECT * FROM {table_name} WHERE symbol = ANY($1)", [ticker]
    ).pl()

def _insert_ticker_info(df: pl.DataFrame, table_name: str, conn: duckdb.DuckDBPyConnection):
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
        table_name=table_name,
        conn=conn,
        pk_cols=["symbol"],
    )


def _get_trading_status(tickers: list[str], table_name: str, conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Get the trading status for a list of tickers."""
    if isinstance(tickers, str):
        tickers = [tickers]
    return conn.execute(
        f"SELECT symbol, trading_status FROM {table_name} WHERE symbol = ANY($1)", [tickers]
    ).pl()

def _update_trading_status(ticker: list[str],  is_active: bool, table_name: str, conn: duckdb.DuckDBPyConnection) -> None:
    """Update the trading status for a given ticker."""
    if isinstance(ticker, str):
        ticker = [ticker]
    conn.execute(
        f"""
        UPDATE {table_name}
        SET trading_status = $1, updated_at = NOW()
        WHERE symbol = ANY($2)
        """,
        [is_active, ticker],
    )

def _delete_info(ticker: list[str], table_name: str, conn: duckdb.DuckDBPyConnection) -> None:
    """Delete the info for a given ticker."""
    if isinstance(ticker, str):
        ticker = [ticker]
    conn.execute(
        f"DELETE FROM {table_name} WHERE symbol = ANY($1)", [ticker]
    )