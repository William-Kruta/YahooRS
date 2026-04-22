from yahoors import Candles
import yfinance as yf
import polars as pl
import pandas as pd
import datetime as dt

from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, list_difference
from ..periphery.stale import get_stale_threshold


class Dividends:
    def __init__(self, db_path: str = None, debug: bool = True):
        self.conn = _init_tables(db_path)
        self.table_name = "dividends"
        self.debug = debug

    def get_dividends(self, tickers: list[str]) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        info = self.get_dividend_info(tickers)
        valid_tickers = info.filter(pl.col("status") == True)["ticker"].to_list()
        invalid_tickers = info.filter(pl.col("status") == False)["ticker"].to_list()
        df = self._read_dividends(tickers)
        local_tickers = df["ticker"].unique().to_list()
        if df.is_empty():
            fresh = self._download_dividends(tickers)
            self.update_dividend_info(tickers, fresh)
            self._insert_dividends(fresh)
            return self._read_dividends(tickers)
        return df

    def _download_dividends(self, tickers: list[str]) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        candles = Candles()
        candle_df = candles.get_candles(tickers, interval="1d", period="max")
        dividends = []
        for t in tickers:
            obj = yf.Ticker(t)
            data = obj.dividends.reset_index()
            data["ticker"] = t
            dividends.append(data)
        df = pl.from_pandas(pd.concat(dividends))
        df = df.rename({"Date": "date", "Dividends": "dividend"})
        df = df[["date", "ticker", "dividend"]]
        df = calculate_dividend_yield(df, candle_df)
        df = df.filter(pl.col("ttm_dividend").is_not_null())
        return df

    def _read_dividends(self, tickers: list[str]) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self.conn.execute(
            f"SELECT * FROM {self.table_name} WHERE ticker IN ({', '.join(['?'] * len(tickers))}) ORDER BY date ASC, ticker",
            tickers,
        ).pl()
        return df

    def _insert_dividends(self, df: pl.DataFrame):
        if df.is_empty():
            return
        insert_data(
            df,
            [
                "date",
                "ticker",
                "dividend",
                "ttm_dividend",
                "close",
                "dividend_yield_pct",
            ],
            self.table_name,
            self.conn,
            pk_cols=["date", "ticker"],
        )

    def get_dividend_info(self, tickers: list[str]) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        df = self._read_dividend_info(tickers)
        if df.is_empty():
            fresh = self._download_dividends(tickers)
            self._insert_dividends(fresh)
            self.update_dividend_info(tickers, fresh)
            return self._read_dividend_info(tickers)
        else:
            missing_tickers = list_difference(df["ticker"].unique().to_list(), tickers)
            if missing_tickers:
                fresh = self._download_dividends(missing_tickers)
                self._insert_dividends(fresh)
                self.update_dividend_info(missing_tickers, fresh)
                return self._read_dividend_info(tickers)
        return df

    def _read_dividend_info(self, tickers: list[str]) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self.conn.execute(
            f"SELECT * FROM dividend_info WHERE ticker IN ({', '.join(['?'] * len(tickers))})",
            tickers,
        ).pl()
        return df

    def update_dividend_info(self, tickers: list[str], dividend_df: pl.DataFrame):
        frequency = parse_dividend_frequency(dividend_df)
        query = f"INSERT OR IGNORE INTO dividend_info (ticker, frequency, status, updated_at) VALUES (?, ?, ?, ?)"
        for t in tickers:
            try:
                freq = frequency[t]
                params = [t, freq, True, dt.datetime.now()]
            except KeyError:
                params = [t, 0, False, dt.datetime.now()]
            self.conn.execute(query, params)


def calculate_dividend_yield(
    dividends: pl.DataFrame,
    candles: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate dividend yield per payment using TTM (trailing 4 payments) / close price on ex-date.
    """
    if dividends.is_empty():
        return dividends.with_columns(
            pl.lit(None).cast(pl.Float64).alias("ttm_dividend"),
            pl.lit(None).cast(pl.Float64).alias("close"),
            pl.lit(None).cast(pl.Float64).alias("dividend_yield_pct"),
        )
    if candles.is_empty():
        return dividends.with_columns(
            pl.lit(None).cast(pl.Float64).alias("ttm_dividend"),
            pl.lit(None).cast(pl.Float64).alias("close"),
            pl.lit(None).cast(pl.Float64).alias("dividend_yield_pct"),
        )

    dividends = dividends.with_columns(
        pl.col("date").dt.replace_time_zone(None).cast(pl.Datetime("us"))
    )

    # TTM dividend: rolling sum of last 4 payments per ticker
    dividends = dividends.sort(["ticker", "date"]).with_columns(
        pl.col("dividend")
        .rolling_sum(window_size=4, min_periods=4)
        .over("ticker")
        .alias("ttm_dividend")
    )

    # asof join: for each dividend date, grab the closest prior close
    result = (
        dividends.sort(["ticker", "date"])
        .join_asof(
            candles.select(["date", "ticker", "close"]).sort(["ticker", "date"]),
            on="date",
            by="ticker",
            strategy="backward",
            check_sortedness=False,
        )
        .with_columns(
            (pl.col("ttm_dividend") / pl.col("close") * 100)
            .round(4)
            .alias("dividend_yield_pct")
        )
    )

    return result


def parse_dividend_frequency(dividends: pl.DataFrame) -> dict[str, str]:
    """
    Infer dividend payment frequency per ticker from historical payment dates.

    Returns dict like {"AAPL": "quarterly", "MSFT": "quarterly"}
    """
    median_gaps = (
        dividends.sort(["ticker", "date"])
        .with_columns(
            pl.col("date").diff().over("ticker").dt.total_days().alias("days_between")
        )
        .filter(pl.col("days_between").is_not_null())
        .group_by("ticker")
        .agg(pl.col("days_between").median().alias("median_days"))
    )

    def _classify(days: float) -> int:
        if days < 45:
            return 12
        if days < 120:
            return 4
        if days < 270:
            return 2
        if days < 400:
            return 1
        return 0

    return {
        row["ticker"]: _classify(row["median_days"])
        for row in median_gaps.iter_rows(named=True)
    }
