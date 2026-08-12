import yfinance as yf
import polars as pl
import pandas as pd
import datetime as dt
import logging

from .candles import Candles
from ..periphery.db import _init_tables
from ..periphery.utils import clean_tickers, list_difference


logger = logging.getLogger(__name__)


class Dividends:
    def __init__(self, db_path: str = None, debug: bool = True):
        self.conn = _init_tables(db_path)
        self.table_name = "dividends"
        self.debug = debug
        self.candles = Candles(db_path, debug=debug)

    def close(self) -> None:
        self.candles.close()
        self.conn.close()

    def __enter__(self) -> "Dividends":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def get_dividends(
        self,
        tickers: list[str],
        force_update: bool = False,
        stale_threshold: dt.timedelta = dt.timedelta(days=30),
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        self.get_dividend_info(
            tickers,
            force_update=force_update,
            stale_threshold=stale_threshold,
        )
        return self._read_dividends(tickers)

    def _download_dividends(self, tickers: list[str]) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        candle_df = self.candles.get_candles(tickers, interval="1d", period="max")
        dividends = []
        for t in tickers:
            try:
                obj = yf.Ticker(t)
                data = obj.dividends.reset_index()
                data["ticker"] = t
                dividends.append(data)
            except Exception as exc:
                logger.warning("Unable to download dividends for %s: %s", t, exc)
        if not dividends:
            return pl.DataFrame()
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
        columns = [
            "date",
            "ticker",
            "dividend",
            "ttm_dividend",
            "close",
            "dividend_yield_pct",
        ]
        df = df.select(columns).unique(subset=["date", "ticker"], keep="first")
        col_names = ", ".join(columns)
        self.conn.execute(
            f"""
            INSERT INTO {self.table_name} ({col_names})
            SELECT {col_names} FROM df
            ON CONFLICT (date, ticker) DO UPDATE SET
                dividend = EXCLUDED.dividend,
                ttm_dividend = EXCLUDED.ttm_dividend,
                close = EXCLUDED.close,
                dividend_yield_pct = EXCLUDED.dividend_yield_pct
            """
        )

    def get_dividend_info(
        self,
        tickers: list[str],
        force_update: bool = False,
        stale_threshold: dt.timedelta = dt.timedelta(days=30),
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self._read_dividend_info(tickers)
        cached_tickers = df["ticker"].to_list() if not df.is_empty() else []
        refresh_tickers = (
            list(tickers) if force_update else list_difference(cached_tickers, tickers)
        )

        if not force_update and not df.is_empty():
            cutoff = dt.datetime.now(dt.timezone.utc) - stale_threshold
            for ticker, updated_at in df.select(["ticker", "updated_at"]).iter_rows():
                if updated_at is None:
                    refresh_tickers.append(ticker)
                    continue
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=dt.timezone.utc)
                if updated_at < cutoff:
                    refresh_tickers.append(ticker)

        refresh_tickers = list(dict.fromkeys(refresh_tickers))
        if refresh_tickers:
            fresh = self._download_dividends(refresh_tickers)
            self._insert_dividends(fresh)
            self.update_dividend_info(refresh_tickers, fresh)

        return self._read_dividend_info(tickers)

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
        frequency = parse_dividend_frequency(dividend_df) if not dividend_df.is_empty() else {}
        query = """
            INSERT INTO dividend_info (ticker, frequency, status, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (ticker) DO UPDATE SET
                frequency = EXCLUDED.frequency,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
        """
        for t in tickers:
            try:
                freq = frequency[t]
                params = [t, freq, True, dt.datetime.now(dt.timezone.utc)]
            except KeyError:
                params = [t, 0, False, dt.datetime.now(dt.timezone.utc)]
            self.conn.execute(query, params)


def calculate_dividend_yield(
    dividends: pl.DataFrame,
    candles: pl.DataFrame,
) -> pl.DataFrame:
    """
    Calculate dividend yield per payment using trailing-365-day dividends / close on ex-date.
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

    # TTM dividend: all payments in the trailing 365 calendar days. A fixed
    # payment count is incorrect for monthly, semiannual, and irregular payers.
    dividends = dividends.sort(["ticker", "date"]).with_columns(
        pl.col("dividend")
        .rolling_sum_by("date", window_size="365d", min_samples=1)
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
            (pl.col("ttm_dividend") / pl.col("close") * 100).round(4).alias("dividend_yield_pct")
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
        .with_columns(pl.col("date").diff().over("ticker").dt.total_days().alias("days_between"))
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
        row["ticker"]: _classify(row["median_days"]) for row in median_gaps.iter_rows(named=True)
    }
