import duckdb
import pandas as pd
import polars as pl
import yfinance as yf
import datetime as dt

from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, list_difference


class Candles:
    def __init__(self, db_path: str = None, debug: bool = True):
        self.conn = _init_tables(db_path)
        self.table_name = "candles"
        self.debug = debug

    def get_candles(
        self,
        tickers: list[str],
        interval: str = "1d",
        period: str = "max",
        stale_threshold: dt.timedelta = dt.timedelta(hours=36),
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self._read_candles(tickers, interval)
        print(f"Local: {df}")

        if self.debug:
            print(f"Local: {df.shape}")

        if df.is_empty():
            fresh = self._download_candles(tickers, interval, period)
            print(f"Fresh1: {fresh}")
            self._insert_candles(fresh)
            return self._read_candles(tickers, interval)

        # Lightweight staleness check via SQL
        latest_dates = self._get_latest_dates(tickers, interval)
        db_tickers = list(latest_dates.keys())
        missing_tickers = list_difference(db_tickers, tickers)

        needs_refresh = False
        now = dt.datetime.now()

        for ticker, date_str in latest_dates.items():
            latest = self._parse_date(date_str)
            if (now - latest) > stale_threshold:
                needs_refresh = True
                start_date = latest.strftime("%Y-%m-%d")
                if self.debug:
                    print(f"Stale: {ticker} (last: {start_date})")
                fresh = self._download_candles([ticker], interval, start=start_date)
                print(f"Fresh: {fresh}")
                self._insert_candles(fresh)

        if missing_tickers:
            needs_refresh = True
            if self.debug:
                print(f"Missing: {missing_tickers}")
            fresh = self._download_candles(missing_tickers, interval, period)
            self._insert_candles(fresh)

        return self._read_candles(tickers, interval) if needs_refresh else df

    def get_last_price(self, tickers: list[str]) -> dict[str, float]:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        # Try lightweight SQL query first
        prices = self._get_latest_prices(tickers)

        # Fall back to full get_candles for any missing tickers
        missing = [t for t in tickers if t not in prices]
        if missing:
            df = self.get_candles(missing)
            rows = (
                df.sort("date")
                .group_by("ticker")
                .agg(pl.col("close").last().alias("price"))
                .iter_rows()
            )
            prices.update({ticker: price for ticker, price in rows})

        return prices

    def get_first_price(
        self, tickers: list[str], select_col: str = "close", alias: str = "value"
    ) -> dict[str, float]:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        df = self.get_candles(tickers)
        rows = (
            df.group_by("ticker")
            .agg(pl.col(select_col).sort_by("date").first().alias(alias))
            .iter_rows()
        )
        return {ticker: price for ticker, price in rows}

    def _download_candles(
        self,
        tickers: list[str],
        interval: str = "1d",
        period: str = "max",
        start: str = None,
        end: str = None,
    ) -> pl.DataFrame:
        if self.debug:
            print(f"Downloading: {tickers}")
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        if start:
            params = {"start": start, "end": end or dt.date.today().isoformat()}
        else:
            params = {"period": period}

        # Download with error handling
        data = yf.download(tickers, interval=interval, **params)
        print(f"Data: {data}")
        if data.empty:
            # If batch failed, try individual downloads to salvage what we can
            if len(tickers) > 1:
                frames = []
                for ticker in tickers:
                    try:
                        single = yf.download(ticker, interval=interval, **params)
                        if not single.empty:
                            single = single.reset_index()
                            if "Ticker" not in single.columns:
                                single["Ticker"] = ticker
                            frames.append(single)
                        elif self.debug:
                            print(f"Skipping {ticker}: no data")
                    except Exception as e:
                        if self.debug:
                            print(f"Failed {ticker}: {e}")
                if not frames:
                    return pl.DataFrame()
                data = pd.concat(frames)
                data["Interval"] = interval
            else:
                return pl.DataFrame()
        else:
            if isinstance(data.columns, pd.MultiIndex):
                data = data.stack(level=1, future_stack=True).reset_index()
            else:
                data = data.reset_index()
                if "Ticker" not in data.columns:
                    data["Ticker"] = tickers[0]
            data["Interval"] = interval

        df = pl.from_pandas(data)
        df = df.rename({c: c.lower() for c in df.columns})
        df = df.drop_nulls(subset=["close", "open", "low", "high", "volume", "ticker"])

        target_cols = [
            "date",
            "ticker",
            "interval",
            "close",
            "open",
            "low",
            "high",
            "volume",
        ]
        cols_to_select = [c for c in target_cols if c in df.columns]
        return df.select(cols_to_select)

    def _read_candles(self, tickers: list[str], interval: str) -> pl.DataFrame:
        return self.conn.execute(
            "SELECT * FROM candles WHERE ticker = ANY($1) AND interval = $2",
            [tickers, interval],
        ).pl()

    def _get_latest_dates(self, tickers: list[str], interval: str) -> dict[str, str]:
        df = self.conn.execute(
            "SELECT ticker, MAX(date) as latest_date FROM candles WHERE ticker = ANY($1) AND interval = $2 GROUP BY ticker",
            [tickers, interval],
        ).pl()
        return {row[0]: row[1] for row in df.iter_rows()}

    def _get_latest_prices(
        self, tickers: list[str], interval: str = "1d"
    ) -> dict[str, float]:
        df = self.conn.execute(
            """
            SELECT c.ticker, c.close
            FROM candles c
            INNER JOIN (
                SELECT ticker, MAX(date) as max_date
                FROM candles
                WHERE ticker = ANY($1) AND interval = $2
                GROUP BY ticker
            ) latest ON c.ticker = latest.ticker AND c.date = latest.max_date AND c.interval = $3
        """,
            [tickers, interval, interval],
        ).pl()
        return {row[0]: row[1] for row in df.iter_rows()}

    def _insert_candles(self, df: pl.DataFrame):
        if df.is_empty():
            return
        db_cols = [
            "date",
            "ticker",
            "interval",
            "close",
            "open",
            "low",
            "high",
            "volume",
        ]
        insert_data(
            df,
            db_cols=db_cols,
            table_name=self.table_name,
            conn=self.conn,
            pk_cols=["date", "ticker", "interval"],
        )
        if self.debug:
            print(f"Inserted {len(df)} records")

    @staticmethod
    def _parse_date(date_val) -> dt.datetime:
        if isinstance(date_val, dt.datetime):
            return date_val
        if isinstance(date_val, dt.date):
            return dt.datetime.combine(date_val, dt.time())
        if isinstance(date_val, str):
            return dt.datetime.strptime(date_val.split(" ")[0], "%Y-%m-%d")
        return dt.datetime.now()
