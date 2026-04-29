import duckdb
import pandas as pd
import polars as pl
import yfinance as yf
import datetime as dt

from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, list_difference
from ..periphery.stale import get_stale_threshold


class Candles:
    def __init__(self, db_path: str = None, debug: bool = True):
        self.conn = _init_tables(db_path)
        self.table_name = "candles"
        self.debug = debug
        self._failed_tickers: set[str] = set()

    def get_candles(
        self,
        tickers: list[str],
        interval: str = "1d",
        period: str = "max",
        start: str = None,
        end: str = None,
        stale_threshold: dt.timedelta = None,
    ) -> pl.DataFrame:
        if stale_threshold is None:
            stale_threshold = get_stale_threshold(interval)
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self._read_candles(tickers, interval, start=start, end=end)

        if df.is_empty():
            fresh = self._download_candles(
                tickers, interval, period, start=start, end=end
            )
            self._insert_candles(fresh)
            return self._read_candles(tickers, interval, start=start, end=end)

        # Lightweight staleness check via SQL
        latest_dates = self._get_latest_dates(tickers, interval)
        earliest_dates = self._get_earliest_dates(tickers, interval)
        db_tickers = list(latest_dates.keys())
        missing_tickers = list_difference(db_tickers, tickers)

        needs_refresh = False
        now = dt.datetime.now()

        # Collect stale and backfill work before downloading so we can batch
        stale_groups: dict[str, list[str]] = {}   # start_date -> tickers
        backfill_groups: dict[str, list[str]] = {} # end_date   -> tickers

        for ticker, date_str in latest_dates.items():
            latest = self._parse_date(date_str)
            if (now - latest) > stale_threshold:
                start_date = latest.strftime("%Y-%m-%d")
                stale_groups.setdefault(start_date, []).append(ticker)

            if start and ticker in earliest_dates:
                earliest = self._parse_date(earliest_dates[ticker])
                requested_start = self._parse_date(start)
                if requested_start < earliest:
                    end_date = earliest.strftime("%Y-%m-%d")
                    backfill_groups.setdefault(end_date, []).append(ticker)

        # Batch download stale tickers grouped by proximity of start date
        if stale_groups:
            needs_refresh = True
            for start_date, batch in self._merge_date_groups(stale_groups).items():
                fresh = self._download_candles(batch, interval, start=start_date, end=now)
                self._insert_candles(fresh)

        # Batch download backfills grouped by their end date
        if backfill_groups:
            needs_refresh = True
            for end_date, batch in backfill_groups.items():
                fresh = self._download_candles(batch, interval, start=start, end=end_date)
                self._insert_candles(fresh)

        if missing_tickers:
            to_fetch = [t for t in missing_tickers if t not in self._failed_tickers]
            if to_fetch:
                needs_refresh = True
                fresh = self._download_candles(to_fetch, interval, period, start=start, end=end)
                self._insert_candles(fresh)
                returned = set(fresh["ticker"].unique().to_list()) if not fresh.is_empty() else set()
                self._failed_tickers.update(set(to_fetch) - returned)

        return (
            self._read_candles(tickers, interval, start=start, end=end).sort(by="date")
            if needs_refresh
            else df
        )

    def get_last_price(
        self, tickers: list[str], select_col: str = "close", alias: str = "value"
    ) -> dict[str, float]:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        df = self.get_candles(tickers)
        rows = (
            df.group_by("ticker")
            .agg(pl.col(select_col).sort_by("date").last().alias(alias))
            .iter_rows()
        )
        return {ticker: price for ticker, price in rows}

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
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        if start:
            params = {"start": start, "end": end or dt.date.today().isoformat()}
        else:
            params = {"period": period}

        print(f"Ticker: {tickers}   Params: {params}")
        data = yf.download(tickers, interval=interval, **params)
        if data.empty:
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
        df = df.drop_nulls(subset=["open", "high", "low", "close", "volume", "ticker"])
        if "datetime" in df.columns:
            df = df.rename({"datetime": "date"})

        target_cols = [
            "date",
            "ticker",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        cols_to_select = [c for c in target_cols if c in df.columns]
        return df.select(cols_to_select)

    def _read_candles(
        self, tickers: list[str], interval: str, start: str = None, end: str = None
    ) -> pl.DataFrame:
        query = "SELECT * FROM candles WHERE ticker = ANY($1) AND interval = $2"
        params = [tickers, interval]
        if start:
            query += f" AND date >= ${len(params) + 1}"
            params.append(start)
        if end:
            query += f" AND date <= ${len(params) + 1}"
            params.append(end)
        query += " ORDER BY date, ticker"
        return self.conn.execute(query, params).pl()

    def _get_latest_dates(self, tickers: list[str], interval: str) -> dict[str, str]:
        df = self.conn.execute(
            "SELECT ticker, MAX(date) as latest_date FROM candles WHERE ticker = ANY($1) AND interval = $2 GROUP BY ticker",
            [tickers, interval],
        ).pl()
        return {row[0]: row[1] for row in df.iter_rows()}

    def _get_earliest_dates(self, tickers: list[str], interval: str) -> dict[str, str]:
        df = self.conn.execute(
            "SELECT ticker, MIN(date) as earliest_date FROM candles WHERE ticker = ANY($1) AND interval = $2 GROUP BY ticker",
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
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        insert_data(
            df,
            db_cols=db_cols,
            table_name=self.table_name,
            conn=self.conn,
            pk_cols=["date", "ticker", "interval"],
        )

    @staticmethod
    def _merge_date_groups(
        groups: dict[str, list[str]], tolerance_days: int = 5
    ) -> dict[str, list[str]]:
        """
        Merge groups whose start dates fall within tolerance_days of each other
        into a single batch, using the earliest date so all tickers are covered.
        Extra rows downloaded for already-cached tickers are deduped on insert.
        """
        sorted_dates = sorted(groups.keys())
        merged: dict[str, list[str]] = {}
        cur_str = sorted_dates[0]
        cur_dt = dt.datetime.strptime(cur_str, "%Y-%m-%d")
        cur_tickers = list(groups[cur_str])

        for date_str in sorted_dates[1:]:
            d = dt.datetime.strptime(date_str, "%Y-%m-%d")
            if (d - cur_dt).days <= tolerance_days:
                cur_tickers.extend(groups[date_str])
            else:
                merged[cur_str] = cur_tickers
                cur_str, cur_dt = date_str, d
                cur_tickers = list(groups[date_str])

        merged[cur_str] = cur_tickers
        return merged

    @staticmethod
    def _parse_date(date_val) -> dt.datetime:
        if isinstance(date_val, dt.datetime):
            return date_val
        if isinstance(date_val, dt.date):
            return dt.datetime.combine(date_val, dt.time())
        if isinstance(date_val, str):
            return dt.datetime.strptime(date_val.split(" ")[0], "%Y-%m-%d")
        return dt.datetime.now()
