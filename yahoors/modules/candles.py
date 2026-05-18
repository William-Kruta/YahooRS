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
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        if stale_threshold is None:
            stale_threshold = get_stale_threshold(interval)
        self._ensure_fresh(tickers, interval, period, start, end, stale_threshold)
        return self._read_candles(tickers, interval, start=start, end=end).sort(by="date")

    def _ensure_fresh(
        self,
        tickers: list[str],
        interval: str = "1d",
        period: str = "max",
        start: str = None,
        end: str = None,
        stale_threshold: dt.timedelta = None,
    ) -> None:
        if stale_threshold is None:
            stale_threshold = get_stale_threshold(interval)

        if not self._has_candles(tickers, interval):
            fresh = self._download_candles(tickers, interval, period, start=start, end=end)
            self._insert_candles(fresh)
            return

        ticker_stats = self._get_ticker_stats(tickers, interval)
        db_tickers = list(ticker_stats.keys())
        missing_tickers = list_difference(db_tickers, tickers)

        now = dt.datetime.now(dt.timezone.utc)
        requested_start = self._parse_date(start) if start else None

        stale_groups: dict[str, list[str]] = {}
        backfill_groups: dict[str, list[str]] = {}

        for ticker, (latest_candle, latest_collected, earliest_candle) in ticker_stats.items():
            if (now - self._parse_date(latest_collected)) > stale_threshold:
                start_date = self._parse_date(latest_candle).strftime("%Y-%m-%d")
                stale_groups.setdefault(start_date, []).append(ticker)

            if requested_start is not None and self._parse_date(earliest_candle) > requested_start:
                end_date = self._parse_date(earliest_candle).strftime("%Y-%m-%d")
                backfill_groups.setdefault(end_date, []).append(ticker)

        if stale_groups:
            for start_date, batch in self._merge_date_groups(stale_groups).items():
                fresh = self._download_candles(batch, interval, start=start_date, end=now)
                self._insert_candles(fresh)

        if backfill_groups:
            for end_date, batch in backfill_groups.items():
                fresh = self._download_candles(batch, interval, start=start, end=end_date)
                self._insert_candles(fresh)

        if missing_tickers:
            to_fetch = [t for t in missing_tickers if t not in self._failed_tickers]
            if to_fetch:
                fresh = self._download_candles(to_fetch, interval, period, start=start, end=end)
                self._insert_candles(fresh)
                returned = set(fresh["ticker"].unique().to_list()) if not fresh.is_empty() else set()
                self._failed_tickers.update(set(to_fetch) - returned)

    def get_last_price(
        self, tickers: list[str], select_col: str = "close", alias: str = "value"
    ) -> dict[str, float]:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        start = (dt.date.today() - dt.timedelta(days=7)).isoformat()
        self._ensure_fresh(tickers, start=start)
        df = self.conn.execute(
            f"SELECT ticker, arg_max({select_col}, date) AS {alias} FROM candles WHERE ticker = ANY($1) AND interval = $2 GROUP BY ticker",
            [tickers, "1d"],
        ).pl()
        return {row[0]: row[1] for row in df.iter_rows()}

    def get_first_price(
        self, tickers: list[str], select_col: str = "close", alias: str = "value"
    ) -> dict[str, float]:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        self._ensure_fresh(tickers)
        df = self.conn.execute(
            f"SELECT ticker, arg_min({select_col}, date) AS {alias} FROM candles WHERE ticker = ANY($1) AND interval = $2 GROUP BY ticker",
            [tickers, "1d"],
        ).pl()
        return {row[0]: row[1] for row in df.iter_rows()}

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
        df = df.select(cols_to_select)
        df = df.with_columns(
            pl.lit(dt.datetime.now(dt.timezone.utc)).alias("collected_at")
        )
        return df

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

    def _has_candles(self, tickers: list[str], interval: str) -> bool:
        return (
            self.conn.execute(
                "SELECT 1 FROM candles WHERE ticker = ANY($1) AND interval = $2 LIMIT 1",
                [tickers, interval],
            ).fetchone()
            is not None
        )

    def _get_ticker_stats(
        self, tickers: list[str], interval: str
    ) -> dict[str, tuple]:
        df = self.conn.execute(
            """
            SELECT
                ticker,
                MAX(date)                        AS latest_candle,
                MAX(COALESCE(collected_at, date)) AS latest_collected,
                MIN(date)                        AS earliest_candle
            FROM candles
            WHERE ticker = ANY($1) AND interval = $2
            GROUP BY ticker
            """,
            [tickers, interval],
        ).pl()
        return {row[0]: (row[1], row[2], row[3]) for row in df.iter_rows()}

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
            "collected_at",
        ]
        final_cols = [c for c in db_cols if c in df.columns]
        df = df.select(final_cols)
        col_names = ", ".join(final_cols)
        self.conn.execute(
            f"""
            INSERT INTO candles ({col_names})
            SELECT {col_names} FROM df
            ON CONFLICT (date, ticker, interval) DO UPDATE SET collected_at = excluded.collected_at
            """
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
            return date_val if date_val.tzinfo else date_val.replace(tzinfo=dt.timezone.utc)
        if isinstance(date_val, dt.date):
            return dt.datetime.combine(date_val, dt.time(), tzinfo=dt.timezone.utc)
        if isinstance(date_val, str):
            parsed = dt.datetime.fromisoformat(date_val)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
        return dt.datetime.now(dt.timezone.utc)
