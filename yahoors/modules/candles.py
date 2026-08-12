import pandas as pd
import polars as pl
import yfinance as yf
import datetime as dt
import re

from ..periphery.db import _init_tables
from ..periphery.utils import clean_tickers, list_difference
from ..periphery.stale import get_stale_threshold


_CANDLE_VALUE_COLUMNS = frozenset(
    {"date", "open", "high", "low", "close", "volume", "collected_at"}
)
_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Candles:
    def __init__(self, db_path: str = None, debug: bool = True):
        self.conn = _init_tables(db_path)
        self.table_name = "candles"
        self.debug = debug
        self._failed_tickers: set[str] = set()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Candles":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def get_candles(
        self,
        tickers: list[str],
        interval: str = "1d",
        period: str = "max",
        start: str = None,
        end: str = None,
        stale_threshold: dt.timedelta = None,
        force_update: bool = False,
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        if stale_threshold is None:
            stale_threshold = get_stale_threshold(interval)
        self._ensure_fresh(
            tickers,
            interval,
            period,
            start,
            end,
            stale_threshold,
            force_update=force_update,
        )
        return self._read_candles(tickers, interval, start=start, end=end).sort(by="date")

    def _ensure_fresh(
        self,
        tickers: list[str],
        interval: str = "1d",
        period: str = "max",
        start: str = None,
        end: str = None,
        stale_threshold: dt.timedelta = None,
        force_update: bool = False,
    ) -> None:
        if stale_threshold is None:
            stale_threshold = get_stale_threshold(interval)

        requested_start = self._requested_start(start=start, period=period, end=end)
        full_history_requested = start is None and end is None and period.lower() == "max"

        if force_update:
            fresh = self._download_candles(tickers, interval, period, start=start, end=end)
            self._insert_candles(fresh)
            self._record_cache_coverage(
                tickers,
                fresh,
                interval,
                coverage_start=requested_start,
                full_history=full_history_requested,
            )
            return

        if not self._has_candles(tickers, interval):
            fresh = self._download_candles(tickers, interval, period, start=start, end=end)
            self._insert_candles(fresh)
            self._record_cache_coverage(
                tickers,
                fresh,
                interval,
                coverage_start=requested_start,
                full_history=full_history_requested,
            )
            return

        ticker_stats = self._get_ticker_stats(tickers, interval)
        cache_coverage = self._get_cache_coverage(tickers, interval)
        db_tickers = list(ticker_stats.keys())
        missing_tickers = list_difference(db_tickers, tickers)

        now = dt.datetime.now(dt.timezone.utc)

        stale_groups: dict[str, list[str]] = {}
        backfill_groups: dict[str, list[str]] = {}
        full_history_tickers: list[str] = []

        for ticker, (latest_candle, latest_collected, earliest_candle) in ticker_stats.items():
            coverage_start, has_full_history = cache_coverage.get(ticker, (None, False))
            needs_full_history = full_history_requested and not has_full_history

            if needs_full_history:
                full_history_tickers.append(ticker)
            elif (now - self._parse_date(latest_collected)) > stale_threshold:
                start_date = self._parse_date(latest_candle).strftime("%Y-%m-%d")
                stale_groups.setdefault(start_date, []).append(ticker)

            effective_coverage_start = (
                self._parse_date(coverage_start)
                if coverage_start is not None
                else self._parse_date(earliest_candle)
            )
            if (
                not needs_full_history
                and requested_start is not None
                and effective_coverage_start > requested_start
            ):
                end_date = self._parse_date(earliest_candle).strftime("%Y-%m-%d")
                backfill_groups.setdefault(end_date, []).append(ticker)

        if stale_groups:
            for start_date, batch in self._merge_date_groups(stale_groups).items():
                fresh = self._download_candles(batch, interval, start=start_date, end=now)
                self._insert_candles(fresh)

        if full_history_tickers:
            fresh = self._download_candles(full_history_tickers, interval, period="max")
            self._insert_candles(fresh)
            self._record_cache_coverage(
                full_history_tickers,
                fresh,
                interval,
                coverage_start=None,
                full_history=True,
            )

        if backfill_groups:
            backfill_start = requested_start.strftime("%Y-%m-%d")
            for end_date, batch in backfill_groups.items():
                fresh = self._download_candles(batch, interval, start=backfill_start, end=end_date)
                self._insert_candles(fresh)
                self._record_cache_coverage(
                    batch,
                    fresh,
                    interval,
                    coverage_start=requested_start,
                    full_history=False,
                )

        if missing_tickers:
            to_fetch = [t for t in missing_tickers if t not in self._failed_tickers]
            if to_fetch:
                fresh = self._download_candles(to_fetch, interval, period, start=start, end=end)
                self._insert_candles(fresh)
                self._record_cache_coverage(
                    to_fetch,
                    fresh,
                    interval,
                    coverage_start=requested_start,
                    full_history=full_history_requested,
                )
                returned = (
                    set(fresh["ticker"].unique().to_list()) if not fresh.is_empty() else set()
                )
                self._failed_tickers.update(set(to_fetch) - returned)

    def get_last_price(
        self,
        tickers: list[str],
        select_col: str = "close",
        alias: str = "value",
        force_update: bool = False,
    ) -> dict[str, float]:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        self._validate_value_query(select_col, alias)
        start = (dt.date.today() - dt.timedelta(days=7)).isoformat()
        self._ensure_fresh(tickers, start=start, force_update=force_update)
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
        self._validate_value_query(select_col, alias)
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

        if self.debug:
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
        df = df.with_columns(pl.lit(dt.datetime.now(dt.timezone.utc)).alias("collected_at"))
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

    def _get_ticker_stats(self, tickers: list[str], interval: str) -> dict[str, tuple]:
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

    def _get_cache_coverage(
        self, tickers: list[str], interval: str
    ) -> dict[str, tuple[dt.datetime | None, bool]]:
        df = self.conn.execute(
            """
            SELECT ticker, coverage_start, full_history
            FROM candle_cache_state
            WHERE ticker = ANY($1) AND interval = $2
            """,
            [tickers, interval],
        ).pl()
        return {row[0]: (row[1], row[2]) for row in df.iter_rows()}

    def _record_cache_coverage(
        self,
        requested_tickers: list[str],
        df: pl.DataFrame,
        interval: str,
        coverage_start: dt.datetime | None,
        full_history: bool,
    ) -> None:
        if df.is_empty() or "ticker" not in df.columns:
            return

        returned_tickers = set(df["ticker"].unique().to_list())
        updated_at = dt.datetime.now(dt.timezone.utc)
        for ticker in requested_tickers:
            if ticker not in returned_tickers:
                continue
            self.conn.execute(
                """
                INSERT INTO candle_cache_state (
                    ticker, interval, coverage_start, full_history, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (ticker, interval) DO UPDATE SET
                    coverage_start = CASE
                        WHEN candle_cache_state.coverage_start IS NULL
                            THEN EXCLUDED.coverage_start
                        WHEN EXCLUDED.coverage_start IS NULL
                            THEN candle_cache_state.coverage_start
                        ELSE LEAST(
                            candle_cache_state.coverage_start,
                            EXCLUDED.coverage_start
                        )
                    END,
                    full_history = (
                        candle_cache_state.full_history OR EXCLUDED.full_history
                    ),
                    updated_at = EXCLUDED.updated_at
                """,
                [ticker, interval, coverage_start, full_history, updated_at],
            )

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
            ON CONFLICT (date, ticker, interval) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                collected_at = excluded.collected_at
            """
        )

    @staticmethod
    def _validate_value_query(select_col: str, alias: str) -> None:
        if select_col not in _CANDLE_VALUE_COLUMNS:
            allowed = ", ".join(sorted(_CANDLE_VALUE_COLUMNS))
            raise ValueError(f"select_col must be one of: {allowed}")
        if not _SQL_IDENTIFIER.fullmatch(alias):
            raise ValueError(
                "alias must contain only ASCII letters, digits, and underscores, "
                "and may not start with a digit"
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

    @classmethod
    def _requested_start(
        cls,
        start: str | None,
        period: str,
        end: str | None,
    ) -> dt.datetime | None:
        if start:
            return cls._parse_date(start)

        normalized = period.lower()
        if normalized == "max":
            return None

        reference = cls._parse_date(end) if end else dt.datetime.now(dt.timezone.utc)
        if normalized == "ytd":
            return reference.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

        period_days = {
            "1d": 1,
            "5d": 7,
            "1mo": 31,
            "3mo": 93,
            "6mo": 186,
            "1y": 366,
            "2y": 732,
            "5y": 1830,
            "10y": 3660,
        }
        days = period_days.get(normalized)
        return reference - dt.timedelta(days=days) if days is not None else None
