import pandas as pd
import polars as pl
import datetime as dt
import yfinance as yf

from dataclasses import dataclass, field
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from .candles import Candles
from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, list_difference


# ── Schema Definitions ──────────────────────────────────────────────


@dataclass
class Column:
    db_name: str
    source_name: str | None = None

    @property
    def needs_rename(self) -> bool:
        return self.source_name is not None and self.source_name != self.db_name


@dataclass
class TableSchema:
    table_name: str
    columns: list[Column]
    pk_cols: list[str] = field(default_factory=list)
    yf_attr: str = ""

    @property
    def db_cols(self) -> list[str]:
        return [c.db_name for c in self.columns]

    @property
    def source_cols(self) -> list[str]:
        return [c.source_name or c.db_name for c in self.columns]

    @property
    def rename_map(self) -> dict[str, str]:
        return {c.source_name: c.db_name for c in self.columns if c.needs_rename}

    def select_and_rename(self, df: pl.DataFrame) -> pl.DataFrame:
        available = [c for c in self.source_cols if c in df.columns]
        df = df.select(available)
        renames = {k: v for k, v in self.rename_map.items() if k in df.columns}
        return df.rename(renames) if renames else df


SCHEMAS = {
    "dates": TableSchema(
        table_name="earnings_dates",
        yf_attr="earnings_dates",
        pk_cols=["earnings_date", "ticker"],
        columns=[
            Column("earnings_date", "Earnings Date"),
            Column("ticker"),
            Column("eps_estimate", "EPS Estimate"),
            Column("reported_eps", "Reported EPS"),
            Column("surprise_pct", "Surprise(%)"),
        ],
    ),
    "estimates": TableSchema(
        table_name="earnings_estimates",
        yf_attr="earnings_estimate",
        pk_cols=[],
        columns=[
            Column("period"),
            Column("ticker"),
            Column("avg"),
            Column("low"),
            Column("high"),
            Column("year_ago_eps", "yearAgoEps"),
            Column("number_of_analysts", "numberOfAnalysts"),
            Column("growth"),
            Column("label"),
            Column("period_label"),
            Column("collected_at"),
        ],
    ),
    "history": TableSchema(
        table_name="earnings_history",
        yf_attr="earnings_history",
        pk_cols=["quarter", "ticker"],
        columns=[
            Column("quarter"),
            Column("ticker"),
            Column("eps_actual", "epsActual"),
            Column("eps_estimate", "epsEstimate"),
            Column("eps_difference", "epsDifference"),
            Column("surprise_pct", "surprisePercent"),
        ],
    ),
}


# ── Earnings Class ──────────────────────────────────────────────────


class Earnings:
    def __init__(self, db_path: str = None, candles_obj: Candles = None):
        self.conn = _init_tables(db_path)

    # ── Public getters ──────────────────────────────────────────

    def get_earnings_dates(self, tickers: list[str]) -> pl.DataFrame:
        return self._get(tickers, schema_key="dates")

    def get_earnings_estimates(self, tickers: list[str]) -> pl.DataFrame:
        return self._get(tickers, schema_key="estimates")

    def get_earnings_history(self, tickers: list[str]) -> pl.DataFrame:
        return self._get(tickers, schema_key="history")

    # ── Core get logic ──────────────────────────────────────────

    def _get(
        self,
        tickers: list[str],
        schema_key: str,
        stale_threshold: dt.timedelta = dt.timedelta(hours=36),
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        df = self._read(tickers, schema_key)

        if df.is_empty():
            data = self._download_earnings(tickers)
            self._insert(data.get(schema_key, pl.DataFrame()), schema_key)
            return self._read(tickers, schema_key)

        cached_tickers = df["ticker"].unique().to_list()
        missing_tickers = [t for t in tickers if t not in cached_tickers]

        needs_refresh = False
        if schema_key == "estimates":
            needs_refresh = self._estimates_are_stale(tickers, stale_threshold)

        if missing_tickers or needs_refresh:
            download_list = missing_tickers if not needs_refresh else tickers
            data = self._download_earnings(download_list)
            self._insert(data.get(schema_key, pl.DataFrame()), schema_key)
            return self._read(tickers, schema_key)

        return df

    def _estimates_are_stale(self, tickers: list[str], threshold: dt.timedelta) -> bool:
        result = self.conn.execute(
            """
            SELECT MIN(max_collected) as oldest_snapshot
            FROM (
                SELECT ticker, MAX(collected_at) as max_collected
                FROM earnings_estimates
                WHERE ticker = ANY($1)
                GROUP BY ticker
            )
            """,
            [tickers],
        ).pl()

        if result.is_empty() or result[0, 0] is None:
            return True

        oldest = result[0, 0]
        if isinstance(oldest, dt.datetime):
            now = dt.datetime.now(dt.timezone.utc)
            if oldest.tzinfo is None:
                oldest = oldest.replace(tzinfo=dt.timezone.utc)
            return (now - oldest) > threshold
        return True

    # ── Download ────────────────────────────────────────────────

    def _download_earnings(self, tickers: list[str]) -> dict[str, pl.DataFrame]:
        if isinstance(tickers, str):
            tickers = [tickers]

        with ThreadPoolExecutor(max_workers=min(8, len(tickers))) as pool:
            results = list(pool.map(_fetch_one, tickers))

        grouped: dict[str, list[pd.DataFrame]] = defaultdict(list)
        for r in results:
            for key, df in r.items():
                grouped[key].append(df)

        output = {}
        for key, frames in grouped.items():
            schema = SCHEMAS[key]
            combined = pl.from_pandas(pd.concat(frames), include_index=True)
            output[key] = schema.select_and_rename(combined)

        # Estimates need extra processing
        if "estimates" in output:
            output["estimates"] = resolve_earnings_periods(output["estimates"])
            output["estimates"] = output["estimates"].with_columns(
                pl.lit(dt.datetime.now()).alias("collected_at")
            )

        return output

    # ── Read ────────────────────────────────────────────────────

    def _read(self, tickers: list[str], schema_key: str) -> pl.DataFrame:
        schema = SCHEMAS[schema_key]

        if schema_key == "estimates":
            return self.conn.execute(
                f"""
                SELECT e.*
                FROM {schema.table_name} e
                INNER JOIN (
                    SELECT ticker, period, MAX(collected_at) as max_collected
                    FROM {schema.table_name}
                    WHERE ticker = ANY($1)
                    GROUP BY ticker, period
                ) latest
                ON e.ticker = latest.ticker
                AND e.period = latest.period
                AND e.collected_at = latest.max_collected
                ORDER BY e.ticker, e.period
                """,
                [tickers],
            ).pl()

        # dates and history are simple reads
        order_col = "earnings_date" if schema_key == "dates" else "quarter"
        return self.conn.execute(
            f"SELECT * FROM {schema.table_name} WHERE ticker = ANY($1) ORDER BY {order_col} DESC",
            [tickers],
        ).pl()

    # ── Insert ──────────────────────────────────────────────────

    def _insert(self, df: pl.DataFrame, schema_key: str):
        if df.is_empty():
            return
        schema = SCHEMAS[schema_key]
        insert_data(
            df,
            db_cols=schema.db_cols,
            table_name=schema.table_name,
            conn=self.conn,
            pk_cols=schema.pk_cols or None,
        )

    # ── Utils ──────────────────────────────────────────────────

    def refresh_pending_earnings(self, tickers: list[str]) -> pl.DataFrame:
        """
        Check for earnings_dates rows where reported_eps is null (not yet reported),
        re-download data for those tickers, and update the rows if results are now available.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)

        # Find tickers with unreported earnings that are in the past
        pending = self.conn.execute(
            """
            SELECT DISTINCT ticker
            FROM earnings_dates
            WHERE ticker = ANY($1)
            AND reported_eps IS NULL
            AND earnings_date <= CURRENT_TIMESTAMP
            """,
            [tickers],
        ).pl()

        if pending.is_empty():
            return self._read(tickers, "dates")

        stale_tickers = pending["ticker"].to_list()

        # Download fresh data for those tickers
        data = self._download_earnings(stale_tickers)
        dates_df = data.get("dates", pl.DataFrame())

        if dates_df.is_empty():
            return self._read(tickers, "dates")

        # Only keep rows that now have a reported_eps value
        reported = dates_df.filter(pl.col("reported_eps").is_not_null())

        if not reported.is_empty():
            # Delete the old null rows for these tickers, then insert the fresh ones
            for ticker in stale_tickers:
                self.conn.execute(
                    """
                    DELETE FROM earnings_dates
                    WHERE ticker = $1
                    AND reported_eps IS NULL
                    AND earnings_date IN (
                        SELECT earnings_date FROM df
                        WHERE ticker = $1 AND reported_eps IS NOT NULL
                    )
                    """,
                    [ticker],
                )
            # Re-insert the full fresh dataset (dedup handles already-existing rows)
            self._insert(dates_df, "dates")

        return self._read(tickers, "dates")


# ── Free Functions ──────────────────────────────────────────────────


def resolve_earnings_periods(
    df: pl.DataFrame,
    reference_date: dt.date | None = None,
) -> pl.DataFrame:
    if reference_date is None:
        reference_date = dt.date.today()

    current_year = reference_date.year
    current_quarter = (reference_date.month - 1) // 3 + 1

    QUARTER_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}

    def _resolve(period: str) -> tuple[dt.date, str, str]:
        offset_str = period[:-1]
        unit = period[-1]
        offset = int(offset_str)

        if unit == "q":
            total_q = current_quarter + offset
            year = current_year + (total_q - 1) // 4
            quarter = ((total_q - 1) % 4) + 1
            month, day = QUARTER_END[quarter]
            resolved = dt.date(year, month, day)
        else:
            year = current_year + offset
            resolved = dt.date(year, 12, 31)

        label = "current" if offset == 0 else "future"
        period_label = "Q" if unit == "q" else "A"
        return resolved, label, period_label

    periods = df["period"].to_list()
    resolved, labels, period_labels = zip(*[_resolve(p) for p in periods])

    return df.with_columns(
        pl.Series("period", list(resolved), dtype=pl.Date),
        pl.Series("label", list(labels), dtype=pl.Utf8),
        pl.Series("period_label", list(period_labels), dtype=pl.String),
    )


def _fetch_one(ticker: str) -> dict[str, pd.DataFrame]:
    obj = yf.Ticker(ticker)
    result = {}
    for key, schema in SCHEMAS.items():
        df = getattr(obj, schema.yf_attr)
        df["ticker"] = ticker
        result[key] = df
    return result
