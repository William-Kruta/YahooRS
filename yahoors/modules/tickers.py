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

COMPANY_INFO_DYNAMIC_STALE_THRESHOLD = dt.timedelta(days=90)


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
        return self._get_cached(
            "income",
            lambda: self._statements_obj.get_income_statement(self.ticker, period="A"),
        )

    @property
    def quarterly_income_statement(self) -> pl.DataFrame:
        return self._get_cached(
            "income_q",
            lambda: self._statements_obj.get_income_statement(self.ticker, period="Q"),
        )

    @property
    def balance_sheet(self) -> pl.DataFrame:
        return self._get_cached(
            "balance",
            lambda: self._statements_obj.get_balance_sheet(self.ticker, period="A"),
        )

    @property
    def quarterly_balance_sheet(self) -> pl.DataFrame:
        return self._get_cached(
            "balance_q",
            lambda: self._statements_obj.get_balance_sheet(self.ticker, period="Q"),
        )

    @property
    def cash_flow(self) -> pl.DataFrame:
        return self._get_cached(
            "cash_flow",
            lambda: self._statements_obj.get_cash_flow(self.ticker, period="A"),
        )

    @property
    def quarterly_cash_flow(self) -> pl.DataFrame:
        return self._get_cached(
            "cash_flow_q",
            lambda: self._statements_obj.get_cash_flow(self.ticker, period="Q"),
        )

    @property
    def ratios(self) -> pl.DataFrame:
        return self._get_cached(
            "ratios", lambda: self._statements_obj.get_ratios(self.ticker, period="A")
        )

    @property
    def quarterly_ratios(self) -> pl.DataFrame:
        return self._get_cached(
            "ratios_q", lambda: self._statements_obj.get_ratios(self.ticker, period="Q")
        )

    @property
    def margins(self) -> pl.DataFrame:
        return self._get_cached(
            "margins", lambda: self._statements_obj.get_margins(self.ticker, period="A")
        )

    @property
    def quarterly_margins(self) -> pl.DataFrame:
        return self._get_cached(
            "margins_q",
            lambda: self._statements_obj.get_margins(self.ticker, period="Q"),
        )

    @property
    def info(self) -> pl.DataFrame:
        return self._get_cached(
            "info", lambda: _get_info(self.ticker, self.table_name, self.conn)
        )

    @property
    def trading_status(self) -> pl.DataFrame:
        return self._get_cached(
            "trading_status",
            lambda: _get_trading_status(self.ticker, self.table_name, self.conn),
        )

    def update_trading_status(self, is_active: bool):
        _update_trading_status(self.ticker, is_active, self.table_name, self.conn)
        self._cache.pop("trading_status", None)
        self._cache.pop("info", None)

    def update_ceo(self, ceo: str | None):
        _update_company_info_fields(
            self.ticker, {"ceo": ceo}, self.table_name, self.conn
        )
        self._cache.pop("info", None)

    def update_full_time_employees(self, full_time_employees: int | None):
        _update_company_info_fields(
            self.ticker,
            {"full_time_employees": full_time_employees},
            self.table_name,
            self.conn,
        )
        self._cache.pop("info", None)

    def update_dynamic_info(
        self,
        refresh_ceo: bool = True,
        refresh_full_time_employees: bool = True,
        refresh_trading_status: bool = True,
    ) -> pl.DataFrame:
        _refresh_dynamic_company_info(
            self.ticker,
            self.table_name,
            self.conn,
            refresh_ceo=refresh_ceo,
            refresh_full_time_employees=refresh_full_time_employees,
            refresh_trading_status=refresh_trading_status,
        )
        self._cache.pop("info", None)
        self._cache.pop("trading_status", None)
        return _read_ticker_info(self.ticker, self.table_name, self.conn)

    def force_update(self) -> pl.DataFrame:
        _force_update_company_info(self.ticker, self.table_name, self.conn)
        self._cache.pop("info", None)
        self._cache.pop("trading_status", None)
        return _read_ticker_info(self.ticker, self.table_name, self.conn)

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

    def update_dynamic_info(
        self,
        tickers: list[str] | str,
        refresh_ceo: bool = True,
        refresh_full_time_employees: bool = True,
        refresh_trading_status: bool = True,
    ) -> pl.DataFrame:
        tickers = clean_tickers([tickers] if isinstance(tickers, str) else tickers)
        _refresh_dynamic_company_info_batch(
            tickers,
            self.table_name,
            self.conn,
            refresh_ceo=refresh_ceo,
            refresh_full_time_employees=refresh_full_time_employees,
            refresh_trading_status=refresh_trading_status,
            candle_obj=self.candles,
        )
        return _read_ticker_info(tickers, self.table_name, self.conn)

    def force_update(self, tickers: list[str] | str) -> pl.DataFrame:
        tickers = clean_tickers([tickers] if isinstance(tickers, str) else tickers)
        _force_update_company_info(tickers, self.table_name, self.conn, candle_obj=self.candles)
        return _read_ticker_info(tickers, self.table_name, self.conn)


def _get_info(ticker: str, table_name: str, conn: duckdb.DuckDBPyConnection):
    if isinstance(ticker, str):
        ticker = [ticker]
    df = _read_ticker_info(ticker, table_name, conn)
    if df.is_empty():
        df = _download_ticker_info(ticker)
        _insert_ticker_info(df, table_name, conn)
        return _read_ticker_info(ticker, table_name, conn)

    stale_tickers = _get_stale_dynamic_company_info_tickers(
        ticker,
        table_name,
        conn,
        threshold=COMPANY_INFO_DYNAMIC_STALE_THRESHOLD,
    )
    if stale_tickers:
        _refresh_dynamic_company_info_batch(stale_tickers, table_name, conn)
        return _read_ticker_info(ticker, table_name, conn)
    return df


def _download_ticker_info(
    tickers: list[str], candle_obj: Candles | None = None
):
    if isinstance(tickers, str):
        tickers = [tickers]

    candles = candle_obj or Candles()
    # Batch fetch first prices and candles upfront
    first_date = candles.get_first_price(tickers, select_col="date", alias="value")
    last_date = candles.get_last_price(tickers, select_col="date", alias="value")
    data = []
    now = dt.datetime.now()
    for t in tickers:
        try:
            obj = yf.Ticker(t)
            info = obj.info
            officers = info.get("companyOfficers") or []

            try:
                ticker_first_date = first_date[t]
                ticker_last_date = last_date[t]
                delta = now - ticker_last_date
                if delta.days < 30:
                    status = True
                else:
                    status = False
            except KeyError:
                status = False
                ticker_first_date = None
            data.append(
                {
                    "symbol": t,
                    "name": info.get("longName", ""),
                    "sector": info.get("sector", ""),
                    "industry": info.get("industry"),
                    "ceo": _extract_ceo_name(officers),
                    "country": info.get("country", ""),
                    "website": info.get("website", ""),
                    "business_summary": info.get("longBusinessSummary", ""),
                    "full_time_employees": info.get("fullTimeEmployees"),
                    "first_trading_day": ticker_first_date,
                    "trading_status": status,
                    "asset_type": info.get("quoteType"),
                }
            )
        except Exception:
            break

    df = pl.DataFrame(data)
    now_utc = dt.datetime.now(dt.timezone.utc)
    df = df.with_columns(
        pl.lit(now_utc).alias("dynamic_updated_at"),
        pl.lit(now_utc).alias("updated_at"),
    )
    return df


def _read_ticker_info(
    ticker: str, table_name: str, conn: duckdb.DuckDBPyConnection
) -> pl.DataFrame:
    if isinstance(ticker, str):
        ticker = [ticker]
    return conn.execute(
        f"SELECT * FROM {table_name} WHERE symbol = ANY($1)", [ticker]
    ).pl()


def _insert_ticker_info(
    df: pl.DataFrame, table_name: str, conn: duckdb.DuckDBPyConnection
):
    if df.is_empty():
        return

    db_cols = [
        "symbol",
        "name",
        "sector",
        "industry",
        "ceo",
        "country",
        "website",
        "business_summary",
        "full_time_employees",
        "first_trading_day",
        "asset_type",
        "trading_status",
        "dynamic_updated_at",
        "updated_at",
    ]
    final_cols = [c for c in db_cols if c in df.columns]
    col_names = ", ".join(final_cols)
    update_set = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in final_cols if c != "symbol"]
    )
    conn.execute(
        f"""
        INSERT INTO {table_name} ({col_names})
        SELECT {col_names} FROM df
        ON CONFLICT(symbol) DO UPDATE SET
            {update_set}
        """
    )


def _extract_ceo_name(officers: list[dict]) -> str | None:
    if not officers:
        return None

    for pattern in ("chief executive officer", "ceo"):
        for officer in officers:
            title = str(officer.get("title") or "").lower()
            if pattern in title:
                name = officer.get("name")
                return str(name) if name else None

    return None


def _get_trading_status(
    tickers: list[str], table_name: str, conn: duckdb.DuckDBPyConnection
) -> pl.DataFrame:
    """Get the trading status for a list of tickers."""
    if isinstance(tickers, str):
        tickers = [tickers]
    return conn.execute(
        f"SELECT symbol, trading_status FROM {table_name} WHERE symbol = ANY($1)",
        [tickers],
    ).pl()


def _update_trading_status(
    ticker: list[str], is_active: bool, table_name: str, conn: duckdb.DuckDBPyConnection
) -> None:
    """Update the trading status for a given ticker."""
    _update_company_info_fields(
        ticker, {"trading_status": is_active}, table_name, conn
    )


def _update_company_info_fields(
    ticker: list[str] | str,
    fields: dict,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    allowed_fields = {"ceo", "full_time_employees", "trading_status"}
    updates = {k: v for k, v in fields.items() if k in allowed_fields}
    if not updates:
        return

    if isinstance(ticker, str):
        ticker = [ticker]

    set_clauses = [f"{col} = ?" for col in updates]
    set_clauses.append("dynamic_updated_at = NOW()")
    params = list(updates.values()) + [ticker]
    conn.execute(
        f"""
        UPDATE {table_name}
        SET {", ".join(set_clauses)}
        WHERE symbol = ANY(?)
        """,
        params,
    )


def _get_stale_dynamic_company_info_tickers(
    tickers: list[str] | str,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
    threshold: dt.timedelta,
) -> list[str]:
    if isinstance(tickers, str):
        tickers = [tickers]

    result = conn.execute(
        f"""
        SELECT symbol
        FROM {table_name}
        WHERE symbol = ANY($1)
        AND (
            dynamic_updated_at IS NULL
            OR dynamic_updated_at < $2
        )
        """,
        [tickers, dt.datetime.now(dt.timezone.utc) - threshold],
    ).pl()
    if result.is_empty():
        return []
    return result["symbol"].to_list()


def _refresh_dynamic_company_info(
    ticker: str,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
    refresh_ceo: bool = True,
    refresh_full_time_employees: bool = True,
    refresh_trading_status: bool = True,
) -> None:
    fresh = _download_ticker_info([ticker])
    if fresh.is_empty():
        return

    row = fresh.row(0, named=True)
    updates = {}
    if refresh_ceo:
        updates["ceo"] = row.get("ceo")
    if refresh_full_time_employees:
        updates["full_time_employees"] = row.get("full_time_employees")
    if refresh_trading_status:
        updates["trading_status"] = row.get("trading_status")
    _update_company_info_fields(ticker, updates, table_name, conn)


def _refresh_dynamic_company_info_batch(
    tickers: list[str],
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
    refresh_ceo: bool = True,
    refresh_full_time_employees: bool = True,
    refresh_trading_status: bool = True,
    candle_obj: Candles | None = None,
) -> None:
    if not tickers:
        return

    fresh = _download_ticker_info(tickers, candle_obj=candle_obj)
    if fresh.is_empty():
        return

    for row in fresh.iter_rows(named=True):
        updates = {}
        if refresh_ceo:
            updates["ceo"] = row.get("ceo")
        if refresh_full_time_employees:
            updates["full_time_employees"] = row.get("full_time_employees")
        if refresh_trading_status:
            updates["trading_status"] = row.get("trading_status")
        _update_company_info_fields(row["symbol"], updates, table_name, conn)


def _force_update_company_info(
    tickers: list[str] | str,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
    candle_obj: Candles | None = None,
) -> None:
    fresh = _download_ticker_info(tickers, candle_obj=candle_obj)
    if fresh.is_empty():
        return
    _insert_ticker_info(fresh, table_name, conn)


def _delete_info(
    ticker: list[str], table_name: str, conn: duckdb.DuckDBPyConnection
) -> None:
    """Delete the info for a given ticker."""
    if isinstance(ticker, str):
        ticker = [ticker]
    conn.execute(f"DELETE FROM {table_name} WHERE symbol = ANY($1)", [ticker])
