import pandas as pd
import polars as pl
import duckdb
from .config import get_db_path


def _init_tables(db_path: str = None) -> duckdb.DuckDBPyConnection:
    if db_path is None:
        db_path = str(get_db_path())
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            date        TIMESTAMP   NOT NULL,
            ticker      VARCHAR     NOT NULL,
            interval    VARCHAR     NOT NULL,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            close       DOUBLE,
            volume      DOUBLE,
            PRIMARY KEY (date, ticker, interval)
        );

        CREATE TABLE IF NOT EXISTS dividends (
            date        TIMESTAMP   NOT NULL,
            ticker      VARCHAR     NOT NULL,
            dividend    DOUBLE      NOT NULL,
            ttm_dividend DOUBLE      NOT NULL,
            close       DOUBLE      NOT NULL,
            dividend_yield_pct DOUBLE      NOT NULL,
            PRIMARY KEY (date, ticker)
        );

        CREATE TABLE IF NOT EXISTS dividend_info (
            ticker          VARCHAR     NOT NULL,
            frequency       INTEGER     NOT NULL,
            status          BOOLEAN     NOT NULL,
            updated_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (ticker)
        );
        
        CREATE TABLE IF NOT EXISTS options (
            contract_symbol    VARCHAR     NOT NULL,
            last_trade_date    TIMESTAMPTZ NOT NULL,
            strike             DOUBLE      NOT NULL,
            stock_price        DOUBLE      NOT NULL,
            last_price         DOUBLE      NOT NULL,
            bid                DOUBLE      NOT NULL,
            ask                DOUBLE      NOT NULL,
            volume             BIGINT      NOT NULL,
            open_interest      BIGINT      NOT NULL,
            implied_volatility DOUBLE      NOT NULL,
            in_the_money       BOOLEAN     NOT NULL,
            expiration         DATE        NOT NULL,
            option_type        VARCHAR     NOT NULL,
            ticker             VARCHAR     NOT NULL,
            dte                INTEGER     NOT NULL,
            collected_at       TIMESTAMPTZ NOT NULL,
            delta              DOUBLE      NOT NULL,
            gamma              DOUBLE      NOT NULL,
            theta              DOUBLE      NOT NULL,
            vega               DOUBLE      NOT NULL,
            bs_price           DOUBLE      NOT NULL,
            prob_profit        DOUBLE      NOT NULL,
            hist_prob_profit   DOUBLE,
            PRIMARY KEY (contract_symbol, collected_at)
        );

        CREATE TABLE IF NOT EXISTS statements (
            date            TIMESTAMP   NOT NULL,
            ticker          VARCHAR     NOT NULL,
            label           VARCHAR     NOT NULL,
            value           DOUBLE      NOT NULL,
            statement_type  VARCHAR     NOT NULL,
            period          VARCHAR     NOT NULL DEFAULT 'A',
            PRIMARY KEY (date, ticker, label, statement_type, period)
        );

        CREATE TABLE IF NOT EXISTS company_info (
            symbol            VARCHAR PRIMARY KEY,
            name              VARCHAR NOT NULL,
            sector            VARCHAR,
            industry          VARCHAR,
            country           VARCHAR,
            website           VARCHAR,
            business_summary  TEXT,
            first_trading_day TIMESTAMP,
            asset_type        VARCHAR,
            trading_status    BOOLEAN,
            updated_at        TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS earnings_dates (
            earnings_date   TIMESTAMPTZ NOT NULL,
            ticker          VARCHAR     NOT NULL,
            eps_estimate    DOUBLE,
            reported_eps    DOUBLE,
            surprise_pct    DOUBLE,
            PRIMARY KEY (earnings_date, ticker)
        );

        CREATE TABLE IF NOT EXISTS earnings_estimates (
            period              VARCHAR NOT NULL,
            ticker              VARCHAR NOT NULL,
            avg                 DOUBLE,
            low                 DOUBLE,
            high                DOUBLE,
            year_ago_eps        DOUBLE,
            number_of_analysts  INTEGER,
            growth              DOUBLE,
            label               VARCHAR,
            period_label        VARCHAR,
            collected_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (period, ticker, collected_at)
        );

        CREATE TABLE IF NOT EXISTS earnings_history (
            quarter         TIMESTAMP   NOT NULL,
            ticker          VARCHAR     NOT NULL,
            eps_actual      DOUBLE,
            eps_estimate    DOUBLE,
            eps_difference  DOUBLE,
            surprise_pct    DOUBLE,
            PRIMARY KEY (quarter, ticker)
        );



    """
    )
    return conn


def insert_data(
    df: pl.DataFrame, db_cols: list, table_name: str, conn, pk_cols: list = None
):
    if df.is_empty():
        return
    final_cols = [c for c in db_cols if c in df.columns]
    df = df.select(final_cols)
    col_names = ", ".join(final_cols)

    if pk_cols:
        pk_where = " AND ".join([f"existing.{c} = df.{c}" for c in pk_cols])
        conn.execute(
            f"""
            INSERT INTO {table_name} ({col_names})
            SELECT {col_names} FROM df
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} existing
                WHERE {pk_where}
            )
        """
        )
    else:
        conn.execute(
            f"INSERT INTO {table_name} ({col_names}) SELECT {col_names} FROM df"
        )


# def insert_data(df: pl.DataFrame, db_cols: list, table_name: str, conn):
#     if df.is_empty():
#         return
#     final_cols = [c for c in db_cols if c in df.columns]
#     df = df.select(final_cols)
#     col_names = ", ".join(final_cols)
#     conn.execute(f"INSERT OR IGNORE INTO {table_name} ({col_names}) SELECT * FROM df")
