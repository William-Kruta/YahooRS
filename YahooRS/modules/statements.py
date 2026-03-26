import datetime as dt
import yfinance as yf
import polars as pl
import pandas as pd

from .candles import Candles
from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, list_difference


class Statements:
    def __init__(self, db_path: str = None, candles_obj: Candles = None):
        self.conn = _init_tables(db_path)
        if candles_obj is None:
            self.candles = Candles(db_path=db_path)
        else:
            self.candles = candles_obj
        self.table_name = "statements"

    def get_statement(
        self, tickers: list[str], statement: str, period: str = "A"
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self._read_statements(tickers, statement=statement, period=period)

        if df.is_empty():
            for ticker in tickers:
                fresh = self._download_statements(ticker, statement, period)
                self._insert_statements(fresh)
        else:
            db_tickers = df["ticker"].unique().to_list()
            missing_tickers = list_difference(db_tickers, tickers)

            # Check staleness per ticker based on most recent date
            latest_per_ticker = (
                df.group_by("ticker")
                .agg(pl.col("date").max().alias("date"))
                .iter_rows()
            )
            stale_threshold = (
                dt.timedelta(days=90) if period == "Q" else dt.timedelta(days=365)
            )
            for ticker, latest_date in latest_per_ticker:
                if isinstance(latest_date, str):
                    latest_date = dt.datetime.fromisoformat(latest_date)
                if (dt.datetime.now() - latest_date) > stale_threshold:
                    fresh = self._download_statements(ticker, statement, period)
                    self._insert_statements(fresh)

            if missing_tickers:
                for ticker in missing_tickers:
                    fresh = self._download_statements(ticker, statement, period)
                    self._insert_statements(fresh)

        df = self._read_statements(tickers, statement=statement, period=period)
        pivoted = df.pivot(on="date", index=["ticker", "label"], values="value")

        # Reverse the date columns (everything after ticker and label)
        if period.upper() == "A":
            df = df.with_columns(
                pl.col("date").str.to_date("%Y-%m-%d %H:%M:%S").dt.year().alias("year")
            )
            pivoted = df.pivot(on="year", index=["ticker", "label"], values="value")
        elif period.upper() == "Q":
            df = df.with_columns(
                pl.col("date")
                .str.to_datetime("%Y-%m-%d %H:%M:%S")
                .map_elements(
                    lambda d: f"{d.year}-Q{(d.month - 1) // 3 + 1}",
                    return_dtype=pl.Utf8,
                )
                .alias("quarter")
            )
            pivoted = df.pivot(on="quarter", index=["ticker", "label"], values="value")
        fixed_cols = ["ticker", "label"]
        date_cols = [c for c in pivoted.columns if c not in fixed_cols]
        pivoted = pivoted.select(fixed_cols + sorted(date_cols))
        return pivoted

    def _download_statements(self, tickers: list[str], statement: str, period: str):
        if isinstance(tickers, str):
            tickers = [tickers]

        data = []
        for ticker in tickers:
            t = yf.Ticker(ticker)
            mapping = {
                "income_statement": {"A": "income_stmt", "Q": "quarterly_income_stmt"},
                "balance_sheet": {"A": "balance_sheet", "Q": "quarterly_balance_sheet"},
                "cash_flow": {"A": "cash_flow", "Q": "quarterly_cash_flow"},
            }
            attr = mapping[statement][period]
            df = getattr(t, attr)

            df = df.reset_index(names="label")
            df.columns = [str(c) if c != "label" else c for c in df.columns]
            melted = df.melt(id_vars="label", var_name="date", value_name="value")
            melted["ticker"] = ticker
            melted["statement_type"] = statement
            melted["period"] = period
            melted = melted[
                ["date", "ticker", "label", "value", "statement_type", "period"]
            ]
            data.append(melted)
        return pl.from_pandas(pd.concat(data))

    def _read_statements(
        self, tickers: list[str], statement: str, period: str
    ) -> pl.DataFrame:
        placeholders = ", ".join(["?"] * len(tickers))
        query = f"SELECT * FROM {self.table_name} WHERE ticker IN ({placeholders}) AND statement_type = ? AND period = ?"
        params = [*tickers, statement, period]
        return pl.read_database(
            query, self.conn, execute_options={"parameters": params}
        )

    def _insert_statements(self, df: pl.DataFrame):
        if df.is_empty():
            return
        db_cols = [
            "date",
            "ticker",
            "label",
            "value",
            "statement_type",
            "period",
        ]
        insert_data(
            df,
            db_cols=db_cols,
            table_name=self.table_name,
            conn=self.conn,
            pk_cols=["date", "ticker", "label", "statement_type", "period"],
        )

    def get_margins(self, tickers: list[str], period: str) -> pl.DataFrame:
        """
        Calculate margins from a pivoted income statement DataFrame.
        Returns long format: ticker, date, margin_name, value
        """
        # Get date columns (everything except ticker and label)

        df = self.get_statement(
            tickers=tickers, statement="income_statement", period=period
        )
        date_cols = [c for c in df.columns if c not in ("ticker", "label")]

        margins = {
            "gross_margin": ("Gross Profit", "Total Revenue"),
            "operating_margin": ("Operating Income", "Total Revenue"),
            "net_margin": (
                "Net Income From Continuing Operation Net Minority Interest",
                "Total Revenue",
            ),
            "ebitda_margin": ("EBITDA", "Total Revenue"),
        }

        results = []

        for ticker in df["ticker"].unique().to_list():
            ticker_df = df.filter(pl.col("ticker") == ticker)

            for margin_name, (numerator_label, denominator_label) in margins.items():
                num_row = ticker_df.filter(pl.col("label") == numerator_label)
                den_row = ticker_df.filter(pl.col("label") == denominator_label)

                if num_row.is_empty() or den_row.is_empty():
                    continue

                for date_col in date_cols:
                    num = num_row[date_col].item()
                    den = den_row[date_col].item()

                    if num is not None and den is not None and den != 0:
                        results.append(
                            {
                                "ticker": ticker,
                                "date": date_col,
                                "margin_name": margin_name,
                                "value": num / den,
                            }
                        )

        return pl.DataFrame(results)

    def get_ratios(
        self,
        tickers: list[str],
        income_df: pl.DataFrame = None,
        balance_sheet_df: pl.DataFrame = None,
        candles_df: pl.DataFrame = None,
        period: str = "A",
    ) -> pl.DataFrame:
        if income_df is None:
            income_df = self.get_statement(tickers, "income_statement", period=period)
        if balance_sheet_df is None:
            balance_sheet_df = self.get_statement(
                tickers, "balance_sheet", period=period
            )
        if candles_df is None:
            candles_df = self.candles.get_candles(tickers)

        date_cols_income = [
            c for c in income_df.columns if c not in ("ticker", "label")
        ]
        date_cols_bs = [
            c for c in balance_sheet_df.columns if c not in ("ticker", "label")
        ]

        REVENUE = "Total Revenue"
        NET_INCOME = "Net Income Common Stockholders"
        EBITDA = "EBITDA"
        SHARES = "Diluted Average Shares"
        TOTAL_DEBT = "Total Debt"
        STOCKHOLDERS_EQUITY = "Stockholders Equity"
        CURRENT_ASSETS = "Current Assets"
        CURRENT_LIABILITIES = "Current Liabilities"
        TANGIBLE_BOOK_VALUE = "Tangible Book Value"

        # Annualization factor: quarterly values need to be multiplied by 4
        annualize = 4 if period == "Q" else 1

        results = []

        for ticker in tickers:
            inc = income_df.filter(pl.col("ticker") == ticker)
            bs = balance_sheet_df.filter(pl.col("ticker") == ticker)

            prices = (
                candles_df.filter(pl.col("ticker") == ticker)
                .sort("date")
                .select(["date", "close"])
            )
            price_dates = prices["date"].to_list()
            price_closes = prices["close"].to_list()

            def get_closest_price(date_str: str) -> float | None:
                target = date_str.split(" ")[0]
                if len(target) == 4:
                    target = f"{target}-12-31"
                elif "-Q" in target:
                    year, q = target.split("-Q")
                    month = int(q) * 3
                    target = f"{year}-{month:02d}-28"

                target_dt = dt.datetime.strptime(target, "%Y-%m-%d")
                best_idx = None
                best_diff = float("inf")
                for idx, d in enumerate(price_dates):
                    d_str = str(d).split(" ")[0]
                    try:
                        d_dt = dt.datetime.strptime(d_str, "%Y-%m-%d")
                        if d_dt <= target_dt:
                            diff = (target_dt - d_dt).days
                            if diff < best_diff:
                                best_diff = diff
                                best_idx = idx
                    except ValueError:
                        continue
                return price_closes[best_idx] if best_idx is not None else None

            def get_val(df: pl.DataFrame, label: str, date_col: str) -> float | None:
                row = df.filter(pl.col("label") == label)
                if row.is_empty():
                    return None
                v = row[date_col].item()
                return v if v is not None and v != 0 else None

            date_cols = [c for c in date_cols_income if c in date_cols_bs]

            for date_col in date_cols:
                revenue = get_val(inc, REVENUE, date_col)
                net_income = get_val(inc, NET_INCOME, date_col)
                ebitda = get_val(inc, EBITDA, date_col)
                shares = get_val(inc, SHARES, date_col)
                total_debt = get_val(bs, TOTAL_DEBT, date_col)
                equity = get_val(bs, STOCKHOLDERS_EQUITY, date_col)
                current_assets = get_val(bs, CURRENT_ASSETS, date_col)
                current_liabilities = get_val(bs, CURRENT_LIABILITIES, date_col)
                book_value = get_val(bs, TANGIBLE_BOOK_VALUE, date_col)

                price = get_closest_price(date_col)

                # Annualize flow metrics for quarterly data
                ann_revenue = revenue * annualize if revenue else None
                ann_net_income = net_income * annualize if net_income else None
                ann_ebitda = ebitda * annualize if ebitda else None

                # P/E
                if price and shares and ann_net_income:
                    eps = ann_net_income / shares
                    if eps != 0:
                        results.append(
                            {
                                "ticker": ticker,
                                "date": date_col,
                                "ratio_name": "P/E",
                                "value": price / eps,
                            }
                        )

                # P/S
                if price and shares and ann_revenue:
                    rev_per_share = ann_revenue / shares
                    if rev_per_share != 0:
                        results.append(
                            {
                                "ticker": ticker,
                                "date": date_col,
                                "ratio_name": "P/S",
                                "value": price / rev_per_share,
                            }
                        )

                # P/B (book value is a point-in-time balance sheet item, no annualization)
                if price and shares and book_value:
                    bv_per_share = book_value / shares
                    if bv_per_share != 0:
                        results.append(
                            {
                                "ticker": ticker,
                                "date": date_col,
                                "ratio_name": "P/B",
                                "value": price / bv_per_share,
                            }
                        )

                # EV/EBITDA
                if price and shares and ann_ebitda and total_debt is not None:
                    market_cap = price * shares
                    ev = market_cap + (total_debt or 0)
                    if ann_ebitda != 0:
                        results.append(
                            {
                                "ticker": ticker,
                                "date": date_col,
                                "ratio_name": "EV/EBITDA",
                                "value": ev / ann_ebitda,
                            }
                        )

                # Debt/Equity (balance sheet items, no annualization)
                if total_debt and equity:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Debt/Equity",
                            "value": total_debt / equity,
                        }
                    )

                # Current Ratio (balance sheet items, no annualization)
                if current_assets and current_liabilities:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Current Ratio",
                            "value": current_assets / current_liabilities,
                        }
                    )

                # ROE (annualize net income, equity is point-in-time)
                if ann_net_income and equity:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "ROE",
                            "value": ann_net_income / equity,
                        }
                    )

        return pl.DataFrame(results)
