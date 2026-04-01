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

    def get_income_statement(self, tickers: list, period: str) -> pl.DataFrame:
        """
        tickers: list, List of tickers to search. Will be converted to a list if single string is passed.
        period: str, Period of financial statements. 'A' for annual and 'Q' for quarterly.

        returns: pl.DataFrame, Dataframe containing the financial statement.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        df = self.get_statement(
            tickers, statement="income_statement", period=period.upper()
        )
        return df

    def get_balance_sheet(self, tickers: list, period: str):
        """
        tickers: list, List of tickers to search. Will be converted to a list if single string is passed.
        period: str, Period of financial statements. 'A' for annual and 'Q' for quarterly.

        returns: pl.DataFrame, Dataframe containing the financial statement.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        df = self.get_statement(
            tickers, statement="balance_sheet", period=period.upper()
        )
        return df

    def get_cash_flow(self, tickers: list, period: str):
        """
        tickers: list, List of tickers to search. Will be converted to a list if single string is passed.
        period: str, Period of financial statements. 'A' for annual and 'Q' for quarterly.

        returns: pl.DataFrame, Dataframe containing the financial statement.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        df = self.get_statement(tickers, statement="cash_flow", period=period.upper())
        return df

    def get_statement(
        self, tickers: list[str], statement: str, period: str = "A"
    ) -> pl.DataFrame:
        """
        tickers: list, List of tickers to search. Will be converted to a list if single string is passed.
        statement: str, key value of the statements, accepted values are ["balance_sheet", "cash_flow", "income_statement"]
        period: str, Period of financial statements. 'A' for annual and 'Q' for quarterly.

        returns: pl.DataFrame, Dataframe containing the financial statement.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        df = self._read_statements(tickers, statement=statement, period=period)

        if df.is_empty():
            for ticker in tickers:
                fresh = self._download_statements(ticker, statement, period)
                fresh = fresh.fill_null(0)
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
        def quarter_end(d):
            q = (d.month - 1) // 3 + 1
            end_month = q * 3
            last_day = {3: 31, 6: 30, 9: 30, 12: 31}[end_month]
            return f"{d.year}-{end_month:02d}-{last_day:02d}"

        if period.upper() == "A":
            try:
                df = df.with_columns(
                    pl.col("date")
                    .str.to_date("%Y-%m-%d %H:%M:%S")
                    .dt.year()
                    .alias("year")
                )
            except pl.exceptions.SchemaError:
                df = df.with_columns(pl.col("date").dt.year().alias("year"))
            df = df.with_columns(
                (pl.col("year").cast(pl.Utf8) + "-12-31").alias("year")
            )
            pivoted = df.pivot(on="year", index=["ticker", "label"], values="value")
        elif period.upper() == "Q":
            try:
                df = df.with_columns(
                    pl.col("date")
                    .str.to_datetime("%Y-%m-%d %H:%M:%S")
                    .map_elements(quarter_end, return_dtype=pl.Utf8)
                    .alias("quarter")
                )
            except pl.exceptions.SchemaError:
                df = df.with_columns(
                    pl.col("date")
                    .map_elements(quarter_end, return_dtype=pl.Utf8)
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
        if isinstance(tickers, str):
            tickers = [tickers]
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
        # Income Statement
        REVENUE = "Total Revenue"
        COGS = "Cost Of Revenue"
        NET_INCOME = "Net Income Common Stockholders"
        EBITDA = "EBITDA"
        SHARES = "Diluted Average Shares"
        # Balance Sheet 
        TOTAL_DEBT = "Total Debt"
        AR = "Accounts Receivable"
        AP = "Accounts Payable"
        TOTAL_ASSETS = "Total Assets"
        STOCKHOLDERS_EQUITY = "Stockholders Equity"
        CURRENT_ASSETS = "Current Assets"
        CURRENT_LIABILITIES = "Current Liabilities"
        TANGIBLE_BOOK_VALUE = "Tangible Book Value"
        CASH_AND_CASH_EQUIVALENTS = "Cash And Cash Equivalents"
        INVENTORY = "Inventory"
        avg_values = {}
        yf_keys = [REVENUE, COGS, NET_INCOME, EBITDA, INVENTORY, AR, AP]
        if period == "Q":
            for ticker in tickers:
                bs = balance_sheet_df.filter(pl.col("ticker") == ticker)
                for key in yf_keys:
                    inv_row = bs.filter(pl.col("label") == key)
                    if not inv_row.is_empty():
                        vals = inv_row.select(date_cols_bs).row(0)
                        dates = date_cols_bs
                        rolling = {}
                        for i, d in enumerate(dates):
                            window = [v for v in vals[max(0, i - 3):i + 1] if v is not None]
                            rolling[d] = sum(window) / len(window) if window else None
                        avg_values[(ticker, key.lower())] = rolling

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
                cash_and_cash_equivalents = get_val(bs, CASH_AND_CASH_EQUIVALENTS, date_col)
                inventory = (
                    avg_values.get((ticker, "inventory"), {}).get(date_col)
                    if period == "Q"
                    else get_val(bs, INVENTORY, date_col)
                )
                if inventory is None:
                    inventory = 0
                ar = (
                    avg_values.get((ticker, "accounts receivable"), {}).get(date_col)
                    if period == "Q"
                    else get_val(bs, AR, date_col)
                )
                ap = (
                    avg_values.get((ticker, "accounts payable"), {}).get(date_col)
                    if period == "Q"
                    else get_val(bs, AP, date_col)
                )
                   


                price = get_closest_price(date_col)

                # Annualize flow metrics for quarterly data
                ann_revenue = (
                    avg_values.get((ticker, "revenue"), {}).get(date_col)
                    if period == "Q"
                    else get_val(inc, REVENUE, date_col)
                )
                ann_cogs = (
                    avg_values.get((ticker, "cogs"), {}).get(date_col)
                    if period == "Q"
                    else get_val(inc, COGS, date_col)
                )
                #ann_revenue = revenue * annualize if revenue else None
                ann_net_income = (
                    avg_values.get((ticker, "net_income"), {}).get(date_col)
                    if period == "Q"
                    else get_val(inc, NET_INCOME, date_col)
                )
                #ann_net_income = net_income * annualize if net_income else None
                ann_ebitda = (
                    avg_values.get((ticker, "ebitda"), {}).get(date_col)
                    if period == "Q"
                    else get_val(inc, EBITDA, date_col)
                )
                #ann_ebitda = ebitda * annualize if ebitda else None

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
                # Quick Ratio (balance sheet items, no annualization)
                if current_assets and current_liabilities:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Quick Ratio",
                            "value": (current_assets - inventory) / current_liabilities,
                        }
                    )
                # Cash Ratio (balance sheet items, no annualization)
                if cash_and_cash_equivalents and current_liabilities:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Cash Ratio",
                            "value": cash_and_cash_equivalents / current_liabilities,
                        }
                    )

                # Working Capital (balance sheet items, no annualization)
                if current_assets and current_liabilities:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Working Capital",
                            "value": current_assets - current_liabilities,
                        }
                    )

                # Asset Turnover (annualize revenue, inventory is point-in-time)
                if ann_revenue and inventory:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Asset Turnover",
                            "value": ann_revenue / inventory,
                        }
                    ) 

                # Inventory Turnover (annualize revenue, inventory is point-in-time)
                if ann_cogs and inventory:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Inventory Turnover",
                            "value": ann_cogs / inventory,
                        }
                    )   
                # Recievables Turnover 
                if ann_revenue and ar:
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "Recievables Turnover",
                            "value": ann_revenue / ar,
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

                # Days Sales Outstanding
                if ann_revenue and ar:
                    receivables_turnover = ann_revenue / ar
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "DSO",
                            "value": 365 / receivables_turnover,
                        }
                    )

                # Days Inventory Outstanding
                if ann_cogs and inventory:
                    inventory_turnover = ann_cogs / inventory
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "DIO",
                            "value": 365 / inventory_turnover,
                        }
                    )

                # Days Payable Outstanding & Cash Conversion Cycle
                if ann_cogs and ap:
                    dpo = (ap / ann_cogs) * 365
                    results.append(
                        {
                            "ticker": ticker,
                            "date": date_col,
                            "ratio_name": "DPO",
                            "value": dpo,
                        }
                    )
                    # CCC requires all three components
                    if ann_revenue and ar and inventory:
                        dso = (ar / ann_revenue) * 365
                        dio = (inventory / ann_cogs) * 365
                        results.append(
                            {
                                "ticker": ticker,
                                "date": date_col,
                                "ratio_name": "Cash Conversion Cycle",
                                "value": dso + dio - dpo,
                            }
                        )

        return pl.DataFrame(results)



    def get_per_share(self, tickers: list[str], period: str = "A"):
        income_statement = self.get_statement(
            tickers=tickers, statement="income_statement", period=period
        )
        balance_sheet = self.get_statement(
            tickers=tickers, statement="balance_sheet", period=period
        )
        cash_flow = self.get_statement(
            tickers=tickers, statement="cash_flow", period=period
        )
        date_cols = [c for c in income_statement.columns if c not in ("ticker", "label")]
        SHARES = "Diluted Average Shares"
        per_share_map = {
            "revenue_per_share": ("Total Revenue", SHARES, "income_statement"),
            "operating_income_per_share": ("Operating Income", SHARES, "income_statement"),
            "net_income_per_share": (
                "Net Income", 
                SHARES, "income_statement"
            ),
            "ebitda_per_share": ("EBITDA", SHARES, "income_statement"),
            # Balance Sheet 
            "cash_per_share": ("Cash and Cash Equivalents", SHARES, "balance_sheet"),
            "debt_per_share": ("Total Debt", SHARES, "balance_sheet"),
            "equity_per_share": ("Total Equity", SHARES, "balance_sheet"),
            # Cash Flow
            "fcf_per_share": ("Free Cash Flow", SHARES, "cash_flow"),
        }

        results = []

        for ticker in income_statement["ticker"].unique().to_list():
            ticker_inc_df = income_statement.filter(pl.col("ticker") == ticker)
            ticker_bal_df = balance_sheet.filter(pl.col("ticker") == ticker)
            ticker_cf_df = cash_flow.filter(pl.col("ticker") == ticker)

            for per_share_label, (numerator_label, denominator_label, statement) in per_share_map.items():
                den_row = ticker_inc_df.filter(pl.col("label") == denominator_label)
                if statement == "income_statement":
                    num_row = ticker_inc_df.filter(pl.col("label") == numerator_label)
                    
                elif statement == "balance_sheet":
                    num_row = ticker_bal_df.filter(pl.col("label") == numerator_label)
                elif statement == "cash_flow":
                    num_row = ticker_cf_df.filter(pl.col("label") == numerator_label)

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
                                "per_share_label": per_share_label,
                                "value": num / den,
                            }
                        )

        return pl.DataFrame(results)
        

    def get_growth_values(self, tickers: list[str], period: str = "A"):
        income_statement = self.get_statement(
            tickers=tickers, statement="income_statement", period=period
        )
        balance_sheet = self.get_statement(
            tickers=tickers, statement="balance_sheet", period=period
        )
        cash_flow = self.get_statement(
            tickers=tickers, statement="cash_flow", period=period
        )
        date_cols = [c for c in income_statement.columns if c not in ("ticker", "label")]

        growth_labels = {
            # Income Statement
            "Revenue Growth": ("income", "Total Revenue"),
            "Net Income Growth": ("income", "Net Income Common Stockholders"),
            "EBITDA Growth": ("income", "EBITDA"),
            "Operating Income Growth": ("income", "Operating Income"),
            "EPS Growth": ("income", "Diluted EPS"),
            # Balance Sheet
            "Cash Growth": ("balance", "Cash And Cash Equivalents"),
            "Debt Growth": ("balance", "Total Debt"),
            "Equity Growth": ("balance", "Stockholders Equity"),
            "Total Assets Growth": ("balance", "Total Assets"),
            # Cash Flow
            "Operating Cash Flow Growth": ("cashflow", "Operating Cash Flow"),
            "Capex Growth": ("cashflow", "Capital Expenditure"),
            "Dividend Growth": ("cashflow", "Common Stock Dividend Paid"),
        }

        source_map = {
            "income": income_statement,
            "balance": balance_sheet,
            "cashflow": cash_flow,
        }

        # For quarterly: compare to same quarter last year (4 periods back)
        # For annual: compare to prior year (1 period back)
        lookback = 4 if period == "Q" else 1

        results = []

        for ticker in tickers:
            for growth_name, (source, label) in growth_labels.items():
                df = source_map[source]
                row = df.filter(
                    (pl.col("ticker") == ticker) & (pl.col("label") == label)
                )
                if row.is_empty():
                    continue

                for i in range(lookback, len(date_cols)):
                    current_col = date_cols[i]
                    prior_col = date_cols[i - lookback]

                    current_val = row[current_col].item()
                    prior_val = row[prior_col].item()

                    if current_val is None or prior_val is None or prior_val == 0:
                        continue

                    growth = (current_val - prior_val) / abs(prior_val)

                    results.append(
                        {
                            "ticker": ticker,
                            "date": current_col,
                            "label": growth_name,
                            "value": growth,
                        }
                    )

        # Add FCF Growth (computed from two cash flow items)
        for ticker in tickers:
            ocf_row = cash_flow.filter(
                (pl.col("ticker") == ticker)
                & (pl.col("label") == "Operating Cash Flow")
            )
            capex_row = cash_flow.filter(
                (pl.col("ticker") == ticker)
                & (pl.col("label") == "Capital Expenditure")
            )
            if ocf_row.is_empty() or capex_row.is_empty():
                continue

            for i in range(lookback, len(date_cols)):
                current_col = date_cols[i]
                prior_col = date_cols[i - lookback]

                ocf_curr = ocf_row[current_col].item()
                capex_curr = capex_row[current_col].item()
                ocf_prior = ocf_row[prior_col].item()
                capex_prior = capex_row[prior_col].item()

                if any(v is None for v in [ocf_curr, capex_curr, ocf_prior, capex_prior]):
                    continue

                fcf_curr = ocf_curr - abs(capex_curr)
                fcf_prior = ocf_prior - abs(capex_prior)

                if fcf_prior == 0:
                    continue

                growth = (fcf_curr - fcf_prior) / abs(fcf_prior)

                results.append(
                    {
                        "ticker": ticker,
                        "date": current_col,
                        "label": "FCF Growth",
                        "value": growth,
                    }
                )

        return pl.DataFrame(results)