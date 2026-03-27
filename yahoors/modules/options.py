import yfinance as yf
import polars as pl
import pandas as pd
import datetime as dt

from .candles import Candles
from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, is_stale, list_difference
from ..periphery.greeks import add_greeks_to_df


class Options:
    def __init__(self, db_path: str = None):
        self.conn = _init_tables(db_path)
        self.table_name = "options"
        self.candles = Candles(db_path)

    def get_options(
        self,
        tickers: list[str],
        get_latest: bool = False,
        expirations: list[str] = [],
        stale_threshold: dt.timedelta = dt.timedelta(days=1),
        force_update: bool = False,
    ) -> pl.DataFrame:
        tickers = clean_tickers(tickers)
        df = self._read_options(tickers, get_latest=get_latest)

        if df.is_empty() or force_update:
            fresh = self._download_options(
                tickers, expirations=expirations, get_latest=get_latest
            )
            self._insert_options(fresh)
        else:
            # Check staleness per ticker based on most recent collected_at
            latest_per_ticker = (
                df.group_by("ticker")
                .agg(pl.col("collected_at").max().alias("collected_at"))
                .iter_rows()
            )
            stale_tickers = []
            cached_tickers = []
            for ticker, collected_at in latest_per_ticker:
                if isinstance(collected_at, str):
                    collected_at = dt.datetime.strptime(
                        collected_at, "%Y-%m-%d %H:%M:%S.%f%z"
                    )
                if (dt.datetime.now(dt.timezone.utc) - collected_at) > stale_threshold:
                    stale_tickers.append(ticker)
                else:
                    cached_tickers.append(ticker)

            missing_tickers = list_difference(cached_tickers + stale_tickers, tickers)
            if stale_tickers:
                fresh = self._download_options(
                    stale_tickers, expirations=expirations, get_latest=get_latest
                )
                self._insert_options(fresh)

            if missing_tickers:
                fresh = self._download_options(
                    missing_tickers, expirations=expirations, get_latest=get_latest
                )
                self._insert_options(fresh)

        return self._read_options(tickers, get_latest=get_latest)

    def _download_options(
        self, tickers: list[str], expirations: list[str] = [], get_latest: bool = True
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]

        risk_free_rate = self.candles.get_last_price(["^TNX"])["^TNX"] / 100.0
        data = []

        for t in tickers:
            try:
                obj = yf.Ticker(t)
                last_price = self.candles.get_last_price([t])[t]
                if not expirations:
                    expirations = obj.options
                    if get_latest:
                        expirations = [expirations[0]]

                for exp in expirations:
                    try:
                        chain = obj.option_chain(exp)
                        calls = chain.calls
                        puts = chain.puts
                        calls["ticker"] = t
                        puts["ticker"] = t
                        calls["option_type"] = "call"
                        puts["option_type"] = "put"
                        calls["stock_price"] = last_price
                        puts["stock_price"] = last_price
                        data.append(calls)
                        data.append(puts)
                    except ValueError:
                        continue
            except Exception:
                break
        df = pl.from_pandas(pd.concat(data))
        df = df.with_columns(
            pl.lit(dt.datetime.now(dt.timezone.utc)).alias("collected_at")
        )
        df = df.with_columns(
            pl.col("contractSymbol")
            .map_elements(parse_expiration, return_dtype=pl.Utf8)
            .str.to_date("%Y-%m-%d")
            .alias("expiration"),
        ).with_columns(
            (pl.col("expiration") - pl.lit(dt.date.today()))
            .dt.total_days()
            .alias("dte")
        )

        df = add_greeks_to_df(df, risk_free_rate=risk_free_rate)
        df = self.calculate_historical_probs(df, self.candles.get_candles(tickers))
        df = df.rename(
            {
                "contractSymbol": "contract_symbol",
                "lastTradeDate": "last_trade_date",
                "strike": "strike",
                "lastPrice": "last_price",
                "bid": "bid",
                "ask": "ask",
                "volume": "volume",
                "openInterest": "open_interest",
                "impliedVolatility": "implied_volatility",
                "inTheMoney": "in_the_money",
                "ticker": "ticker",
                "dte": "dte",
                "stock_price": "stock_price",
            }
        ).select(
            [
                "contract_symbol",
                "last_trade_date",
                "strike",
                "stock_price",
                "last_price",
                "bid",
                "ask",
                "volume",
                "open_interest",
                "implied_volatility",
                "in_the_money",
                "expiration",
                "option_type",
                "ticker",
                "dte",
                "collected_at",
                "delta",
                "gamma",
                "theta",
                "vega",
                "bs_price",
                "prob_profit",
                "hist_prob_profit",
            ]
        )
        # return pl.from_pandas()
        df = df.with_columns(
            pl.col("volume", "open_interest").fill_null(0),
            pl.col("bid", "ask", "last_price", "implied_volatility").fill_null(0.0),
        )
        return df

    def calculate_historical_probs(
        self,
        options_df: pl.DataFrame,
        candles_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Add a 'hist_prob_profit' column to options_df based on historical return distributions.

        options_df requires: ticker, dte, strike, option_type, bid, ask, last_price, stock_price
        candles_df requires: ticker, date, close (sorted by date)
        """
        # Pre-extract closing prices per ticker
        ticker_data: dict[str, list[float]] = {}
        for ticker in candles_df["ticker"].unique().to_list():
            closes = (
                candles_df.filter(pl.col("ticker") == ticker)
                .sort("date")["close"]
                .to_list()
            )
            ticker_data[ticker] = closes

        # Cache: (ticker, dte) -> list of historical returns
        dist_cache: dict[tuple[str, int], list[float]] = {}

        tickers = options_df["ticker"].to_list()
        dtes = options_df["dte"].to_list()
        strikes = options_df["strike"].to_list()
        option_types = options_df["option_type"].to_list()
        bids = options_df["bid"].to_list()
        asks = options_df["ask"].to_list()
        last_prices = options_df["lastPrice"].to_list()
        stock_prices = options_df["stock_price"].to_list()

        hist_probs: list[float | None] = []

        for i in range(len(tickers)):
            ticker = tickers[i] or ""
            dte = dtes[i] or 0
            strike = strikes[i] or 0.0
            opt_type = option_types[i] or ""
            current_price = stock_prices[i] or 0.0
            bid = bids[i] or 0.0
            ask = asks[i] or 0.0
            last = last_prices[i] or 0.0

            if ask > 0.0 and bid > 0.0:
                premium = (bid + ask) / 2.0
            elif ask > 0.0:
                premium = ask
            elif bid > 0.0:
                premium = bid
            else:
                premium = last

            breakeven = strike + premium if opt_type == "call" else strike - premium

            if dte <= 0 or current_price <= 0.0:
                is_itm = (
                    (current_price >= strike)
                    if opt_type == "call"
                    else (current_price <= strike)
                )
                hist_probs.append(1.0 if is_itm else 0.0)
                continue

            key = (ticker, dte)
            if key not in dist_cache:
                closes = ticker_data.get(ticker)
                if closes and len(closes) > dte:
                    returns = [
                        (closes[t] / closes[t - dte]) - 1.0
                        for t in range(dte, len(closes))
                    ]
                    dist_cache[key] = returns

            returns = dist_cache.get(key)
            if returns:
                target_ret = (breakeven / current_price) - 1.0
                if opt_type == "call":
                    hits = sum(1 for r in returns if r >= target_ret)
                else:
                    hits = sum(1 for r in returns if r <= target_ret)
                hist_probs.append(hits / len(returns))
            else:
                hist_probs.append(None)

        return options_df.with_columns(pl.Series("hist_prob_profit", hist_probs))

    def _read_options(
        self, tickers: list[str], get_latest: bool = False
    ) -> pl.DataFrame:
        if get_latest:
            return self.conn.execute(
                """
                SELECT * FROM options
                WHERE ticker = ANY($1)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY contract_symbol
                    ORDER BY collected_at DESC
                ) = 1
            """,
                [tickers],
            ).pl()
        else:
            return self.conn.execute(
                "SELECT * FROM options WHERE ticker = ANY($1)", [tickers]
            ).pl()

    def _insert_options(self, df: pl.DataFrame):
        if df.is_empty():
            return

        db_cols = [
            "contract_symbol",
            "last_trade_date",
            "strike",
            "stock_price",
            "last_price",
            "bid",
            "ask",
            "volume",
            "open_interest",
            "implied_volatility",
            "in_the_money",
            "expiration",
            "option_type",
            "ticker",
            "dte",
            "collected_at",
            "delta",
            "gamma",
            "theta",
            "vega",
            "bs_price",
            "prob_profit",
            "hist_prob_profit",
        ]
        insert_data(
            df,
            db_cols=db_cols,
            table_name=self.table_name,
            conn=self.conn,
            pk_cols=["contract_symbol", "collected_at"],
        )


def parse_expiration(contract: str) -> str:
    # e.g. AAPL260327C00110000 -> 260327 -> 2026-03-27
    # Find the position of C or P to locate the date portion
    for i, ch in enumerate(contract):
        if ch in ("C", "P") and contract[i + 1 :].isdigit():
            date_str = contract[i - 6 : i]
            return dt.datetime.strptime(date_str, "%y%m%d").strftime("%Y-%m-%d")
    return None
