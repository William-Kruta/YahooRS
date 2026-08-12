import yfinance as yf
import polars as pl
import pandas as pd
import datetime as dt
from bisect import bisect_left
import logging
from typing import Literal

from .candles import Candles
from .earnings import Earnings
from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers, list_difference
from ..periphery.greeks import add_greeks_to_df


logger = logging.getLogger(__name__)


RENAME = {
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

SELECTION = [
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
    "dtr",
    "collected_at",
    "delta",
    "gamma",
    "theta",
    "vega",
    "bs_price",
    "prob_profit",
    "hist_prob_profit",
]


class Options:
    def __init__(self, db_path: str = None):
        self.conn = _init_tables(db_path)
        self.table_name = "options"
        self.candles = Candles(db_path)
        self.earnings = Earnings(db_path)

    def close(self) -> None:
        self.earnings.close()
        self.candles.close()
        self.conn.close()

    def __enter__(self) -> "Options":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _calc_date_deltas(
        self,
        dates: list[str],
        max_dte: int,
        min_dte: int = 0,
        ref_date: str = None,
        date_format: str = "%Y-%m-%d",
    ) -> dict[str, int]:

        cur_date = (
            dt.datetime.strptime(ref_date, date_format).date()
            if ref_date is not None
            else dt.date.today()
        )
        deltas = {
            d: delta
            for d in dates
            if min_dte
            <= (delta := (dt.datetime.strptime(d, date_format).date() - cur_date).days)
            <= max_dte
        }
        return deltas

    def get_options_by_dte_range(
        self,
        tickers: list,
        min_dte: int = 0,
        max_dte: int = 10,
        min_bid: float = 0,
        min_ask: float = 0,
        option_type: Literal["call", "put", "*"] = "*",
        side: Literal["long", "short", "*"] = "*",
        rfr_ticker: str = "^TNX",
        force_update: bool = False,
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        if min_dte < 0:
            raise ValueError("min_dte must be non-negative")
        if max_dte < min_dte:
            raise ValueError("max_dte must be greater than or equal to min_dte")
        if option_type not in ("call", "put", "*"):
            raise ValueError(f"option_type must be 'call', 'put', or '*', got {option_type!r}")
        if side not in ("long", "short", "*"):
            raise ValueError(f"side must be 'long', 'short', or '*', got {side!r}")

        frames: list[pl.DataFrame] = []
        for ticker in tickers:
            try:
                available = yf.Ticker(ticker).options
            except Exception as exc:
                logger.warning("Unable to list option expirations for %s: %s", ticker, exc)
                continue

            expirations = list(
                self._calc_date_deltas(
                    dates=available,
                    min_dte=min_dte,
                    max_dte=max_dte,
                )
            )
            if not expirations:
                continue
            frame = self.get_options(
                [ticker],
                get_latest=True,
                expirations=expirations,
                force_update=force_update,
                rfr_ticker=rfr_ticker,
            )
            if not frame.is_empty():
                frames.append(frame)

        if not frames:
            return pl.DataFrame()

        df = pl.concat(frames, how="diagonal_relaxed").filter(
            pl.col("dte").is_between(min_dte, max_dte),
            pl.col("bid") >= min_bid,
            pl.col("ask") >= min_ask,
        )
        if option_type != "*":
            df = df.filter(pl.col("option_type") == option_type)
        if side == "short":
            df = df.with_columns(
                (1 - pl.col("prob_profit")).alias("prob_profit"),
                (1 - pl.col("hist_prob_profit")).alias("hist_prob_profit"),
            )
        return df

    def _iterate_chain_type(
        self, chain: pd.DataFrame, option_type: str, ticker: str, stock_price: float
    ):
        chain["ticker"] = ticker.upper()
        chain["option_type"] = option_type.lower()
        chain["stock_price"] = stock_price
        return chain

    def get_options(
        self,
        tickers: list[str],
        get_latest: bool = False,
        expirations: list[str] | None = None,
        stale_threshold: dt.timedelta = dt.timedelta(days=1),
        force_update: bool = False,
        min_date: dt.datetime | dt.date | str | None = None,
        rfr_ticker: str = "^TNX",
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = clean_tickers(tickers)
        expirations = expirations or []
        requested_expirations = {str(expiration) for expiration in expirations}
        min_date_dt = self._normalize_min_date(min_date)
        df = self._read_options(tickers, get_latest=get_latest, min_date=min_date_dt)

        if df.is_empty() or force_update:
            fresh = self._download_options(
                tickers,
                expirations=expirations,
                get_latest=get_latest,
                rfr_ticker=rfr_ticker,
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
                    collected_at = dt.datetime.strptime(collected_at, "%Y-%m-%d %H:%M:%S.%f%z")
                if (dt.datetime.now(dt.timezone.utc) - collected_at) > stale_threshold:
                    stale_tickers.append(ticker)
                else:
                    cached_tickers.append(ticker)

            known_tickers = cached_tickers + stale_tickers
            missing_tickers = list_difference(known_tickers, tickers)
            coverage_missing_tickers = []
            if requested_expirations:
                cached_expirations = {
                    ticker: {
                        str(value)
                        for value in df.filter(pl.col("ticker") == ticker)["expiration"].unique()
                    }
                    for ticker in known_tickers
                }
                coverage_missing_tickers = [
                    ticker
                    for ticker in tickers
                    if not requested_expirations.issubset(cached_expirations.get(ticker, set()))
                ]

            refresh_tickers = list(
                dict.fromkeys(stale_tickers + missing_tickers + coverage_missing_tickers)
            )
            if refresh_tickers:
                fresh = self._download_options(
                    refresh_tickers,
                    expirations=expirations,
                    get_latest=get_latest,
                    rfr_ticker=rfr_ticker,
                )
                self._insert_options(fresh)

        result = self._read_options(
            tickers,
            get_latest=get_latest,
            min_date=min_date_dt,
        )
        if requested_expirations and not result.is_empty():
            result = result.filter(
                pl.col("expiration").cast(pl.String).is_in(sorted(requested_expirations))
            )
        return result

    def _download_options(
        self,
        tickers: list[str],
        expirations: list[str] | None = None,
        get_latest: bool = True,
        rfr_ticker: str = "^TNX",
    ) -> pl.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]
        expirations = expirations or []

        risk_free_rate = self.candles.get_last_price([rfr_ticker])[rfr_ticker] / 100.0
        prices = self.candles.get_last_price(tickers)
        data = []

        for t in tickers:
            try:
                obj = yf.Ticker(t)
                stock_price = prices.get(t)
                ticker_expirations = expirations or obj.options
                if not expirations and get_latest:
                    ticker_expirations = [ticker_expirations[0]]

                for exp in ticker_expirations:
                    try:
                        chain = obj.option_chain(exp)
                        calls = chain.calls
                        puts = chain.puts
                        calls["ticker"] = t
                        puts["ticker"] = t
                        calls["option_type"] = "call"
                        puts["option_type"] = "put"
                        calls["stock_price"] = stock_price
                        puts["stock_price"] = stock_price
                        data.append(calls)
                        data.append(puts)
                    except ValueError as exc:
                        logger.warning(
                            "Unable to download %s option chain for %s: %s",
                            exp,
                            t,
                            exc,
                        )
                        continue
            except Exception as exc:
                logger.warning("Unable to download options for %s: %s", t, exc)
                continue
        if not data:
            return pl.DataFrame()
        df = pl.from_pandas(pd.concat(data))
        df = df.with_columns(pl.lit(dt.datetime.now(dt.timezone.utc)).alias("collected_at"))
        df = df.with_columns(
            pl.col("contractSymbol")
            .map_elements(parse_expiration, return_dtype=pl.Utf8)
            .str.to_date("%Y-%m-%d")
            .alias("expiration"),
        ).with_columns(
            (pl.col("expiration") - pl.lit(dt.date.today())).dt.total_days().alias("dte")
        )
        df = add_greeks_to_df(df, risk_free_rate=risk_free_rate)
        df = self.calculate_historical_probs(df, self.candles.get_candles(tickers))
        df = self._add_days_to_report(df, tickers)
        df = df.rename(mapping=RENAME).select(SELECTION)
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
        # Pre-extract trading dates and closing prices per ticker. DTE is measured
        # in calendar days, so historical windows must not use a raw row offset
        # (which would incorrectly interpret 30 DTE as 30 trading sessions).
        ticker_data: dict[str, tuple[list[dt.date], list[float]]] = {}
        for ticker in candles_df["ticker"].unique().to_list():
            ticker_candles = candles_df.filter(pl.col("ticker") == ticker).sort("date")
            dates = [
                value.date() if isinstance(value, dt.datetime) else value
                for value in ticker_candles["date"].to_list()
            ]
            ticker_data[ticker] = (dates, ticker_candles["close"].to_list())

        # Cache: (ticker, dte) -> list of historical returns
        dist_cache: dict[tuple[str, int], list[float]] = {}

        tickers = options_df["ticker"].to_list()
        dtes = options_df["dte"].to_list()
        strikes = options_df["strike"].to_list()
        option_types = options_df["option_type"].to_list()
        bids = options_df["bid"].to_list()
        asks = options_df["ask"].to_list()
        # last_prices = options_df["last_price"].to_list()
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
            # last = last_prices[i] or 0.0

            if ask > 0.0 and bid > 0.0:
                premium = (bid + ask) / 2.0
            elif ask > 0.0:
                premium = ask
            elif bid > 0.0:
                premium = bid
            else:
                premium = 0.0

            breakeven = strike + premium if opt_type == "call" else strike - premium

            if dte <= 0 or current_price <= 0.0:
                is_itm = (
                    (current_price >= strike) if opt_type == "call" else (current_price <= strike)
                )
                hist_probs.append(1.0 if is_itm else 0.0)
                continue

            key = (ticker, dte)
            if key not in dist_cache:
                history = ticker_data.get(ticker)
                if history:
                    dates, closes = history
                    returns = []
                    for start_idx, start_date in enumerate(dates):
                        target_date = start_date + dt.timedelta(days=dte)
                        end_idx = bisect_left(dates, target_date, lo=start_idx + 1)
                        if end_idx < len(dates) and closes[start_idx]:
                            returns.append((closes[end_idx] / closes[start_idx]) - 1.0)
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
        self,
        tickers: list[str],
        get_latest: bool = False,
        min_date: dt.datetime | None = None,
    ) -> pl.DataFrame:
        if min_date is None:
            where_clause = "WHERE ticker = ANY($1)"
            params = [tickers]
        else:
            where_clause = "WHERE ticker = ANY($1) AND collected_at > $2"
            params = [tickers, min_date]

        if get_latest:
            return self.conn.execute(
                f"""
                SELECT * FROM options
                {where_clause}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY contract_symbol
                    ORDER BY collected_at DESC
                ) = 1
            """,
                params,
            ).pl()
        else:
            return self.conn.execute(f"SELECT * FROM options {where_clause}", params).pl()

    @staticmethod
    def _normalize_min_date(
        min_date: dt.datetime | dt.date | str | None,
    ) -> dt.datetime | None:
        if min_date is None:
            return None
        if isinstance(min_date, dt.datetime):
            return (
                min_date.replace(tzinfo=dt.timezone.utc)
                if min_date.tzinfo is None
                else min_date.astimezone(dt.timezone.utc)
            )
        if isinstance(min_date, dt.date):
            return dt.datetime.combine(min_date, dt.time.min, tzinfo=dt.timezone.utc)
        if isinstance(min_date, str):
            parsed = dt.datetime.fromisoformat(min_date.strip().replace("Z", "+00:00"))
            return (
                parsed.replace(tzinfo=dt.timezone.utc)
                if parsed.tzinfo is None
                else parsed.astimezone(dt.timezone.utc)
            )
        raise TypeError("min_date must be None, datetime, date, or ISO-8601 datetime string.")

    def _insert_options(self, df: pl.DataFrame):
        if df.is_empty():
            return
        # Greeks columns are NOT NULL in the schema; a contract with no solvable
        # IV would abort the whole batch insert, so keep it out of the DB.
        df = df.filter(pl.col("delta").is_not_null())
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
            "dtr",
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

    def _add_days_to_report(self, options_df: pl.DataFrame, tickers: list[str]) -> pl.DataFrame:
        earnings_df = self.earnings.get_earnings_dates(tickers)
        if earnings_df.is_empty():
            return options_df.with_columns(pl.lit(None).cast(pl.Int64).alias("dtr"))

        today = dt.datetime.now(dt.timezone.utc).date()
        # dtr = days until the NEXT upcoming report; tickers with no future
        # earnings date on record get a null dtr via the left join.
        next_earnings = (
            earnings_df.filter(pl.col("earnings_date").dt.date() >= today)
            .group_by("ticker")
            .agg(pl.col("earnings_date").min().alias("earnings_date"))
            .with_columns(
                (pl.col("earnings_date").dt.date() - pl.lit(today))
                .dt.total_days()
                .cast(pl.Int64)
                .alias("dtr")
            )
            .select(["ticker", "dtr"])
        )
        return options_df.join(next_earnings, on="ticker", how="left")


def parse_expiration(contract: str) -> str:
    # e.g. AAPL260327C00110000 -> 260327 -> 2026-03-27
    # Find the position of C or P to locate the date portion
    for i, ch in enumerate(contract):
        if ch in ("C", "P") and contract[i + 1 :].isdigit():
            date_str = contract[i - 6 : i]
            return dt.datetime.strptime(date_str, "%y%m%d").strftime("%Y-%m-%d")
    return None
