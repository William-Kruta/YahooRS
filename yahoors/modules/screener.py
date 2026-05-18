import polars as pl
import datetime as dt
from .options import Options


def add_yield_columns(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    return df.with_columns(
        (pl.col("strike") * 100).alias("collateral"),
        pl.when((pl.col("bid") > 0) & (pl.col("ask") > 0))
        .then((pl.col("bid") + pl.col("ask")) / 2.0)
        .when(pl.col("ask") > 0)
        .then(pl.col("ask"))
        .when(pl.col("bid") > 0)
        .then(pl.col("bid"))
        .otherwise(pl.col("last_price"))
        .alias("premium"),
    ).with_columns(
        (pl.col("premium") / pl.col("strike")).alias("roc"),
        (pl.col("premium") / pl.col("strike") * 365.0 / pl.col("dte")).alias(
            "annualized_roc"
        ),
    )


def cash_secured_puts(
    tickers: list,
    max_dte: int,
    max_collateral: float,
    min_dte: int = 0,
    min_collateral: float = 0.0,
    min_premium: float = 0.10,
    min_roc: float = 0.005,
    itm: bool = False,
    max_trade_age: dt.timedelta | None = dt.timedelta(hours=2),
) -> pl.DataFrame:
    op = Options()
    df = op.get_options_by_dte_range(
        tickers, min_dte=min_dte, max_dte=max_dte, option_type="put"
    )
    return options_screener(
        df,
        min_dte=min_dte,
        max_dte=max_dte,
        in_the_money=itm,
        long=False,
        min_collateral=min_collateral,
        max_collateral=max_collateral,
        min_premium=min_premium,
        min_roc=min_roc,
        max_trade_age=max_trade_age,
    )


def options_screener(
    options_df: pl.DataFrame,
    min_dte: int = 0,
    max_dte: int = 365,
    in_the_money: bool = False,
    long: bool = False,
    min_collateral: float = 0.0,
    max_collateral: float = float("inf"),
    min_premium: float = 0.10,
    min_roc: float = 0.005,
    max_trade_age: dt.timedelta | None = dt.timedelta(hours=2),
) -> pl.DataFrame:
    side = "long" if long else "short"
    df = (
        options_df.lazy()
        .with_columns(pl.lit(side).alias("side"))
        # Flip prob_profit for short positions
        .with_columns(
            pl.when(pl.lit(long))
            .then(pl.col("prob_profit"))
            .otherwise(1.0 - pl.col("prob_profit"))
            .alias("prob_profit")
        )
        # Filter DTE range
        .filter(pl.col("dte").is_between(min_dte, max_dte))
        # Filter ITM/OTM
        .filter(pl.col("in_the_money") == in_the_money)
        # Short strategies require a non-zero bid (that's the fill price)
        # Long strategies just need any market
        .filter(pl.col("bid") > 0 if not long else (pl.col("bid") > 0) | (pl.col("ask") > 0) | (pl.col("last_price") > 0))
        # Drop contracts where greeks couldn't be computed (no valid IV)
        .filter(pl.col("prob_profit").is_not_null())
        # Collateral, premium, roc, annualized_roc
        .pipe(add_yield_columns)
        # Filter collateral range
        .filter(pl.col("collateral").is_between(min_collateral, max_collateral))
        # Max loss per share (informational — worst case, not used in EV)
        .with_columns(
            pl.when(pl.lit(long))
            .then(pl.col("premium"))
            .when(pl.col("option_type") == "put")
            .then(pl.col("strike") - pl.col("premium"))
            .otherwise(pl.col("stock_price") - pl.col("premium"))
            .alias("max_loss_per_share")
        )
        # Expected return: edge over fair value (premium collected minus BS fair value)
        # EV = premium - bs_price, normalized by strike
        .with_columns(
            ((pl.col("premium") - pl.col("bs_price")) / pl.col("strike")).alias(
                "expected_return"
            )
        )
        # Minimum premium and ROC filters
        .filter(pl.col("premium") > min_premium)
        .filter(pl.col("roc") > min_roc)
        .sort("expected_return", descending=True)
        .collect()
    )
    if max_trade_age is not None:
        df = filter_stale_trades(df, max_age=max_trade_age)
    return df


def filter_stale_trades(
    df: pl.DataFrame, max_age: dt.timedelta = dt.timedelta(hours=24)
) -> pl.DataFrame:
    cutoff = dt.datetime.now(dt.timezone.utc) - max_age
    return df.filter(
        pl.col("last_trade_date").cast(pl.Datetime("us", "UTC")) >= pl.lit(cutoff)
    )
