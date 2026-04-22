import polars as pl
import datetime as dt


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
        # Collateral = strike * 100
        .with_columns((pl.col("strike") * 100).alias("collateral"))
        # Filter DTE range
        .filter(pl.col("dte").is_between(min_dte, max_dte))
        # Filter ITM/OTM
        .filter(pl.col("in_the_money") == in_the_money)
        # Must have some bid or ask
        .filter((pl.col("bid") > 0) | (pl.col("ask") > 0))
        # Filter collateral range
        .filter(pl.col("collateral").is_between(min_collateral, max_collateral))
        # Premium: prefer mid, fall back to whichever exists, then last_price
        .with_columns(
            pl.when((pl.col("bid") > 0) & (pl.col("ask") > 0))
            .then((pl.col("bid") + pl.col("ask")) / 2.0)
            .when(pl.col("ask") > 0)
            .then(pl.col("ask"))
            .when(pl.col("bid") > 0)
            .then(pl.col("bid"))
            .otherwise(pl.col("last_price"))
            .alias("premium")
        )
        # ROC and annualized ROC
        .with_columns(
            [
                (pl.col("premium") / pl.col("strike")).alias("roc"),
                (pl.col("premium") / pl.col("strike") * 365.0 / pl.col("dte")).alias(
                    "annualized_roc"
                ),
            ]
        )
        # Max loss per share
        .with_columns(
            pl.when(pl.lit(long))
            .then(pl.col("premium"))
            .when(pl.col("option_type") == "put")
            .then(pl.col("strike") - pl.col("premium"))
            .otherwise(pl.col("stock_price") - pl.col("premium"))
            .alias("max_loss_per_share")
        )
        # Expected return normalized by strike
        .with_columns(
            (
                (
                    pl.col("premium") * pl.col("prob_profit")
                    - pl.col("max_loss_per_share") * (1.0 - pl.col("prob_profit"))
                )
                / pl.col("strike")
            ).alias("expected_return")
        )
        # Minimum premium and ROC filters
        .filter(pl.col("premium") > min_premium)
        .filter(pl.col("roc") > min_roc)
        .sort("expected_return", descending=True)
        .collect()
    )
    df = filter_stale_trades(df, max_age=dt.timedelta(hours=2))
    return df


def filter_stale_trades(
    df: pl.DataFrame, max_age: dt.timedelta = dt.timedelta(hours=24)
) -> pl.DataFrame:
    cutoff = dt.datetime.now(dt.timezone.utc) - max_age
    return df.filter(
        pl.col("last_trade_date").cast(pl.Datetime("us", "UTC")) >= pl.lit(cutoff)
    )
