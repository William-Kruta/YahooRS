import polars as pl


def add_indicators(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.lazy()
        # SMA
        .with_columns(
            [
                pl.col("close").rolling_mean(window_size=20).alias("sma_20"),
                pl.col("close").rolling_mean(window_size=50).alias("sma_50"),
                pl.col("close").rolling_mean(window_size=200).alias("sma_200"),
            ]
        )
        # EMA
        .with_columns(
            [
                pl.col("close").ewm_mean(span=10, adjust=True).alias("ema_10"),
                pl.col("close").ewm_mean(span=20, adjust=True).alias("ema_20"),
            ]
        )
        # Bollinger Bands
        .with_columns(
            pl.col("close").rolling_std(window_size=20).alias("std_20"),
        )
        .with_columns(
            [
                (pl.col("sma_20") + pl.col("std_20") * 2.0).alias("bb_upper"),
                pl.col("sma_20").alias("bb_middle"),
                (pl.col("sma_20") - pl.col("std_20") * 2.0).alias("bb_lower"),
            ]
        )
        # MACD
        .with_columns(
            [
                pl.col("close").ewm_mean(span=12, adjust=True).alias("ema_12"),
                pl.col("close").ewm_mean(span=26, adjust=True).alias("ema_26"),
            ]
        )
        .with_columns(
            (pl.col("ema_12") - pl.col("ema_26")).alias("macd"),
        )
        .with_columns(
            pl.col("macd").ewm_mean(span=9, adjust=True).alias("macd_signal"),
        )
        .with_columns(
            (pl.col("macd") - pl.col("macd_signal")).alias("macd_histogram"),
        )
        # RSI
        .with_columns(
            pl.col("close").diff(1).alias("diff"),
        )
        .with_columns(
            [
                pl.when(pl.col("diff") > 0)
                .then(pl.col("diff"))
                .otherwise(0.0)
                .alias("gain"),
                pl.when(pl.col("diff") < 0)
                .then(pl.col("diff").abs())
                .otherwise(0.0)
                .alias("loss"),
            ]
        )
        .with_columns(
            [
                pl.col("gain").ewm_mean(span=14, adjust=True).alias("avg_gain"),
                pl.col("loss").ewm_mean(span=14, adjust=True).alias("avg_loss"),
            ]
        )
        .with_columns(
            (100.0 - (100.0 / (1.0 + pl.col("avg_gain") / pl.col("avg_loss")))).alias(
                "rsi"
            ),
        )
        # ATR
        .with_columns(pl.col("close").shift(1).alias("prev_close"))
        .with_columns(
            [
                (pl.col("high") - pl.col("low")).alias("tr1"),
                (pl.col("high") - pl.col("prev_close")).abs().alias("tr2"),
                (pl.col("low") - pl.col("prev_close")).abs().alias("tr3"),
            ]
        )
        .with_columns(
            pl.max_horizontal("tr1", "tr2", "tr3").alias("tr"),
        )
        .with_columns(
            pl.col("tr").rolling_mean(window_size=14).alias("atr"),
        )
        # Select final columns
        .select(
            [
                "date",
                "ticker",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "sma_20",
                "sma_50",
                "sma_200",
                "ema_12",
                "ema_26",
                "macd",
                "macd_signal",
                "macd_histogram",
                "rsi",
                "bb_upper",
                "bb_middle",
                "bb_lower",
                "atr",
            ]
        )
        .collect()
    )
