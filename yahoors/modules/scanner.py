import polars as pl

from .candles import Candles
from .options import Options
from .statements import Statements
from .screener import options_screener
from ..periphery.config import get_db_path
from ..periphery.utils import clean_tickers
from ..periphery.technical_analysis import add_indicators


def load_universe(path: str, ticker_col: str = "symbol") -> list[str]:
    df = pl.read_csv(path)
    tickers = (
        df.select(pl.col(ticker_col))
        .filter(pl.col(ticker_col).is_not_null())
        .filter(~pl.col(ticker_col).str.starts_with("--"))
        .get_column(ticker_col)
        .to_list()
    )
    return clean_tickers(tickers)


def prescreen_with_candles(
    tickers: list[str],
    candles_obj: Candles,
    max_collateral: float = float("inf"),
    min_bb_width: float = 0.12,
    max_rsi: float = 55.0,
    candle_period: str = "1y",
) -> tuple[list[str], pl.DataFrame]:
    """
    Fast pre-filter using cached candle data + technicals. No options API calls.

    max_collateral: max cash to post (filters stocks where strike*100 > budget)
    min_bb_width:   (bb_upper - bb_lower) / close — proxy for elevated IV / juicy premiums
    max_rsi:        exclude overbought stocks; lower = more oversold bias
    Returns (filtered_tickers, stats_df sorted by bb_width descending)
    """
    df = candles_obj.get_candles(tickers, interval="1d", period=candle_period)
    if df.is_empty():
        return [], pl.DataFrame()

    df = add_indicators(df)

    latest = df.sort("date").group_by("ticker").last()

    max_price = max_collateral / 100.0

    stats = (
        latest.lazy()
        .with_columns(
            ((pl.col("bb_upper") - pl.col("bb_lower")) / pl.col("close")).alias("bb_width_pct")
        )
        .filter(pl.col("close") <= max_price)
        .filter(pl.col("bb_width_pct") >= min_bb_width)
        .filter(pl.col("rsi") <= max_rsi)
        .filter(pl.col("rsi").is_not_null())
        .select(["ticker", "close", "rsi", "bb_width_pct", "atr"])
        .sort("bb_width_pct", descending=True)
        .collect()
    )

    return stats.get_column("ticker").to_list(), stats


def quality_filter(
    tickers: list[str],
    statements_obj: Statements,
    period: str = "A",
) -> list[str]:
    """
    Keeps tickers with positive net income in the most recent cached period.
    Uses a direct DB query — no downloads, no pivot. Tickers with no cached
    statement data pass through rather than being excluded.
    """
    if not tickers:
        return []
    try:
        placeholders = ", ".join(["?" for _ in tickers])
        result = statements_obj.conn.execute(
            f"""
            SELECT ticker
            FROM statements
            WHERE label = 'Net Income'
              AND statement_type = 'income_statement'
              AND period = ?
              AND ticker IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
              AND value > 0
            """,
            [period] + tickers,
        ).pl()
        if result.is_empty():
            return tickers
        passing = set(result["ticker"].to_list())
        # tickers with no cached data pass through; only exclude confirmed negatives
        no_data = set(tickers) - set(
            statements_obj.conn.execute(
                f"SELECT DISTINCT ticker FROM statements WHERE ticker IN ({placeholders}) AND period = ?",
                tickers + [period],
            ).pl()["ticker"].to_list()
        )
        return [t for t in tickers if t in passing or t in no_data]
    except Exception as e:
        print(f"[quality_filter] warning: DB query failed ({e}), returning unfiltered list")
        return tickers


def run_screener(
    tickers: list[str],
    options_obj: Options,
    min_dte: int = 14,
    max_dte: int = 60,
    max_collateral: float = float("inf"),
    min_premium: float = 0.10,
    min_roc: float = 0.005,
    verbose: bool = False,
) -> pl.DataFrame:
    if not tickers:
        return pl.DataFrame()
    options_df = options_obj.get_options(tickers, get_latest=True)
    if options_df.is_empty():
        return pl.DataFrame()
    puts = options_df.filter(pl.col("option_type") == "put")
    if verbose:
        print(f"[screener] {len(tickers)} tickers → {len(options_df)} option rows → {len(puts)} puts")
    return options_screener(
        puts,
        min_dte=min_dte,
        max_dte=max_dte,
        in_the_money=False,
        long=False,
        max_collateral=max_collateral,
        min_premium=min_premium,
        min_roc=min_roc,
        max_trade_age=None,  # scanner runs batch/offline; don't discard by last_trade_date
    )


def scan_for_csps(
    universe_csv: str,
    db_path: str = None,
    ticker_col: str = "symbol",
    max_collateral: float = float("inf"),
    min_bb_width: float = 0.12,
    max_rsi: float = 55.0,
    apply_quality_filter: bool = False,
    watchlist: list[str] | None = None,
    min_dte: int = 14,
    max_dte: int = 60,
    min_premium: float = 0.10,
    min_roc: float = 0.005,
    verbose: bool = False,
) -> pl.DataFrame:
    """
    Full pipeline: universe → candle prescreen → optional quality filter → options screener.

    Example:
        results = scan_for_csps(
            "VOO.csv",
            max_collateral=10_000,   # can write puts on stocks up to $100
            max_rsi=50,
            min_dte=21,
            max_dte=45,
        )
    """
    if db_path is None:
        db_path = str(get_db_path())

    candles_obj = Candles(db_path, debug=False)
    options_obj = Options(db_path)

    # Stage 1: load universe, merge optional watchlist
    universe = load_universe(universe_csv, ticker_col)
    if watchlist:
        extra = clean_tickers(watchlist)
        universe = list(dict.fromkeys(universe + extra))
    if verbose:
        print(f"[scanner] universe: {len(universe)} tickers")

    # Stage 2: candle prescreen (cheap — uses cached data + technicals)
    candidates, _stats = prescreen_with_candles(
        universe,
        candles_obj,
        max_collateral=max_collateral,
        min_bb_width=min_bb_width,
        max_rsi=max_rsi,
    )
    if verbose:
        print(f"[scanner] after candle prescreen: {len(candidates)} candidates  (bb_width≥{min_bb_width}, rsi≤{max_rsi})")

    # Stage 3: quality filter (optional — requires downloading statements)
    if apply_quality_filter and candidates:
        statements_obj = Statements(db_path)
        candidates = quality_filter(candidates, statements_obj)
        if verbose:
            print(f"[scanner] after quality filter: {len(candidates)} candidates")

    # Stage 4: fetch options + screen
    results = run_screener(
        candidates,
        options_obj,
        min_dte=min_dte,
        max_dte=max_dte,
        max_collateral=max_collateral,
        min_premium=min_premium,
        min_roc=min_roc,
        verbose=verbose,
    )
    if verbose:
        print(f"[scanner] final results: {len(results)} contracts")
    return results
